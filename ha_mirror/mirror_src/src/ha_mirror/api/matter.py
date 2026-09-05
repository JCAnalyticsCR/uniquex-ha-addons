"""
POST /api/onboarding/matter/comisionar — sumar un aparato con su código.

── QUÉ RESUELVE ─────────────────────────────────────────────────────────────
Los aparatos Matter traen un código impreso —un QR y, al lado, once dígitos
para escribir a mano— y ese código es TODO lo que hace falta para sumarlos a
la casa. No hay formulario, no hay cuenta de fabricante, no hay app intermedia.

Es el camino de alta más simple que existe, y era el único que la app no tenía:
la opción "Escanear código" estaba dibujada pero apagada, con el texto
"Todavía no disponible en esta casa".

── POR QUÉ UN ENDPOINT PROPIO ───────────────────────────────────────────────
Comisionar NO es un config flow: es el comando `matter/commission` del
WebSocket de Home Assistant. Por eso no entra por el asistente de alta, que
habla el protocolo de formularios.

Y va como puerta DEDICADA, no como un pasamanos genérico de comandos WS —
mismo criterio que `/api/pronostico` y `/api/costumbres`. Un canal por el que
el cliente pueda mandar cualquier comando de HA es una llave maestra; éste solo
sabe pasar un código de emparejamiento.

── LO QUE SE MIDIÓ ANTES DE ESCRIBIRLO ──────────────────────────────────────
Contra la casa real, el 2026-09-04: la integración `matter` está `loaded` y el
comando existe — con un código inventado contestó "Commission with code failed
for node 1", que es el fallo correcto para un código inválido. O sea que la
casa puede comisionar; lo que faltaba era la puerta.

🔪 `_send_command` NO devuelve `success: false`: cuando HA contesta un result
con error, `_handle_result` le pone `HaProtocolError` al future. O sea que el
camino normal de un código malo es una EXCEPCIÓN, no un valor de retorno. Un
endpoint que solo mire `respuesta["success"]` contesta 500 en el caso más
común de todos —el cliente tipeó mal el código— en vez de explicarlo.

── 🔪 POR QUÉ ESTO NO ESPERA LA RESPUESTA ───────────────────────────────────
La primera versión de este endpoint esperaba a que Home Assistant contestara.
Funcionaba contra el servidor Matter 7.0.0, que rechazaba un código sin aparato
detrás **al instante**.

Al actualizar la casa al servidor 9.2.0 eso cambió, y se midió:

    matter/commission con un código sin aparato → 180.2 segundos

Tiene sentido: la versión nueva de verdad sale a buscar el aparato por radio
antes de rendirse, en vez de rechazar el código de entrada. Es mejor
comportamiento. Pero rompe el diseño, porque:

    lo que tarda el peor caso ......... 180 s
    lo que espera Cloudflare .......... 100 s  → corta con un 524 que no es JSON

O sea que **por el túnel no existe ningún timeout que sirva**: cualquiera que
alcance para el caso real llega después del corte. No es un número mal elegido,
es que la pregunta no se puede contestar de una sola vez.

Así que la comisión arranca y contesta enseguida con un número de trabajo, y la
app pregunta cómo va. El resultado REAL —incluido el mensaje de Matter, que es
el que dice qué hacer— llega completo por la segunda puerta, sin pelear con
ningún corte.

── EL CÓDIGO ES UNA CREDENCIAL ──────────────────────────────────────────────
Quien tiene el código de emparejamiento puede sumar ese aparato a una casa. No
se registra en ningún log: acá no hay un solo `logger` que reciba el código, ni
siquiera recortado.
"""

from __future__ import annotations

import asyncio
import re
import secrets
import time
from dataclasses import dataclass
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from ha_mirror.auth import require_api_key
from ha_mirror.errors import HaConnectError, HaProtocolError, UpstreamNotReadyError

logger = structlog.get_logger(__name__)

router = APIRouter()

#: Cuánto espera el trabajo de fondo a Home Assistant.
#:
#: Medido contra la casa real con el servidor Matter 9.2.0: un código sin
#: aparato detrás tarda **180.2 s** en fallar. 240 deja aire para un aparato
#: lento sin quedarse esperando para siempre. Este número ya NO pelea con
#: Cloudflare: nadie está del otro lado esperando esta respuesta.
TIMEOUT_SEGUNDOS = 240.0

#: Cuánto se guarda el resultado después de terminar. Diez minutos alcanza para
#: que la app pregunte, y para que alguien vuelva a mirar si cerró la hoja.
VIDA_DEL_TRABAJO = 600.0

#: Los dos formatos que imprime un aparato Matter.
#:
#: · 11 dígitos — el código "manual", el que está escrito debajo del QR.
#: · `MT:` + base38 — la carga del QR, que es lo que devuelve la cámara.
#:
#: Se validan acá para no mandarle basura a Home Assistant, y porque un formato
#: equivocado se explica mucho mejor desde este lado que traduciendo el error
#: interno del servidor Matter.
_RE_MANUAL = re.compile(r"^\d{11}$")
_RE_QR = re.compile(r"^MT:[0-9A-Z.$%*+\-/:]{10,}$", re.IGNORECASE)


class Comisionar(BaseModel):
    #: Se acepta con espacios o guiones: la gente los copia del envase así.
    codigo: str = Field(min_length=6, max_length=120)


class Arrancado(BaseModel):
    """Lo que se contesta al empezar. Todavía no se sabe nada del aparato."""

    trabajo: str
    estado: str = "buscando"
    #: Cuánto puede tardar, para que la app muestre una espera honesta y no un
    #: giro infinito. Es el número MEDIDO contra la casa, no una estimación
    #: amable: 180 segundos.
    segundos_estimados: int = 180


class Comisionado(BaseModel):
    """
    Cómo va, o cómo salió.

    `estado`:
      · `buscando` — la casa está buscando el aparato por radio.
      · `listo`    — entró.
      · `fallo`    — no entró, y `motivo` dice qué hacer.
      · `perdido`  — no sabemos de ese trabajo (el Mirror se reinició).
    """

    estado: str
    #: Qué contarle a la persona, en español. Nunca lleva el código adentro.
    motivo: str | None = None
    #: Lo que dijo Home Assistant, tal cual y en inglés. Para soporte, no para
    #: mostrar: la app lo esconde detrás de "Ver detalle".
    detalle: str | None = None
    #: Cuánto lleva buscando. Deja que la app diga "van 40 s de unos 180".
    segundos: int = 0


def _limpiar(codigo: str) -> str:
    """Saca espacios y guiones. `MT:` conserva el resto tal cual."""
    return re.sub(r"[\s\-]", "", codigo).strip()


def _traducir(crudo: str) -> str:
    """
    Del error de Matter a una frase que sirva para actuar.

    No se inventa precisión: cuando no reconocemos el error, se dice que no se
    pudo y se ofrece lo único que de verdad suele arreglarlo (acercarlo y
    reiniciar el emparejamiento). Mentir un diagnóstico es peor que no darlo.
    """
    t = crudo.lower()
    if "already" in t and ("commission" in t or "part of" in t or "fabric" in t):
        return (
            "Ese aparato ya está emparejado con otra casa o con otra app. "
            "Hay que reiniciarlo de fábrica antes de sumarlo acá."
        )
    # 🔪 EL MENSAJE DE MATTER CAMBIÓ AL ACTUALIZAR EL SERVIDOR.
    #
    #   servidor 7.0.0 → "Commission with code failed for node 1."
    #   servidor 9.2.0 → "Commission failed: discovery of node with
    #                     discriminator 15 failed: No commissionable device
    #                     was discovered"
    #
    # El nuevo dice MUCHO más: no es que el código esté mal, es que nadie
    # contestó. Son dos consejos distintos —"revisá lo que escribiste" contra
    # "poné el aparato en modo de emparejamiento"— y el viejo no los separaba.
    #
    # Se reconocen LOS DOS a propósito: el frontend tiene que tolerar un Mirror
    # y una casa más viejos, y hay casas del parque que siguen en el servidor 7.
    if "discover" in t or "no commissionable" in t:
        return (
            "No apareció ningún aparato esperando emparejarse. Ponelo en modo "
            "de emparejamiento —casi siempre es mantener un botón hasta que "
            "parpadee— y volvé a intentar sin moverte de al lado."
        )
    if "invalid" in t or "format" in t or "checksum" in t:
        return "El código no es válido. Revisá que esté completo y bien copiado."
    if "timeout" in t or "timed out" in t or "not found" in t or "unreachable" in t:
        return (
            "No se encontró el aparato. Tiene que estar encendido, cerca de la "
            "casa y en modo de emparejamiento (suele quedar parpadeando)."
        )
    return (
        "No se pudo sumar. Probá acercarlo, apagarlo y encenderlo, y volver a "
        "poner el código."
    )


# ── El registro de trabajos ───────────────────────────────────────────────────
#
# Vive en memoria a propósito: un emparejamiento a medias no significa nada
# después de reiniciar el Mirror, y guardarlo en disco sería dejar por escrito
# el rastro de una operación que lleva una credencial de por medio.
#
# Si el Mirror se reinicia, la app recibe `perdido` y manda a mirar la lista —
# que es lo correcto, porque la comisión puede haber terminado bien del otro
# lado mientras nosotros perdíamos la nota.


@dataclass
class _Trabajo:
    estado: str
    empezado: float
    motivo: str | None = None
    detalle: str | None = None


_trabajos: dict[str, _Trabajo] = {}

#: Las tareas de fondo se guardan acá para que el recolector de basura no se
#: las lleve a mitad de camino: asyncio solo mantiene una referencia débil a lo
#: que devuelve `create_task`, y una tarea sin dueño puede desaparecer sola.
_tareas: set[asyncio.Task[None]] = set()


def _limpiar_viejos() -> None:
    ahora = time.monotonic()
    for clave in [
        k
        for k, t in _trabajos.items()
        if t.estado != "buscando" and ahora - t.empezado > VIDA_DEL_TRABAJO
    ]:
        _trabajos.pop(clave, None)


def _hay_uno_corriendo() -> bool:
    """
    ¿Ya se está comisionando algo?

    El servidor Matter empareja de a uno: dos comisiones a la vez se pelean por
    la misma radio y las dos salen peor. Se rechaza la segunda con un mensaje
    claro, en vez de dejar que fallen las dos sin que nadie entienda por qué.
    """
    return any(t.estado == "buscando" for t in _trabajos.values())


def _guardar(
    clave: str, estado: str, motivo: str | None = None, detalle: str | None = None
) -> None:
    t = _trabajos.get(clave)
    if t is None:
        return
    t.estado, t.motivo, t.detalle = estado, motivo, detalle


async def _comisionar_de_fondo(upstream: Any, codigo: str, clave: str) -> None:
    """Espera a Home Assistant sin que nadie del otro lado esté esperando."""
    try:
        respuesta: dict[str, Any] = await upstream.send_command(
            {"type": "matter/commission", "code": codigo},
            timeout=TIMEOUT_SEGUNDOS,
        )
    except (UpstreamNotReadyError, HaConnectError):
        _guardar(clave, "fallo", "Se perdió la conexión con la casa mientras buscaba.")
        return
    except TimeoutError:
        logger.warning("matter.comisionar_timeout")
        _guardar(
            clave,
            "fallo",
            "La casa estuvo cuatro minutos buscando y no lo encontró. Revisá "
            "que esté encendido, cerca y en modo de emparejamiento.",
        )
        return
    except asyncio.CancelledError:
        raise
    except HaProtocolError as exc:
        # El camino NORMAL de un código sin aparato detrás. El código NO se
        # registra en ningún lado, ni recortado.
        crudo = str(exc)
        logger.warning("matter.comisionar_fallo", error=crudo[:200])
        _guardar(clave, "fallo", _traducir(crudo), crudo[:300])
        return
    except Exception as exc:  # noqa: BLE001
        crudo = str(exc)
        logger.warning("matter.comisionar_error", error=crudo[:200])
        _guardar(clave, "fallo", _traducir(crudo), crudo[:300])
        return

    # Defensivo: hoy `_send_command` levanta excepción en vez de devolver
    # `success: false`. Si eso cambia, esto lo atrapa en vez de cantar victoria.
    if isinstance(respuesta, dict) and respuesta.get("success") is False:
        error = respuesta.get("error") or {}
        crudo = str(error.get("message") or "commission failed")
        _guardar(clave, "fallo", _traducir(crudo), crudo[:300])
        return

    logger.info("matter.comisionado")
    _guardar(clave, "listo")


# ── Endpoints ─────────────────────────────────────────────────────────────────


@router.post(
    "/api/onboarding/matter/comisionar",
    response_model=Arrancado,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Empieza a sumar un aparato Matter con su código de emparejamiento",
)
async def comisionar(
    request: Request,
    cuerpo: Comisionar,
    _: None = Depends(require_api_key),
) -> Arrancado:
    codigo = _limpiar(cuerpo.codigo)
    if not (_RE_MANUAL.match(codigo) or _RE_QR.match(codigo)):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Ese código no tiene la forma de un código Matter: son 11 dígitos, "
                "o el texto que empieza con MT: del código QR."
            ),
        )

    upstream = getattr(request.app.state, "upstream", None)
    if upstream is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Sin conexión con la casa.",
        )

    _limpiar_viejos()
    if _hay_uno_corriendo():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ya se está sumando un aparato. Esperá a que ese termine.",
        )

    # `token_urlsafe` y no un contador: el número de trabajo viaja en la URL de
    # la consulta, y un contador dejaría contar cuántos aparatos se sumaron.
    clave = secrets.token_urlsafe(12)
    _trabajos[clave] = _Trabajo(estado="buscando", empezado=time.monotonic())

    tarea = asyncio.create_task(_comisionar_de_fondo(upstream, codigo, clave))
    _tareas.add(tarea)
    tarea.add_done_callback(_tareas.discard)

    return Arrancado(trabajo=clave)


@router.get(
    "/api/onboarding/matter/comisionar/{trabajo}",
    response_model=Comisionado,
    summary="Cómo va el aparato que se está sumando",
)
async def estado_comision(
    trabajo: str,
    _: None = Depends(require_api_key),
) -> Comisionado:
    """
    Siempre 200, incluso si no conocemos el trabajo.

    Un 404 acá haría que la app pintara un error por una pregunta cuya
    respuesta honesta —"no sé, fijate en la lista"— es perfectamente útil.
    """
    t = _trabajos.get(trabajo)
    if t is None:
        return Comisionado(
            estado="perdido",
            motivo=(
                "Se perdió el rastro de ese emparejamiento. Fijate en la lista: "
                "si el aparato aparece, entró igual."
            ),
        )
    return Comisionado(
        estado=t.estado,
        motivo=t.motivo,
        detalle=t.detalle,
        segundos=int(time.monotonic() - t.empezado),
    )
