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

── EL CÓDIGO ES UNA CREDENCIAL ──────────────────────────────────────────────
Quien tiene el código de emparejamiento puede sumar ese aparato a una casa. No
se registra en ningún log: acá no hay un solo `logger` que reciba el código, ni
siquiera recortado.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from ha_mirror.auth import require_api_key
from ha_mirror.errors import HaConnectError, HaProtocolError, UpstreamNotReadyError

logger = structlog.get_logger(__name__)

router = APIRouter()

#: Comisionar habla con el aparato por radio: se enciende, se une al Thread o
#: al WiFi de la casa y recién ahí contesta. Es MUCHO más que el timeout normal
#: de 10 s de `send_command`.
#:
#: 🔪 NO son los 120 s que uno pondría. La app llega hasta acá por el túnel de
#: Cloudflare (`mirror-fortunata.uniquexcr.com`), y Cloudflare corta cualquier
#: respuesta que tarde más de 100 s con su propia página de error 524 — que no
#: es JSON, así que el frontend ni siquiera podría leer el motivo. Esperar 120
#: garantizaría que el peor caso llegue como pantalla rota en vez de como
#: mensaje. 90 deja margen para que NUESTRA respuesta salga primero.
TIMEOUT_SEGUNDOS = 90.0

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


class Comisionado(BaseModel):
    """
    Cómo salió. SIEMPRE 200 salvo que el código ni siquiera tenga la forma.

    Un código equivocado no es un error del servidor: es el desenlace más
    común de esta pantalla, y merece una frase, no una pantalla roja.
    """

    ok: bool
    #: Qué contarle a la persona, en español. Nunca lleva el código adentro.
    motivo: str | None = None
    #: Lo que dijo Home Assistant, tal cual y en inglés. Para soporte, no para
    #: mostrar: la app lo esconde detrás de "Ver detalle".
    detalle: str | None = None


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


@router.post(
    "/api/onboarding/matter/comisionar",
    response_model=Comisionado,
    summary="Suma un aparato Matter a la casa con su código de emparejamiento",
)
async def comisionar(
    request: Request,
    cuerpo: Comisionar,
    _: None = Depends(require_api_key),
) -> Comisionado:
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

    try:
        respuesta: dict[str, Any] = await upstream.send_command(
            {"type": "matter/commission", "code": codigo},
            timeout=TIMEOUT_SEGUNDOS,
        )
    except (UpstreamNotReadyError, HaConnectError):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Sin conexión con la casa.",
        ) from None
    except TimeoutError:
        # Dos minutos sin respuesta. Puede que el aparato SÍ se haya sumado y
        # solo tardó de más, así que no se afirma que falló: se manda a mirar.
        logger.warning("matter.comisionar_timeout")
        return Comisionado(
            ok=False,
            motivo=(
                "El aparato tardó demasiado en contestar. Revisá en la lista si "
                "apareció igual; si no, acercalo y volvé a intentar."
            ),
        )
    except asyncio.CancelledError:
        raise
    except HaProtocolError as exc:
        # El camino NORMAL de un código malo. El texto de HA se guarda como
        # detalle técnico; lo que se muestra es la traducción. El código NO se
        # registra en ningún lado.
        crudo = str(exc)
        logger.warning("matter.comisionar_fallo", error=crudo[:200])
        return Comisionado(ok=False, motivo=_traducir(crudo), detalle=crudo[:300])
    except Exception as exc:  # noqa: BLE001
        crudo = str(exc)
        logger.warning("matter.comisionar_error", error=crudo[:200])
        return Comisionado(ok=False, motivo=_traducir(crudo), detalle=crudo[:300])

    # Defensivo: hoy `_send_command` levanta excepción en vez de devolver
    # `success: false`. Si eso cambia, esto lo atrapa en vez de cantar victoria.
    if isinstance(respuesta, dict) and respuesta.get("success") is False:
        error = respuesta.get("error") or {}
        crudo = str(error.get("message") or "commission failed")
        return Comisionado(ok=False, motivo=_traducir(crudo), detalle=crudo[:300])

    logger.info("matter.comisionado")
    return Comisionado(ok=True)
