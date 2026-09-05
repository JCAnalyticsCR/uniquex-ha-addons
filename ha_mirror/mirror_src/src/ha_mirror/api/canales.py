"""
POST /api/cameras/sondeo — ¿hay cámaras en los canales que la app no muestra?

── PARA QUÉ ─────────────────────────────────────────────────────────────────
El NVR de esta casa tiene 32 canales y la app muestra 20. Saber si en los otros
12 hay cámaras conectadas requería que alguien abriera el DMSS. Esto lo
averigua solo.

── QUÉ PUEDE PEDIR EL CLIENTE, Y QUÉ NO ─────────────────────────────────────
Solo **números de canal**, y acotados. No una URL, ni un host, ni un usuario.
La URL la construye el servidor derivándola de una cámara que ya funciona.

Es la misma regla que ya se aplicó al arreglar el bypass del allowlist de HLS:
un parámetro de seguridad que viaja en el pedido no es una garantía. Ver la
cabecera de `sondeo_canales.py`.

── ES LENTO Y ESTÁ BIEN QUE LO SEA ──────────────────────────────────────────
Cada canal puede tardar hasta 30 s (medido: un `frame.jpeg` en frío tarda entre
2,5 y 26 s), y van de a uno para no robarle sesiones RTSP al video que alguien
esté mirando. Sondear 12 canales puede llevar varios minutos.

Por eso **no se contesta esperando**: arranca, devuelve un número de trabajo, y
la app pregunta cómo va — el mismo patrón que `matter/comisionar`, y por la
misma razón medida: Cloudflare corta cualquier respuesta que pase de 100 s.
"""

from __future__ import annotations

import asyncio
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from ha_mirror.auth import require_api_key
from ha_mirror.sondeo_canales import (
    CANAL_MAX,
    CANAL_MIN,
    ResultadoCanal,
    SondeoCanales,
)

logger = structlog.get_logger(__name__)

router = APIRouter()

#: Cuánto se guarda el resultado. Un sondeo largo + alguien que vuelve a mirar.
VIDA_DEL_TRABAJO = 1800.0


class Sondeo(BaseModel):
    """Solo números. Ninguna URL, ningún host, ninguna credencial."""

    canales: list[int] = Field(min_length=1, max_length=48)


class Arrancado(BaseModel):
    trabajo: str
    estado: str = "sondeando"
    cuantos: int
    #: Estimación honesta: hasta 30 s por canal, de a uno.
    segundos_estimados: int


class Avance(BaseModel):
    """
    Cómo va, o cómo salió.

    `estado`: `sondeando` · `listo` · `fallo` · `perdido`.
    Los resultados van llegando: se puede mirar el avance sin esperar el final.
    """

    estado: str
    hechos: int = 0
    total: int = 0
    segundos: int = 0
    resultados: list[dict[str, Any]] = []
    motivo: str | None = None


@dataclass
class _Trabajo:
    estado: str
    empezado: float
    total: int
    resultados: list[ResultadoCanal] = field(default_factory=list)
    motivo: str | None = None


_trabajos: dict[str, _Trabajo] = {}
_tareas: set[asyncio.Task[None]] = set()


def _limpiar_viejos() -> None:
    ahora = time.monotonic()
    for k in [
        k for k, t in _trabajos.items()
        if t.estado != "sondeando" and ahora - t.empezado > VIDA_DEL_TRABAJO
    ]:
        _trabajos.pop(k, None)


def _hay_uno_corriendo() -> bool:
    """
    Un sondeo a la vez.

    Dos sondeos en paralelo abrirían el doble de sesiones RTSP contra un NVR que
    tiene pocas — y se las quitarían al video que el cliente está mirando.
    """
    return any(t.estado == "sondeando" for t in _trabajos.values())


async def _sondear_de_fondo(cliente: Any, canales: list[int], clave: str) -> None:
    t = _trabajos.get(clave)
    if t is None:
        return
    try:
        sondeo = SondeoCanales(
            base_url=cliente._go2rtc_base_url,  # noqa: SLF001
            auth=cliente._go2rtc_auth,  # noqa: SLF001
            session=cliente._require_session(),  # noqa: SLF001
        )
        # Barrer restos de un sondeo anterior que se haya cortado a la mitad.
        await sondeo.limpiar_huerfanos()

        # Se sondea de a uno y se va guardando, para poder mirar el avance.
        for canal in canales:
            parciales, motivo = await sondeo.sondear([canal])
            if motivo is not None:
                t.estado, t.motivo = "fallo", motivo
                return
            t.resultados.extend(parciales)
        t.estado = "listo"
        con = sum(1 for r in t.resultados if r.estado == "con_camara")
        logger.info("sondeo.terminado", canales=len(canales), con_camara=con)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.warning("sondeo.error", error=str(exc)[:160])
        t.estado, t.motivo = "fallo", f"No se pudo completar el sondeo: {str(exc)[:120]}"


@router.post(
    "/api/cameras/sondeo",
    response_model=Arrancado,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Empieza a buscar cámaras en canales del NVR que la app no muestra",
)
async def empezar_sondeo(
    request: Request,
    cuerpo: Sondeo,
    _: None = Depends(require_api_key),
) -> Arrancado:
    canales = sorted({c for c in cuerpo.canales})
    fuera = [c for c in canales if not (CANAL_MIN <= c <= CANAL_MAX)]
    if fuera:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Canales fuera de rango ({CANAL_MIN}-{CANAL_MAX}): {fuera}",
        )

    cliente = getattr(request.app.state, "camera_media", None)
    if cliente is None or not getattr(cliente, "webrtc_enabled", False):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Esta casa no tiene go2rtc configurado, así que no hay canales que sondear.",
        )

    _limpiar_viejos()
    if _hay_uno_corriendo():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ya hay un sondeo en curso. Esperá a que termine.",
        )

    clave = secrets.token_urlsafe(12)
    _trabajos[clave] = _Trabajo(estado="sondeando", empezado=time.monotonic(), total=len(canales))
    tarea = asyncio.create_task(_sondear_de_fondo(cliente, canales, clave))
    _tareas.add(tarea)
    tarea.add_done_callback(_tareas.discard)

    return Arrancado(trabajo=clave, cuantos=len(canales), segundos_estimados=len(canales) * 30)


@router.get(
    "/api/cameras/sondeo/{trabajo}",
    response_model=Avance,
    summary="Cómo va el sondeo de canales",
)
async def ver_sondeo(trabajo: str, _: None = Depends(require_api_key)) -> Avance:
    t = _trabajos.get(trabajo)
    if t is None:
        return Avance(estado="perdido", motivo="No hay rastro de ese sondeo.")
    return Avance(
        estado=t.estado,
        hechos=len(t.resultados),
        total=t.total,
        segundos=int(time.monotonic() - t.empezado),
        resultados=[
            {"canal": r.canal, "estado": r.estado, "bytes": r.bytes, "detalle": r.detalle}
            for r in t.resultados
        ],
        motivo=t.motivo,
    )
