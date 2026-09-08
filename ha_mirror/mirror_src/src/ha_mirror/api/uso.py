"""
Cuántas horas por día estuvo encendido cada aparato, de verdad.

── POR QUÉ EXISTE ───────────────────────────────────────────────────────────
La pantalla de Consumo estimaba el mes con `vatios de catálogo × horas
SUPUESTAS`. Medido contra el historial real de la casa el 2026-09-07, esas
horas estaban lejísimos:

    Toma tv sala          supuesto 3 h/día  →  medido 23,5 h/día
    Apagador Game Room-1  supuesto 3 h/día  →  medido 15,3 h/día
    Apagador despensa -2  supuesto 3 h/día  →  medido 10,1 h/día

El total pasaba de la estimación de la app a unos 363 kWh/mes. No era un ajuste
fino: era un orden de magnitud en varios aparatos.

🔪 **LAS HORAS SE PUEDEN MEDIR; LOS VATIOS NO.** Home Assistant graba cada
encendido y cada apagado, así que el tiempo de uso es dato duro. Lo que sigue
siendo estimación es el vataje de la carga que cuelga detrás del apagador, y eso
la pantalla ya lo rotula. Esta ruta convierte la mitad medible en medida.

── POR QUÉ EN LOTES Y NO DE UNA ─────────────────────────────────────────────
El WebSocket del Mirror corta mensajes a 10 MB (`ha_upstream`). Una casa con 250
aparatos y apagadores que parpadean —900 caídas en tres días, medido— puede
acercarse a ese techo en una sola respuesta de siete días. Un mensaje que pasa
el límite no se trunca: **mata la conexión con la casa**, y con ella el estado
en vivo de toda la app. Por eso se pide de a `TAMANO_LOTE`.

── POR QUÉ SE CACHEA ────────────────────────────────────────────────────────
Recorrer una semana de historial es caro para la cajita, y el resultado cambia
lentísimo: una hora más de uso sobre un promedio de siete días mueve el número
en un 0,6%. Media hora de caché es gratis en precisión y evita que cada apertura
de la pantalla haga trabajar al recorder.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, Field

from ha_mirror.auth import require_api_key
from ha_mirror.errors import HaProtocolError, UpstreamNotReadyError

logger = structlog.get_logger(__name__)

router = APIRouter()

#: Cuántas entidades se piden por mensaje. Ver la cabecera.
TAMANO_LOTE = 40

#: El historial de una semana no cambia de un minuto a otro.
CACHE_SEGUNDOS = 1800.0

#: Solo dominios que representan una CARGA eléctrica. Un `sensor` o un
#: `binary_sensor` no consumen, y pedir su historial es trabajo tirado.
DOMINIOS = ("switch.", "light.", "fan.")

#: Menos que esto es ruido: un aparato que estuvo encendido tres minutos en toda
#: la semana no aporta nada a un cálculo de consumo y sí ensucia la lista.
MINIMO_HORAS = 0.05


class UsoResponse(BaseModel):
    disponible: bool
    dias: int = 0
    #: entity_id → horas encendido POR DÍA, promediadas sobre la ventana.
    horas: dict[str, float] = Field(default_factory=dict)
    #: Cuántas entidades se miraron, hayan estado encendidas o no.
    revisadas: int = 0


def _horas_encendido(filas: list[dict[str, Any]], fin_ts: float) -> float:
    """
    Suma el tiempo en `on` de una serie de estados.

    HA manda `minimal_response`: cada fila trae `s` (estado) y `lu`
    (last_updated, epoch). El primer elemento es el estado al inicio de la
    ventana, así que el tramo previo ya viene contemplado.

    🔪 El último tramo se cierra contra AHORA y no contra el último cambio. Sin
    eso, un aparato encendido hace tres días cuenta cero — que es exactamente el
    que más consume.
    """
    total = 0.0
    estado_prev: str | None = None
    t_prev: float | None = None
    for fila in filas:
        estado = fila.get("s")
        t = fila.get("lu")
        if not isinstance(t, (int, float)):
            continue
        if estado_prev == "on" and t_prev is not None:
            total += (t - t_prev) / 3600.0
        estado_prev = estado if isinstance(estado, str) else None
        t_prev = float(t)
    if estado_prev == "on" and t_prev is not None:
        total += (fin_ts - t_prev) / 3600.0
    return max(0.0, total)


@router.get(
    "/api/uso/horas",
    response_model=UsoResponse,
    summary="Horas por día que estuvo encendido cada aparato, medidas",
)
async def horas_de_uso(
    request: Request,
    dias: int = Query(default=7, ge=1, le=30),
    _: None = Depends(require_api_key),
) -> UsoResponse:
    """
    SIEMPRE 200. Una casa sin historial o caída devuelve `disponible: false` y
    la pantalla sigue usando sus horas supuestas — degradar, no romper.
    """
    store = getattr(request.app.state, "store", None)
    upstream = getattr(request.app.state, "upstream", None)
    if store is None or upstream is None:
        return UsoResponse(disponible=False)

    cache: dict[int, tuple[float, UsoResponse]] = getattr(
        request.app.state, "_uso_cache", {}
    )
    guardado = cache.get(dias)
    if guardado is not None and time.monotonic() - guardado[0] < CACHE_SEGUNDOS:
        return guardado[1]

    objetivos = [
        eid for eid in store.get_all_states() if eid.startswith(DOMINIOS)
    ]
    if not objetivos:
        return UsoResponse(disponible=False)

    fin = datetime.now(UTC)
    ini = fin - timedelta(days=dias)
    fin_ts = fin.timestamp()

    horas: dict[str, float] = {}
    for i in range(0, len(objetivos), TAMANO_LOTE):
        lote = objetivos[i : i + TAMANO_LOTE]
        try:
            resp = await upstream.send_command(
                {
                    "type": "history/history_during_period",
                    "start_time": ini.isoformat(),
                    "end_time": fin.isoformat(),
                    "entity_ids": lote,
                    "minimal_response": True,
                    "no_attributes": True,
                },
                timeout=60.0,
            )
        except (UpstreamNotReadyError, HaProtocolError) as exc:
            # Un lote que falla NO tira el resto: con seis lotes, rendirse en el
            # primero deja la pantalla sin nada cuando cinco sextos del dato
            # estaban disponibles.
            logger.info("uso.lote_fallido", desde=i, detalle=str(exc)[:120])
            continue
        except Exception:
            logger.warning("uso.lote_error", desde=i)
            continue

        for eid, filas in (resp.get("result") or {}).items():
            if not isinstance(filas, list):
                continue
            h = _horas_encendido(filas, fin_ts)
            if h > MINIMO_HORAS:
                horas[eid] = round(h / dias, 3)

    if not horas:
        # Ni un solo aparato con historial: o el recorder está apagado, o la
        # casa no contestó ningún lote. En los dos casos es "no disponible", no
        # "todos consumen cero".
        return UsoResponse(disponible=False, dias=dias, revisadas=len(objetivos))

    salida = UsoResponse(
        disponible=True, dias=dias, horas=horas, revisadas=len(objetivos)
    )
    cache[dias] = (time.monotonic(), salida)
    request.app.state._uso_cache = cache
    logger.info(
        "uso.calculado", dias=dias, revisadas=len(objetivos), con_uso=len(horas)
    )
    return salida
