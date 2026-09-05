"""
GET /api/pronostico — el pronóstico del tiempo de la casa.

── POR QUÉ ES UN ENDPOINT PROPIO Y NO UN FLAG EN /api/service ────────────────
Home Assistant entrega el pronóstico por un servicio que DEVUELVE datos
(`weather.get_forecasts`, con `SupportsResponse.ONLY`). El proxy genérico de
servicios de este Mirror contesta 202 y no devuelve nada, así que la tentación
obvia era agregarle un `return_response` y listo.

**No se hizo, y es a propósito.** Ese proxy es exactamente el lugar por el que
ya se coló capacidad sensible cuatro veces en este proyecto. Dejar que
*cualquier* servicio devuelva su cuerpo de respuesta convierte un canal de
"ejecutar acciones" en un canal de "leer lo que sea que HA quiera contarme", y
eso es una vía de fuga de datos que hoy no existe.

Este endpoint solo sabe preguntar el clima. No se puede usar para otra cosa,
no acepta un dominio ni un servicio del cliente, y es de solo lectura.

── LA CACHÉ NO ES UNA OPTIMIZACIÓN, ES LO CORRECTO ───────────────────────────
met.no publica una vez por hora. Sin caché, cada carga de cada teléfono de la
casa dispararía una llamada de servicio a Home Assistant por un dato que no
cambió — y esta casa ya tiene un enlace que se cae solo. Veinte minutos deja el
dato fresco y reduce el tráfico a lo que de verdad hace falta.
"""

from __future__ import annotations

import time
from typing import Any

import structlog
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from ha_mirror.auth import require_api_key
from ha_mirror.errors import UpstreamNotReadyError
from ha_mirror.models import leer_atributo

logger = structlog.get_logger(__name__)

router = APIRouter()

#: Cuánto vale un pronóstico antes de volver a pedirlo. Ver la cabecera.
TTL_SEGUNDOS = 20 * 60

#: Cuánto se espera a HA. El servicio consulta la caché de la integración, no
#: sale a internet, así que si tarda más que esto es que algo anda mal.
TIMEOUT_SEGUNDOS = 15.0


class Pronostico(BaseModel):
    """
    Qué se pudo averiguar del tiempo.

    `disponible: false` NO es un error: es la respuesta honesta cuando la casa
    no tiene integración de clima o el enlace está caído. El frontend esconde la
    sección y sigue andando — el mismo contrato que usa `encontrados`.
    """

    disponible: bool
    #: De qué entidad salió. Sirve para depurar y para que el frontend no tenga
    #: que adivinar cuál es la del clima.
    entity_id: str | None = None
    #: Hasta 6 entradas, una por día.
    diario: list[dict[str, Any]] = []
    #: Hasta 24 entradas, una por hora.
    horario: list[dict[str, Any]] = []
    #: Licencia del proveedor. met.no EXIGE mostrarla mientras se use su dato.
    atribucion: str | None = None


# ── Caché ─────────────────────────────────────────────────────────────────────

_cache: Pronostico | None = None
_cache_ms: float = 0.0


def _vigente() -> Pronostico | None:
    if _cache is None:
        return None
    if time.monotonic() - _cache_ms > TTL_SEGUNDOS:
        return None
    return _cache


def _guardar(p: Pronostico) -> None:
    global _cache, _cache_ms
    _cache = p
    _cache_ms = time.monotonic()


# ── Búsqueda de la entidad del clima ──────────────────────────────────────────


def _entidad_de_clima(store: Any) -> str | None:
    """
    La primera entidad `weather.*` que la casa publique.

    Se busca en el registro en vez de codificar `weather.forecast_casa`: ese es
    el nombre en Fortunatta, pero cada casa nombra la suya distinto y este
    Mirror corre en varias. Si hay más de una, gana la primera por orden
    alfabético — determinista, que es lo que importa para que dos llamadas
    seguidas no contesten cosas distintas.
    """
    estados: dict[str, Any] = store.get_all_states()
    candidatas = sorted(eid for eid in estados if eid.startswith("weather."))
    return candidatas[0] if candidatas else None


def _lista(bruto: Any) -> list[dict[str, Any]]:
    """Normaliza lo que devuelva HA a una lista de diccionarios, o vacío."""
    if not isinstance(bruto, list):
        return []
    return [x for x in bruto if isinstance(x, dict)]


async def _pedir(upstream: Any, entity_id: str, tipo: str) -> list[dict[str, Any]]:
    """
    Una llamada a `weather.get_forecasts`.

    `return_response` es OBLIGATORIO acá: el servicio está declarado como
    `SupportsResponse.ONLY`, así que sin esa bandera Home Assistant contesta con
    un error en vez de con el pronóstico.
    """
    resultado = await upstream.send_service_call(
        "weather",
        "get_forecasts",
        {
            "service_data": {"type": tipo},
            "target": {"entity_id": entity_id},
            "return_response": True,
        },
    )
    # 🔪 HA envuelve la respuesta TRES veces, no dos, y `_send_command` agrega
    # una más: resuelve el future con el MENSAJE COMPLETO, no con su `result`.
    # O sea que el camino real es
    #     msg["result"]["response"]["<entity_id>"]["forecast"]
    # Leer un nivel de menos devuelve `None` en silencio y la sección aparece
    # vacía sin que nada falle — el peor tipo de defecto. Verificado hablando
    # WebSocket directo con la casa (2026-09-05).
    if not isinstance(resultado, dict):
        return []
    interno = resultado.get("result")
    respuesta = interno.get("response") if isinstance(interno, dict) else None
    if not isinstance(respuesta, dict):
        return []
    porEntidad = respuesta.get(entity_id)
    if not isinstance(porEntidad, dict):
        return []
    return _lista(porEntidad.get("forecast"))


# ── Endpoint ──────────────────────────────────────────────────────────────────


@router.get(
    "/api/pronostico",
    response_model=Pronostico,
    summary="Pronóstico del tiempo de la casa (diario y por hora)",
)
async def obtener_pronostico(
    request: Request,
    _: None = Depends(require_api_key),
) -> Pronostico:
    """
    Devuelve SIEMPRE 200. Cuando no se puede saber, `disponible: false`.

    Nunca 502: que no haya clima no es una falla del Mirror, y un error acá
    haría que el frontend pintara una pantalla rota por una sección decorativa.
    """
    cacheado = _vigente()
    if cacheado is not None:
        return cacheado

    store = request.app.state.store
    upstream = getattr(request.app.state, "upstream", None)

    entity_id = _entidad_de_clima(store)
    if entity_id is None or upstream is None:
        # Casa sin integración de clima. Se cachea igual: preguntar cada vez por
        # algo que no existe es gastar en balde.
        vacio = Pronostico(disponible=False)
        _guardar(vacio)
        return vacio

    try:
        diario = await _pedir(upstream, entity_id, "daily")
        horario = await _pedir(upstream, entity_id, "hourly")
    except UpstreamNotReadyError:
        # Enlace caído. NO se cachea: en cuanto vuelva queremos el dato de una,
        # y guardar un "no disponible" por veinte minutos convertiría un corte
        # de dos segundos en veinte minutos sin pronóstico.
        return Pronostico(disponible=False, entity_id=entity_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("pronostico.fallo", entity_id=entity_id, error=str(exc))
        return Pronostico(disponible=False, entity_id=entity_id)

    # `leer_atributo` y NO `.attributes.get(...)`: `HaState.attributes` es un
    # modelo de Pydantic, no un dict, y no tiene `.get()`. Ver models.py.
    atribucion = leer_atributo(store.get_state(entity_id), "attribution")

    p = Pronostico(
        disponible=bool(diario or horario),
        entity_id=entity_id,
        diario=diario,
        horario=horario,
        atribucion=atribucion if isinstance(atribucion, str) else None,
    )
    _guardar(p)
    logger.info(
        "pronostico.ok", entity_id=entity_id, dias=len(diario), horas=len(horario)
    )
    return p
