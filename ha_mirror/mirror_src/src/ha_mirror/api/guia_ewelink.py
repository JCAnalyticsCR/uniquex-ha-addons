"""
GET /api/onboarding/guias/ewelink — en qué paso está la casa para conectar
sus apagadores eWeLink. Solo lectura. La lógica está en `ha_mirror/guia_ewelink.py`.

SIEMPRE 200. Si la casa no contesta, `disponible:false`: la app muestra los pasos
igual, sin marcar ninguno como hecho. No saber no es estar bien, ni estar mal.

Cada consulta por separado, como en `salud.py`: que falle la de la versión no
puede borrar lo que se sabe de las cuentas.
"""

from __future__ import annotations

from typing import Any

import structlog
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from ha_mirror.auth import require_api_key
from ha_mirror.guia_ewelink import DOMINIO_SONOFF, calcular

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/onboarding", tags=["onboarding"])


class GuiaEwelink(BaseModel):
    disponible: bool
    version_ha: str | None = None
    marketplace: bool = False
    otro_puente: bool = False
    hacs: bool = False
    sonoff_descargado: bool = False
    cuentas: int = 0
    cuentas_con_problema: int = 0
    aparatos: int = 0
    aparatos_sin_responder: int = 0


async def _resultado(upstream: Any, mensaje: dict[str, Any]) -> tuple[bool, Any]:
    resp = await upstream.send_command(mensaje, timeout=10.0)
    return bool(resp.get("success")), resp.get("result")


@router.get("/guias/ewelink", response_model=GuiaEwelink, summary="Avance de la conexión eWeLink")
async def guia_ewelink(request: Request, _: None = Depends(require_api_key)) -> GuiaEwelink:
    upstream = getattr(request.app.state, "upstream", None)
    store = getattr(request.app.state, "store", None)
    if upstream is None:
        return GuiaEwelink(disponible=False)

    version: Any = None
    entradas: list[dict[str, Any]] = []
    registro: list[dict[str, Any]] = []
    en_disco = False
    leidas = 0

    for nombre, mensaje in (
        ("version", {"type": "get_config"}),
        ("entradas", {"type": "config_entries/get"}),
        ("registro", {"type": "config/entity_registry/list"}),
        # Si la integración está en el disco de la casa (descargada y HA ya
        # reiniciado). Contesta `success:false` si no está: es la respuesta
        # normal antes del paso 2, no un error.
        ("disco", {"type": "manifest/get", "integration": DOMINIO_SONOFF}),
    ):
        try:
            ok, res = await _resultado(upstream, mensaje)
        except Exception:  # noqa: BLE001 — cada consulta falla sola
            logger.info("guia_ewelink.consulta_fallo", consulta=nombre)
            continue
        leidas += 1
        if nombre == "version" and ok and isinstance(res, dict):
            version = res.get("version")
        elif nombre == "entradas" and ok and isinstance(res, list):
            entradas = [e for e in res if isinstance(e, dict)]
        elif nombre == "registro" and ok and isinstance(res, list):
            registro = [r for r in res if isinstance(r, dict)]
        elif nombre == "disco":
            en_disco = ok

    if leidas == 0:
        return GuiaEwelink(disponible=False)

    estados: dict[str, str] = {}
    if store is not None:
        try:
            estados = {
                eid: str(getattr(st, "state", "")) for eid, st in store.get_all_states().items()
            }
        except Exception:  # noqa: BLE001
            logger.warning("guia_ewelink.store_fallo", exc_info=True)

    e = calcular(
        version=version,
        entradas=entradas,
        registro=registro,
        sonoff_en_disco=en_disco,
        estados=estados,
    )
    return GuiaEwelink(
        disponible=True,
        version_ha=e.version_ha,
        marketplace=e.marketplace,
        otro_puente=e.otro_puente,
        hacs=e.hacs,
        sonoff_descargado=e.sonoff_descargado,
        cuentas=e.cuentas,
        cuentas_con_problema=e.cuentas_con_problema,
        aparatos=e.aparatos,
        aparatos_sin_responder=e.aparatos_sin_responder,
    )
