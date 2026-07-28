"""
Preferencias KV del cliente — layout del Inicio y configuración persistente.

Single-tenant (tenant_id=1). El cliente guarda el orden de las secciones del
Inicio para que sobreviva recargas, instalaciones de PWA y cambios de
dispositivo. Al ser un KV simple, el contrato con el frontend es mínimo:
GET para leer, PUT para reemplazar.

Seguridad: misma guardia X-API-Key que el resto de los routers del mirror.
"""

from __future__ import annotations

import re
from typing import Any

import structlog
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field, field_validator

from ha_mirror.auth import require_api_key
from ha_mirror.db import Database

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/preferences", tags=["preferences"])

# Patrón de ítem válido: solo minúsculas, dígitos, guión y guión bajo (1-64 chars).
_ITEM_RE = re.compile(r"^[a-z0-9_-]{1,64}$")


# ---------------------------------------------------------------------------
# Modelos
# ---------------------------------------------------------------------------


class HomeLayoutBody(BaseModel):
    # max_length=64 rechaza en el PARSING (antes de construir toda la lista) —
    # barato ante un payload hostil. El field_validator hace regex + dedupe.
    order: list[str] = Field(..., max_length=64)

    @field_validator("order")
    @classmethod
    def validar_order(cls, v: list[str]) -> list[str]:
        """Valida ítems y deduplica preservando la primera aparición."""
        resultado: list[str] = []
        vistos: set[str] = set()
        for item in v:
            if not _ITEM_RE.fullmatch(item):
                raise ValueError(
                    f"ítem inválido: {item!r} — debe coincidir con ^[a-z0-9_-]{{1,64}}$"
                )
            if item not in vistos:
                resultado.append(item)
                vistos.add(item)
        return resultado


class HomeLayoutResponse(BaseModel):
    order: list[str]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tenant_id(request: Request) -> int:
    """Tenant único (N=1). Se lee de settings para no hardcodear el 1 acá."""
    return int(getattr(request.app.state.settings, "tenant_id", 1))


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/home-layout", response_model=HomeLayoutResponse, summary="Lee el layout del Inicio")
async def get_home_layout(
    request: Request,
    _: None = Depends(require_api_key),
) -> HomeLayoutResponse:
    """
    Devuelve el orden de secciones guardado por el cliente.

    Si nunca se guardó, o el dato almacenado no tiene la forma esperada, devuelve
    lista vacía para que el frontend use su orden por defecto sin errores.
    """
    db: Database = request.app.state.db
    valor: Any = await db.get_preference("home_layout", _tenant_id(request))
    if valor is None or not isinstance(valor, dict) or not isinstance(valor.get("order"), list):
        return HomeLayoutResponse(order=[])
    # Filtro defensivo: ignorar cualquier ítem que no sea string (dato corrupto parcial)
    order = [i for i in valor["order"] if isinstance(i, str)]
    return HomeLayoutResponse(order=order)


@router.put(
    "/home-layout",
    response_model=HomeLayoutResponse,
    summary="Guarda el layout del Inicio",
)
async def put_home_layout(
    body: HomeLayoutBody,
    request: Request,
    _: None = Depends(require_api_key),
) -> HomeLayoutResponse:
    """
    Reemplaza el orden de secciones del Inicio.

    El validador de HomeLayoutBody deduplica preservando la primera aparición:
    ["luces","luces","tv"] → ["luces","tv"]. Ítems que no cumplen el patrón
    ^[a-z0-9_-]{1,64}$ o listas de más de 64 elementos generan un 422.
    """
    db: Database = request.app.state.db
    await db.set_preference("home_layout", {"order": body.order}, _tenant_id(request))
    logger.info("preferences.home_layout_updated", items=len(body.order))
    return HomeLayoutResponse(order=body.order)
