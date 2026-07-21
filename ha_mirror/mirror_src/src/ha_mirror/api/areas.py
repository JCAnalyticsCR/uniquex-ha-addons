"""GET /api/areas — áreas enriquecidas con sus entidades."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from ha_mirror.auth import require_api_key
from ha_mirror.models import AreaSummary

router = APIRouter()


class AreasResponse(BaseModel):
    areas: list[AreaSummary]
    count: int


@router.get(
    "/api/areas",
    response_model=AreasResponse,
    summary="Lista áreas con entidades agrupadas",
)
async def list_areas(
    request: Request,
    _: None = Depends(require_api_key),
) -> AreasResponse:
    """
    Retorna todas las áreas del registry con sus entity_ids agrupados.
    La lista de entidades por área es la base de la vista 'por habitación'
    del frontend (caso de uso principal según L1.10).
    """
    store = request.app.state.store
    areas = store.get_areas_enriched()
    return AreasResponse(areas=areas, count=len(areas))
