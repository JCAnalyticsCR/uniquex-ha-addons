"""GET /api/entities — listado de entidades desde el cache hidratado."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from ha_mirror.auth import require_api_key
from ha_mirror.models import EntitySummary

router = APIRouter()


class EntitiesResponse(BaseModel):
    entities: list[EntitySummary]
    fetched_at: datetime
    cache_version: int
    count: int


@router.get(
    "/api/entities",
    response_model=EntitiesResponse,
    summary="Lista todas las entidades cacheadas",
)
async def list_entities(
    request: Request,
    domain: str | None = None,
    area_id: str | None = None,
    _: None = Depends(require_api_key),
) -> EntitiesResponse:
    """
    Retorna todas las entidades del cache hidratado.
    No llama a HA en cada request.
    Filtros opcionales: ?domain=cover&area_id=habitacion_principal
    """
    store = request.app.state.store
    summaries = store.get_all_entity_summaries()

    result = list(summaries.values())

    if domain:
        result = [e for e in result if e.domain == domain]
    if area_id:
        result = [e for e in result if e.area_id == area_id]

    return EntitiesResponse(
        entities=result,
        fetched_at=datetime.now(UTC),
        cache_version=store.cache_version,
        count=len(result),
    )


@router.get(
    "/api/entities/{entity_id:path}",
    response_model=EntitySummary,
    summary="Obtiene una entidad por entity_id",
)
async def get_entity(
    entity_id: str,
    request: Request,
    _: None = Depends(require_api_key),
) -> EntitySummary:
    """Retorna el estado actual de una entidad específica desde el cache."""
    store = request.app.state.store
    summary = store.get_entity_summary(entity_id)
    if summary is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Entidad '{entity_id}' no encontrada en el cache",
        )
    return summary
