"""GET /api/health — estado del mirror y del upstream HA."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from ha_mirror.auth import require_api_key
from ha_mirror.models import HealthResponse

router = APIRouter()


@router.get(
    "/api/health",
    response_model=HealthResponse,
    summary="Estado del mirror y upstream HA",
)
async def health(
    request: Request,
    _: None = Depends(require_api_key),
) -> HealthResponse:
    """
    Retorna el estado actual del mirror: conexión upstream, reconexiones,
    clientes WS activos y estado de la caché.
    """
    store = request.app.state.store
    settings = request.app.state.settings

    return HealthResponse(
        upstream_connected=store.connected,
        upstream_state=store.upstream_state,
        last_event_ts=store.last_event_ts,
        ws_reconnects_total=store.reconnect_count,
        connected_ws_clients=store.get_subscriber_count(),
        tenant_id=settings.tenant_id,
        # app.state.mirror_version se populó en el lifespan (main.py).
        # No usamos request.app.version directamente porque starlette tipea
        # Request.app como Starlette (sin .version), mientras que app.state
        # es Any y no genera error con mypy strict.
        version=request.app.state.mirror_version,
    )
