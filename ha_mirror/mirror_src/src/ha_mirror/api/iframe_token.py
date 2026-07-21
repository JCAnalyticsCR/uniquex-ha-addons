"""GET /api/iframe-token — URL firmada temporal para el iframe de HA."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from ha_mirror.auth import require_api_key
from ha_mirror.iframe_token import generate_iframe_url

router = APIRouter()


class IframeTokenResponse(BaseModel):
    url: str
    view: str
    expires_in: int


@router.get(
    "/api/iframe-token",
    response_model=IframeTokenResponse,
    summary="Genera URL firmada temporal para el iframe de HA",
)
async def get_iframe_token(
    request: Request,
    view: str = "0",
    _: None = Depends(require_api_key),
) -> IframeTokenResponse:
    """
    Genera una URL temporalmente firmada (15 min) al HA real en el tailnet.

    El componente <HAEmbed view="energia" /> del frontend llama a este endpoint
    y usa la URL como src del iframe. Así el LLAT nunca llega al browser.

    Vistas soportadas: 0, energia, historial, camaras, mapa, admin, hacs, media.
    """
    settings = request.app.state.settings
    result = generate_iframe_url(view=view, settings=settings)
    return IframeTokenResponse(**result)
