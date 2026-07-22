"""Endpoints autenticados para snapshots y señalizacion WebRTC."""

from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from ha_mirror.auth import require_api_key
from ha_mirror.camera_media import CameraMediaError

router = APIRouter(prefix="/api/cameras", tags=["cameras"])

_ENTITY_ID_PATTERN = re.compile(r"^camera\.[a-z0-9_]+$")
_MAX_SDP_CHARS = 256 * 1024


def _validate_camera(request: Request, entity_id: str) -> None:
    """Solo permite entidades camera existentes en el cache de HA."""
    if not _ENTITY_ID_PATTERN.fullmatch(entity_id):
        raise HTTPException(status_code=400, detail="Invalid camera entity")
    summary = request.app.state.store.get_entity_summary(entity_id)
    if summary is None or summary.domain != "camera":
        raise HTTPException(status_code=404, detail="Camera not found")


def _media_error(exc: CameraMediaError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.code)


@router.get("/{entity_id}/snapshot", summary="Snapshot seguro de una camara HA")
async def camera_snapshot(
    entity_id: str,
    request: Request,
    _: None = Depends(require_api_key),
) -> Response:
    _validate_camera(request, entity_id)
    try:
        payload = await request.app.state.camera_media.get_snapshot(entity_id)
    except CameraMediaError as exc:
        raise _media_error(exc) from exc

    return Response(
        content=payload.body,
        media_type=payload.content_type,
        headers={
            "Cache-Control": "private, no-store, max-age=0",
            "Pragma": "no-cache",
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.get("/{entity_id}/capabilities", summary="Capacidades de medios de una camara")
async def camera_capabilities(
    entity_id: str,
    request: Request,
    _: None = Depends(require_api_key),
) -> dict[str, object]:
    _validate_camera(request, entity_id)
    media = request.app.state.camera_media
    return {
        "entity_id": entity_id,
        "snapshot": True,
        "webrtc": media.has_stream(entity_id),
        "webrtc_transport": "whep" if media.has_stream(entity_id) else None,
    }


@router.post("/{entity_id}/webrtc", summary="Intercambio SDP con go2rtc")
async def camera_webrtc(
    entity_id: str,
    request: Request,
    _: None = Depends(require_api_key),
) -> Response:
    _validate_camera(request, entity_id)
    content_type = request.headers.get("content-type", "").split(";", 1)[0].lower()
    if content_type != "application/sdp":
        raise HTTPException(status_code=415, detail="Content-Type must be application/sdp")

    raw_offer = await request.body()
    if not raw_offer or len(raw_offer) > _MAX_SDP_CHARS:
        raise HTTPException(status_code=413, detail="Invalid SDP offer size")
    try:
        offer = raw_offer.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid SDP encoding") from exc
    if not offer.startswith("v=0"):
        raise HTTPException(status_code=400, detail="Invalid SDP offer")

    try:
        answer = await request.app.state.camera_media.exchange_webrtc_offer(
            entity_id,
            offer,
            request.headers.get("user-agent"),
        )
    except CameraMediaError as exc:
        raise _media_error(exc) from exc

    return Response(
        content=answer,
        media_type="application/sdp",
        status_code=201,
        headers={
            "Cache-Control": "private, no-store, max-age=0",
            "X-Content-Type-Options": "nosniff",
        },
    )
