"""Endpoints autenticados para snapshots y señalizacion WebRTC."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ha_mirror.auth import require_api_key
from ha_mirror.camera_media import CameraMediaError, _SEG_REF_MAX_LEN, _SEG_REF_RE

router = APIRouter(prefix="/api/cameras", tags=["cameras"])

_ENTITY_ID_PATTERN = re.compile(r"^camera\.[a-z0-9_]+$")
_MAX_SDP_CHARS = 256 * 1024


def _validate_camera(request: Request, entity_id: str) -> None:
    """Permite camaras de HA o IDs virtuales presentes en el mapa allowlist."""
    if not _ENTITY_ID_PATTERN.fullmatch(entity_id):
        raise HTTPException(status_code=400, detail="Invalid camera entity")
    if request.app.state.camera_media.has_stream(entity_id):
        return
    summary = request.app.state.store.get_entity_summary(entity_id)
    if summary is None or summary.domain != "camera":
        raise HTTPException(status_code=404, detail="Camera not found")


def _media_error(exc: CameraMediaError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.code)


class CameraInfo(BaseModel):
    """Camara disponible para la app: entidad de HA o stream go2rtc mapeado."""

    entity_id: str
    name: str
    source: str  # "go2rtc" | "homeassistant"
    snapshot: bool
    webrtc: bool
    state: str | None = None


class CameraListResponse(BaseModel):
    cameras: list[CameraInfo]
    count: int


def _derive_name(entity_id: str) -> str:
    """camera.nvr_c07_costado_garaje -> 'C07 COSTADO GARAJE'."""
    slug = entity_id.split(".", 1)[1]
    if slug.startswith("nvr_"):
        slug = slug[4:]
    return slug.replace("_", " ").strip().upper() or entity_id


@router.get("", response_model=CameraListResponse, summary="Camaras disponibles")
async def list_cameras(
    request: Request,
    _: None = Depends(require_api_key),
) -> CameraListResponse:
    """
    Devuelve la lista de camaras que la app puede mostrar.

    `camera_stream_map` es la lista curada (un canal del NVR por entrada): si
    tiene contenido, manda ella. Si esta vacia se caen las camaras `camera.*`
    de Home Assistant, para no dejar la vista sin nada.

    Agregar una camara nueva = agregarla a go2rtc + al mapa del add-on.
    La app la muestra sola, sin redeploy del frontend.
    """
    media = request.app.state.camera_media
    store = request.app.state.store
    labels: dict[str, str] = request.app.state.settings.camera_label_map

    summaries = store.get_all_entity_summaries()
    ha_cameras = {
        entity_id: summary
        for entity_id, summary in summaries.items()
        if summary.domain == "camera"
    }

    mapped = media.list_stream_entities()
    entity_ids = sorted(mapped) if mapped else sorted(ha_cameras)
    cameras: list[CameraInfo] = []
    for entity_id in entity_ids:
        summary = ha_cameras.get(entity_id)
        friendly = getattr(summary, "friendly_name", None) if summary else None
        cameras.append(
            CameraInfo(
                entity_id=entity_id,
                name=labels.get(entity_id) or friendly or _derive_name(entity_id),
                source="go2rtc" if media.has_stream(entity_id) else "homeassistant",
                snapshot=True,
                webrtc=media.has_stream(entity_id),
                state=summary.state if summary else None,
            )
        )

    return CameraListResponse(cameras=cameras, count=len(cameras))


def _clamp_int(raw: str | None, lo: int, hi: int) -> int | None:
    """Parsea un query param entero y lo acota a [lo, hi]; None si falta/invalido."""
    if raw is None:
        return None
    try:
        return max(lo, min(hi, int(raw)))
    except (TypeError, ValueError):
        return None


@router.get("/{entity_id}/snapshot", summary="Snapshot seguro de una camara HA")
async def camera_snapshot(
    entity_id: str,
    request: Request,
    _: None = Depends(require_api_key),
) -> Response:
    _validate_camera(request, entity_id)
    # `w` (ancho) y `q` (calidad) opcionales: el grid del celular pide miniaturas
    # livianas; el fullscreen usa video en vivo. Acotados para evitar abusos.
    width = _clamp_int(request.query_params.get("w"), 120, 1280)
    quality = _clamp_int(request.query_params.get("q"), 20, 100)
    # `live=1`: lo manda el visor a pantalla completa cuando usa el bucle de
    # snapshots como video (iPhone: Safari no trae MediaSource). Ese bucle
    # necesita cuadros que AVANCEN, asi que el cache le aplica un TTL de 0,8 s y
    # no le sirve nada vencido — con el TTL normal de 30 s el reloj de la camara
    # se veia congelado.
    live = request.query_params.get("live") == "1"
    media = request.app.state.camera_media
    try:
        payload = await media.get_snapshot(
            entity_id, width=width, quality=quality, live=live
        )
    except CameraMediaError as exc:
        raise _media_error(exc) from exc

    headers = {
        "Cache-Control": "private, no-store, max-age=0",
        "Pragma": "no-cache",
        "X-Content-Type-Options": "nosniff",
    }
    # Edad del cuadro en segundos. El Mirror sirve del cache y refresca en
    # segundo plano (ver camera_media.CameraMediaClient), asi que la imagen puede
    # no ser del instante: lo decimos en vez de simularlo. Es una camara de
    # seguridad — que la app pueda avisar "hace 40 s" si algun dia lo quiere.
    age = media.snapshot_age(entity_id, width=width, quality=quality)
    if age is not None:
        headers["X-Snapshot-Age"] = str(int(age))

    return Response(content=payload.body, media_type=payload.content_type, headers=headers)


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


@router.get("/{entity_id}/stream.mp4", summary="Video fMP4 continuo de una camara")
async def camera_mp4_stream(
    entity_id: str,
    request: Request,
    _: None = Depends(require_api_key),
) -> StreamingResponse:
    _validate_camera(request, entity_id)
    context = request.app.state.camera_media.open_mp4_stream(entity_id)
    try:
        upstream = await context.__aenter__()
    except CameraMediaError as exc:
        raise _media_error(exc) from exc

    async def body() -> AsyncIterator[bytes]:
        try:
            async for chunk in upstream.content.iter_chunked(64 * 1024):
                if chunk:
                    yield chunk
        finally:
            await context.__aexit__(None, None, None)

    return StreamingResponse(
        body(),
        media_type="video/mp4",
        headers={
            "Cache-Control": "private, no-store, max-age=0",
            "Pragma": "no-cache",
            "Content-Disposition": "inline",
            "X-Accel-Buffering": "no",
            "X-Content-Type-Options": "nosniff",
        },
    )


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


@router.get("/{entity_id}/index.m3u8", summary="Playlist HLS para Safari/iOS")
async def camera_hls_playlist(
    entity_id: str,
    request: Request,
    _: None = Depends(require_api_key),
) -> Response:
    """
    Playlist HLS con URIs de segmento reescritas a nuestro propio dominio.

    Safari no soporta MSE-over-WebSocket (ManagedMediaSource se congela).
    En cambio, reproduce HLS nativamente con un <video> estandar. go2rtc
    emite la playlist en /api/stream.m3u8; la proxeamos reescribiendo todas
    las URIs de segmento para que el navegador nunca vea go2rtc directamente.

    Content-Type: application/vnd.apple.mpegurl (requerido por Safari).
    Cache-Control: no-store porque la playlist cambia con cada segmento nuevo.
    """
    _validate_camera(request, entity_id)
    media = request.app.state.camera_media

    # HLS solo tiene sentido para camaras mapeadas en go2rtc; las camaras
    # nativas de HA no tienen stream que go2rtc pueda emitir como HLS.
    if not media.has_stream(entity_id):
        raise HTTPException(status_code=404, detail="camera_stream_not_configured")

    # Prefijo absoluto de nuestro endpoint de segmentos (sin host — Cloudflare
    # no cambia el path, asi que este path funciona en prod igual que en local).
    our_seg_prefix = f"/api/cameras/{entity_id}/hls-segment"

    try:
        playlist = await media.fetch_hls_playlist(entity_id, our_seg_prefix=our_seg_prefix)
    except CameraMediaError as exc:
        raise _media_error(exc) from exc

    return Response(
        content=playlist.encode("utf-8"),
        media_type="application/vnd.apple.mpegurl",
        headers={
            "Cache-Control": "private, no-store, max-age=0",
            "Pragma": "no-cache",
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.get("/{entity_id}/hls-segment", summary="Proxy de segmento HLS (Safari)")
async def camera_hls_segment(
    entity_id: str,
    request: Request,
    _: None = Depends(require_api_key),
) -> Response:
    """
    Proxea un segmento HLS desde go2rtc hacia el navegador.

    El parametro `_seg` contiene la referencia opaca generada al reescribir
    la playlist: path+query de go2rtc sin scheme, sin host y sin el stream
    name (que viaja server-side desde el allowlist). El stream name nunca
    viene del request del navegador — ese es el mecanismo anti-SSRF.

    Content-Type: el que devuelva go2rtc (video/mp4 o video/mp2t).
    """
    _validate_camera(request, entity_id)
    media = request.app.state.camera_media

    if not media.has_stream(entity_id):
        raise HTTPException(status_code=404, detail="camera_stream_not_configured")

    seg_ref = request.query_params.get("_seg", "")
    if not seg_ref:
        raise HTTPException(status_code=400, detail="Missing _seg parameter")

    # Validacion en la capa API antes de llegar al cliente: mismas reglas que
    # _resolve_seg_ref en camera_media.py. El cliente re-valida internamente
    # como defensa en profundidad.
    if len(seg_ref) > _SEG_REF_MAX_LEN or not _SEG_REF_RE.fullmatch(seg_ref):
        raise HTTPException(status_code=400, detail="Invalid _seg parameter")

    try:
        payload = await media.fetch_hls_segment(entity_id, seg_ref)
    except CameraMediaError as exc:
        raise _media_error(exc) from exc

    return Response(
        content=payload.body,
        media_type=payload.content_type,
        headers={
            "Cache-Control": "private, no-store, max-age=0",
            "X-Content-Type-Options": "nosniff",
        },
    )
