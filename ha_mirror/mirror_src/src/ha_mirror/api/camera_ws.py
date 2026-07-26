"""
WebSocket /ws/cameras/{entity_id} — puente MSE-over-WebSocket hacia go2rtc.

Por que un puente WS y no el fMP4 por HTTP: Cloudflare bufferea el streaming
HTTP y el video en vivo se congela/entrecorta. El WebSocket no lo bufferea igual
(el /ws/state ya va fluido por ese mismo motivo). Este endpoint reusa esa via:
autentica igual que /ws/state, resuelve el stream desde el allowlist del Mirror
y hace de puente TRANSPARENTE bidireccional con el /api/ws de go2rtc, que habla
el protocolo MSE directamente con el <video> del navegador. El Mirror no
interpreta los frames: solo los reenvia (TEXT como TEXT, BINARY como BINARY).

Seguridad:
  - Auth con authenticate_ws ANTES de accept (cierra 4001/4029 si falla), igual
    que /ws/state.
  - El nombre del stream go2rtc NUNCA sale del navegador: se resuelve contra el
    allowlist del Mirror (camera_media). Si el entity_id no esta mapeado, cierra
    con 4004 (anti-SSRF).
  - La api_key nunca se loguea.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress

import aiohttp
import structlog
from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

router = APIRouter()

# El puente es una conexion de larga vida: sin timeout total para no cortar el
# stream, solo un limite razonable para el handshake inicial contra go2rtc.
_GO2RTC_TIMEOUT = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10)


@router.websocket("/ws/cameras/{entity_id}")
async def ws_camera(
    websocket: WebSocket,
    entity_id: str,
    api_key: str | None = Query(default=None),
) -> None:
    """
    Puente WebSocket entre el navegador y el /api/ws de go2rtc.

    El cliente pasa ?api_key=<MIRROR_API_KEY> en la URL (el browser no permite
    headers custom en el upgrade WS — mismo compromiso Fase 1 que /ws/state).
    """
    # Importacion tardia para evitar ciclo con auth (auth importa config).
    from ha_mirror.auth import authenticate_ws

    # Validar antes de aceptar. authenticate_ws cierra el WS con el codigo
    # apropiado (4001 key invalida / 4029 rate limit) si la autenticacion falla.
    try:
        await authenticate_ws(websocket, api_key)
    except Exception:
        # authenticate_ws ya cerro el WebSocket con el codigo correspondiente.
        return

    # Resolver el stream contra el allowlist del servidor (nunca desde el browser).
    media = websocket.app.state.camera_media
    target = media.go2rtc_ws_target(entity_id)

    await websocket.accept()

    if target is None:
        # entity_id sin stream mapeado (o go2rtc sin configurar) → cerrar 4004.
        await websocket.close(code=4004, reason="Camara sin stream disponible")
        return

    go2rtc_url, go2rtc_auth = target

    try:
        async with (
            aiohttp.ClientSession(timeout=_GO2RTC_TIMEOUT) as session,
            session.ws_connect(go2rtc_url, auth=go2rtc_auth) as upstream,
        ):
            logger.info("camera_ws.bridge_open", entity_id=entity_id)
            await _bridge(websocket, upstream)
    except WebSocketDisconnect:
        pass
    except aiohttp.ClientError as exc:
        logger.warning(
            "camera_ws.go2rtc_connection_failed",
            entity_id=entity_id,
            err=str(exc),
        )
    except Exception as exc:  # noqa: BLE001 — no dejar que un fallo tumbe el worker
        logger.exception("camera_ws.unexpected_error", entity_id=entity_id, exc=str(exc))
    finally:
        # Cerrar el lado del cliente si sigue abierto (go2rtc cayo primero, o error).
        with suppress(Exception):
            await websocket.close()
        logger.info("camera_ws.bridge_closed", entity_id=entity_id)


async def _bridge(
    client_ws: WebSocket,
    upstream_ws: aiohttp.ClientWebSocketResponse,
) -> None:
    """
    Puente transparente bidireccional entre el cliente y go2rtc.

    Dos tareas concurrentes; la primera que termina (desconexion o error de
    cualquier lado) cancela la otra. Los frames se reenvian sin interpretar.
    """

    async def go2rtc_to_client() -> None:
        async for msg in upstream_ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                await client_ws.send_text(msg.data)
            elif msg.type == aiohttp.WSMsgType.BINARY:
                await client_ws.send_bytes(msg.data)
            elif msg.type in (
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSING,
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.ERROR,
            ):
                break

    async def client_to_go2rtc() -> None:
        while True:
            msg = await client_ws.receive()
            if msg["type"] == "websocket.disconnect":
                break
            text = msg.get("text")
            if text is not None:
                await upstream_ws.send_str(text)
                continue
            data = msg.get("bytes")
            if data is not None:
                await upstream_ws.send_bytes(data)

    tasks = {
        asyncio.create_task(go2rtc_to_client(), name="camera_ws_g2c"),
        asyncio.create_task(client_to_go2rtc(), name="camera_ws_c2g"),
    }
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    for task in pending:
        task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await task
    # Consumir excepciones de la(s) tarea(s) terminada(s) para no dejar
    # "Task exception was never retrieved"; las desconexiones son esperables.
    for task in done:
        with suppress(Exception):
            task.result()
