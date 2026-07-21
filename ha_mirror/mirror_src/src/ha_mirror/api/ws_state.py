"""
WebSocket /ws/state — canal de eventos en tiempo real al frontend.

Protocolo:
  1. Cliente conecta con ?api_key=<key>
  2. Server autentica (cierra con 4001 si falla)
  3. Server envía snapshot completo del estado actual
  4. Server hace stream de diffs (state_changed, connection_status,
     service_complete, service_timeout) hasta que el cliente desconecta

El fan-out usa asyncio.Queue con drop si está llena — cliente lento no
bloquea el loop del upstream.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

import structlog
from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from ha_mirror.models import WsSnapshot
from ha_mirror.prometheus_metrics import WS_CLIENTS_CONNECTED, WS_MESSAGES_SENT

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

router = APIRouter()


@router.websocket("/ws/state")
async def ws_state(
    websocket: WebSocket,
    api_key: str | None = Query(default=None),
) -> None:
    """
    WebSocket de estado en tiempo real.

    El cliente debe pasar ?api_key=<MIRROR_API_KEY> en la URL de conexión.
    """
    # Importación tardía para evitar ciclo con auth (auth importa config)
    from ha_mirror.auth import authenticate_ws

    # Validar antes de aceptar la conexión WS.
    # authenticate_ws aplica rate-limiting por IP y cierra el WS con el código
    # apropiado (4001 key inválida / 4029 rate limit) si la autenticación falla.
    try:
        await authenticate_ws(websocket, api_key)
    except Exception:
        # authenticate_ws ya cerró el WebSocket con el código correspondiente.
        return

    await websocket.accept()

    store = websocket.app.state.store
    client_id = str(uuid.uuid4())[:8]

    logger.info("ws.client_connected", client_id=client_id)
    WS_CLIENTS_CONNECTED.inc()

    # Registrar suscriptor y obtener su queue
    queue = store.subscribe(client_id)

    try:
        # Enviar snapshot inicial con el estado completo
        await _send_snapshot(websocket, store, client_id)

        # Lanzar ambas corutinas como tasks concurrentes.
        # Usamos gather con return_exceptions=False: la primera que falla cancela la otra.
        stream_task = asyncio.create_task(
            _stream_events(websocket, queue, client_id),
            name=f"ws_stream_{client_id}",
        )
        recv_task = asyncio.create_task(
            _recv_pings(websocket, client_id),
            name=f"ws_recv_{client_id}",
        )
        done, pending = await asyncio.wait(
            {stream_task, recv_task},
            return_when=asyncio.FIRST_EXCEPTION,
        )
        # Cancelar la tarea que quedó viva
        for t in pending:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, WebSocketDisconnect, Exception):
                pass
        # Propagar excepción si alguna tarea falló con algo distinto a desconexión
        for t in done:
            exc = t.exception()
            if exc and not isinstance(exc, (WebSocketDisconnect, asyncio.CancelledError)):
                raise exc

    except WebSocketDisconnect:
        logger.info("ws.client_disconnected", client_id=client_id)
    except asyncio.CancelledError:
        logger.info("ws.client_task_cancelled", client_id=client_id)
    except Exception as exc:
        logger.exception("ws.unexpected_error", client_id=client_id, exc=str(exc))
    finally:
        store.unsubscribe(client_id)
        WS_CLIENTS_CONNECTED.dec()


async def _send_snapshot(websocket: WebSocket, store: Any, client_id: str) -> None:
    """Envía el snapshot completo del estado actual al cliente."""
    states = store.get_all_entity_summaries()
    areas = store.get_areas_enriched()
    services = store.get_services()

    snapshot = WsSnapshot(
        states={eid: s for eid, s in states.items()},
        areas=areas,
        services=services,
        cache_version=store.cache_version,
    )

    payload = snapshot.model_dump(mode="json")
    await websocket.send_text(json.dumps(payload))

    logger.info(
        "ws.snapshot_sent",
        client_id=client_id,
        entity_count=len(states),
        cache_version=store.cache_version,
    )
    WS_MESSAGES_SENT.labels(type="snapshot").inc()


async def _stream_events(
    websocket: WebSocket,
    queue: asyncio.Queue[dict[str, Any]],
    client_id: str,
) -> None:
    """Lee eventos de la queue y los envía al cliente WS."""
    while True:
        try:
            msg = await asyncio.wait_for(queue.get(), timeout=60.0)
        except asyncio.TimeoutError:
            # Keepalive: si no hay eventos en 60s, ping interno
            continue

        try:
            await websocket.send_text(json.dumps(msg))
            WS_MESSAGES_SENT.labels(type=msg.get("type", "unknown")).inc()
        except (WebSocketDisconnect, RuntimeError):
            # Cliente desconectado — el except* en ws_state lo capturará
            raise WebSocketDisconnect(code=1001)


async def _recv_pings(websocket: WebSocket, client_id: str) -> None:
    """
    Maneja pings del cliente → responde pong.

    También detecta desconexión del cliente desde el lado receptor.
    """
    while True:
        try:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            if data.get("type") == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
        except WebSocketDisconnect:
            raise
        except json.JSONDecodeError:
            pass  # Frame malformado del cliente — ignorar
        except Exception:
            raise WebSocketDisconnect(code=1001)
