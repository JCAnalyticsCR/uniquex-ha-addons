"""
HAUpstream — cliente WebSocket persistente hacia Home Assistant.

Implementa el protocolo WS de HA completo:
  WS connect → auth_required → auth → auth_ok → supported_features
  → hidratación (get_config, get_states, entity/device/area registry, get_services)
  → subscribe_events state_changed
  → loop: listener + heartbeat (TaskGroup)

En caso de desconexión: backoff exponencial 1s→30s con jitter ±50%, re-hidratación completa.
HaAuthError (auth_invalid) detiene el retry — intervención humana requerida.
"""

from __future__ import annotations

import asyncio
import random
import time
from datetime import UTC, datetime
from typing import Any

import aiohttp
import structlog

from ha_mirror.correlations import CorrelationTracker
from ha_mirror.errors import HaAuthError, HaConnectError, HaProtocolError
from ha_mirror.models import (
    HaAreaRegistryEntry,
    HaDeviceRegistryEntry,
    HaEntityRegistryEntry,
    HaState,
    StateChangedEvent,
)
from ha_mirror.prometheus_metrics import (
    EVENT_LAG_SECONDS,
    EVENTS_RECEIVED,
    HYDRATION_DURATION,
    HYDRATION_ENTITY_COUNT,
    UPSTREAM_AUTH_FAILURES,
    UPSTREAM_CONNECTED,
    UPSTREAM_RECONNECTS,
)
from ha_mirror.state_store import StateStore

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# Intervalo y timeout del heartbeat ping/pong
_PING_INTERVAL = 30.0
_PING_TIMEOUT = 10.0

# Backoff: base 1s, cap 30s, jitter multiplicativo ±50% del intervalo actual
_BACKOFF_BASE = 1.0
_BACKOFF_CAP = 30.0
_BACKOFF_JITTER = 0.5


class HAUpstream:
    """
    Gestiona la única conexión WebSocket persistente al Home Assistant del cliente.

    Patrón de uso: instanciar una vez, llamar run_forever() en un asyncio task.
    El task se cancela en el lifespan de FastAPI al apagar.
    """

    def __init__(
        self,
        ha_ws_url: str,
        llat: str,
        store: StateStore,
        correlations: CorrelationTracker,
        ping_interval: float = _PING_INTERVAL,
        ping_timeout: float = _PING_TIMEOUT,
        service_call_timeout: float = 5.0,
    ) -> None:
        self._ha_ws_url = ha_ws_url
        # LLAT en memoria como str; no se logguea ni se serializa nunca
        self._llat = llat
        self._store = store
        self._correlations = correlations
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._service_call_timeout = service_call_timeout

        # Contador monotónico de IDs de mensajes WS (por sesión)
        self._id_counter: int = 0
        # Map id → Future para correlación de result messages
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        # Referencia al WS activo (para call_service desde afuera)
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        # Lock para serializar envíos al WS
        self._send_lock = asyncio.Lock()

    # -------------------------------------------------------------------------
    # API pública
    # -------------------------------------------------------------------------

    async def run_forever(self) -> None:
        """
        Loop principal con backoff exponencial.

        Solo termina en:
        - HaAuthError (auth_invalid — no retry)
        - CancelledError (shutdown del lifespan)
        """
        backoff = _BACKOFF_BASE
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    await self._connect_and_run(session)
                    # Llegamos aquí si la conexión terminó limpiamente (raro)
                    backoff = _BACKOFF_BASE
                except HaAuthError:
                    UPSTREAM_AUTH_FAILURES.inc()
                    logger.error(
                        "ha.auth_failed",
                        msg="LLAT inválido — rotación manual requerida. Mirror detenido.",
                    )
                    await self._store.mark_disconnected()
                    UPSTREAM_CONNECTED.set(0)
                    raise  # Propaga hacia el lifespan; el proceso no intenta reconectar
                except asyncio.CancelledError:
                    raise  # Shutdown limpio
                except (HaConnectError, HaProtocolError) as exc:
                    logger.warning("ha.disconnected", reason=str(exc), next_backoff=backoff)
                except Exception as exc:
                    logger.exception("ha.unexpected_error", exc=str(exc))
                finally:
                    await self._store.mark_disconnected()
                    UPSTREAM_CONNECTED.set(0)
                    self._ws = None
                    self._cancel_all_pending()

                UPSTREAM_RECONNECTS.inc()
                await self._store.mark_reconnecting()

                # Backoff con jitter: sleep = backoff * uniform(0.5, 1.5)
                jitter = 1.0 + _BACKOFF_JITTER * (2 * random.random() - 1)
                sleep_time = min(backoff * jitter, _BACKOFF_CAP)
                logger.info("ha.reconnect_wait", sleep_seconds=round(sleep_time, 2))
                await asyncio.sleep(sleep_time)
                backoff = min(backoff * 2, _BACKOFF_CAP)

    async def send_service_call(
        self, domain: str, service: str, service_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Envía un call_service al upstream y espera el result.

        Timeout: self._service_call_timeout segundos.
        Lanza UpstreamNotReadyError si no hay WS activo.
        """
        from ha_mirror.errors import UpstreamNotReadyError

        if self._ws is None or not self._store.connected:
            raise UpstreamNotReadyError("Upstream no conectado")

        msg = {
            "type": "call_service",
            "domain": domain,
            "service": service,
            **service_data,
        }
        return await self._send_command(msg, timeout=self._service_call_timeout)

    # -------------------------------------------------------------------------
    # Conexión y handshake
    # -------------------------------------------------------------------------

    async def _connect_and_run(self, session: aiohttp.ClientSession) -> None:
        """Establece WS, autentica, hidrata y corre el loop de mensajes."""
        try:
            async with session.ws_connect(
                self._ha_ws_url,
                timeout=aiohttp.ClientWSTimeout(ws_receive=_PING_INTERVAL + _PING_TIMEOUT + 5),
                heartbeat=None,  # Heartbeat manual para controlar el ID
                max_msg_size=10 * 1024 * 1024,  # 10 MB — payloads de registry pueden ser grandes
            ) as ws:
                self._ws = ws
                self._id_counter = 0
                self._pending.clear()

                await self._store.mark_upstream_state("AUTHENTICATING")
                await self._authenticate(ws)

                await self._store.mark_upstream_state("HYDRATING")
                t0 = time.monotonic()
                await self._hydrate(ws)
                HYDRATION_DURATION.observe(time.monotonic() - t0)

                # Loop principal: listener + heartbeat en TaskGroup
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self._listen(ws), name="ha_listener")
                    tg.create_task(self._heartbeat(ws), name="ha_heartbeat")

        except HaAuthError:
            # auth_invalid — propagar sin retry (lanzada en _authenticate, antes del TaskGroup)
            raise
        except aiohttp.ClientConnectorError as exc:
            raise HaConnectError(f"TCP connect failed: {exc}") from exc
        except aiohttp.WSServerHandshakeError as exc:
            raise HaConnectError(f"WS upgrade failed: {exc.status}") from exc
        except aiohttp.ServerDisconnectedError as exc:
            raise HaConnectError("Server disconnected abruptly") from exc
        except aiohttp.ClientResponseError as exc:
            raise HaConnectError(f"HTTP error during connect: {exc.status}") from exc
        except BaseExceptionGroup as eg:
            # TaskGroup propaga como ExceptionGroup — extraer la primera excepción relevante
            for exc in eg.exceptions:
                if isinstance(exc, HaAuthError):
                    raise HaAuthError("auth_invalid en TaskGroup") from exc
                if isinstance(exc, (HaConnectError, HaProtocolError)):
                    raise HaConnectError(str(exc)) from exc
            raise HaConnectError(f"TaskGroup error: {eg}") from eg

    async def _authenticate(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """
        Ejecuta el handshake de autenticación HA.

        Secuencia: recv auth_required → send auth → recv auth_ok
        Seguido de: send supported_features {coalesce_messages: 1}
        """
        msg = await self._recv_json(ws)
        if msg.get("type") != "auth_required":
            raise HaProtocolError(f"Esperaba auth_required, recibí: {msg.get('type')}")

        ha_version = msg.get("ha_version", "unknown")
        logger.info("ha.auth_required", ha_version=ha_version)

        # Enviar auth — LLAT nunca va a los logs
        await ws.send_json({"type": "auth", "access_token": self._llat})

        result = await self._recv_json(ws)
        if result.get("type") == "auth_invalid":
            raise HaAuthError(f"auth_invalid: {result.get('message', '')}")
        if result.get("type") != "auth_ok":
            raise HaProtocolError(f"Esperaba auth_ok, recibí: {result.get('type')}")

        logger.info("ha.auth_ok", ha_version=result.get("ha_version", ha_version))

        # coalesce_messages deshabilitado (0): HA enviaría resultados de hidratación
        # en frames binarios coalescidos que _listen descarta, causando TimeoutError a 30s.
        # Con 0, HA usa frames TEXT normales JSON — compatible con el parser actual.
        # TODO: implementar parser de frames binarios (array msgpack-like) para habilitar coalesce.
        self._id_counter += 1
        await ws.send_json(
            {
                "id": self._id_counter,
                "type": "supported_features",
                "features": {"coalesce_messages": 0},
            }
        )

    async def _hydrate(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """
        Hidratación completa del estado.

        Orden obligatorio según L1.2:
        get_config → get_states → entity_registry/list_for_display
        → device_registry/list → area_registry/list → get_services
        → subscribe_events state_changed

        Usa _send_command_direct en lugar de _send_command porque durante la
        hidratación _listen aún no está corriendo (se lanza en el TaskGroup
        posterior). _send_command_direct lee frames del WS directamente sin
        depender del mecanismo de _pending + Future.
        """
        # 1. get_config (metadata, sin datos grandes) — fire-and-forget; su result
        #    será descartado silenciosamente por _send_command_direct en los pasos
        #    siguientes si llega antes que el result esperado.
        await self._send_command_fire(ws, {"type": "get_config"})

        # 2. get_states — lista completa de estados actuales
        states_result = await self._send_command_direct(ws, {"type": "get_states"}, timeout=60.0)
        raw_states: list[dict[str, Any]] = states_result.get("result") or []

        # 3. entity_registry/list_for_display — payload reducido vs list
        entity_result = await self._send_command_direct(
            ws, {"type": "config/entity_registry/list_for_display"}, timeout=30.0
        )
        # list_for_display retorna {"entities": [...], "entity_categories": {...}}
        raw_entities: list[dict[str, Any]] = (
            entity_result.get("result", {}).get("entities") or []
            if isinstance(entity_result.get("result"), dict)
            else entity_result.get("result") or []
        )

        # 4. device_registry/list
        device_result = await self._send_command_direct(
            ws, {"type": "config/device_registry/list"}, timeout=30.0
        )
        raw_devices: list[dict[str, Any]] = device_result.get("result") or []

        # 5. area_registry/list
        area_result = await self._send_command_direct(
            ws, {"type": "config/area_registry/list"}, timeout=30.0
        )
        raw_areas: list[dict[str, Any]] = area_result.get("result") or []

        # 6. get_services
        services_result = await self._send_command_direct(
            ws, {"type": "get_services"}, timeout=30.0
        )
        services: dict[str, Any] = services_result.get("result") or {}

        # Parsear modelos (toleramos campos extra con extra='allow')
        states = [HaState.model_validate(s) for s in raw_states]
        entities = self._parse_entities(raw_entities)
        devices = [HaDeviceRegistryEntry.model_validate(d) for d in raw_devices]
        areas = [HaAreaRegistryEntry.model_validate(a) for a in raw_areas]

        logger.info(
            "ha.hydration_complete",
            states=len(states),
            entities=len(entities),
            devices=len(devices),
            areas=len(areas),
        )
        HYDRATION_ENTITY_COUNT.set(len(states))

        # Persistir en el store (notifica a clientes WS vía fan-out interno)
        await self._store.hydrate(states, entities, devices, areas, services)
        UPSTREAM_CONNECTED.set(1)

        # 7. subscribe_events state_changed
        sub_result = await self._send_command_direct(
            ws,
            {"type": "subscribe_events", "event_type": "state_changed"},
            timeout=10.0,
        )
        if not sub_result.get("success"):
            raise HaProtocolError("subscribe_events falló")

        logger.info("ha.subscribed_state_changed")

    # -------------------------------------------------------------------------
    # Loop de mensajes
    # -------------------------------------------------------------------------

    async def _listen(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """
        Loop principal que despacha mensajes del upstream.

        Termina cuando WS cierra o llega error. TaskGroup cancela el heartbeat.
        """
        async for raw_msg in ws:
            if raw_msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data: dict[str, Any] = raw_msg.json()
                except Exception:
                    logger.warning("ha.malformed_json_frame")
                    continue
                await self._dispatch(data)

            elif raw_msg.type == aiohttp.WSMsgType.BINARY:
                # HA puede enviar mensajes coalescidos en binary (msgpack-like array)
                # Por ahora ignoramos y logueamos para detectar si esto ocurre
                logger.debug("ha.binary_frame_ignored", size=len(raw_msg.data))

            elif raw_msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                logger.warning(
                    "ha.ws_closed",
                    type=raw_msg.type.name,
                    code=getattr(raw_msg, "data", None),
                )
                raise HaConnectError(f"WS closed: {raw_msg.type.name}")

    async def _dispatch(self, msg: dict[str, Any]) -> None:
        """Despacha un mensaje recibido según su tipo."""
        msg_type = msg.get("type")

        if msg_type == "event":
            await self._handle_event(msg)

        elif msg_type == "result":
            await self._handle_result(msg)

        elif msg_type == "pong":
            # El heartbeat usa pending futures igual que los comandos
            msg_id = msg.get("id")
            if msg_id and msg_id in self._pending:
                fut = self._pending[msg_id]
                if not fut.done():
                    fut.set_result(msg)

        elif msg_type in ("auth_required", "auth_ok", "auth_invalid"):
            # No deberían llegar aquí post-handshake
            logger.warning("ha.unexpected_auth_frame", type=msg_type)

        else:
            logger.debug("ha.unknown_message_type", type=msg_type)

    async def _handle_event(self, msg: dict[str, Any]) -> None:
        """Procesa evento state_changed y actualiza el store."""
        event_data = msg.get("event", {})
        if event_data.get("event_type") != "state_changed":
            return

        try:
            event = StateChangedEvent.model_validate(event_data)
        except Exception as exc:
            logger.warning("ha.event_parse_error", exc=str(exc))
            return

        EVENTS_RECEIVED.inc()

        # Calcular lag desde time_fired
        # Normalizar a UTC aware para comparación homogénea (evita datetime.utcnow() deprecado)
        fired_at = event.time_fired
        if fired_at.tzinfo is None:
            fired_aware = fired_at.replace(tzinfo=UTC)
        else:
            fired_aware = fired_at.astimezone(UTC)
        lag = max(0.0, (datetime.now(UTC) - fired_aware).total_seconds())
        EVENT_LAG_SECONDS.observe(lag)

        entity_id = event.data.entity_id
        new_state = event.data.new_state

        # Resolver correlación si hay alguna pendiente para esta entidad
        correlation_id = await self._correlations.resolve_by_entity(entity_id)

        # Actualizar store (fan-out incluido)
        await self._store.apply_state_changed(entity_id, new_state, correlation_id)

        # Notificar service_complete si había correlación
        if correlation_id:
            await self._store.fanout_service_complete(
                correlation_id=correlation_id,
                entity_id=entity_id,
                success=True,
            )

    async def _handle_result(self, msg: dict[str, Any]) -> None:
        """Resuelve el Future del pending map para el id del result."""
        msg_id = msg.get("id")
        if msg_id is None:
            return
        fut = self._pending.get(msg_id)
        if fut and not fut.done():
            if msg.get("success"):
                fut.set_result(msg)
            else:
                error = msg.get("error", {})
                fut.set_exception(
                    HaProtocolError(
                        f"HA result error: {error.get('code')} — {error.get('message')}"
                    )
                )

    # -------------------------------------------------------------------------
    # Heartbeat
    # -------------------------------------------------------------------------

    async def _heartbeat(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """
        Envía ping cada _PING_INTERVAL segundos.

        Si el pong no llega en _PING_TIMEOUT: declara conexión muerta.
        ID del ping es parte de la secuencia monotónica (no ID fijo).
        """
        while True:
            await asyncio.sleep(self._ping_interval)
            try:
                result = await self._send_command(
                    {"type": "ping"}, ws=ws, timeout=self._ping_timeout
                )
                if result.get("type") != "pong":
                    logger.warning("ha.heartbeat_unexpected_response", type=result.get("type"))
            except TimeoutError:
                logger.error("ha.heartbeat_timeout", timeout=self._ping_timeout)
                raise HaConnectError("Heartbeat timeout — socket muerto")

    # -------------------------------------------------------------------------
    # Helpers de envío / recepción
    # -------------------------------------------------------------------------

    async def _send_command(
        self,
        msg: dict[str, Any],
        ws: aiohttp.ClientWebSocketResponse | None = None,
        timeout: float = 10.0,
    ) -> dict[str, Any]:
        """
        Envía un mensaje con ID y espera el result correspondiente.

        Usa el WS activo (self._ws) si no se pasa ws explícitamente.
        """
        target_ws = ws or self._ws
        if target_ws is None:
            raise HaConnectError("No hay WS activo")

        async with self._send_lock:
            self._id_counter += 1
            msg_id = self._id_counter
            msg["id"] = msg_id

            fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
            self._pending[msg_id] = fut

            await target_ws.send_json(msg)

        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending.pop(msg_id, None)

    async def _send_command_direct(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        msg: dict[str, Any],
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """
        Versión de _send_command para la fase de hidratación.

        Durante _hydrate no hay _listen activo, por lo que no hay nadie
        que resuelva los futures de _pending.  Este método lee frames
        directamente del WS en un loop hasta recibir el result con el
        ID esperado, descartando cualquier mensaje intermedio (p.ej.
        el result de get_config en fire-and-forget).

        No se usa _pending ni asyncio.Future: la sincronización es
        lineal dentro del mismo coroutine de hidratación.
        """
        async with self._send_lock:
            self._id_counter += 1
            msg_id = self._id_counter
            msg["id"] = msg_id
            await ws.send_json(msg)

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError(f"Timeout esperando result id={msg_id} ({msg.get('type')})")
            try:
                raw = await asyncio.wait_for(ws.receive(), timeout=remaining)
            except TimeoutError:
                raise TimeoutError(f"Timeout esperando result id={msg_id} ({msg.get('type')})")

            if raw.type == aiohttp.WSMsgType.TEXT:
                try:
                    parsed = raw.json()
                except Exception:
                    logger.warning("ha.malformed_json_frame_during_hydration")
                    continue

                # HA puede enviar un array JSON coalescido (coalesce_messages activo
                # del lado del servidor aunque nosotros hayamos pedido 0) o cuando
                # la primera respuesta agrupa supported_features + get_config result.
                # Expandir el array y buscar el result con el ID esperado.
                candidates: list[dict[str, Any]] = parsed if isinstance(parsed, list) else [parsed]

                found: dict[str, Any] | None = None
                for item in candidates:
                    if not isinstance(item, dict):
                        continue
                    if item.get("type") == "result" and item.get("id") == msg_id:
                        found = item
                        break
                    logger.debug(
                        "ha.hydration_discard_frame",
                        type=item.get("type"),
                        id=item.get("id"),
                        expected_id=msg_id,
                    )

                if found is not None:
                    if not found.get("success"):
                        error = found.get("error", {})
                        raise HaProtocolError(
                            f"HA result error id={msg_id}: "
                            f"{error.get('code')} — {error.get('message')}"
                        )
                    return found
                continue

            elif raw.type == aiohttp.WSMsgType.BINARY:
                # HA con coalesce_messages podría enviar binario; ignorar y continuar
                logger.debug("ha.binary_frame_during_hydration", size=len(raw.data))
                continue

            elif raw.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                raise HaConnectError(f"WS cerrado durante hidratación: {raw.type.name}")

    async def _send_command_fire(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        msg: dict[str, Any],
    ) -> None:
        """Envía un mensaje con ID pero no espera result (fire-and-forget)."""
        async with self._send_lock:
            self._id_counter += 1
            msg["id"] = self._id_counter
            await ws.send_json(msg)

    @staticmethod
    async def _recv_json(ws: aiohttp.ClientWebSocketResponse) -> dict[str, Any]:
        """Lee el próximo mensaje texto del WS y lo deserializa."""
        raw = await ws.receive()
        if raw.type == aiohttp.WSMsgType.TEXT:
            return raw.json()  # type: ignore[return-value]
        if raw.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
            raise HaConnectError(f"WS cerrado durante handshake: {raw.type.name}")
        raise HaProtocolError(f"Tipo de mensaje inesperado: {raw.type.name}")

    def _parse_entities(self, raw: list[dict[str, Any]]) -> list[HaEntityRegistryEntry]:
        """
        Parsea entities de list_for_display.

        list_for_display usa claves comprimidas (ei, en, ic, etc.).
        El modelo las acepta vía alias configurados en HaEntityRegistryEntry.
        """
        result = []
        for item in raw:
            try:
                # Normalizar claves comprimidas a nombres canónicos
                normalized = {
                    "entity_id": item.get("ei") or item.get("entity_id", ""),
                    "name": item.get("en") or item.get("name"),
                    "icon": item.get("ic") or item.get("icon"),
                    "platform": item.get("pl") or item.get("platform"),
                    "area_id": item.get("ai") or item.get("area_id"),
                    "device_id": item.get("di") or item.get("device_id"),
                    "hidden_by": item.get("hb") or item.get("hidden_by"),
                    "disabled_by": item.get("db") or item.get("disabled_by"),
                }
                result.append(HaEntityRegistryEntry.model_validate(normalized))
            except Exception as exc:
                logger.debug("ha.entity_parse_skip", exc=str(exc))
        return result

    def _cancel_all_pending(self) -> None:
        """Cancela todos los futures pendientes al desconectar."""
        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()
        self._pending.clear()
