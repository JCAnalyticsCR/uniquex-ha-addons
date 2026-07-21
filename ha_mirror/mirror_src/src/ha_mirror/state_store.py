"""
Store en memoria del estado HA.

Patrón mutex copy-on-read: se toma el lock solo durante la mutación,
se libera ANTES del fan-out a suscriptores, evitando que un cliente
lento bloquee el procesamiento de eventos upstream.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import structlog

from ha_mirror.models import (
    AreaSummary,
    EntitySummary,
    HaAreaRegistryEntry,
    HaDeviceRegistryEntry,
    HaEntityRegistryEntry,
    HaState,
    WsConnectionStatus,
    WsServiceComplete,
    WsServiceTimeout,
    WsStateChanged,
)

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class StateStore:
    """
    Store en memoria del estado HA con fan-out WebSocket.

    Thread-safety: asyncio.Lock protege mutaciones. El fan-out ocurre
    FUERA del lock para no bloquear el loop durante sends lentos.
    """

    def __init__(self, queue_maxsize: int = 1000) -> None:
        self._lock = asyncio.Lock()

        # Estado principal
        self._states: dict[str, HaState] = {}
        self._entities: dict[str, HaEntityRegistryEntry] = {}
        self._devices: dict[str, HaDeviceRegistryEntry] = {}
        self._areas: dict[str, HaAreaRegistryEntry] = {}
        self._services: dict[str, Any] = {}

        # Metadatos del upstream
        self._connected: bool = False
        self._upstream_state: str = "DISCONNECTED"
        self._last_event_ts: datetime | None = None
        self._reconnect_count: int = 0
        self._cache_version: int = 0

        # Suscriptores WebSocket: cada uno tiene su queue
        self._subscribers: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._queue_maxsize = queue_maxsize

    # -------------------------------------------------------------------------
    # Mutaciones del upstream
    # -------------------------------------------------------------------------

    async def hydrate(
        self,
        states: list[HaState],
        entities: list[HaEntityRegistryEntry],
        devices: list[HaDeviceRegistryEntry],
        areas: list[HaAreaRegistryEntry],
        services: dict[str, Any],
    ) -> None:
        """Reemplaza todo el estado en memoria post-hidratación del upstream."""
        async with self._lock:
            self._states = {s.entity_id: s for s in states}
            self._entities = {e.entity_id: e for e in entities}
            self._devices = {d.id: d for d in devices}
            self._areas = {a.area_id: a for a in areas}
            self._services = services
            self._connected = True
            self._upstream_state = "READY"
            self._cache_version += 1
            version = self._cache_version

        logger.info(
            "store.hydrated",
            states=len(states),
            entities=len(entities),
            areas=len(areas),
            cache_version=version,
        )

        # Fan-out del snapshot fuera del lock
        await self._fanout_connection_status("connected")

    async def apply_state_changed(
        self,
        entity_id: str,
        new_state: HaState | None,
        correlation_id: str | None = None,
    ) -> None:
        """Actualiza el estado de una entidad y hace fan-out del diff."""
        async with self._lock:
            if new_state is not None:
                self._states[entity_id] = new_state
            else:
                self._states.pop(entity_id, None)
            self._last_event_ts = datetime.now(UTC)
            # Construir mensaje fuera del lock no es posible sin copia,
            # así que construimos aquí con los datos necesarios
            summary = self._build_entity_summary_locked(entity_id, new_state)

        # Fan-out fuera del lock
        msg = WsStateChanged(
            entity_id=entity_id,
            new_state=summary,
            correlation_id=correlation_id,
        )
        await self._fanout(msg.model_dump())

    async def mark_disconnected(self) -> None:
        """Marca el upstream como desconectado y notifica a los clientes WS."""
        async with self._lock:
            self._connected = False
            self._upstream_state = "DISCONNECTED"
            self._reconnect_count += 1

        await self._fanout_connection_status("disconnected")

    async def mark_reconnecting(self) -> None:
        """Indica a clientes que el upstream está reconectando."""
        async with self._lock:
            self._upstream_state = "RECONNECTING"

        await self._fanout_connection_status("reconnecting")

    async def mark_upstream_state(self, state: str) -> None:
        """Actualiza el estado del upstream sin fan-out (AUTHENTICATING, HYDRATING)."""
        async with self._lock:
            self._upstream_state = state

    # -------------------------------------------------------------------------
    # Lecturas (sin lock — datos inmutables durante lectura en asyncio single-thread)
    # -------------------------------------------------------------------------

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def upstream_state(self) -> str:
        return self._upstream_state

    @property
    def last_event_ts(self) -> datetime | None:
        return self._last_event_ts

    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    @property
    def cache_version(self) -> int:
        return self._cache_version

    def get_state(self, entity_id: str) -> HaState | None:
        return self._states.get(entity_id)

    def get_all_states(self) -> dict[str, HaState]:
        """Retorna copia shallow del dict de estados."""
        return dict(self._states)

    def get_entity_summary(self, entity_id: str) -> EntitySummary | None:
        state = self._states.get(entity_id)
        if state is None:
            return None
        return self._build_entity_summary_locked(entity_id, state)

    def get_all_entity_summaries(self) -> dict[str, EntitySummary]:
        return {
            eid: summary
            for eid, state in self._states.items()
            if (summary := self._build_entity_summary_locked(eid, state)) is not None
        }

    def get_areas_enriched(self) -> list[AreaSummary]:
        """Áreas con lista de entity_ids que pertenecen a cada área."""
        area_entities: dict[str, list[str]] = {aid: [] for aid in self._areas}

        for entity_id, entry in self._entities.items():
            if entry.area_id and entry.area_id in area_entities:
                area_entities[entry.area_id].append(entity_id)

        result = []
        for area in self._areas.values():
            result.append(
                AreaSummary(
                    area_id=area.area_id,
                    name=area.name,
                    icon=area.icon,
                    entity_ids=sorted(area_entities.get(area.area_id, [])),
                )
            )
        return result

    def get_services(self) -> dict[str, Any]:
        return dict(self._services)

    def get_subscriber_count(self) -> int:
        return len(self._subscribers)

    # -------------------------------------------------------------------------
    # Gestión de suscriptores WebSocket
    # -------------------------------------------------------------------------

    def subscribe(self, client_id: str) -> asyncio.Queue[dict[str, Any]]:
        """Registra un suscriptor y devuelve su queue."""
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self._queue_maxsize)
        self._subscribers[client_id] = q
        logger.info("ws.subscriber_added", client_id=client_id, total=len(self._subscribers))
        return q

    def unsubscribe(self, client_id: str) -> None:
        """Elimina el suscriptor al desconectarse."""
        self._subscribers.pop(client_id, None)
        logger.info("ws.subscriber_removed", client_id=client_id, total=len(self._subscribers))

    # -------------------------------------------------------------------------
    # Fan-out (operaciones privadas)
    # -------------------------------------------------------------------------

    async def _fanout(self, message: dict[str, Any]) -> None:
        """
        Envía un mensaje a todas las queues de suscriptores.

        Si una queue está llena, descarta el mensaje más viejo e inserta el nuevo.
        Nunca bloquea — el put_nowait garantiza retorno inmediato.
        """
        # Copiar keys para no iterar sobre dict mutante
        for client_id, q in list(self._subscribers.items()):
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                # Drop oldest message, insert new
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    q.put_nowait(message)
                except asyncio.QueueFull:
                    logger.warning("ws.queue_full_drop", client_id=client_id)

    async def _fanout_connection_status(self, status: str) -> None:
        msg = WsConnectionStatus(upstream=status)
        await self._fanout(msg.model_dump())

    async def fanout_service_complete(
        self, correlation_id: str, entity_id: str | None, success: bool
    ) -> None:
        """Notifica a todos los clientes que un service call fue confirmado."""
        msg = WsServiceComplete(
            correlation_id=correlation_id,
            entity_id=entity_id,
            success=success,
        )
        await self._fanout(msg.model_dump())

    async def fanout_service_timeout(self, correlation_id: str) -> None:
        """Notifica a todos los clientes que un service call expiró sin confirmación."""
        msg = WsServiceTimeout(correlation_id=correlation_id)
        await self._fanout(msg.model_dump())

    # -------------------------------------------------------------------------
    # Helpers privados
    # -------------------------------------------------------------------------

    def _build_entity_summary_locked(
        self, entity_id: str, state: HaState | None
    ) -> EntitySummary | None:
        """Construye EntitySummary combinando state + registry. Sin lock — llamar con datos locales."""
        if state is None:
            return None
        entry = self._entities.get(entity_id)
        return EntitySummary(
            entity_id=entity_id,
            state=state.state,
            attributes=dict(state.attributes.model_extra or {}),
            last_changed=state.last_changed,
            last_updated=state.last_updated,
            friendly_name=state.friendly_name or (entry.name if entry else None),
            area_id=entry.area_id if entry else None,
            device_id=entry.device_id if entry else None,
            domain=state.domain,
        )
