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

        # Entidades inyectadas por conectores EXTERNOS a HA (p.ej. Crestron vía
        # crestron_connector). Se registran acá para que hydrate() (que reemplaza
        # todo el estado de HA) las PRESERVE en vez de borrarlas en cada
        # rehidratación del upstream. _external_areas guarda las áreas propias de
        # esos conectores para reinyectarlas por la misma razón.
        self._external_entity_ids: set[str] = set()
        self._external_areas: dict[str, HaAreaRegistryEntry] = {}

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
            # Snapshot de lo anterior para reinyectar las entidades externas: sin
            # esto, cada rehidratación de HA (reconexión del upstream) borraría los
            # dispositivos Crestron del store hasta el próximo poll del connector,
            # haciéndolos "parpadear" (desaparecer de la app durante ~poll_interval s).
            old_states = self._states
            old_entities = self._entities

            new_states = {s.entity_id: s for s in states}
            new_entities = {e.entity_id: e for e in entities}

            for eid in self._external_entity_ids:
                # Solo rellenamos huecos: si HA trajera una entidad con el mismo id
                # (no debería, por el prefijo `crestron_`), gana HA.
                if eid not in new_states and eid in old_states:
                    new_states[eid] = old_states[eid]
                if eid not in new_entities and eid in old_entities:
                    new_entities[eid] = old_entities[eid]

            self._states = new_states
            self._entities = new_entities
            self._devices = {d.id: d for d in devices}
            self._areas = {a.area_id: a for a in areas}
            # Reinyectar las áreas de conectores externos (idempotente).
            for area_id, area in self._external_areas.items():
                self._areas.setdefault(area_id, area)
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

    async def upsert_external(
        self,
        states: list[HaState],
        entities: list[HaEntityRegistryEntry],
        area: HaAreaRegistryEntry | None = None,
    ) -> None:
        """
        Merge ADITIVO de entidades externas a HA (p.ej. Crestron).

        A diferencia de hydrate(), NO reemplaza el estado de HA: agrega/actualiza
        las entidades dadas en self._states/_entities/_areas, las registra como
        externas (para que hydrate las preserve) y hace fan-out de un
        WsStateChanged por cada entidad cuyo ESTADO o ATRIBUTOS cambiaron
        (comparación que ignora timestamps para no spamear en cada poll).
        """
        changed: list[tuple[str, EntitySummary | None]] = []
        added_new = False

        async with self._lock:
            # Registry de entidades: siempre sobrescribe (nombre/área pueden variar).
            for entry in entities:
                self._entities[entry.entity_id] = entry
                self._external_entity_ids.add(entry.entity_id)

            # Área del conector (idempotente): no pisa una de HA con el mismo id.
            if area is not None:
                self._areas.setdefault(area.area_id, area)
                self._external_areas[area.area_id] = area

            # Estados: detectar cambios reales para decidir el fan-out.
            for state in states:
                self._external_entity_ids.add(state.entity_id)
                old = self._states.get(state.entity_id)
                self._states[state.entity_id] = state

                if old is None:
                    added_new = True
                    changed.append(
                        (state.entity_id, self._build_entity_summary_locked(state.entity_id, state))
                    )
                    continue

                old_attrs = dict(old.attributes.model_extra or {})
                new_attrs = dict(state.attributes.model_extra or {})
                if old.state != state.state or old_attrs != new_attrs:
                    changed.append(
                        (state.entity_id, self._build_entity_summary_locked(state.entity_id, state))
                    )

            if added_new:
                # El set de entidades creció: bump para que un cliente que se
                # reconecte reciba un snapshot con versión nueva.
                self._cache_version += 1

        # Fan-out fuera del lock (mismo patrón que apply_state_changed).
        for entity_id, summary in changed:
            msg = WsStateChanged(entity_id=entity_id, new_state=summary, correlation_id=None)
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

    def _area_efectiva(self, entry: HaEntityRegistryEntry) -> str | None:
        """
        Área REAL de una entidad: la suya, o la de su device.

        En Home Assistant el área normalmente se asigna al DEVICE; la entidad
        solo lleva `area_id` propio cuando alguien la movió a mano (override).
        El frontend de HA resuelve `entity.area_id ?? device.area_id`, y sin ese
        mismo fallback el Mirror reportaba 0 de 418 entidades con área — cuando
        en la casa de Fortunatta hay 35 que sí la tienen vía device (Sala TV y
        las persianas Somfy). Eso dejaba `/api/areas` devolviendo las 5 áreas
        con `entity_ids: []` y la app cayendo siempre al fallback por nombre.
        """
        if entry.area_id:
            return entry.area_id
        if entry.device_id:
            device = self._devices.get(entry.device_id)
            if device is not None:
                return device.area_id
        return None

    def get_areas_enriched(self) -> list[AreaSummary]:
        """Áreas con lista de entity_ids que pertenecen a cada área."""
        area_entities: dict[str, list[str]] = {aid: [] for aid in self._areas}

        for entity_id, entry in self._entities.items():
            area_id = self._area_efectiva(entry)
            if area_id and area_id in area_entities:
                area_entities[area_id].append(entity_id)

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
            # Mismo fallback device→área que `get_areas_enriched`: si no,
            # /api/entities reporta area_id null en las 418 aunque el device
            # tenga área, y cualquier consumidor que agrupe por área ve la casa
            # vacía. Ver `_area_efectiva`.
            area_id=self._area_efectiva(entry) if entry else None,
            device_id=entry.device_id if entry else None,
            domain=state.domain,
        )
