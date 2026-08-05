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
    WsSnapshot,
    WsStateChanged,
)

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# Tope de entidades que la reconciliación post-rehidratación manda una por una.
# Por debajo, el diff es SIEMPRE más barato en bytes que un snapshot: el snapshot
# lleva las 418 entidades MÁS el registro completo de servicios de HA (que no
# cambia entre reconexiones y pesa tanto como los estados). Por encima estamos
# reconstruyendo media casa entidad por entidad, y ahí manda otro costo: son N
# frames sobre el túnel de Cloudflare y N slots de la cola de 1000 del cliente,
# contra UN frame del snapshot. 200 ≈ la mitad de las entidades de esta casa.
_RESYNC_MAX_DIFF = 200


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

            # Qué cambió DE VERDAD entre la caché vieja y la nueva. Se calcula acá
            # adentro porque `old_states` solo existe dentro del lock, y solo si hay
            # alguien escuchando: el upstream se rehidrata ~60 veces por hora y de
            # madrugada no hay ninguna app abierta — en ese caso esto cuesta cero.
            cambiados = (
                self._diff_estados_locked(old_states, new_states) if self._subscribers else []
            )
            resync: list[tuple[str, EntitySummary | None]] | None = None
            if 0 < len(cambiados) <= _RESYNC_MAX_DIFF:
                # Las summaries se arman acá porque necesitan los registries que
                # acabamos de reemplazar (nombre y área de cada entidad).
                resync = [
                    (
                        entity_id,
                        self._build_entity_summary_locked(entity_id, self._states.get(entity_id)),
                    )
                    for entity_id in cambiados
                ]

        logger.info(
            "store.hydrated",
            states=len(states),
            entities=len(entities),
            areas=len(areas),
            cache_version=version,
            resync_entidades=len(cambiados),
        )

        # Fan-out fuera del lock. El estado fresco va ANTES del "connected": las
        # colas por cliente son FIFO, así que cuando la app se pinta en verde los
        # datos que respaldan ese verde ya viajan adelante en su propia cola.
        await self._fanout_resync(cambiados, resync)
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

    async def _fanout_resync(
        self,
        cambiados: list[str],
        resync: list[tuple[str, EntitySummary | None]] | None,
    ) -> None:
        """
        Reconcilia a los clientes ya conectados después de una rehidratación.

        EL BUG (medido 2026-08-04): el Mirror pierde el enlace con HA ~1 vez por
        minuto. Al recuperarse rehidrataba su caché con el estado fresco de la casa
        pero solo emitía `connection_status`, y la app —que procesa el `snapshot`
        ÚNICAMENTE al abrir el socket— seguía pintando el estado viejo como si
        fuera actual. Los contadores del día: `snapshot` 195 contra
        `connection_status` 6687; se estimaban 40-75 eventos por hora que nunca se
        reconciliaban. En la práctica: una luz que alguien apagó por el interruptor
        de pared durante el corte se quedaba encendida en la app hasta que el
        usuario cerraba y reabría la app.

        QUÉ MANDAMOS: solo lo que cambió, con `state_changed`, que es un tipo que
        el frontend ya entiende (y ya coalesce en un solo render por frame). En un
        corte de 20 s con la casa quieta el diff es VACÍO y esto cuesta cero bytes.
        Reenviar el snapshot completo en cada recuperación (~60/h) es justo la
        carga que hay que evitar: lleva las 418 entidades más el registro entero de
        servicios de HA, y el túnel de Cloudflare ya se saturó una vez y se llevó
        puestas las cámaras.

        POR QUÉ NO HAY LÍMITE DE FRECUENCIA: el propio diff ya es el freno, y uno
        guiado por datos en vez de por reloj — si no cambió nada no se manda nada.
        Un freno por tiempo solo podría hacer dos cosas, y las dos son peores:
        dejar al cliente con datos viejos a sabiendas (lo único inaceptable acá) o
        degradar el snapshot a N mensajes sueltos, que son MÁS frames que el
        snapshot que quería evitar.

        `resync=None` significa que el diff pasó `_RESYNC_MAX_DIFF`: ahí va un
        snapshot y punto. Ese camino es autolimitante — la caché ya convergió a la
        realidad, así que la rehidratación siguiente vuelve a dar un diff chico.
        Es además el caso del cliente que abrió la app con el upstream caído: antes
        se quedaba con la casa vacía hasta reconectar, ahora se llena solo cuando
        entra la primera hidratación.
        """
        if not cambiados:
            return

        if resync is None:
            logger.warning("store.resync_por_snapshot", entidades=len(cambiados))
            await self._fanout_snapshot()
            return

        # Mismo shape que `apply_state_changed` — para el cliente es indistinguible
        # de un evento normal de HA, y `new_state=None` sigue significando
        # "la entidad ya no existe".
        for entity_id, summary in resync:
            msg = WsStateChanged(entity_id=entity_id, new_state=summary, correlation_id=None)
            await self._fanout(msg.model_dump())

    async def _fanout_snapshot(self) -> None:
        """
        Empuja un snapshot completo a los suscriptores.

        Idéntico al que `/ws/state` manda al conectar, así que el cliente lo trata
        con el mismo código: reemplaza su estado entero. Es el único camino que
        también refresca áreas y servicios (el diff solo lleva estados).
        """
        msg = WsSnapshot(
            states=self.get_all_entity_summaries(),
            areas=self.get_areas_enriched(),
            services=self.get_services(),
            cache_version=self._cache_version,
        )
        await self._fanout(msg.model_dump(mode="json"))

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

    def _diff_estados_locked(
        self,
        old_states: dict[str, HaState],
        new_states: dict[str, HaState],
    ) -> list[str]:
        """
        entity_ids que cambiaron entre dos cachés (incluye altas y bajas).

        NO compara `last_changed` / `last_updated` A PROPÓSITO: cuando Home
        Assistant se reinicia les pone la hora del arranque a las 418 entidades sin
        que nada haya cambiado de verdad, así que mirar los timestamps convertiría
        CADA rehidratación en un diff de la casa entera — exactamente el reenvío
        masivo que estamos evitando. Es el mismo criterio que ya usa
        `upsert_external` para no spamear en cada poll de Crestron.

        LIMITACIÓN CONOCIDA: solo mira estados. Si alguien renombra una entidad o
        la mueve de área en HA sin que cambie su estado, eso viaja recién en el
        próximo snapshot (reconexión del cliente, o el camino de diff masivo). Es
        un cambio manual y raro; el estado en vivo es lo que la app pinta.
        """
        cambiados: list[str] = []

        for entity_id, new_state in new_states.items():
            old = old_states.get(entity_id)
            if old is None:
                cambiados.append(entity_id)
                continue
            if old.state != new_state.state:
                cambiados.append(entity_id)
                continue
            if (old.attributes.model_extra or {}) != (new_state.attributes.model_extra or {}):
                cambiados.append(entity_id)

        # Bajas: la entidad desapareció de HA. El summary queda en None y el
        # mensaje sale como `new_state: null`, que es lo que el contrato define.
        for entity_id in old_states:
            if entity_id not in new_states:
                cambiados.append(entity_id)

        return cambiados

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
