"""
CrestronConnector — integra dispositivos Crestron Home al mirror.

Responsabilidad (Agente 2):
  1. Polling supervisado del CrestronClient (patrón run_forever + backoff, como
     ha_upstream.HAUpstream). Un ciclo que falla se loguea y reintenta; NUNCA
     tumba el add-on.
  2. Mapea cada CrestronDevice a un HaState + HaEntityRegistryEntry con la
     convención de entity_id `<domain>.crestron_<id>_<slug>` y hace un UPSERT
     ADITIVO en el StateStore (no reemplaza el estado de HA).
  3. Traduce service calls del frontend (light/cover/scene) a llamadas del
     cliente y aplica una confirmación optimista al store tras ejecutar.

Seguridad:
  - El token/AuthKey vive dentro del CrestronClient; el connector nunca lo
    logguea ni lo serializa.
  - Anti-SSRF: el connector no acepta URLs del frontend. La base_url del cliente
    sale del allowlist de settings (config.py), nunca de un request.
"""

from __future__ import annotations

import asyncio
import re
from contextlib import suppress
from datetime import UTC, datetime
from typing import Literal

import structlog

from ha_mirror.correlations import CorrelationTracker
from ha_mirror.crestron_client import CrestronClient, CrestronDevice, CrestronError
from ha_mirror.models import (
    HaAreaRegistryEntry,
    HaAttributes,
    HaEntityRegistryEntry,
    HaState,
)
from ha_mirror.state_store import StateStore

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# Backoff del loop de polling ante fallos del cliente (mismo espíritu que el
# backoff de ha_upstream, pero sin jitter — el polling ya es de baja frecuencia).
_BACKOFF_BASE = 2.0
_BACKOFF_CAP = 60.0

# Slug del name: a-z0-9 y guion bajo. Todo lo demás colapsa a "_".
_SLUG_RE = re.compile(r"[^a-z0-9]+")

# CoverEntityFeature de HA: OPEN(1) | CLOSE(2) | SET_POSITION(4) | STOP(8) = 15.
# Se lo damos a la app para que muestre los controles de persiana completos.
_COVER_SUPPORTED_FEATURES = 1 | 2 | 4 | 8

# Estados textuales que tratamos como binarios (para binary_sensor). El primer
# set mapea a "on"; cualquier otro status conocido cae a "off".
_BINARY_ON = frozenset(
    {"on", "open", "opened", "detected", "motion", "active", "true", "1", "occupied", "home"}
)
_BINARY_OFF = frozenset(
    {"off", "closed", "clear", "no_motion", "inactive", "false", "0", "unoccupied", "away"}
)

# Domain HA -> tipo Crestron, para pasarle un "hint" a get_device() y que
# pruebe el endpoint tipado correcto primero en vez de los 3 a ciegas
# (2026-08-12: get_device ahora prueba /lights, /shades, /scenes en orden
# porque no sabe de antemano cual es -- pero el connector SI lo sabe, por el
# domain del service call).
_DOMAIN_TO_CRESTRON_TYPE: dict[str, Literal["light", "shade", "scene"]] = {
    "light": "light",
    "cover": "shade",
    "scene": "scene",
}


def _slugify(name: str | None) -> str:
    """Slug estable del name: minúsculas, [a-z0-9_], sin guiones de borde."""
    slug = _SLUG_RE.sub("_", (name or "").strip().lower()).strip("_")
    return slug or "device"


class CrestronConnector:
    """
    Puente entre un CrestronClient y el StateStore del mirror.

    Uso: instanciar en el lifespan, llamar start() para arrancar el polling y
    close() en el teardown. Un único task de polling supervisado mantiene el
    store al día; el enrutado de control lo dispara api/service.py vía
    owns_entity() + handle_service_call().
    """

    def __init__(
        self,
        *,
        client: CrestronClient,
        store: StateStore,
        correlations: CorrelationTracker,
        poll_interval: float,
        area_id: str,
    ) -> None:
        self._client = client
        self._store = store
        # Guardado por paridad con el resto del stack; el enrutado de Crestron no
        # usa el CorrelationTracker (no hay state_changed de HA que resolver —
        # la confirmación se hace vía fanout_service_complete en service.py).
        self._correlations = correlations
        self._poll_interval = poll_interval
        self._area_id = area_id

        # Map entity_id -> device_id (Crestron). Se REEMPLAZA entero en cada poll
        # (asignación atómica en asyncio) para que un dispositivo que el vendor
        # dejó de reportar salga del ruteo de control.
        self._entity_by_id: dict[str, int] = {}

        self._task: asyncio.Task[None] | None = None

    # -------------------------------------------------------------------------
    # Ciclo de vida
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Arranca el task de polling supervisado (idempotente)."""
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="crestron_connector")
            logger.info(
                "crestron.connector_started",
                poll_interval=self._poll_interval,
                area_id=self._area_id,
            )

    async def close(self) -> None:
        """Cancela el task de polling y espera su cierre limpio."""
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None
            logger.info("crestron.connector_stopped")

    async def _run(self) -> None:
        """
        Loop de polling supervisado con backoff.

        Solo termina en CancelledError (shutdown). Cualquier otro fallo de un
        ciclo se loguea y se reintenta con backoff exponencial acotado —
        NUNCA propaga hacia el lifespan ni tumba el add-on.
        """
        backoff = _BACKOFF_BASE
        while True:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except CrestronError as exc:
                logger.warning(
                    "crestron.poll_error",
                    code=getattr(exc, "code", None),
                    status=getattr(exc, "status_code", None),
                    next_backoff=round(backoff, 1),
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _BACKOFF_CAP)
                continue
            except Exception:  # noqa: BLE001 — un loop de fondo jamás debe tumbar el add-on
                logger.exception("crestron.poll_unexpected")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _BACKOFF_CAP)
                continue

            backoff = _BACKOFF_BASE
            await asyncio.sleep(self._poll_interval)

    # -------------------------------------------------------------------------
    # Polling
    # -------------------------------------------------------------------------

    async def _poll_once(self) -> None:
        """Un ciclo: login perezoso → get_devices → mapear → upsert aditivo."""
        if not self._client.connected:
            await self._client.login()

        devices = await self._client.get_devices()

        states: list[HaState] = []
        entries: list[HaEntityRegistryEntry] = []
        new_map: dict[str, int] = {}

        for dev in devices:
            mapped = self._map_device(dev)
            if mapped is None:
                continue
            state, entry = mapped
            states.append(state)
            entries.append(entry)
            new_map[entry.entity_id] = dev.id

        # Reemplazo atómico del map (single-thread asyncio): los dispositivos que
        # el vendor dejó de reportar dejan de ser ruteables para control.
        self._entity_by_id = new_map

        await self._store.upsert_external(states, entries, self._area_entry())

        logger.debug("crestron.polled", devices=len(devices), mapped=len(states))

    # -------------------------------------------------------------------------
    # Enrutado de control (lo invoca api/service.py)
    # -------------------------------------------------------------------------

    def owns_entity(self, entity_id: str) -> bool:
        """True si el entity_id corresponde a un dispositivo Crestron ruteable."""
        return entity_id in self._entity_by_id

    async def handle_service_call(
        self,
        domain: str,
        service: str,
        entity_id: str | list[str] | None,
        data: dict | None = None,
    ) -> None:
        """
        Traduce un service call del frontend a la llamada del cliente Crestron.

        Mapea light.turn_on/turn_off/toggle → set_light, cover.* → set_shade,
        scene.turn_on → recall_scene. Tras ejecutar cada acción refresca el
        dispositivo (get_device) y aplica el estado nuevo al store para
        confirmación optimista.

        Lanza (propaga) si alguna acción falla, para que service.py haga
        fanout_service_complete(success=False).
        """
        data = data or {}
        ids = [entity_id] if isinstance(entity_id, str) else list(entity_id or [])

        for eid in ids:
            device_id = self._entity_by_id.get(eid)
            if device_id is None:
                continue
            await self._dispatch_one(domain, service, eid, device_id, data)
            await self._refresh_device(device_id, hint=_DOMAIN_TO_CRESTRON_TYPE.get(domain))

    async def _dispatch_one(
        self, domain: str, service: str, entity_id: str, device_id: int, data: dict
    ) -> None:
        """Ejecuta UNA acción de control contra el cliente Crestron."""
        svc = service.lower().strip()

        if domain == "light":
            if svc == "turn_off":
                await self._client.set_light(device_id, on=False)
            elif svc == "toggle":
                prev = self._store.get_state(entity_id)
                currently_on = prev is not None and prev.state == "on"
                await self._client.set_light(device_id, on=not currently_on)
            elif svc in ("turn_on", "brightness", "set_brightness"):
                await self._client.set_light(
                    device_id, on=True, level=_extract_light_level(data)
                )
            else:
                raise ValueError(f"servicio light no soportado por Crestron: {service!r}")

        elif domain == "cover":
            if svc in ("open_cover", "open"):
                await self._client.set_shade(device_id, action="open")
            elif svc in ("close_cover", "close"):
                await self._client.set_shade(device_id, action="close")
            elif svc in ("stop_cover", "stop"):
                await self._client.set_shade(device_id, action="stop")
            elif svc in ("set_cover_position", "set_position"):
                await self._client.set_shade(
                    device_id, position=_extract_position(data)
                )
            else:
                raise ValueError(f"servicio cover no soportado por Crestron: {service!r}")

        elif domain == "scene":
            if svc in ("turn_on", "recall", "apply"):
                await self._client.recall_scene(device_id)
            else:
                raise ValueError(f"servicio scene no soportado por Crestron: {service!r}")

        else:
            raise ValueError(f"dominio no manejado por Crestron: {domain!r}")

    async def _refresh_device(
        self, device_id: int, *, hint: Literal["light", "shade", "scene"] | None
    ) -> None:
        """
        Confirmación optimista: relee el dispositivo y actualiza el store.

        `hint` viene del domain del service call que disparó esta acción —
        se lo pasamos a `get_device()` para que pruebe el endpoint tipado
        correcto primero en vez de los 3 a ciegas (ver `_DOMAIN_TO_CRESTRON_TYPE`).

        Si get_device falla NO propagamos: la acción ya se ejecutó y el próximo
        ciclo de polling reconciliará el estado. Solo lo logueamos.
        """
        try:
            dev = await self._client.get_device(device_id, hint=hint)
        except CrestronError as exc:
            logger.info(
                "crestron.refresh_skip",
                device_id=device_id,
                code=getattr(exc, "code", None),
            )
            return

        mapped = self._map_device(dev, scene_activated=(hint == "scene"))
        if mapped is None:
            return
        state, entry = mapped
        await self._store.upsert_external([state], [entry], None)

    # -------------------------------------------------------------------------
    # Mapeo device -> entidad
    # -------------------------------------------------------------------------

    def _map_device(
        self, dev: CrestronDevice, *, scene_activated: bool = False
    ) -> tuple[HaState, HaEntityRegistryEntry] | None:
        """
        Convierte un CrestronDevice en (HaState, HaEntityRegistryEntry).

        Preserva timestamps: si el estado lógico no cambió respecto de lo que ya
        hay en el store, reusa el HaState previo (así last_changed refleja el
        cambio real y no el instante del último poll).
        """
        domain = self._resolve_domain(dev)
        if domain is None:
            return None

        entity_id = f"{domain}.crestron_{dev.id}_{_slugify(dev.name)}"
        prev = self._store.get_state(entity_id)
        now = datetime.now(UTC)

        state_str, attrs = self._state_and_attrs(
            dev, domain, prev_state=prev.state if prev else None,
            scene_activated=scene_activated, now=now,
        )

        if (
            prev is not None
            and prev.state == state_str
            and dict(prev.attributes.model_extra or {}) == attrs
        ):
            ha_state = prev  # sin cambios: preserva last_changed/last_updated
        else:
            ha_state = HaState(
                entity_id=entity_id,
                state=state_str,
                attributes=HaAttributes.model_validate(attrs),
                last_changed=now,
                last_updated=now,
            )

        entry = HaEntityRegistryEntry(
            entity_id=entity_id,
            name=dev.name,
            platform="crestron",
            area_id=self._area_id,
        )
        return ha_state, entry

    def _resolve_domain(self, dev: CrestronDevice) -> str | None:
        """Domain HA para un CrestronDevice según su device_type."""
        dt = dev.device_type
        if dt == "light":
            return "light"
        if dt == "shade":
            return "cover"
        if dt == "scene":
            return "scene"
        if dt == "sensor":
            return "binary_sensor" if _is_binary_status(dev.status) else "sensor"
        # "unknown" u otro: lo exponemos como sensor para que al menos aparezca.
        return "sensor"

    def _state_and_attrs(
        self,
        dev: CrestronDevice,
        domain: str,
        *,
        prev_state: str | None,
        scene_activated: bool,
        now: datetime,
    ) -> tuple[str, dict]:
        """Estado textual + atributos para la entidad, por dominio."""
        # Fuera de alcance / dispositivo caído: HA usa "unavailable".
        if not dev.reachable:
            return "unavailable", {"friendly_name": dev.name}

        if domain == "light":
            level = dev.level or 0
            brightness = round(level / 100 * 255)
            return (
                "on" if level > 0 else "off",
                {
                    "friendly_name": dev.name,
                    "brightness": brightness,
                    "color_mode": "brightness",
                    "supported_color_modes": ["brightness"],
                },
            )

        if domain == "cover":
            position = dev.level if dev.level is not None else 0
            return (
                "open" if position > 0 else "closed",
                {
                    "friendly_name": dev.name,
                    "current_position": position,
                    "device_class": "shade",
                    "supported_features": _COVER_SUPPORTED_FEATURES,
                },
            )

        if domain == "scene":
            # El estado de una scene es el timestamp de la última activación (como
            # HA). El polling no lo trae, así que: si ya conocíamos la scene,
            # preservamos su estado (evita fan-out en cada poll); si es nueva, o
            # si se acaba de activar por control, usamos "ahora".
            state = now.isoformat() if (scene_activated or prev_state is None) else prev_state
            return state, {"friendly_name": dev.name}

        if domain == "binary_sensor":
            return _binary_state(dev.status), {"friendly_name": dev.name}

        # sensor
        if dev.status:
            state = dev.status
        elif dev.level is not None:
            state = str(dev.level)
        else:
            state = "unknown"
        return state, {"friendly_name": dev.name}

    def _area_entry(self) -> HaAreaRegistryEntry:
        """Área Crestron para que la app agrupe estos dispositivos."""
        return HaAreaRegistryEntry(
            area_id=self._area_id,
            name="Crestron",
            icon="mdi:home-automation",
        )


# ---------------------------------------------------------------------------
# Helpers de extracción de service_data (todo validado/acotado antes de usar)
# ---------------------------------------------------------------------------


def _extract_light_level(data: dict) -> int | None:
    """Nivel 0-100 desde service_data (brightness 0-255 o brightness_pct 0-100)."""
    if "brightness_pct" in data:
        return _clamp_pct(data["brightness_pct"])
    if "brightness" in data:
        try:
            return _clamp_pct(round(float(data["brightness"]) / 255 * 100))
        except (TypeError, ValueError):
            return None
    return None


def _extract_position(data: dict) -> int:
    """Posición 0-100 desde service_data['position']; default 0 si falta/invalida."""
    return _clamp_pct(data.get("position", 0))


def _clamp_pct(value: object) -> int:
    """Entero acotado a 0-100."""
    try:
        return max(0, min(100, int(float(value))))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _is_binary_status(status: str | None) -> bool:
    """True si el status del sensor luce binario (para elegir binary_sensor)."""
    if not status:
        return False
    s = status.strip().lower()
    return s in _BINARY_ON or s in _BINARY_OFF


def _binary_state(status: str | None) -> str:
    """Normaliza el status de un binary_sensor a 'on'/'off'."""
    if not status:
        return "off"
    return "on" if status.strip().lower() in _BINARY_ON else "off"
