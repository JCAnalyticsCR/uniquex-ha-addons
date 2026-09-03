"""
Módulo de onboarding del Mirror.

Permite que el cliente organice sus dispositivos sin abrir Home Assistant:
- Overrides por entidad (habitación, nombre visible, icono, orden, ocultar).
- Habitaciones propias (custom_rooms), independientes de las áreas de HA.
- Detección de dispositivos nuevos con baseline automático.
- Rescan de integraciones (reload de config entries vía WS admin de HA).

Diseñado para funcionar en CUALQUIER casa/instancia: sin referencias
a clientes, entidades ni integraciones específicas de ningún proyecto.
"""

from __future__ import annotations

import asyncio
import re
import time
import unicodedata
from typing import Any

import structlog

from ha_mirror.db import Database
from ha_mirror.errors import HaProtocolError, UpstreamNotReadyError
from ha_mirror.onboarding_familias import enmascarar_titulo, es_recargable, familia_de

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Excepciones de dominio del módulo de onboarding
# ---------------------------------------------------------------------------


class OnboardingError(Exception):
    """Base para errores de dominio del módulo de onboarding."""


class OnboardingAdminRequiredError(OnboardingError):
    """Token HA sin permisos admin — 501."""


class OnboardingRescanInProgressError(OnboardingError):
    """Ya hay un rescan en curso — 409."""


class OnboardingEntryNotFoundError(OnboardingError):
    """entry_id desconocido — 404."""


class OnboardingForbiddenDomainError(OnboardingError):
    """Dominio en deny-list o no elegible — 403."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _slug(name: str) -> str:
    """
    Convierte un nombre a slug para room_id.

    Resultado: minúsculas, sin tildes, solo [a-z0-9-], máx 48 chars.
    Ejemplo: "Sala de Estar" → "sala-de-estar"
    """
    # Normalizar Unicode y quitar marcas de acento
    normalizada = unicodedata.normalize("NFD", name)
    ascii_str = normalizada.encode("ascii", "ignore").decode("ascii")
    minusculas = ascii_str.lower()
    # Reemplazar cualquier secuencia de no-alfanumérico por guión
    slug = re.sub(r"[^a-z0-9]+", "-", minusculas)
    # Quitar guiones al inicio y al final
    slug = slug.strip("-")
    return slug[:48]


# ---------------------------------------------------------------------------
# Servicio de onboarding
# ---------------------------------------------------------------------------


class OnboardingService:
    """
    Lógica de dominio del módulo de onboarding.

    Instanciado una vez en el lifespan (app.state.onboarding).
    Stateless salvo el lock de rescan y la caché de capabilities.
    """

    # Tiempos de estabilización del rescan — como atributos de clase para
    # que los tests puedan sobreescribirlos sin monkeypatch global.
    POLL_INTERVAL: float = 0.5       # segundos entre polls de estabilización
    STABLE_SECS: float = 2.0         # segundos sin cambio para considerar estable
    MAX_WAIT_SECS: float = 25.0      # tope máximo de espera por entry
    CAPABILITIES_CACHE_SECS: float = 60.0  # tiempo de vida de la caché de capabilities

    def __init__(
        self,
        store: Any,
        upstream: Any,
        db: Database,
        mirror_version: str,
    ) -> None:
        self._store = store
        self._upstream = upstream
        self._db = db
        self._mirror_version = mirror_version
        # Lock asyncio para single-flight en rescan
        self._rescan_lock = asyncio.Lock()
        # Caché de capabilities: tenant_id → (monotonic_timestamp, result_dict)
        self._caps_cache: dict[int, tuple[float, dict[str, Any]]] = {}

    # -------------------------------------------------------------------------
    # Capabilities
    # -------------------------------------------------------------------------

    async def get_capabilities(self, tenant_id: int = 1) -> dict[str, Any]:
        """
        Sonda al HA para determinar capacidades admin del token actual.

        El resultado se cachea 60 s en memoria por tenant para no martillar
        a HA en cada render del frontend.

        SIEMPRE retorna un dict (nunca lanza): si HA está caído o el token
        no tiene permisos admin, retorna admin=False con 200 hacia el cliente.
        """
        cached = self._caps_cache.get(tenant_id)
        if cached is not None:
            ts, data = cached
            if time.monotonic() - ts < self.CAPABILITIES_CACHE_SECS:
                return data

        result = await self._probe_capabilities()
        self._caps_cache[tenant_id] = (time.monotonic(), result)
        return result

    def invalidate_capabilities_cache(self, tenant_id: int = 1) -> None:
        """Invalida la caché de capabilities (llamado después de un rescan exitoso)."""
        self._caps_cache.pop(tenant_id, None)

    async def _probe_capabilities(self) -> dict[str, Any]:
        """
        Ejecuta la sonda WS a HA (config_entries/get).

        Degrada silenciosamente ante: upstream caído, token sin admin,
        cualquier error de protocolo. Nunca loguea el resultado completo
        (puede contener emails en títulos de config entries).
        """
        try:
            resp = await self._upstream.send_command(
                {"type": "config_entries/get"}, timeout=10.0
            )
            entries: list[dict[str, Any]] = resp.get("result") or []
            systems = self._build_systems(entries)
            logger.info(
                "onboarding.capabilities_ok",
                entries=len(entries),
                rescan_eligible=sum(1 for s in systems if s["rescan_supported"]),
            )
            return {
                "admin": True,
                "mirror_version": self._mirror_version,
                "features": {
                    "overrides": True,
                    "pending": True,
                    "rescan": any(s["rescan_supported"] for s in systems),
                },
                "systems": systems,
            }
        except UpstreamNotReadyError:
            logger.debug("onboarding.capabilities_upstream_down")
        except HaProtocolError as exc:
            # "unauthorized" → token sin permisos admin (normal en instalaciones
            # sin token del Supervisor). Log sin detalles del error completo.
            if "unauthorized" in str(exc).lower():
                logger.info("onboarding.capabilities_unauthorized")
            else:
                logger.warning("onboarding.capabilities_protocol_error", exc_type=type(exc).__name__)
        except Exception:
            logger.warning("onboarding.capabilities_unexpected_error")
        return self._no_admin_response()

    def _no_admin_response(self) -> dict[str, Any]:
        """Respuesta cuando admin=False (upstream caído o token sin permisos)."""
        return {
            "admin": False,
            "mirror_version": self._mirror_version,
            "features": {"overrides": True, "pending": True, "rescan": False},
            "systems": [],
        }

    def _build_systems(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Construye la lista de sistemas a partir de las config entries de HA."""
        systems = []
        for entry in entries:
            domain = entry.get("domain", "")
            title = entry.get("title", domain)
            state = entry.get("state", "")
            entry_id = entry.get("entry_id", "")
            supports_unload = entry.get("supports_unload")  # None si el campo no viene

            # El título se enmascara ANTES de salir del Mirror: HA titula las
            # integraciones de nube con la cuenta que las configuró, que suele
            # ser la del INSTALADOR (ver enmascarar_titulo). Nunca se devuelve
            # el título crudo, ni siquiera para las familias conocidas.
            title = enmascarar_titulo(title)
            family, family_label = familia_de(domain, title)
            rescan_supported = (
                state == "loaded"
                and es_recargable(domain)
                and (supports_unload is None or supports_unload is True)
            )

            # Solo logueamos entry_id/domain/state — el title puede traer emails
            logger.debug(
                "onboarding.entry_parsed",
                entry_id=entry_id,
                domain=domain,
                state=state,
                rescan_supported=rescan_supported,
            )
            systems.append(
                {
                    "entry_id": entry_id,
                    "domain": domain,
                    "title": title,
                    "state": state,
                    "family": family,
                    "family_label": family_label,
                    "rescan_supported": rescan_supported,
                }
            )
        return systems

    # -------------------------------------------------------------------------
    # Overrides
    # -------------------------------------------------------------------------

    async def get_overrides(self, tenant_id: int = 1) -> dict[str, Any]:
        """Devuelve todos los overrides y habitaciones custom del tenant."""
        overrides_list = await self._db.list_overrides(tenant_id)
        rooms_list = await self._db.list_rooms(tenant_id)
        return {
            "overrides": {o["entity_id"]: o for o in overrides_list},
            "rooms": rooms_list,
        }

    async def upsert_override(
        self,
        entity_id: str,
        provided_fields: dict[str, Any],
        tenant_id: int = 1,
    ) -> dict[str, Any]:
        """
        Merge parcial de un override.

        `provided_fields` contiene SOLO los campos presentes en el body
        (usando model_fields_set del router). Campos omitidos se conservan.
        None explícito en provided_fields limpia el campo.

        Si al final todos los campos quedan null/False → borra la fila.
        """
        # Verificar que la entidad existe en el store
        if self._store.get_state(entity_id) is None:
            raise KeyError(f"Entidad {entity_id!r} no encontrada en el store")

        # Leer estado actual
        actual = await self._db.get_override(entity_id, tenant_id)
        base: dict[str, Any]
        if actual is None:
            base = {
                "room_id": None,
                "display_name": None,
                "icon": None,
                "hidden": False,
                "sort_order": None,
            }
        else:
            base = {
                "room_id": actual["room_id"],
                "display_name": actual["display_name"],
                "icon": actual["icon"],
                "hidden": actual["hidden"],
                "sort_order": actual["sort_order"],
            }

        # Merge: provided_fields gana sobre base
        merged = {**base, **provided_fields}

        # Si queda "vacío" → borrar la fila
        if (
            merged["room_id"] is None
            and merged["display_name"] is None
            and merged["icon"] is None
            and not merged["hidden"]
            and merged["sort_order"] is None
        ):
            await self._db.delete_override(entity_id, tenant_id)
            return {"entity_id": entity_id, "cleared": True}

        # Guardar y devolver
        return await self._db.save_override(
            entity_id,
            room_id=merged["room_id"],
            display_name=merged["display_name"],
            icon=merged["icon"],
            hidden=bool(merged["hidden"]),
            sort_order=merged["sort_order"],
            tenant_id=tenant_id,
        )

    async def delete_override(self, entity_id: str, tenant_id: int = 1) -> None:
        """Borra el override de una entidad (idempotente)."""
        await self._db.delete_override(entity_id, tenant_id)

    async def batch_overrides(
        self,
        items: list[dict[str, Any]],
        tenant_id: int = 1,
    ) -> dict[str, Any]:
        """
        Aplica overrides en lote.

        Cada item tiene "entity_id" + campos opcionales (mismo contrato que PUT).
        Items cuya entidad no existe en el store van a "skipped" sin error.
        """
        overrides_result: dict[str, Any] = {}
        skipped: list[str] = []

        for item in items:
            entity_id = item.get("entity_id", "")
            fields = {k: v for k, v in item.items() if k != "entity_id"}
            try:
                result = await self.upsert_override(entity_id, fields, tenant_id)
                overrides_result[entity_id] = result
            except KeyError:
                skipped.append(entity_id)

        return {"overrides": overrides_result, "skipped": skipped}

    # -------------------------------------------------------------------------
    # Custom rooms
    # -------------------------------------------------------------------------

    async def get_rooms(self, tenant_id: int = 1) -> dict[str, Any]:
        """Lista de habitaciones custom del tenant."""
        rooms = await self._db.list_rooms(tenant_id)
        return {"rooms": rooms}

    async def create_room(
        self,
        name: str,
        icon: str | None,
        tenant_id: int = 1,
    ) -> dict[str, Any]:
        """
        Crea una habitación custom.

        room_id = "custom:" + slug(name).
        409 si el room_id resultante ya existe.
        422 si el slug queda vacío (nombre con solo caracteres no ASCII).
        """
        import aiosqlite

        slugged = _slug(name)
        if not slugged:
            raise ValueError(f"El nombre {name!r} produce un slug vacío")

        room_id = f"custom:{slugged}"
        max_order = await self._db.get_rooms_max_sort_order(tenant_id)
        sort_order = max_order + 1

        try:
            return await self._db.create_room(
                room_id=room_id,
                name=name,
                icon=icon,
                sort_order=sort_order,
                tenant_id=tenant_id,
            )
        except aiosqlite.IntegrityError:
            raise ValueError(f"Ya existe una habitación con room_id {room_id!r}") from None

    async def update_room(
        self,
        room_id: str,
        provided_fields: dict[str, Any],
        tenant_id: int = 1,
    ) -> dict[str, Any]:
        """
        Actualiza una habitación custom.

        404 si no existe. Solo acepta room_id con prefijo "custom:".
        """
        resultado = await self._db.update_room(
            room_id,
            name=provided_fields.get("name"),
            icon=provided_fields.get("icon"),
            sort_order=provided_fields.get("sort_order"),
            tenant_id=tenant_id,
        )
        if resultado is None:
            raise KeyError(f"Habitación {room_id!r} no encontrada")
        return resultado

    async def delete_room(self, room_id: str, tenant_id: int = 1) -> None:
        """Borra una habitación custom y limpia overrides que la referencian."""
        await self._db.delete_room(room_id, tenant_id)

    # -------------------------------------------------------------------------
    # Pending (detección de dispositivos nuevos)
    # -------------------------------------------------------------------------

    async def get_pending(self, tenant_id: int = 1) -> dict[str, Any]:
        """
        Devuelve entidades no revisadas por el cliente.

        Primera visita con tabla vacía → baseline: siembra todas las entidades
        actuales como conocidas (baseline_created=True, new_entities=[]).
        Visitas posteriores → inserta las entidades nuevas del store como
        pendientes y devuelve las no-acknowledged.
        """
        total_conocidas = await self._db.count_known_entities(tenant_id)

        # Entidades actuales en el store
        actuales = set(self._store.get_all_states().keys())

        if total_conocidas == 0:
            # Baseline: primera vez → sembrar todo como acknowledged
            await self._db.seed_known_entities(
                list(actuales), acknowledged=True, tenant_id=tenant_id
            )
            return {"new_entities": [], "baseline_created": True}

        # Insertar las entidades del store que no están registradas aún
        if actuales:
            await self._db.insert_unknown_entities(list(actuales), tenant_id=tenant_id)

        # Devolver las no-acknowledged que todavía existen en el store
        pendientes = await self._db.list_unacknowledged(tenant_id)
        visibles = [p for p in pendientes if p["entity_id"] in actuales]

        return {"new_entities": visibles, "baseline_created": False}

    async def ack_pending(
        self, entity_ids: list[str], tenant_id: int = 1
    ) -> dict[str, Any]:
        """Marca entidades como revisadas. Devuelve cuántas fueron marcadas."""
        n = await self._db.acknowledge_entities(entity_ids, tenant_id)
        return {"acknowledged": n}

    # -------------------------------------------------------------------------
    # Rescan
    # -------------------------------------------------------------------------

    async def rescan(
        self, entry_id: str | None = None, tenant_id: int = 1
    ) -> dict[str, Any]:
        """
        Recarga una o todas las integraciones elegibles.

        Validaciones antes de adquirir el lock (orden importa):
        - 502 si el upstream no está conectado
        - 501 si el token no tiene permisos admin
        - 409 si ya hay un rescan en curso

        Dentro del lock:
        - 404 si entry_id desconocido
        - 403 si el dominio está en la deny-list
        """
        # Verificar conectividad primero
        if not self._store.connected:
            raise UpstreamNotReadyError("Upstream no conectado")

        # Verificar permisos admin (usa caché)
        caps = await self.get_capabilities(tenant_id)
        if not caps["admin"]:
            raise OnboardingAdminRequiredError("Se requieren permisos admin en HA")

        # Single-flight: rechazar si ya hay uno en curso
        if self._rescan_lock.locked():
            raise OnboardingRescanInProgressError("Rescan ya en curso")

        async with self._rescan_lock:
            return await self._do_rescan(entry_id, caps, tenant_id)

    async def _do_rescan(
        self,
        entry_id: str | None,
        caps: dict[str, Any],
        tenant_id: int,
    ) -> dict[str, Any]:
        """Ejecuta el rescan efectivo (ya dentro del lock)."""
        t_start = time.monotonic()
        systems = caps["systems"]

        # Determinar qué entries recargar
        if entry_id is not None:
            target = next((s for s in systems if s["entry_id"] == entry_id), None)
            if target is None:
                raise OnboardingEntryNotFoundError(f"Entry {entry_id!r} desconocido")
            if not es_recargable(target["domain"]):
                raise OnboardingForbiddenDomainError(
                    f"Dominio {target['domain']!r} en deny-list"
                )
            to_reload = [target]
        else:
            to_reload = [s for s in systems if s["rescan_supported"]]

        # Snapshot antes del reload
        antes: set[str] = set(self._store.get_all_states().keys())

        reloaded = []
        for system in to_reload:
            eid = system["entry_id"]
            ok, error = await self._reload_entry(eid)
            reloaded.append(
                {
                    "entry_id": eid,
                    "domain": system["domain"],
                    "title": system["title"],
                    "ok": ok,
                    "error": error,
                }
            )
            # Esperar estabilización por entry (incluso si el reload falló:
            # HA puede haberlo procesado parcialmente)
            await self._wait_stable()

        # Snapshot después de todos los reloads
        despues: set[str] = set(self._store.get_all_states().keys())

        new_entities = list(despues - antes)
        removed_entities = list(antes - despues)

        # Insertar entidades nuevas como pendientes de revisión
        if new_entities:
            await self._db.insert_unknown_entities(new_entities, tenant_id=tenant_id)

        # Invalidar caché de capabilities (el estado de las integraciones cambió)
        self.invalidate_capabilities_cache(tenant_id)

        duration_ms = int((time.monotonic() - t_start) * 1000)

        logger.info(
            "onboarding.rescan_done",
            reloaded=len(reloaded),
            new=len(new_entities),
            removed=len(removed_entities),
            duration_ms=duration_ms,
        )

        return {
            "reloaded": reloaded,
            "new_entities": new_entities,
            "removed_entities": removed_entities,
            "duration_ms": duration_ms,
        }

    async def _reload_entry(self, entry_id: str) -> tuple[bool, str | None]:
        """
        Dispara el reload de un config entry vía WS.

        Un fallo NO aborta el loop de entries (un entry con error devuelve
        ok=False + error corto, sin stack ni tokens).
        """
        try:
            await self._upstream.send_command(
                {
                    "type": "call_service",
                    "domain": "homeassistant",
                    "service": "reload_config_entry",
                    "service_data": {"entry_id": entry_id},
                },
                timeout=30.0,
            )
            logger.info("onboarding.entry_reloaded", entry_id=entry_id)
            return True, None
        except Exception as exc:
            # Error corto, sin stack ni contenido del mensaje que pueda traer tokens
            error_msg = f"{type(exc).__name__}: {str(exc)[:200]}"
            logger.warning("onboarding.entry_reload_failed", entry_id=entry_id, error=error_msg)
            return False, error_msg

    async def _wait_stable(self) -> None:
        """
        Espera a que el conjunto de entity_ids del store no cambie por STABLE_SECS.

        Pollea cada POLL_INTERVAL segundos. Máximo MAX_WAIT_SECS por entry.
        """
        prev = frozenset(self._store.get_all_states().keys())
        stable_since = time.monotonic()
        deadline = stable_since + self.MAX_WAIT_SECS

        while True:
            await asyncio.sleep(self.POLL_INTERVAL)
            now = time.monotonic()

            if now >= deadline:
                logger.debug("onboarding.rescan_stabilization_timeout")
                break

            current = frozenset(self._store.get_all_states().keys())
            if current != prev:
                prev = current
                stable_since = now
            elif now - stable_since >= self.STABLE_SECS:
                logger.debug(
                    "onboarding.rescan_stabilized",
                    stable_secs=round(now - stable_since, 2),
                )
                break
