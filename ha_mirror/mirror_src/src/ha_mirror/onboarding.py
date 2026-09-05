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
from collections.abc import Callable
from typing import Any

import structlog

from ha_mirror.db import Database
from ha_mirror.errors import HaProtocolError, UpstreamNotReadyError
from ha_mirror.onboarding_familias import (
    FAMILIAS,
    enmascarar_titulo,
    es_recargable,
    familia_de,
    nombre_de_marca,
    puede_darse_de_alta,
)

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


class OnboardingFlowNoEncontradoError(OnboardingError):
    """El formulario de alta no existe o ya terminó — 404."""


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
        ha_http_url: str | None = None,
        ha_token_provider: Callable[[], str] | None = None,
    ) -> None:
        self._store = store
        self._upstream = upstream
        self._db = db
        self._mirror_version = mirror_version
        # Solo para el asistente de alta, que es REST. Opcionales para no romper
        # a quien ya construye el servicio con cuatro argumentos: sin ellos el
        # alta se apaga sola y el resto del modulo sigue igual.
        #
        # 🔪 El token se pide por FUNCION, no se guarda. `main.py` borra su copia
        # local apenas termina de usarla, a proposito, para achicar la ventana en
        # que anda dando vueltas por memoria. Guardarlo aca lo tendria vivo todo
        # el tiempo que viva el add-on, y eso seria deshacer esa decision de
        # costado. Con un proveedor, existe solo durante la llamada.
        self._ha_http_url = ha_http_url
        self._ha_token_provider = ha_token_provider
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
                    # Si la sonda admin funciono, `config_entries/flow/progress`
                    # tambien va a funcionar: los dos son comandos admin del
                    # mismo WebSocket. Se declara aca para que la app pueda
                    # esconder el aviso entero en las casas donde no aplica, sin
                    # tener que pedir la lista para descubrir que no hay lista.
                    "encontrados": True,
                    # El alta necesita ADEMAS la via REST, que depende de que el
                    # add-on tenga la URL y el token. Sin eso el asistente no se
                    # ofrece, aunque el WebSocket admin funcione.
                    "alta": self._flow_client() is not None,
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
            "features": {
                "overrides": True,
                "pending": True,
                "rescan": False,
                "encontrados": False,
                "alta": False,
            },
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
    # Encontrados (lo que HA ya descubrió y nadie confirmó)
    # -------------------------------------------------------------------------

    async def encontrados(self, tenant_id: int = 1) -> dict[str, Any]:
        """
        Aparatos que Home Assistant ya detectó y que están esperando a alguien.

        POR QUÉ ESTO NO ES LO MISMO QUE `pending`
        -----------------------------------------
        `pending` son entidades que YA existen en la casa y solo les falta
        nombre y habitación. Esto son cosas que **todavía no existen en la app**:
        HA las vio en la red y dejó un formulario a medio llenar esperando que
        alguien lo conteste. Mientras nadie lo conteste, el aparato no existe.

        Y ahí está el problema que esto viene a resolver: **ese formulario solo
        se ve entrando a Home Assistant**. En la casa de referencia había cuatro
        esperando, y uno de ellos era un `reauth` de Overkiz — las persianas
        Somfy llevaban MESES sin funcionar y HA tenía la pregunta hecha desde el
        primer día. Nadie la vio porque nadie abre HA. Ese es exactamente el
        agujero que la app tapa.

        LOS DOS TONOS
        -------------
        La misma llamada devuelve dos cosas que no se parecen en nada:

          · `reauth` / `reconfigure` → algo QUE FUNCIONABA se rompió. Es una
            falla y hay que decirlo así.
          · `zeroconf` / `dhcp` / `ssdp` / `usb` / `bluetooth` → apareció algo
            nuevo en la red. Es una oferta, sin urgencia.

        Se devuelve `es_falla` ya resuelto para que la app no tenga que conocer
        los nombres internos de HA.

        Degrada igual que `capabilities`: si el token no es admin o la casa está
        caída, devuelve la lista vacía y `disponible: False`. Nunca revienta —
        esto se pinta en una pantalla que el cliente abre todos los días.
        """
        try:
            resp = await self._upstream.send_command(
                {"type": "config_entries/flow/progress"}, timeout=10.0
            )
        except UpstreamNotReadyError:
            logger.debug("onboarding.encontrados_upstream_down")
            return {"disponible": False, "encontrados": []}
        except HaProtocolError as exc:
            if "unauthorized" in str(exc).lower():
                logger.info("onboarding.encontrados_unauthorized")
            else:
                logger.warning(
                    "onboarding.encontrados_protocol_error",
                    exc_type=type(exc).__name__,
                )
            return {"disponible": False, "encontrados": []}
        except Exception:
            logger.warning("onboarding.encontrados_unexpected_error")
            return {"disponible": False, "encontrados": []}

        flows: list[dict[str, Any]] = resp.get("result") or []
        salida = [
            item
            for item in (self._parsear_flow(f) for f in flows)
            if item is not None
        ]

        logger.info(
            "onboarding.encontrados_ok",
            total=len(flows),
            visibles=len(salida),
            fallas=sum(1 for s in salida if s["es_falla"]),
        )
        return {"disponible": True, "encontrados": salida}

    # Orígenes que significan "esto ANTES funcionaba y ahora no".
    _ORIGENES_DE_FALLA = frozenset({"reauth", "reconfigure"})

    def _parsear_flow(self, flow: dict[str, Any]) -> dict[str, Any] | None:
        """
        Un flow crudo de HA → lo que la app necesita. None si no se debe mostrar.

        Se filtra por la MISMA deny-list del rescan. Un flow de `hassio` o de
        `update` no es un aparato de la casa: es administración del sistema, y
        eso sigue siendo terreno nuestro y no del cliente.
        """
        handler = flow.get("handler") or ""
        if not handler or not es_recargable(handler):
            return None

        flow_id = flow.get("flow_id")
        if not flow_id:
            return None

        contexto = flow.get("context") or {}
        origen = str(contexto.get("source") or "desconocido")
        family, family_label = familia_de(handler, handler)

        return {
            "flow_id": flow_id,
            "handler": handler,
            "family": family,
            "family_label": family_label,
            "origen": origen,
            "es_falla": origen in self._ORIGENES_DE_FALLA,
            "paso": flow.get("step_id"),
            "nombre": self._nombre_del_flow(contexto, family_label),
        }

    @staticmethod
    def _nombre_del_flow(contexto: dict[str, Any], respaldo: str) -> str:
        """
        Un nombre legible para el aparato encontrado.

        HA deja pistas en `title_placeholders`, pero son distintas en cada
        integración: `name`, `device`, `host`, `gateway_id`… Se prueban en orden
        de cuán humano es cada una.

        🔪 Va enmascarado. `title_placeholders` viene de HA y **puede traer datos
        de un tercero** — el correo del instalador ya se filtró una vez por esta
        misma vía. La regla que quedó: antes de pintar algo que vino de HA,
        preguntarse de quién es.
        """
        ph = contexto.get("title_placeholders") or {}
        if isinstance(ph, dict):
            for clave in ("name", "device", "title", "model", "host", "gateway_id"):
                valor = ph.get(clave)
                if isinstance(valor, str) and valor.strip():
                    return enmascarar_titulo(valor.strip())
        return respaldo


    # -------------------------------------------------------------------------
    # Alta de dispositivos (el asistente)
    # -------------------------------------------------------------------------

    async def marcas_de_alta(self) -> dict[str, Any]:
        """
        Qué se puede dar de alta desde la app, agrupado por familia.

        Es el cruce de tres cosas:
          1. lo que HA sabe dar de alta en ESTA casa (728 dominios medidos en la
             casa de referencia),
          2. la lista negra —lo que NO es un aparato de la casa: sistema del
             gateway, descubrimiento, helpers de HA—,
          3. y el nombre en español de las familias que conocemos; el resto sale
             con su nombre de dominio presentable.

        El cruce con (1) importa: ofrecer una marca que esa casa no soporta es
        prometer algo que va a fallar recién cuando la persona ya eligió.

        Nunca lanza: sin admin o con la casa caída devuelve `disponible:false` y
        la app esconde el botón.
        """
        cliente = self._flow_client()
        if cliente is None:
            return {"disponible": False, "familias": []}

        try:
            soportados = set(await cliente.marcas_disponibles())
        except (UpstreamNotReadyError, HaProtocolError):
            logger.info("onboarding.alta_marcas_no_disponible")
            return {"disponible": False, "familias": []}
        except Exception:
            logger.warning("onboarding.alta_marcas_error_inesperado")
            return {"disponible": False, "familias": []}

        por_familia: dict[str, dict[str, Any]] = {}
        for dominio in sorted(soportados):
            if not puede_darse_de_alta(dominio):
                continue
            family, family_label = familia_de(dominio, dominio)
            grupo = por_familia.setdefault(
                family, {"family": family, "family_label": family_label, "marcas": []}
            )
            # El label del GRUPO es la familia; el de la marca es la marca. Con
            # la lista blanca los dos eran lo mismo porque cada familia tenía
            # una marca conocida. Con 728 dominios cayendo en "otros", repetir
            # el label de la familia en cada fila dejaría una lista de cientos
            # de entradas idénticas e inservible.
            conocida = FAMILIAS.get(dominio)
            grupo["marcas"].append(
                {
                    "handler": dominio,
                    "label": conocida[1] if conocida else nombre_de_marca(dominio),
                }
            )

        familias = sorted(por_familia.values(), key=lambda g: g["family_label"])
        logger.info(
            "onboarding.alta_marcas_ok",
            soportados=len(soportados),
            ofrecidos=sum(len(g["marcas"]) for g in familias),
        )
        return {"disponible": True, "familias": familias}

    async def iniciar_alta(self, handler: str) -> dict[str, Any]:
        """
        Arranca el formulario de una marca.

        🔪 EL CANDADO SE APLICA ACÁ, antes de tocar HA. El `handler` viene del
        navegador, así que son dos preguntas distintas y hacen falta las dos:

          1. ¿Es un aparato de la casa? — la lista negra. Bloquea el sistema del
             gateway (supervisor, backups, HACS) y la infraestructura interna.
          2. ¿EXISTE de verdad en esta casa? — se cruza con `flow_handlers`, o
             sea con lo que HA dice que sabe dar de alta acá.

        La (2) reemplaza a la lista blanca de 30 marcas que había antes. Es
        mejor candado y a la vez mucha más cobertura: el permiso ya no sale de
        una lista escrita a mano —que dejaba afuera el 96% del catálogo y había
        que ampliar a mano por cada cliente que compraba algo— sino del propio
        Home Assistant. Lo que HA no sabe hacer, no se ofrece; lo que sabe y es
        un aparato, sí.
        """
        if not puede_darse_de_alta(handler):
            raise OnboardingForbiddenDomainError(
                f"La marca {handler!r} no se puede dar de alta desde la app"
            )
        cliente = self._flow_client()
        if cliente is None:
            raise OnboardingAdminRequiredError("Se requieren permisos admin en HA")

        # Se pregunta ANTES de arrancar nada. Un dominio inventado que llegara a
        # HA daría un error feo y un formulario colgado a medio abrir.
        try:
            soportados = set(await cliente.marcas_disponibles())
        except (UpstreamNotReadyError, HaProtocolError):
            raise
        if handler not in soportados:
            logger.warning("onboarding.alta_handler_inexistente", handler=handler)
            raise OnboardingForbiddenDomainError(
                f"Esta casa no sabe dar de alta {handler!r}"
            )

        crudo = await cliente.iniciar(handler)
        logger.info("onboarding.alta_iniciada", handler=handler, tipo=crudo.get("type"))
        return self._paso_limpio(crudo)

    async def avanzar_alta(self, flow_id: str, datos: dict[str, Any]) -> dict[str, Any]:
        """
        Contesta un paso del formulario.

        🔪 SEGUNDA VUELTA DE LA LISTA BLANCA, y hace falta.

        Un `flow_id` también viene del navegador. Sin revisar de quién es, la
        app dejaría avanzar CUALQUIER formulario abierto en HA —incluido uno del
        Supervisor— con solo adivinar o conocer su id. Así que antes de mandar
        nada se busca el flow entre los que están en curso y se comprueba que su
        marca esté permitida.

        La comprobación se hace contra la lista de flows en curso (WebSocket, de
        solo lectura) y no contra un registro nuestro, a propósito: los
        descubrimientos de HA —el reauth de las persianas, por ejemplo— no los
        inició la app, y aun así tienen que poder continuarse desde acá.

        `datos` es lo que escribió la persona. Pasa de largo: no se loguea, no se
        guarda, no se cachea.
        """
        await self._exigir_flow_permitido(flow_id)
        cliente = self._flow_client()
        if cliente is None:
            raise OnboardingAdminRequiredError("Se requieren permisos admin en HA")

        crudo = await cliente.avanzar(flow_id, datos)
        # Se loguea el TIPO de resultado, nunca el contenido.
        logger.info("onboarding.alta_avanzada", tipo=crudo.get("type"))
        return self._paso_limpio(crudo)

    async def cancelar_alta(self, flow_id: str) -> None:
        """Abandona un formulario. Misma comprobación que avanzar."""
        await self._exigir_flow_permitido(flow_id)
        cliente = self._flow_client()
        if cliente is None:
            raise OnboardingAdminRequiredError("Se requieren permisos admin en HA")
        await cliente.cancelar(flow_id)
        logger.info("onboarding.alta_cancelada")

    # --- Helpers del alta ----------------------------------------------------

    def _flow_client(self) -> Any:
        """
        El cliente REST, o None si esta casa no puede usarlo.

        Se construye por llamada y no en el constructor porque depende del token,
        y porque así una casa sin admin no arrastra un objeto que no sirve.
        """
        if self._ha_http_url is None or self._ha_token_provider is None:
            return None
        try:
            token = self._ha_token_provider()
        except Exception:
            logger.warning("onboarding.alta_sin_token")
            return None
        if not token:
            return None

        from ha_mirror.ha_flow_client import HaFlowClient

        return HaFlowClient(self._ha_http_url, token)

    async def _exigir_flow_permitido(self, flow_id: str) -> None:
        """Revienta si el flow no existe o si su marca no está permitida."""
        try:
            resp = await self._upstream.send_command(
                {"type": "config_entries/flow/progress"}, timeout=10.0
            )
        except UpstreamNotReadyError:
            raise
        except HaProtocolError as exc:
            if "unauthorized" in str(exc).lower():
                raise OnboardingAdminRequiredError("Se requieren permisos admin en HA") from exc
            raise

        for f in resp.get("result") or []:
            if f.get("flow_id") == flow_id:
                handler = str(f.get("handler") or "")
                if not puede_darse_de_alta(handler):
                    logger.warning(
                        "onboarding.alta_handler_prohibido", handler=handler
                    )
                    raise OnboardingForbiddenDomainError(
                        f"El formulario pertenece a {handler!r}, que no se maneja desde la app"
                    )
                return
        raise OnboardingFlowNoEncontradoError(f"Formulario {flow_id!r} desconocido")

    def _paso_limpio(self, crudo: dict[str, Any]) -> dict[str, Any]:
        """
        La respuesta de HA, filtrada a lo que la app necesita.

        Se copian campos uno por uno en vez de reenviar el diccionario entero.
        HA devuelve tambien `result` cuando el alta termina —la config entry
        recien creada, que puede traer adentro lo que la persona escribio,
        contraseñas incluidas— y eso no tiene por que llegar al navegador.

        `title` y los placeholders van enmascarados: HA los rellena con datos del
        aparato y del sistema, y ya sabemos que por ahi puede salir el correo de
        un tercero.
        """
        tipo = str(crudo.get("type") or "")
        salida: dict[str, Any] = {
            "tipo": tipo,
            "flow_id": crudo.get("flow_id"),
            "handler": crudo.get("handler"),
            "paso": crudo.get("step_id"),
        }

        if tipo == "form":
            salida["campos"] = self._campos_limpios(crudo.get("data_schema"))
            salida["errores"] = crudo.get("errors") or {}
        elif tipo == "create_entry":
            # Solo el nombre. NUNCA `result`.
            salida["titulo"] = enmascarar_titulo(str(crudo.get("title") or ""))
        elif tipo == "abort":
            salida["razon"] = str(crudo.get("reason") or "")
        elif tipo in ("external_step", "external_step_done", "progress", "show_progress"):
            # Estos son los que NO se pueden completar desde la app: mandan a
            # abrir una ventana de la nube del fabricante o esperan a algo de
            # afuera. Se declaran tal cual para que la app lo diga sin inventar.
            salida["paso"] = crudo.get("step_id")

        ph = crudo.get("description_placeholders")
        if isinstance(ph, dict) and ph:
            salida["textos"] = {
                str(k): enmascarar_titulo(str(v)) for k, v in ph.items()
            }
        return salida

    @staticmethod
    def _campos_limpios(schema: Any) -> list[dict[str, Any]]:
        """
        El `data_schema` de HA, filtrado.

        Esto es lo que hace que el espejo sea generico: HA no solo acepta el
        formulario, lo DESCRIBE. Cada campo viene con nombre, tipo y si es
        obligatorio, asi que la app puede dibujar el formulario de cualquier
        marca sin que nadie programe una pantalla por marca.

        Se pasan solo las claves que la app sabe dibujar. Un campo con una forma
        que no entendemos se deja pasar igual con su tipo crudo: es mejor
        mostrarlo como texto que esconderlo y que el alta no se pueda completar.
        """
        if not isinstance(schema, list):
            return []
        campos: list[dict[str, Any]] = []
        for bruto in schema:
            if not isinstance(bruto, dict):
                continue
            nombre = bruto.get("name")
            if not nombre:
                continue
            campo: dict[str, Any] = {
                "nombre": str(nombre),
                "tipo": str(bruto.get("type") or "string"),
                "obligatorio": bool(bruto.get("required")),
            }
            if "default" in bruto:
                campo["valor_inicial"] = bruto["default"]
            if bruto.get("options"):
                campo["opciones"] = bruto["options"]
            # HA marca asi los campos de contraseña. La app tiene que pintarlos
            # ocultos: en una tablet en la cocina, una clave en claro se lee
            # desde el otro lado de la mesa.
            if bruto.get("format") == "password" or "password" in str(nombre).lower():
                campo["secreto"] = True
            campos.append(campo)
        return campos

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
