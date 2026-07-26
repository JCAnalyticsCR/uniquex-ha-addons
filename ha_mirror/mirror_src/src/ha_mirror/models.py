"""
Modelos Pydantic v2 para el dominio del mirror.

HaState y derivados: shape de los objetos que vienen de HA.
ServiceCallRequest: shape de los requests del frontend al mirror.
Scene / SceneInput / SceneStep: escenas custom del cliente (CRUD /api/scenes).
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


# ---------------------------------------------------------------------------
# Modelos HA (upstream → mirror)
# ---------------------------------------------------------------------------


class HaAttributes(BaseModel):
    """Atributos de una entidad HA — schema abierto por diseño."""

    model_config = ConfigDict(extra="allow")


class HaContext(BaseModel):
    """Contexto de un evento HA."""

    id: str
    parent_id: str | None = None
    user_id: str | None = None


class HaState(BaseModel):
    """Estado actual de una entidad HA."""

    model_config = ConfigDict(extra="allow")

    entity_id: str
    state: str
    attributes: HaAttributes = Field(default_factory=HaAttributes)
    last_changed: datetime
    last_updated: datetime
    context: HaContext | None = None

    @property
    def domain(self) -> str:
        return self.entity_id.split(".", 1)[0]

    @property
    def friendly_name(self) -> str | None:
        return self.attributes.model_extra.get("friendly_name") if self.attributes.model_extra else None


class StateChangedData(BaseModel):
    """Data del evento state_changed de HA."""

    entity_id: str
    old_state: HaState | None = None
    new_state: HaState | None = None

    @model_validator(mode="after")
    def at_least_one_state(self) -> "StateChangedData":
        """Entidad nueva: old_state=None. Entidad eliminada: new_state=None."""
        if self.old_state is None and self.new_state is None:
            raise ValueError("old_state y new_state no pueden ser ambos None")
        return self


class StateChangedEvent(BaseModel):
    """Evento completo state_changed tal como llega por WebSocket de HA."""

    event_type: str
    data: StateChangedData
    origin: str
    time_fired: datetime
    context: HaContext | None = None


# ---------------------------------------------------------------------------
# Modelos de registry HA
# ---------------------------------------------------------------------------


class HaEntityRegistryEntry(BaseModel):
    """
    Entrada del entity registry (list_for_display, campos comprimidos).

    list_for_display usa claves comprimidas. El upstream _parse_entities()
    normaliza al construir el dict antes de validar, por lo que usamos
    nombres canónicos directamente sin alias complejos.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    entity_id: str
    name: str | None = None
    icon: str | None = None
    platform: str | None = None
    area_id: str | None = None
    device_id: str | None = None
    hidden_by: str | None = None
    disabled_by: str | None = None


class HaDeviceRegistryEntry(BaseModel):
    """Entrada del device registry."""

    model_config = ConfigDict(extra="allow")

    id: str
    name: str | None = None
    manufacturer: str | None = None
    model: str | None = None
    area_id: str | None = None


class HaAreaRegistryEntry(BaseModel):
    """Entrada del area registry."""

    model_config = ConfigDict(extra="allow")

    area_id: str
    name: str
    icon: str | None = None
    aliases: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Modelos del mirror (frontend ← mirror)
# ---------------------------------------------------------------------------


class EntitySummary(BaseModel):
    """Proyección de una entidad para el frontend — combina state + registry."""

    entity_id: str
    state: str
    attributes: dict[str, Any]
    last_changed: datetime
    last_updated: datetime
    friendly_name: str | None
    area_id: str | None
    device_id: str | None
    domain: str


class AreaSummary(BaseModel):
    """Área enriquecida con lista de entidades para el frontend."""

    area_id: str
    name: str
    icon: str | None
    entity_ids: list[str]


class ServiceCallRequest(BaseModel):
    """Request body de POST /api/service/{domain}/{service}."""

    entity_id: str | None = None
    service_data: dict[str, Any] = Field(default_factory=dict)
    target: dict[str, Any] | None = None


class ServiceCallResponse(BaseModel):
    """Respuesta 202 de /api/service/{domain}/{service}."""

    correlation_id: str
    accepted: bool = True
    message: str = "Comando aceptado. Estado llegará por /ws/state."


class HealthResponse(BaseModel):
    """Respuesta de GET /api/health."""

    upstream_connected: bool
    upstream_state: str  # DISCONNECTED | AUTHENTICATING | HYDRATING | READY | AUTH_FAILED
    last_event_ts: datetime | None
    ws_reconnects_total: int
    connected_ws_clients: int
    tenant_id: int


# ---------------------------------------------------------------------------
# Escenas custom del cliente (CRUD /api/scenes)
#
# Concepto PARALELO a las entidades `scene.*` de HA: estas viven en la SQLite
# del mirror, las arma el usuario desde la app y son una lista ordenada de
# service calls. El contrato (slugs de icono/acento, límites) está congelado:
# el frontend depende de él, no cambiar sin coordinar ambos lados.
# ---------------------------------------------------------------------------

# Set fijo de iconos — el frontend mapea cada slug a un SVG propio.
SceneIcon = Literal[
    "moon",
    "sun",
    "home",
    "away",
    "movie",
    "gym",
    "party",
    "sleep",
    "shield",
    "sparkles",
]

# Set fijo de acentos — cada uno es un gradiente del tema "Cálido hogareño".
SceneAccent = Literal["warm", "cool", "gold", "green", "neutral"]

# Límites del contrato. Se validan acá (no en el router) para que el CRUD y
# cualquier otro consumidor del modelo compartan exactamente las mismas reglas.
SCENE_NAME_MAX = 60
SCENE_DESCRIPTION_MAX = 160
SCENE_STEPS_MAX = 64
SCENE_CAMERAS_MAX = 12
SCENE_STEP_DATA_MAX_KEYS = 12

# Un paso puede apuntar a UNA entidad suelta o a un GRUPO: varias entidades del
# mismo domain a las que se les da la MISMA accion (5 luces -> apagar todas). HA
# aplica la accion a todo el target de una sola llamada; el tope es de cordura
# para no inflar el payload ni la tarjeta de grupo del builder.
SCENE_STEP_ENTITIES_MAX = 40

# Máximo de escenas por tenant. Es un tope de cordura: la lista completa viaja
# en cada GET /api/scenes y se renderiza entera en la app.
SCENES_PER_TENANT_MAX = 60

_SCENE_STEP_SCALARS = (str, int, float, bool)
_SCENE_CAMERA_PATTERN = re.compile(r"^camera\.[a-z0-9_]+$")
_SCENE_ENTITY_PATTERN = re.compile(r"^[a-z_]+\.[a-z0-9_]+$")


def _es_valor_de_paso_valido(valor: Any) -> bool:
    """Escalar simple o lista plana de escalares. Nada anidado más profundo."""
    if isinstance(valor, _SCENE_STEP_SCALARS):
        return True
    if isinstance(valor, list):
        return all(isinstance(item, _SCENE_STEP_SCALARS) for item in valor)
    return False


def _validar_entity_id(entity_id: str, domain: str) -> None:
    """
    Un entity_id de paso válido: matchea el patrón y su prefijo es el domain.

    Se aplica igual a la entidad suelta y a cada miembro de un grupo. El chequeo
    de prefijo cierra el hueco de declarar {"domain":"light","entity_id":"hassio.x"}
    para colar algo administrativo por la deny-list (que mira el domain declarado)
    mientras HA ejecuta sobre otra cosa.
    """
    if not _SCENE_ENTITY_PATTERN.fullmatch(entity_id):
        raise ValueError(f"entity_id invalido: {entity_id!r}")
    if entity_id.split(".", 1)[0] != domain:
        raise ValueError(f"entity_id '{entity_id}' no pertenece al domain '{domain}'")


class SceneStep(BaseModel):
    """
    Una acción de la escena: un service call de HA sobre una o varias entidades.

    entity_id apunta a UNA entidad suelta ("switch.x") o a un GRUPO —una lista de
    entidades del MISMO domain a las que se les da la misma acción ("apagar estas
    5 luces")—. Es HA-native: `target.entity_id` acepta tanto el string como la
    lista, así que el grupo se resuelve en UNA sola llamada de servicio. Las
    escenas viejas (entity_id string) siguen siendo válidas sin tocar nada.
    """

    domain: str = Field(pattern=r"^[a-z_]{1,32}$")
    service: str = Field(pattern=r"^[a-z_]{1,32}$")
    entity_id: str | list[str]
    data: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validar_paso(self) -> SceneStep:
        """
        Valida el/los entity_id del paso y la forma de `data`.

        Entidad suelta: matchea el patrón y pertenece al domain del paso. Grupo:
        lista no vacía de a lo sumo SCENE_STEP_ENTITIES_MAX, cada uno con patrón y
        domain válidos y sin repetidos (HA aplicaría la acción dos veces a la misma
        entidad si no). El chequeo por-entidad cierra el hueco de colar un domain
        administrativo por la deny-list mientras HA ejecuta sobre otra cosa.
        """
        if isinstance(self.entity_id, str):
            _validar_entity_id(self.entity_id, self.domain)
        else:
            if not self.entity_id:
                raise ValueError("entity_id como grupo no puede ser una lista vacía")
            if len(self.entity_id) > SCENE_STEP_ENTITIES_MAX:
                raise ValueError(
                    f"un grupo admite como maximo {SCENE_STEP_ENTITIES_MAX} entidades"
                )
            for entity_id in self.entity_id:
                _validar_entity_id(entity_id, self.domain)
            if len(set(self.entity_id)) != len(self.entity_id):
                raise ValueError("un grupo no admite entidades repetidas")
        if len(self.data) > SCENE_STEP_DATA_MAX_KEYS:
            raise ValueError(f"data admite como maximo {SCENE_STEP_DATA_MAX_KEYS} claves")
        for clave, valor in self.data.items():
            if not isinstance(clave, str):
                raise ValueError("las claves de data deben ser strings")
            if not _es_valor_de_paso_valido(valor):
                raise ValueError(
                    f"valor invalido en data['{clave}']: solo str, int, float, bool "
                    "o listas planas de esos tipos"
                )
        return self


class SceneInput(BaseModel):
    """Body de POST /api/scenes y PUT /api/scenes/{scene_id}."""

    name: str = Field(min_length=1, max_length=SCENE_NAME_MAX)
    icon: SceneIcon
    accent: SceneAccent
    description: str = Field(default="", max_length=SCENE_DESCRIPTION_MAX)
    confirm_required: bool = False
    steps: list[SceneStep] = Field(min_length=1, max_length=SCENE_STEPS_MAX)
    cameras: list[str] = Field(default_factory=list, max_length=SCENE_CAMERAS_MAX)

    @model_validator(mode="after")
    def validar_escena(self) -> SceneInput:
        """Nombre sin espacios de relleno y cámaras destacadas del dominio camera."""
        self.name = self.name.strip()
        if not self.name:
            raise ValueError("name no puede ser solo espacios")
        for entity_id in self.cameras:
            if not _SCENE_CAMERA_PATTERN.fullmatch(entity_id):
                raise ValueError(f"camara invalida: {entity_id!r}")
        if len(set(self.cameras)) != len(self.cameras):
            raise ValueError("cameras no admite entidades repetidas")
        return self


class Scene(SceneInput):
    """Escena persistida — lo que devuelve el mirror al frontend."""

    id: str
    created_at: str
    updated_at: str
    last_activated_at: str | None = None


# ---------------------------------------------------------------------------
# Mensajes WebSocket mirror → frontend
# ---------------------------------------------------------------------------


class WsSnapshot(BaseModel):
    """Snapshot inicial enviado al conectar al WS."""

    type: str = "snapshot"
    states: dict[str, EntitySummary]
    areas: list[AreaSummary]
    services: dict[str, Any]
    cache_version: int


class WsStateChanged(BaseModel):
    """Diff de estado enviado por cada state_changed."""

    type: str = "state_changed"
    entity_id: str
    new_state: EntitySummary | None
    correlation_id: str | None = None


class WsConnectionStatus(BaseModel):
    """Notificación de cambio de estado del upstream a HA."""

    type: str = "connection_status"
    upstream: str  # connected | reconnecting | disconnected


class WsServiceComplete(BaseModel):
    """Confirmación de que un service call resultó en state_changed."""

    type: str = "service_complete"
    correlation_id: str
    entity_id: str | None
    success: bool = True


class WsServiceTimeout(BaseModel):
    """Timeout: no llegó state_changed en el plazo esperado."""

    type: str = "service_timeout"
    correlation_id: str
