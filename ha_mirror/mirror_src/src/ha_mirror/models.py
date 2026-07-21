"""
Modelos Pydantic v2 para el dominio del mirror.

HaState y derivados: shape de los objetos que vienen de HA.
ServiceCallRequest: shape de los requests del frontend al mirror.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

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
