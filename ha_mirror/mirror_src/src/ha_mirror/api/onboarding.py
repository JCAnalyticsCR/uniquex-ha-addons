"""
Endpoints del módulo de onboarding.

Permite que la app UniquexCR organice los dispositivos del cliente
(habitaciones, nombres visibles, ocultar, orden) y descubra integraciones
nuevas — todo sin abrir Home Assistant.

Todos los endpoints exigen X-API-Key (require_api_key), igual que el
resto de los routers del mirror.

Prefijo: /api/onboarding
"""

from __future__ import annotations

import re
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ha_mirror.auth import require_api_key
from ha_mirror.errors import UpstreamNotReadyError
from ha_mirror.onboarding import (
    OnboardingAdminRequiredError,
    OnboardingEntryNotFoundError,
    OnboardingForbiddenDomainError,
    OnboardingRescanInProgressError,
    OnboardingService,
)

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/onboarding", tags=["onboarding"])

# Expresión regular para entity_id válido en path parameters
_ENTITY_ID_RE = re.compile(r"^[a-z_]+\.[a-z0-9_]+$")
# Solo habitaciones custom (prefijo "custom:") son editables/borrables.
# Se valida con la EXPRESIÓN COMPLETA y no con un `startswith("custom:")`: la
# ruta usa `{room_id:path}` (necesario para que los dos puntos de "custom:sala"
# no se coman como separador), así que sin este ancla cualquier cosa que
# empiece con el prefijo —de cualquier largo y con cualquier carácter— llegaba
# hasta la capa de datos. Hoy no hay inyección posible (las consultas van
# parametrizadas y un id inexistente es un no-op), pero dejar la validación
# escrita y sin usar es justo cómo se cuela el día que alguien agregue una
# ruta nueva que sí construya algo con este valor.
_CUSTOM_ROOM_RE = re.compile(r"^custom:[a-z0-9_:-]{1,48}$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _svc(request: Request) -> OnboardingService:
    """Extrae el OnboardingService del estado de la app."""
    return request.app.state.onboarding  # type: ignore[no-any-return]


def _tenant_id(request: Request) -> int:
    """Tenant único (N=1). Se lee de settings para no hardcodear el 1."""
    return int(getattr(request.app.state.settings, "tenant_id", 1))


# ---------------------------------------------------------------------------
# Modelos de request
# ---------------------------------------------------------------------------


class OverrideBody(BaseModel):
    """
    Body para PUT /overrides/{entity_id}.

    Todos los campos son opcionales (merge parcial): los omitidos se conservan,
    los enviados como null se limpian. Los validadores rechazan con 422
    si el valor proporcionado no cumple el formato.
    """

    model_config = ConfigDict(extra="forbid")

    room_id: str | None = None
    display_name: str | None = None
    icon: str | None = None
    hidden: bool = False
    sort_order: int | None = None

    @field_validator("display_name", mode="before")
    @classmethod
    def _strip_display_name(cls, v: Any) -> str | None:
        if v is None:
            return None
        if not isinstance(v, str):
            raise ValueError("display_name debe ser string o null")
        stripped = v.strip()
        if not stripped:
            return None  # vacío → null (limpia el campo)
        if len(stripped) > 80:
            raise ValueError("display_name supera 80 caracteres")
        return stripped

    @field_validator("room_id")
    @classmethod
    def _validate_room_id(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not re.fullmatch(r"[a-z0-9_:-]{1,64}", v):
            raise ValueError("room_id inválido: solo [a-z0-9_:-], máximo 64 chars")
        return v

    @field_validator("icon")
    @classmethod
    def _validate_icon(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not re.fullmatch(r"[a-z0-9_-]{1,32}", v):
            raise ValueError("icon inválido: solo [a-z0-9_-], máximo 32 chars")
        return v

    @field_validator("sort_order")
    @classmethod
    def _validate_sort_order(cls, v: int | None) -> int | None:
        if v is None:
            return None
        if not (0 <= v <= 9999):
            raise ValueError("sort_order debe estar entre 0 y 9999")
        return v


class BatchItem(BaseModel):
    """Un item dentro del body del batch de overrides."""

    model_config = ConfigDict(extra="forbid")

    entity_id: str
    room_id: str | None = None
    display_name: str | None = None
    icon: str | None = None
    hidden: bool = False
    sort_order: int | None = None

    @field_validator("display_name", mode="before")
    @classmethod
    def _strip_display_name(cls, v: Any) -> str | None:
        if v is None:
            return None
        if not isinstance(v, str):
            raise ValueError("display_name debe ser string o null")
        stripped = v.strip()
        return stripped or None

    @field_validator("room_id")
    @classmethod
    def _validate_room_id(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not re.fullmatch(r"[a-z0-9_:-]{1,64}", v):
            raise ValueError("room_id inválido")
        return v

    @field_validator("icon")
    @classmethod
    def _validate_icon(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not re.fullmatch(r"[a-z0-9_-]{1,32}", v):
            raise ValueError("icon inválido")
        return v

    @field_validator("sort_order")
    @classmethod
    def _validate_sort_order(cls, v: int | None) -> int | None:
        if v is None:
            return None
        if not (0 <= v <= 9999):
            raise ValueError("sort_order fuera de rango")
        return v


class BatchBody(BaseModel):
    """Body para POST /overrides/batch."""

    items: list[BatchItem] = Field(..., min_length=1, max_length=200)


class RoomCreateBody(BaseModel):
    """Body para POST /rooms."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=60)
    icon: str | None = None

    @field_validator("icon")
    @classmethod
    def _validate_icon(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not re.fullmatch(r"[a-z0-9_-]{1,32}", v):
            raise ValueError("icon inválido: solo [a-z0-9_-], máximo 32 chars")
        return v


class RoomUpdateBody(BaseModel):
    """Body para PUT /rooms/{room_id}."""

    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=60)
    icon: str | None = None
    sort_order: int | None = None

    @field_validator("icon")
    @classmethod
    def _validate_icon(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not re.fullmatch(r"[a-z0-9_-]{1,32}", v):
            raise ValueError("icon inválido")
        return v

    @field_validator("sort_order")
    @classmethod
    def _validate_sort_order(cls, v: int | None) -> int | None:
        if v is None:
            return None
        if not (0 <= v <= 9999):
            raise ValueError("sort_order debe estar entre 0 y 9999")
        return v


class AckBody(BaseModel):
    """Body para POST /pending/ack."""

    entity_ids: list[str] = Field(..., min_length=1, max_length=500)


class RescanBody(BaseModel):
    """Body para POST /rescan. entry_id=None → todas las elegibles."""

    entry_id: str | None = None


# ---------------------------------------------------------------------------
# Endpoints — Capabilities
# ---------------------------------------------------------------------------


@router.get(
    "/capabilities",
    summary="Capacidades del módulo de onboarding",
)
async def get_capabilities(
    request: Request,
    _: None = Depends(require_api_key),
) -> dict[str, Any]:
    """
    Sonda al HA para determinar si el token tiene permisos admin.

    SIEMPRE responde 200. Si el upstream está caído o el token no tiene
    permisos admin, devuelve admin=False con la funcionalidad reducida.
    El resultado se cachea 60 s para no martillar a HA en cada render.
    """
    svc = _svc(request)
    return await svc.get_capabilities(_tenant_id(request))


# ---------------------------------------------------------------------------
# Endpoints — Overrides
# ---------------------------------------------------------------------------


@router.get(
    "/overrides",
    summary="Lee todos los overrides y habitaciones custom",
)
async def get_overrides(
    request: Request,
    _: None = Depends(require_api_key),
) -> dict[str, Any]:
    """
    Devuelve el mapa de overrides por entity_id y la lista de habitaciones custom.
    """
    svc = _svc(request)
    return await svc.get_overrides(_tenant_id(request))


@router.put(
    "/overrides/{entity_id}",
    summary="Actualiza el override de una entidad (merge parcial)",
)
async def put_override(
    entity_id: str,
    body: OverrideBody,
    request: Request,
    _: None = Depends(require_api_key),
) -> dict[str, Any]:
    """
    Merge parcial sobre el override de la entidad.

    - Campos omitidos → se conservan.
    - Campos con valor → se actualizan.
    - Campos con null → se limpian.
    - Si todos los campos quedan null/False → se borra la fila (devuelve cleared=True).
    - 404 si entity_id no existe en el cache del store.
    - 422 si algún valor es inválido.
    """
    if not _ENTITY_ID_RE.fullmatch(entity_id):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="entity_id inválido: debe coincidir con ^[a-z_]+\\.[a-z0-9_]+$",
        )

    # Solo los campos explícitamente presentes en el body → merge parcial
    provided = body.model_dump(include=body.model_fields_set)

    svc = _svc(request)
    try:
        return await svc.upsert_override(entity_id, provided, _tenant_id(request))
    except KeyError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Entidad {entity_id!r} no encontrada en el store",
        ) from None


@router.delete(
    "/overrides/{entity_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Elimina el override de una entidad",
)
async def delete_override(
    entity_id: str,
    request: Request,
    _: None = Depends(require_api_key),
) -> Response:
    """Borra el override. Idempotente: si no existía también retorna 204."""
    svc = _svc(request)
    await svc.delete_override(entity_id, _tenant_id(request))
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/overrides/batch",
    summary="Actualiza overrides en lote",
)
async def batch_overrides(
    body: BatchBody,
    request: Request,
    _: None = Depends(require_api_key),
) -> dict[str, Any]:
    """
    Aplica overrides en lote (hasta 200 items).

    Items cuya entidad no existe en el store van a 'skipped' sin error.
    """
    svc = _svc(request)
    # Convertir cada item a dict con solo los campos provistos (model_fields_set)
    items_dicts: list[dict[str, Any]] = []
    for item in body.items:
        item_dict: dict[str, Any] = {"entity_id": item.entity_id}
        # Incluir solo los campos explícitamente seteados (excluyendo entity_id)
        provided = item.model_dump(include=item.model_fields_set, exclude={"entity_id"})
        item_dict.update(provided)
        items_dicts.append(item_dict)

    return await svc.batch_overrides(items_dicts, _tenant_id(request))


# ---------------------------------------------------------------------------
# Endpoints — Rooms
# ---------------------------------------------------------------------------


@router.get(
    "/rooms",
    summary="Lista de habitaciones custom",
)
async def get_rooms(
    request: Request,
    _: None = Depends(require_api_key),
) -> dict[str, Any]:
    """Solo habitaciones custom; las áreas de HA salen por /api/areas."""
    svc = _svc(request)
    return await svc.get_rooms(_tenant_id(request))


@router.post(
    "/rooms",
    status_code=status.HTTP_201_CREATED,
    summary="Crea una habitación custom",
)
async def create_room(
    body: RoomCreateBody,
    request: Request,
    _: None = Depends(require_api_key),
) -> dict[str, Any]:
    """
    Crea una habitación custom.

    room_id = "custom:" + slug(name). 409 si ya existe ese room_id.
    sort_order = max_existente + 1.
    """
    svc = _svc(request)
    try:
        return await svc.create_room(body.name, body.icon, _tenant_id(request))
    except ValueError as exc:
        msg = str(exc)
        if "ya existe" in msg.lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=msg,
            ) from None
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=msg,
        ) from None


@router.put(
    "/rooms/{room_id:path}",
    summary="Actualiza una habitación custom",
)
async def update_room(
    room_id: str,
    body: RoomUpdateBody,
    request: Request,
    _: None = Depends(require_api_key),
) -> dict[str, Any]:
    """
    Actualiza una habitación custom.

    Solo acepta room_id con prefijo 'custom:'. 404 si no existe.
    """
    if not _CUSTOM_ROOM_RE.fullmatch(room_id):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Solo se pueden editar habitaciones con prefijo 'custom:'",
        )

    # Construir dict de campos provistos
    provided: dict[str, Any] = {}
    if "name" in body.model_fields_set and body.name is not None:
        provided["name"] = body.name
    if "icon" in body.model_fields_set:
        provided["icon"] = body.icon
    if "sort_order" in body.model_fields_set and body.sort_order is not None:
        provided["sort_order"] = body.sort_order

    svc = _svc(request)
    try:
        return await svc.update_room(room_id, provided, _tenant_id(request))
    except KeyError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Habitación {room_id!r} no encontrada",
        ) from None


@router.delete(
    "/rooms/{room_id:path}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Elimina una habitación custom",
)
async def delete_room(
    room_id: str,
    request: Request,
    _: None = Depends(require_api_key),
) -> Response:
    """
    Borra una habitación custom y limpia los overrides que la referencian.

    Los overrides que apuntaban a este room_id quedan con room_id=null
    (misma transacción). Idempotente: si ya no existía también retorna 204.
    """
    if not _CUSTOM_ROOM_RE.fullmatch(room_id):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Solo se pueden borrar habitaciones con prefijo 'custom:'",
        )
    svc = _svc(request)
    await svc.delete_room(room_id, _tenant_id(request))
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ---------------------------------------------------------------------------
# Endpoints — Pending
# ---------------------------------------------------------------------------


@router.get(
    "/pending",
    summary="Entidades nuevas pendientes de revisión",
)
async def get_pending(
    request: Request,
    _: None = Depends(require_api_key),
) -> dict[str, Any]:
    """
    Devuelve las entidades que aparecieron después del baseline.

    Primera llamada con tabla vacía → baseline automático (baseline_created=True,
    new_entities=[]). Instalaciones nuevas nunca ven cientos de dispositivos
    "nuevos" al abrir el módulo por primera vez.
    """
    svc = _svc(request)
    return await svc.get_pending(_tenant_id(request))


@router.post(
    "/pending/ack",
    summary="Confirma entidades revisadas",
)
async def ack_pending(
    body: AckBody,
    request: Request,
    _: None = Depends(require_api_key),
) -> dict[str, Any]:
    """Marca entidades como revisadas. Devuelve cuántas fueron marcadas."""
    svc = _svc(request)
    return await svc.ack_pending(body.entity_ids, _tenant_id(request))


# ---------------------------------------------------------------------------
# Endpoint — Rescan
# ---------------------------------------------------------------------------


@router.post(
    "/rescan",
    summary="Recarga integraciones de HA",
)
async def post_rescan(
    body: RescanBody,
    request: Request,
    _: None = Depends(require_api_key),
) -> dict[str, Any]:
    """
    Dispara el reload de una o todas las integraciones elegibles.

    - 501: token HA sin permisos admin.
    - 502: upstream HA desconectado.
    - 404: entry_id desconocido.
    - 403: dominio en deny-list.
    - 409: ya hay un rescan en curso.
    """
    svc = _svc(request)
    try:
        return await svc.rescan(body.entry_id, _tenant_id(request))
    except UpstreamNotReadyError:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream HA desconectado",
        ) from None
    except OnboardingAdminRequiredError:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Se requieren permisos admin en HA (token del Supervisor)",
        ) from None
    except OnboardingRescanInProgressError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Rescan ya en curso",
        ) from None
    except OnboardingEntryNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from None
    except OnboardingForbiddenDomainError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(exc),
        ) from None
