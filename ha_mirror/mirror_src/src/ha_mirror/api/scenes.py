"""
CRUD + activación de escenas custom del cliente.

Una escena es una lista ordenada de service calls que el usuario arma desde la
app ("Buenas noches" = apagar switches + bajar persianas). Viven en la SQLite
del mirror (`/data`, sobrevive updates del add-on), NO en Home Assistant: son
un concepto paralelo a las entidades `scene.*` que HA ya expone por el snapshot.

Seguridad: cada paso pasa por la deny-list de `api/service.py` DOS veces —
al guardar (POST/PUT) para no persistir una escena que va a fallar, y otra vez
al activar, porque la deny-list puede crecer entre que se guardó y se ejecutó.
La lista NO se duplica acá: se importa `_is_service_forbidden`, que es el único
gate de ejecución del mirror.
"""

from __future__ import annotations

import asyncio
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel

from ha_mirror.api.service import _is_service_forbidden
from ha_mirror.auth import require_api_key
from ha_mirror.correlations import CorrelationTracker
from ha_mirror.db import Database
from ha_mirror.models import SCENES_PER_TENANT_MAX, Scene, SceneInput, SceneStep
from ha_mirror.prometheus_metrics import (
    SERVICE_CALL_CONFIRMED,
    SERVICE_CALL_DURATION,
    SERVICE_CALL_TIMEOUT,
    SERVICE_CALLS_TOTAL,
)

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/scenes", tags=["scenes"])

# El id lo genera el mirror con uuid4().hex. Filtrar acá evita pegarle a SQLite
# con ids arbitrarios que vengan de la URL.
_SCENE_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")


# ---------------------------------------------------------------------------
# Modelos de respuesta (solo los consume este router — igual que EntitiesResponse)
# ---------------------------------------------------------------------------


class SceneListResponse(BaseModel):
    scenes: list[Scene]
    count: int


class SceneActivateResponse(BaseModel):
    """
    Respuesta 202 de POST /api/scenes/{scene_id}/activate.

    Shape congelado en el contrato con el frontend: exactamente estas tres
    claves. No agregar campos sin coordinar con las rutas BFF de Next.
    """

    scene_id: str
    steps: int
    correlation_ids: list[str]


@dataclass
class _PasoPreparado:
    """Un paso con su correlación ya registrada, listo para dispararse."""

    domain: str
    service: str
    entity_id: str
    payload: dict[str, Any]
    correlation_id: str
    future: asyncio.Future[bool]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tenant_id(request: Request) -> int:
    """Tenant único (N=1). Se lee de settings para no hardcodear el 1 acá."""
    return int(getattr(request.app.state.settings, "tenant_id", 1))


def _rechazar_servicios_prohibidos(
    request: Request,
    steps: list[SceneStep],
    *,
    momento: str,
    scene_id: str | None = None,
) -> None:
    """
    403 si algún paso toca un servicio administrativo.

    `momento` es "guardar" o "activar" — solo para el log; el detail que ve el
    cliente es genérico para no revelar la deny-list (mismo criterio que
    api/service.py).
    """
    for indice, paso in enumerate(steps):
        if not _is_service_forbidden(paso.domain, paso.service):
            continue
        logger.warning(
            "scene.forbidden_service",
            momento=momento,
            scene_id=scene_id,
            step=indice,
            domain=paso.domain,
            service=paso.service,
            ip=request.client.host if request.client else "unknown",
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="service not allowed",
        )


async def _buscar_escena(request: Request, scene_id: str) -> Scene:
    """Lee la escena o levanta 404. Un id malformado no puede existir → 404."""
    if not _SCENE_ID_PATTERN.fullmatch(scene_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Scene not found")
    db: Database = request.app.state.db
    fila = await db.get_scene(scene_id, _tenant_id(request))
    if fila is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Scene not found")
    return Scene.model_validate(fila)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


@router.get("", response_model=SceneListResponse, summary="Lista las escenas del cliente")
async def list_scenes(
    request: Request,
    _: None = Depends(require_api_key),
) -> SceneListResponse:
    """Todas las escenas guardadas, en orden de creación (las nuevas al final)."""
    db: Database = request.app.state.db
    filas = await db.list_scenes(_tenant_id(request))
    escenas = [Scene.model_validate(fila) for fila in filas]
    return SceneListResponse(scenes=escenas, count=len(escenas))


@router.post(
    "",
    response_model=Scene,
    status_code=status.HTTP_201_CREATED,
    summary="Crea una escena",
)
async def create_scene(
    body: SceneInput,
    request: Request,
    _: None = Depends(require_api_key),
) -> Scene:
    """
    Persiste una escena nueva. El id lo genera el mirror (hex32).

    Valida la deny-list ANTES de guardar: una escena con un servicio prohibido
    nunca llegaría a ejecutarse, así que no tiene sentido dejarla en la DB
    esperando para dar un 403 más tarde.
    """
    _rechazar_servicios_prohibidos(request, body.steps, momento="guardar")

    db: Database = request.app.state.db
    tenant_id = _tenant_id(request)

    # Tope de cordura: la lista entera viaja en cada GET y se renderiza completa.
    if await db.count_scenes(tenant_id) >= SCENES_PER_TENANT_MAX:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Maximo de {SCENES_PER_TENANT_MAX} escenas alcanzado",
        )

    scene_id = uuid.uuid4().hex
    fila = await db.create_scene(
        scene_id=scene_id,
        name=body.name,
        icon=body.icon,
        accent=body.accent,
        description=body.description,
        confirm_required=body.confirm_required,
        steps=[paso.model_dump() for paso in body.steps],
        cameras=body.cameras,
        tenant_id=tenant_id,
    )
    logger.info(
        "scene.created",
        scene_id=scene_id,
        name=body.name,
        steps=len(body.steps),
        cameras=len(body.cameras),
    )
    return Scene.model_validate(fila)


@router.get("/{scene_id}", response_model=Scene, summary="Obtiene una escena por id")
async def get_scene(
    scene_id: str,
    request: Request,
    _: None = Depends(require_api_key),
) -> Scene:
    return await _buscar_escena(request, scene_id)


@router.put("/{scene_id}", response_model=Scene, summary="Reemplaza una escena")
async def update_scene(
    scene_id: str,
    body: SceneInput,
    request: Request,
    _: None = Depends(require_api_key),
) -> Scene:
    """Reemplazo total (no PATCH): el frontend manda la escena completa."""
    if not _SCENE_ID_PATTERN.fullmatch(scene_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Scene not found")
    _rechazar_servicios_prohibidos(request, body.steps, momento="guardar", scene_id=scene_id)

    db: Database = request.app.state.db
    fila = await db.update_scene(
        scene_id,
        name=body.name,
        icon=body.icon,
        accent=body.accent,
        description=body.description,
        confirm_required=body.confirm_required,
        steps=[paso.model_dump() for paso in body.steps],
        cameras=body.cameras,
        tenant_id=_tenant_id(request),
    )
    if fila is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Scene not found")

    logger.info("scene.updated", scene_id=scene_id, name=body.name, steps=len(body.steps))
    return Scene.model_validate(fila)


@router.delete(
    "/{scene_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Borra una escena",
)
async def delete_scene(
    scene_id: str,
    request: Request,
    _: None = Depends(require_api_key),
) -> Response:
    if not _SCENE_ID_PATTERN.fullmatch(scene_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Scene not found")

    db: Database = request.app.state.db
    if not await db.delete_scene(scene_id, _tenant_id(request)):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Scene not found")

    logger.info("scene.deleted", scene_id=scene_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ---------------------------------------------------------------------------
# Activación
# ---------------------------------------------------------------------------


@router.post(
    "/{scene_id}/activate",
    response_model=SceneActivateResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Activa una escena (respuesta inmediata 202)",
)
async def activate_scene(
    scene_id: str,
    request: Request,
    _: None = Depends(require_api_key),
) -> SceneActivateResponse:
    """
    Dispara los pasos EN ORDEN y responde 202 al toque.

    Las correlaciones se registran ANTES de responder para poder devolver la
    lista completa de correlation_ids: el frontend ya sabe qué
    `service_complete` / `service_timeout` esperar por /ws/state antes de que
    llegue el primero.
    """
    escena = await _buscar_escena(request, scene_id)

    # Segunda pasada por la deny-list: la escena pudo guardarse cuando el par
    # (domain, service) todavía estaba permitido.
    _rechazar_servicios_prohibidos(request, escena.steps, momento="activar", scene_id=scene_id)

    store = request.app.state.store
    upstream = request.app.state.upstream
    correlations: CorrelationTracker = request.app.state.correlations
    db: Database = request.app.state.db
    settings = request.app.state.settings

    if not store.connected:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream HA desconectado. Reintentá en unos segundos.",
        )

    # CorrelationTracker encola por entidad (FIFO), asi que una escena que toca
    # varias veces la misma entidad —un climate con set_hvac_mode +
    # set_temperature + set_fan_mode— registra las 3 correlaciones sin pisarse:
    # cada state_changed va resolviendo la mas antigua. El tracking de la escena
    # se hace por correlation_id (no por resolve_by_entity), asi ninguna queda a
    # merced de que otra sobre la misma entidad la reemplace.
    preparados: list[_PasoPreparado] = []
    for paso in escena.steps:
        correlation_id = correlations.generate_id()
        corr = await correlations.register(
            correlation_id=correlation_id,
            domain=paso.domain,
            service=paso.service,
            entity_id=paso.entity_id,
        )
        await db.log_service_call(
            correlation_id=correlation_id,
            domain=paso.domain,
            service=paso.service,
            entity_id=paso.entity_id,
            target={"entity_id": paso.entity_id},
        )
        SERVICE_CALLS_TOTAL.labels(domain=paso.domain, service=paso.service).inc()

        payload: dict[str, Any] = {"target": {"entity_id": paso.entity_id}}
        if paso.data:
            payload["service_data"] = paso.data

        preparados.append(
            _PasoPreparado(
                domain=paso.domain,
                service=paso.service,
                entity_id=paso.entity_id,
                payload=payload,
                correlation_id=correlation_id,
                future=corr.future,
            )
        )

    await db.touch_scene_activation(scene_id, _tenant_id(request))

    asyncio.create_task(
        _ejecutar_escena(
            scene_id=scene_id,
            pasos=preparados,
            upstream=upstream,
            correlations=correlations,
            store=store,
            db=db,
            timeout=settings.service_call_timeout,
        ),
        name=f"scene_activate_{scene_id[:8]}",
    )

    logger.info(
        "scene.activated",
        scene_id=scene_id,
        name=escena.name,
        steps=len(preparados),
    )

    return SceneActivateResponse(
        scene_id=scene_id,
        steps=len(preparados),
        correlation_ids=[paso.correlation_id for paso in preparados],
    )


async def _ejecutar_escena(
    scene_id: str,
    pasos: list[_PasoPreparado],
    upstream: object,
    correlations: CorrelationTracker,
    store: object,
    db: Database,
    timeout: float,
) -> None:
    """
    Tarea background: despacha todos los pasos y trackea sus confirmaciones.

    El arreglo del "se cuelgan las escenas" es partir el trabajo en dos fases,
    en vez de reusar _execute_and_track (que acopla envio + espera y por eso
    hacia N * timeout secuencial):

    Fase 1 — DESPACHO en orden. `send_service_call` espera solo el `result`
    (ACK) del WS de HA, que es de milisegundos, NO el state_changed. Por eso
    awaitear cada envio en secuencia PRESERVA el orden de salida (bajar la
    persiana antes de apagar la luz) sin bloquear la escena: nunca se paga el
    service_call_timeout entre pasos. Una escena de 12 equipos idempotentes que
    antes tardaba ~60s (12 x 5s) ahora despacha todo en < ~2s.

    Fase 2 — CONFIRMACION en paralelo. Las esperas por state_changed corren
    todas a la vez (asyncio.gather), asi la escena entera cuesta ~un timeout y
    no N. Cada correlacion se resuelve por su correlation_id (el que ya viaja en
    el 202) y el tracker encola por entidad, asi que los pasos multiples sobre
    un mismo climate ya no se pisan.

    M3 — un paso que NO se pudo DESPACHAR (HA caido a mitad -> el envio lanza)
    NO aborta los demas: se limpia y se cuenta. Al terminar, si hubo fallos de
    despacho se emite scene_partial por /ws/state para que el front avise (el
    usuario ya vio "Listo"). Un timeout de CONFIRMACION no cuenta como fallo: un
    equipo ya en su estado destino no emite state_changed y eso es lo normal —
    justo el caso que colgaba la escena.
    """
    # ── Fase 1: despacho ordenado (rapido, no espera confirmacion) ───────────
    confirmables: list[_PasoPreparado] = []
    fallos_despacho = 0
    for indice, paso in enumerate(pasos):
        try:
            await upstream.send_service_call(  # type: ignore[attr-defined]
                paso.domain, paso.service, paso.payload
            )
            confirmables.append(paso)
        except Exception as exc:
            fallos_despacho += 1
            await _limpiar_paso_no_despachado(paso, correlations, store, db)
            logger.warning(
                "scene.step_failed",
                scene_id=scene_id,
                step=indice,
                domain=paso.domain,
                service=paso.service,
                entity_id=paso.entity_id,
                correlation_id=paso.correlation_id,
                exc=str(exc),
            )

    # ── Fase 2: confirmaciones en paralelo (no se bloquean entre si) ─────────
    if confirmables:
        await asyncio.gather(
            *(_confirmar_paso(paso, correlations, store, db, timeout) for paso in confirmables)
        )

    # ── M3: aviso de escena a medias si algun paso no llego a despacharse ────
    if fallos_despacho:
        await _emitir_scene_partial(
            store, scene_id, fallidos=fallos_despacho, total=len(pasos)
        )

    logger.info(
        "scene.finished",
        scene_id=scene_id,
        steps=len(pasos),
        fallos_despacho=fallos_despacho,
    )


async def _confirmar_paso(
    paso: _PasoPreparado,
    correlations: CorrelationTracker,
    store: object,
    db: Database,
    timeout: float,
) -> None:
    """
    Espera la confirmacion de UN paso ya despachado.

    Es la mitad de espera de _execute_and_track, replicada aca porque ese helper
    acopla el envio (que en la escena ya se hizo en la fase 1) con la espera.
    Exito: el Future lo resuelve _handle_event del upstream al llegar el
    state_changed y ese mismo handler ya hizo el fanout de service_complete;
    aca solo se sella la DB y las metricas. Timeout: se avisa service_timeout y
    se cierra la correlacion. El shield evita que el timeout de wait_for cancele
    el Future si un state_changed tardio todavia lo estuviera resolviendo.
    """
    t_start = time.monotonic()
    try:
        await asyncio.wait_for(asyncio.shield(paso.future), timeout=timeout)
        elapsed = time.monotonic() - t_start
        SERVICE_CALL_DURATION.labels(domain=paso.domain).observe(elapsed)
        SERVICE_CALL_CONFIRMED.labels(domain=paso.domain).inc()
        await db.complete_service_call(paso.correlation_id, "confirmed")
    except asyncio.TimeoutError:
        SERVICE_CALL_TIMEOUT.labels(domain=paso.domain).inc()
        await store.fanout_service_timeout(paso.correlation_id)  # type: ignore[attr-defined]
        await db.complete_service_call(paso.correlation_id, "timeout")
        await correlations.remove(paso.correlation_id)


async def _limpiar_paso_no_despachado(
    paso: _PasoPreparado,
    correlations: CorrelationTracker,
    store: object,
    db: Database,
) -> None:
    """
    Cierra un paso que NO se pudo enviar a HA (mirror del except de
    _execute_and_track): sella la DB en 'error', saca la correlacion del tracker
    para que no quede colgada esperando un state_changed que no va a llegar, y
    avisa al front con service_complete(success=False).
    """
    await db.complete_service_call(paso.correlation_id, "error")
    await correlations.remove(paso.correlation_id)
    await store.fanout_service_complete(  # type: ignore[attr-defined]
        correlation_id=paso.correlation_id,
        entity_id=None,
        success=False,
    )


async def _emitir_scene_partial(
    store: object,
    scene_id: str,
    *,
    fallidos: int,
    total: int,
) -> None:
    """
    Fan-out de un evento scene_partial por /ws/state (M3).

    Se arma el dict a mano y se usa store._fanout —el mismo camino que
    fanout_service_complete / fanout_service_timeout usan por dentro— porque el
    modelo tipado (models.py) y un metodo publico dedicado (state_store.py)
    viven fuera de la particion de este modulo. La forma es estable y parte del
    contrato con el front: type/scene_id/failed_steps/total_steps.
    """
    await store._fanout(  # type: ignore[attr-defined]
        {
            "type": "scene_partial",
            "scene_id": scene_id,
            "failed_steps": fallidos,
            "total_steps": total,
        }
    )
