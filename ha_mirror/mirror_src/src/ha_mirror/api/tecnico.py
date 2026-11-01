"""
/api/tecnico/* — el canal técnico (0.54.0). Ver `ha_mirror/tecnico.py`.

Cada ruta exige la `X-API-Key` de siempre Y la firma técnica de la plataforma.
Un pedido sin firma, con firma vencida, repetida o de otra caja contesta 401 sin
decir por qué (el motivo queda en el log).
"""

from __future__ import annotations

from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from ha_mirror.auth import require_api_key
from ha_mirror.errors import HaProtocolError, UpstreamNotReadyError
from ha_mirror.tecnico import (
    CanalTecnico,
    MemoriaDeNonces,
    TecnicoNoAutorizado,
    TecnicoProhibido,
    verificar_firma,
)

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/tecnico", tags=["tecnico"])


async def exigir_firma_tecnica(request: Request) -> None:
    app = request.app
    nonces = getattr(app.state, "nonces_tecnicos", None)
    if nonces is None:
        nonces = app.state.nonces_tecnicos = MemoriaDeNonces()
    identidad = getattr(app.state, "device_identity", None)
    # La ruta CRUDA: `request.url.path` viene decodificada y un `%2F` firmado no
    # sería el mismo texto que se verifica.
    crudo = request.scope.get("raw_path")
    camino = crudo.decode("latin-1") if isinstance(crudo, bytes) else request.url.path
    ruta = camino + (f"?{request.url.query}" if request.url.query else "")
    h = request.headers
    try:
        verificar_firma(
            device_id=getattr(identidad, "device_id", None),
            metodo=request.method,
            ruta=ruta,
            cuerpo=await request.body(),
            firma=h.get("x-uniquex-tecnico-firma"),
            key_id=h.get("x-uniquex-tecnico-key-id"),
            ts=h.get("x-uniquex-tecnico-ts"),
            nonce=h.get("x-uniquex-tecnico-nonce"),
            nonces=nonces,
        )
    except TecnicoNoAutorizado as exc:
        logger.warning("tecnico.rechazado", motivo=str(exc), ruta=request.url.path)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="No autorizado"
        ) from exc
    logger.warning("tecnico.pedido", metodo=request.method, ruta=request.url.path)


def _canal(request: Request) -> CanalTecnico:
    app = request.app
    canal = getattr(app.state, "canal_tecnico", None)
    if canal is None:
        settings = app.state.settings

        def _cliente() -> Any:
            from ha_mirror.ha_flow_client import HaFlowClient

            url = getattr(settings, "ha_http_url", None)
            try:
                token = settings.get_ha_token()
            except Exception:  # noqa: BLE001 — sin token no hay formularios, no es un 500
                return None
            return HaFlowClient(url, token) if url and token else None

        canal = app.state.canal_tecnico = CanalTecnico(
            app.state.upstream, app.state.store, _cliente
        )
    return canal


async def _hacer(coro: Any) -> Any:
    try:
        return await coro
    except TecnicoProhibido as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except UpstreamNotReadyError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Sin conexión con Home Assistant",
        ) from exc
    except HaProtocolError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)[:300]) from exc


DEPENDENCIAS = [Depends(require_api_key), Depends(exigir_firma_tecnica)]


class PedidoActualizar(BaseModel):
    entity_id: str = Field(min_length=8, max_length=120)
    respaldo: bool = True


class PedidoFlujo(BaseModel):
    handler: str = Field(min_length=2, max_length=60)


class PedidoAvanzar(BaseModel):
    datos: dict[str, Any] = Field(default_factory=dict)


class PedidoRepo(BaseModel):
    repositorio: str = Field(min_length=3, max_length=200)


@router.get(
    "/estado", dependencies=DEPENDENCIAS, summary="Integraciones y actualizaciones de la caja"
)
async def estado(request: Request) -> dict[str, Any]:
    return await _hacer(_canal(request).estado())


@router.post(
    "/tienda/recargar", dependencies=DEPENDENCIAS, summary="Refresca la tienda del Supervisor"
)
async def recargar_tienda(request: Request) -> dict[str, Any]:
    return await _hacer(_canal(request).recargar_tienda())


@router.post(
    "/actualizar", dependencies=DEPENDENCIAS, summary="Instala una actualización (con respaldo)"
)
async def actualizar(request: Request, pedido: PedidoActualizar) -> dict[str, Any]:
    return await _hacer(_canal(request).actualizar(pedido.entity_id, pedido.respaldo))


@router.post("/reiniciar", dependencies=DEPENDENCIAS, summary="Reinicia Home Assistant")
async def reiniciar(request: Request) -> dict[str, Any]:
    return await _hacer(_canal(request).reiniciar_ha())


@router.post(
    "/flujo",
    dependencies=DEPENDENCIAS,
    summary="Arranca el formulario de una integración de la lista",
)
async def flujo_iniciar(request: Request, pedido: PedidoFlujo) -> dict[str, Any]:
    return await _hacer(_canal(request).flujo_iniciar(pedido.handler))


@router.get("/flujo/{flow_id}", dependencies=DEPENDENCIAS, summary="En qué paso está un formulario")
async def flujo_leer(request: Request, flow_id: str) -> dict[str, Any]:
    return await _hacer(_canal(request).flujo_leer(flow_id))


@router.post(
    "/flujo/{flow_id}", dependencies=DEPENDENCIAS, summary="Contesta un paso del formulario"
)
async def flujo_avanzar(request: Request, flow_id: str, pedido: PedidoAvanzar) -> dict[str, Any]:
    return await _hacer(_canal(request).flujo_avanzar(flow_id, pedido.datos))


@router.post(
    "/hacs/descargar",
    dependencies=DEPENDENCIAS,
    summary="Descarga una integración de la comunidad de la lista",
)
async def hacs_descargar(request: Request, pedido: PedidoRepo) -> dict[str, Any]:
    return await _hacer(_canal(request).hacs_descargar(pedido.repositorio))
