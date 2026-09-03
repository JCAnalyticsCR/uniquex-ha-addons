"""
POST /api/service/{domain}/{service} — ejecuta un service call en HA.

Responde INMEDIATAMENTE con 202 Accepted + correlation_id.
El call upstream se hace en background.
La confirmación llega por /ws/state como {"type": "service_complete", ...}.
Si pasan service_call_timeout segundos sin state_changed: service_timeout.
"""

from __future__ import annotations

import asyncio
import time

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status

from ha_mirror.auth import require_api_key
from ha_mirror.correlations import CorrelationTracker
from ha_mirror.db import Database
from ha_mirror.errors import HaConnectError, UpstreamNotReadyError
from ha_mirror.models import ServiceCallRequest, ServiceCallResponse
from ha_mirror.prometheus_metrics import (
    SERVICE_CALL_CONFIRMED,
    SERVICE_CALL_DURATION,
    SERVICE_CALL_TIMEOUT,
    SERVICE_CALLS_TOTAL,
)

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Fix de seguridad C1 — lista negra de servicios administrativos.
# El frontend controla la casa (luces, persianas, clima, media, cerraduras...),
# pero NUNCA debe poder administrar el sistema. Sin esto, una API key
# comprometida daría control total del gateway (reboot, backups, addons, RCE).
# ---------------------------------------------------------------------------
_FORBIDDEN_DOMAINS: frozenset[str] = frozenset(
    {
        "hassio",
        "supervisor",
        "backup",
        "hardware",
        "host",
        "addon",
        "addons",
        # Fix C4 — "update.install" arranca un salto de versión del Core desde la casa.
        # Un token comprometido podría forzar un update no solicitado. Bloqueado en el
        # BFF y también acá (defensa en profundidad).
        "update",
    }
)
_FORBIDDEN_SERVICES: frozenset[tuple[str, str]] = frozenset(
    {
        ("homeassistant", "restart"),
        ("homeassistant", "stop"),
        ("homeassistant", "reload_all"),
        ("homeassistant", "reload_core_config"),
        ("homeassistant", "reload_config_entry"),
        ("homeassistant", "reload_custom_templates"),
        ("recorder", "purge"),
        ("recorder", "purge_entities"),
        ("system_log", "clear"),
    }
)


def _is_service_forbidden(domain: str, service: str) -> bool:
    """True si (domain, service) es administrativo y no debe exponerse al frontend."""
    d = domain.lower().strip()
    s = service.lower().strip()
    return d in _FORBIDDEN_DOMAINS or (d, s) in _FORBIDDEN_SERVICES


def _crestron_entity_arg(body: ServiceCallRequest) -> str | list[str] | None:
    """Entidad(es) objetivo del call: entity_id directo o target.entity_id."""
    if body.entity_id is not None:
        return body.entity_id
    if body.target:
        return body.target.get("entity_id")
    return None


def _crestron_route(crestron: object, entity_arg: str | list[str] | None) -> bool:
    """True si el/los entity_id objetivo pertenecen al connector Crestron."""
    if crestron is None or entity_arg is None:
        return False
    if isinstance(entity_arg, str):
        return crestron.owns_entity(entity_arg)  # type: ignore[attr-defined]
    if isinstance(entity_arg, list):
        return any(
            isinstance(e, str) and crestron.owns_entity(e)  # type: ignore[attr-defined]
            for e in entity_arg
        )
    return False


async def _execute_crestron_call(
    *,
    crestron: object,
    store: object,
    db: Database,
    correlation_id: str,
    domain: str,
    service: str,
    entity_arg: str | list[str] | None,
    confirm_entity_id: str | None,
    data: dict,
) -> None:
    """
    Tarea background: ejecuta el control Crestron y confirma por WS.

    Mantiene el mismo contrato que la vía HA: el frontend ya recibió 202 +
    correlation_id y espera un service_complete (success True/False) por /ws/state.
    """
    try:
        await crestron.handle_service_call(domain, service, entity_arg, data)  # type: ignore[attr-defined]
        await store.fanout_service_complete(correlation_id, confirm_entity_id, True)  # type: ignore[attr-defined]
        await db.complete_service_call(correlation_id, "confirmed")
        logger.info("service.crestron_confirmed", correlation_id=correlation_id)
    except Exception as exc:  # noqa: BLE001 — cualquier fallo se reporta como service_complete=False
        await store.fanout_service_complete(correlation_id, confirm_entity_id, False)  # type: ignore[attr-defined]
        await db.complete_service_call(correlation_id, "error")
        logger.error(
            "service.crestron_error",
            correlation_id=correlation_id,
            domain=domain,
            service=service,
            exc=str(exc),
        )


@router.post(
    "/api/service/{domain}/{service}",
    response_model=ServiceCallResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ejecuta un service call en HA (respuesta inmediata 202)",
)
async def call_service(
    domain: str,
    service: str,
    body: ServiceCallRequest,
    request: Request,
    _: None = Depends(require_api_key),
) -> ServiceCallResponse:
    """
    Acepta el service call y retorna correlation_id inmediatamente.

    El frontend sabe que el comando fue recibido (202) y espera
    la confirmación por WebSocket (service_complete o service_timeout).

    Dominios soportados según Ev4: cover, media_player, light, switch,
    input_button, script, y cualquier otro que HA acepte.
    """
    # Fix C1 — rechazar servicios administrativos peligrosos (403 sin revelar la lista).
    if _is_service_forbidden(domain, service):
        logger.warning(
            "service.forbidden",
            domain=domain,
            service=service,
            ip=request.client.host if request.client else "unknown",
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="service not allowed",
        )

    store = request.app.state.store
    upstream = request.app.state.upstream
    correlations: CorrelationTracker = request.app.state.correlations
    db: Database = request.app.state.db
    settings = request.app.state.settings

    # --- Enrutado Crestron -------------------------------------------------
    # Si la entidad objetivo pertenece al connector Crestron, la manejamos acá
    # (no va al upstream HA). Se hace ANTES del chequeo store.connected para que
    # el control Crestron funcione aunque el upstream HA esté caído — son buses
    # independientes. Mismo contrato de respuesta que la vía HA: 202 +
    # correlation_id ahora, service_complete/timeout después por /ws/state.
    crestron = getattr(request.app.state, "crestron", None)
    entity_arg = _crestron_entity_arg(body)
    if _crestron_route(crestron, entity_arg):
        correlation_id = correlations.generate_id()
        await db.log_service_call(
            correlation_id=correlation_id,
            domain=domain,
            service=service,
            entity_id=body.entity_id,
            target=body.target,
        )
        SERVICE_CALLS_TOTAL.labels(domain=domain, service=service).inc()
        asyncio.create_task(
            _execute_crestron_call(
                crestron=crestron,
                store=store,
                db=db,
                correlation_id=correlation_id,
                domain=domain,
                service=service,
                entity_arg=entity_arg,
                confirm_entity_id=body.entity_id,
                data=body.service_data,
            ),
            name=f"crestron_call_{correlation_id[:8]}",
        )
        logger.info(
            "service.crestron_accepted",
            correlation_id=correlation_id,
            domain=domain,
            service=service,
            entity_id=body.entity_id,
        )
        return ServiceCallResponse(correlation_id=correlation_id)

    if not store.connected:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream HA desconectado. Reintentá en unos segundos.",
        )

    # Generar correlation_id y registrar ANTES de enviar
    correlation_id = correlations.generate_id()
    corr = await correlations.register(
        correlation_id=correlation_id,
        domain=domain,
        service=service,
        entity_id=body.entity_id,
    )

    # Persistir en DB (status='pending')
    await db.log_service_call(
        correlation_id=correlation_id,
        domain=domain,
        service=service,
        entity_id=body.entity_id,
        target=body.target,
    )

    SERVICE_CALLS_TOTAL.labels(domain=domain, service=service).inc()

    # Construir payload para HA
    service_payload: dict = {}
    if body.entity_id:
        service_payload["target"] = {"entity_id": body.entity_id}
    if body.target:
        service_payload["target"] = {**(service_payload.get("target") or {}), **body.target}
    if body.service_data:
        service_payload["service_data"] = body.service_data

    # Lanzar el call en background — NO esperamos el result aquí
    asyncio.create_task(
        _execute_and_track(
            correlation_id=correlation_id,
            domain=domain,
            service=service,
            payload=service_payload,
            upstream=upstream,
            correlations=correlations,
            store=store,
            db=db,
            timeout=settings.service_call_timeout,
            pending_future=corr.future,
        ),
        name=f"service_call_{correlation_id[:8]}",
    )

    logger.info(
        "service.accepted",
        correlation_id=correlation_id,
        domain=domain,
        service=service,
        entity_id=body.entity_id,
    )

    return ServiceCallResponse(correlation_id=correlation_id)


async def _execute_and_track(
    correlation_id: str,
    domain: str,
    service: str,
    payload: dict,
    upstream: object,
    correlations: CorrelationTracker,
    store: object,
    db: Database,
    timeout: float,
    pending_future: asyncio.Future,
) -> None:
    """
    Tarea background: envía el call al upstream y espera confirmación.

    La confirmación llega en dos vías:
    1. El Future se resuelve cuando _handle_event en ha_upstream detecta
       state_changed para la entidad correlacionada.
    2. Si el Future no se resuelve en `timeout` segundos, se emite service_timeout.
    """
    t_start = time.monotonic()
    try:
        # Enviar al upstream HA
        await upstream.send_service_call(domain, service, payload)  # type: ignore[attr-defined]

        # Esperar confirmación vía state_changed (el Future lo resuelve _handle_event)
        try:
            success = await asyncio.wait_for(asyncio.shield(pending_future), timeout=timeout)
            elapsed = time.monotonic() - t_start
            SERVICE_CALL_DURATION.labels(domain=domain).observe(elapsed)
            SERVICE_CALL_CONFIRMED.labels(domain=domain).inc()
            await db.complete_service_call(correlation_id, "confirmed")
            logger.info(
                "service.confirmed",
                correlation_id=correlation_id,
                elapsed=round(elapsed, 3),
            )
        except asyncio.TimeoutError:
            SERVICE_CALL_TIMEOUT.labels(domain=domain).inc()
            await store.fanout_service_timeout(correlation_id)  # type: ignore[attr-defined]
            await db.complete_service_call(correlation_id, "timeout")
            await correlations.remove(correlation_id)
            logger.warning(
                "service.timeout",
                correlation_id=correlation_id,
                timeout=timeout,
            )

    except (UpstreamNotReadyError, HaConnectError) as exc:
        await db.complete_service_call(correlation_id, "error")
        await correlations.remove(correlation_id)
        # Notificar al cliente que el call falló
        await store.fanout_service_complete(  # type: ignore[attr-defined]
            correlation_id=correlation_id,
            entity_id=None,
            success=False,
        )
        logger.error("service.upstream_error", correlation_id=correlation_id, exc=str(exc))
    except Exception as exc:
        await db.complete_service_call(correlation_id, "error")
        await correlations.remove(correlation_id)
        logger.exception("service.unexpected_error", correlation_id=correlation_id, exc=str(exc))
