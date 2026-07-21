"""
Tracking de correlation IDs para service calls.

Cuando el frontend hace POST /api/service → recibe correlation_id.
Cuando llega state_changed por el upstream, se busca si hay correlación
pendiente y se resuelve, permitiendo el fan-out de 'service_complete'.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

import structlog

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


@dataclass
class PendingCorrelation:
    """Registro de un service call en espera de confirmación vía state_changed."""

    correlation_id: str
    domain: str
    service: str
    entity_id: str | None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    # Future que se resuelve cuando llega state_changed o timeout
    future: asyncio.Future[bool] = field(
        default_factory=lambda: asyncio.get_running_loop().create_future()
    )


class CorrelationTracker:
    """
    Mapa en memoria de correlaciones pendientes.

    El diseño es simple para N=1: no hay purga periódica compleja,
    el timeout se maneja con asyncio.wait_for en el caller.
    """

    def __init__(self) -> None:
        # correlation_id → PendingCorrelation
        self._pending: dict[str, PendingCorrelation] = {}
        # entity_id → correlation_id (para resolver por entidad afectada)
        self._entity_map: dict[str, str] = {}
        self._lock = asyncio.Lock()

    def generate_id(self) -> str:
        return str(uuid.uuid4())

    async def register(
        self,
        correlation_id: str,
        domain: str,
        service: str,
        entity_id: str | None,
    ) -> PendingCorrelation:
        """Registra una correlación nueva. Retorna el objeto con el Future."""
        corr = PendingCorrelation(
            correlation_id=correlation_id,
            domain=domain,
            service=service,
            entity_id=entity_id,
        )
        async with self._lock:
            self._pending[correlation_id] = corr
            if entity_id:
                # Si ya hay una correlación para esta entidad, la reemplaza
                self._entity_map[entity_id] = correlation_id

        logger.debug(
            "correlation.registered",
            correlation_id=correlation_id,
            domain=domain,
            service=service,
            entity_id=entity_id,
        )
        return corr

    async def resolve_by_entity(self, entity_id: str, success: bool = True) -> str | None:
        """
        Resuelve la correlación pendiente para una entidad.

        Retorna el correlation_id resuelto, o None si no había pendiente.
        Llamado cuando llega un state_changed del upstream.
        """
        async with self._lock:
            corr_id = self._entity_map.pop(entity_id, None)
            if corr_id is None:
                return None
            corr = self._pending.pop(corr_id, None)

        if corr is None:
            return None

        if not corr.future.done():
            corr.future.set_result(success)

        logger.debug(
            "correlation.resolved",
            correlation_id=corr_id,
            entity_id=entity_id,
            success=success,
        )
        return corr_id

    async def remove(self, correlation_id: str) -> None:
        """Elimina una correlación (por timeout o cancelación)."""
        async with self._lock:
            corr = self._pending.pop(correlation_id, None)
            if corr and corr.entity_id:
                self._entity_map.pop(corr.entity_id, None)

    @property
    def pending_count(self) -> int:
        return len(self._pending)
