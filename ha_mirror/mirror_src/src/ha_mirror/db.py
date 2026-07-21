"""
Capa de persistencia SQLite con aiosqlite.

Schema embebido (sin Alembic en Fase 1).
- tenants: 1 row, tenant_id constante = 1, shape multi-tenant en frío.
- service_calls_log: historial de calls con estado y correlation_id.
- events_log: log de state_changed (retention 7 días) para debugging post-mortem.

WAL mode habilitado para permitir lectores concurrentes mientras el upstream
escribe eventos (evita lock contention en la Pi).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiosqlite
import structlog

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

_CREATE_TABLES_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

-- Tenant único (N=1). Shape multi-tenant en frío: tenant_id constante = 1.
CREATE TABLE IF NOT EXISTS tenants (
    id              INTEGER PRIMARY KEY,
    ha_url          TEXT NOT NULL,
    display_name    TEXT NOT NULL DEFAULT 'Casa',
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Insertar el tenant único si no existe
INSERT OR IGNORE INTO tenants (id, ha_url)
VALUES (1, 'ws://localhost:8123/api/websocket');

-- Log de service calls (correlación 202 + confirmación WS)
CREATE TABLE IF NOT EXISTS service_calls_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id       INTEGER NOT NULL DEFAULT 1 REFERENCES tenants(id),
    correlation_id  TEXT NOT NULL UNIQUE,
    domain          TEXT NOT NULL,
    service         TEXT NOT NULL,
    entity_id       TEXT,
    target_json     TEXT,
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending|confirmed|timeout|error
    started_at      TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_scl_correlation
    ON service_calls_log(correlation_id);
CREATE INDEX IF NOT EXISTS idx_scl_tenant_time
    ON service_calls_log(tenant_id, started_at DESC);

-- Log de eventos state_changed (debugging post-mortem, retention 7 días)
CREATE TABLE IF NOT EXISTS events_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id       INTEGER NOT NULL DEFAULT 1 REFERENCES tenants(id),
    entity_id       TEXT NOT NULL,
    old_state       TEXT,
    new_state       TEXT,
    fired_at        TEXT NOT NULL,
    received_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_events_tenant_fired
    ON events_log(tenant_id, fired_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_entity
    ON events_log(entity_id, fired_at DESC);
"""


class Database:
    """Wrapper aiosqlite con schema embebido y helpers de dominio."""

    def __init__(self, db_path: Path, events_retention_days: int = 7) -> None:
        self._db_path = db_path
        self._retention_days = events_retention_days
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        """Abre la conexión y crea el schema si no existe."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(str(self._db_path))
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(_CREATE_TABLES_SQL)
        await self._conn.commit()
        logger.info("db.connected", path=str(self._db_path))

    async def close(self) -> None:
        """Cierra la conexión limpiamente."""
        if self._conn:
            await self._conn.close()
            self._conn = None

    @asynccontextmanager
    async def _cursor(self) -> AsyncIterator[aiosqlite.Cursor]:
        if self._conn is None:
            raise RuntimeError("DB no conectada — llamar connect() primero")
        async with self._conn.cursor() as cur:
            yield cur

    # -------------------------------------------------------------------------
    # Service calls log
    # -------------------------------------------------------------------------

    async def log_service_call(
        self,
        correlation_id: str,
        domain: str,
        service: str,
        entity_id: str | None,
        target: dict[str, Any] | None,
    ) -> None:
        """Registra un service call en estado 'pending'."""
        if self._conn is None:
            return
        await self._conn.execute(
            """
            INSERT INTO service_calls_log
                (correlation_id, domain, service, entity_id, target_json, status)
            VALUES (?, ?, ?, ?, ?, 'pending')
            """,
            (
                correlation_id,
                domain,
                service,
                entity_id,
                json.dumps(target) if target else None,
            ),
        )
        await self._conn.commit()

    async def complete_service_call(self, correlation_id: str, status: str = "confirmed") -> None:
        """Marca un service call como confirmado o timeout."""
        if self._conn is None:
            return
        await self._conn.execute(
            """
            UPDATE service_calls_log
            SET status = ?, completed_at = datetime('now')
            WHERE correlation_id = ?
            """,
            (status, correlation_id),
        )
        await self._conn.commit()

    # -------------------------------------------------------------------------
    # Events log
    # -------------------------------------------------------------------------

    async def log_event(
        self,
        entity_id: str,
        old_state: str | None,
        new_state: str | None,
        fired_at: datetime,
    ) -> None:
        """Guarda un evento state_changed en el log (opcional, para debugging)."""
        if self._conn is None:
            return
        await self._conn.execute(
            """
            INSERT INTO events_log (entity_id, old_state, new_state, fired_at)
            VALUES (?, ?, ?, ?)
            """,
            (entity_id, old_state, new_state, fired_at.isoformat()),
        )
        await self._conn.commit()

    async def purge_old_events(self) -> int:
        """Elimina eventos más viejos que retention_days. Retorna filas eliminadas."""
        if self._conn is None:
            return 0
        # received_at es insertado por SQLite datetime('now') → formato naive sin sufijo tz.
        # El cutoff debe ser naive también para que la comparación de strings ISO funcione.
        cutoff = (
            datetime.now(UTC).replace(tzinfo=None) - timedelta(days=self._retention_days)
        ).isoformat()
        async with self._conn.execute(
            "DELETE FROM events_log WHERE received_at < ?", (cutoff,)
        ) as cur:
            deleted = cur.rowcount
        await self._conn.commit()
        if deleted:
            logger.info("db.events_purged", count=deleted, retention_days=self._retention_days)
        return deleted

    # -------------------------------------------------------------------------
    # Tenant config
    # -------------------------------------------------------------------------

    async def update_tenant_ha_url(self, tenant_id: int, ha_url: str) -> None:
        """Actualiza la URL del HA para el tenant."""
        if self._conn is None:
            return
        await self._conn.execute(
            "UPDATE tenants SET ha_url = ?, updated_at = datetime('now') WHERE id = ?",
            (ha_url, tenant_id),
        )
        await self._conn.commit()

    async def get_tenant(self, tenant_id: int) -> dict[str, Any] | None:
        """Lee la fila del tenant."""
        if self._conn is None:
            return None
        async with self._conn.execute("SELECT * FROM tenants WHERE id = ?", (tenant_id,)) as cur:
            row = await cur.fetchone()
            if row is None:
                return None
            return dict(row)
