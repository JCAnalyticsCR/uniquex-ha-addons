"""
Capa de persistencia SQLite con aiosqlite.

Schema embebido (sin Alembic en Fase 1).
- tenants: 1 row, tenant_id constante = 1, shape multi-tenant en frío.
- service_calls_log: historial de calls con estado y correlation_id.
- events_log: log de state_changed (retention 7 días) para debugging post-mortem.
- scenes: escenas custom del cliente (CRUD /api/scenes), persistentes en /data.

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


def utc_now_iso() -> str:
    """
    Timestamp ISO8601 con offset explícito (`...+00:00`).

    Distinto a propósito del `datetime('now')` de SQLite (naive, sin sufijo tz)
    que usan events_log y service_calls_log: los timestamps de las escenas
    viajan al frontend y `new Date("2026-07-23T21:36:00")` se interpreta como
    hora LOCAL del navegador. Con el offset explícito no hay ambigüedad.
    Sigue siendo ordenable lexicográficamente porque el offset es constante.
    """
    return datetime.now(UTC).isoformat(timespec="seconds")


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

-- Escenas custom del cliente (las arma desde la app; NO son las scene.* de HA).
-- Diseñada COMPLETA de entrada a propósito: no hay migraciones y
-- CREATE TABLE IF NOT EXISTS no agrega columnas a una tabla ya creada. La DB
-- vive en /data y sobrevive a los updates del add-on: un schema incompleto
-- se arrastraría para siempre.
CREATE TABLE IF NOT EXISTS scenes (
    id                 TEXT PRIMARY KEY,                 -- uuid4().hex (32 chars)
    tenant_id          INTEGER NOT NULL DEFAULT 1 REFERENCES tenants(id),
    name               TEXT NOT NULL,
    icon               TEXT NOT NULL,                    -- moon|sun|home|away|movie|gym|party|sleep|shield|sparkles
    accent             TEXT NOT NULL,                    -- warm|cool|gold|green|neutral
    description        TEXT NOT NULL DEFAULT '',
    confirm_required   INTEGER NOT NULL DEFAULT 0,       -- 0|1 (SQLite no tiene BOOLEAN)
    steps_json         TEXT NOT NULL,                    -- [{"domain","service","entity_id","data"}]
    cameras_json       TEXT NOT NULL DEFAULT '[]',       -- ["camera.nvr_c08_garaje", ...]
    created_at         TEXT NOT NULL,                    -- ISO8601 con offset, escrito desde Python
    updated_at         TEXT NOT NULL,
    last_activated_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_scenes_tenant_created
    ON scenes(tenant_id, created_at);
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

    def _require_conn(self) -> aiosqlite.Connection:
        """Conexión o error ruidoso — usado por el CRUD de escenas (ver sección)."""
        if self._conn is None:
            raise RuntimeError("DB no conectada — llamar connect() primero")
        return self._conn

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

    # -------------------------------------------------------------------------
    # Escenas custom (CRUD de /api/scenes)
    #
    # Decisión explícita: a diferencia de los métodos de arriba (que fallan en
    # silencio con `if self._conn is None: return`), esta sección usa
    # _require_conn() y revienta con RuntimeError. Devolver "lista vacía" o
    # "no encontrada" cuando en realidad la DB no está montada haría que la app
    # borrara de la UI escenas que sí existen en /data — mejor un 500 ruidoso.
    #
    # Los métodos devuelven dicts listos para `Scene.model_validate(...)`:
    # el (de)serializado JSON de steps/cameras vive acá, igual que
    # log_service_call serializa `target`.
    # -------------------------------------------------------------------------

    @staticmethod
    def _scene_row_to_dict(row: aiosqlite.Row) -> dict[str, Any]:
        """Fila cruda → dict con steps/cameras ya parseados."""
        return {
            "id": row["id"],
            "name": row["name"],
            "icon": row["icon"],
            "accent": row["accent"],
            "description": row["description"] or "",
            "confirm_required": bool(row["confirm_required"]),
            "steps": json.loads(row["steps_json"]),
            "cameras": json.loads(row["cameras_json"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "last_activated_at": row["last_activated_at"],
        }

    async def count_scenes(self, tenant_id: int = 1) -> int:
        """Cuántas escenas tiene el tenant (para el tope de cordura)."""
        conn = self._require_conn()
        async with conn.execute(
            "SELECT COUNT(*) AS n FROM scenes WHERE tenant_id = ?", (tenant_id,)
        ) as cur:
            row = await cur.fetchone()
            return int(row["n"]) if row else 0

    async def list_scenes(self, tenant_id: int = 1) -> list[dict[str, Any]]:
        """Todas las escenas del tenant, en orden de creación (las nuevas al final)."""
        conn = self._require_conn()
        async with conn.execute(
            "SELECT * FROM scenes WHERE tenant_id = ? ORDER BY created_at, id",
            (tenant_id,),
        ) as cur:
            rows = await cur.fetchall()
        return [self._scene_row_to_dict(row) for row in rows]

    async def get_scene(self, scene_id: str, tenant_id: int = 1) -> dict[str, Any] | None:
        """Una escena por id, o None si no existe para ese tenant."""
        conn = self._require_conn()
        async with conn.execute(
            "SELECT * FROM scenes WHERE id = ? AND tenant_id = ?",
            (scene_id, tenant_id),
        ) as cur:
            row = await cur.fetchone()
            if row is None:
                return None
            return self._scene_row_to_dict(row)

    async def create_scene(
        self,
        *,
        scene_id: str,
        name: str,
        icon: str,
        accent: str,
        description: str,
        confirm_required: bool,
        steps: list[dict[str, Any]],
        cameras: list[str],
        tenant_id: int = 1,
    ) -> dict[str, Any]:
        """Inserta una escena nueva y devuelve la fila resultante."""
        conn = self._require_conn()
        ahora = utc_now_iso()
        await conn.execute(
            """
            INSERT INTO scenes
                (id, tenant_id, name, icon, accent, description, confirm_required,
                 steps_json, cameras_json, created_at, updated_at, last_activated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                scene_id,
                tenant_id,
                name,
                icon,
                accent,
                description,
                int(confirm_required),
                json.dumps(steps, separators=(",", ":")),
                json.dumps(cameras, separators=(",", ":")),
                ahora,
                ahora,
            ),
        )
        await conn.commit()
        creada = await self.get_scene(scene_id, tenant_id)
        if creada is None:  # pragma: no cover — solo si otro proceso borra en el medio
            raise RuntimeError(f"La escena {scene_id} desapareció tras el INSERT")
        return creada

    async def update_scene(
        self,
        scene_id: str,
        *,
        name: str,
        icon: str,
        accent: str,
        description: str,
        confirm_required: bool,
        steps: list[dict[str, Any]],
        cameras: list[str],
        tenant_id: int = 1,
    ) -> dict[str, Any] | None:
        """Reemplaza una escena entera. None si el id no existe (→ 404 en el router)."""
        conn = self._require_conn()
        async with conn.execute(
            """
            UPDATE scenes
            SET name = ?, icon = ?, accent = ?, description = ?, confirm_required = ?,
                steps_json = ?, cameras_json = ?, updated_at = ?
            WHERE id = ? AND tenant_id = ?
            """,
            (
                name,
                icon,
                accent,
                description,
                int(confirm_required),
                json.dumps(steps, separators=(",", ":")),
                json.dumps(cameras, separators=(",", ":")),
                utc_now_iso(),
                scene_id,
                tenant_id,
            ),
        ) as cur:
            actualizadas = cur.rowcount
        await conn.commit()
        if not actualizadas:
            return None
        return await self.get_scene(scene_id, tenant_id)

    async def delete_scene(self, scene_id: str, tenant_id: int = 1) -> bool:
        """True si borró algo; False si el id no existía (→ 404 en el router)."""
        conn = self._require_conn()
        async with conn.execute(
            "DELETE FROM scenes WHERE id = ? AND tenant_id = ?", (scene_id, tenant_id)
        ) as cur:
            borradas = cur.rowcount
        await conn.commit()
        return bool(borradas)

    async def touch_scene_activation(self, scene_id: str, tenant_id: int = 1) -> str | None:
        """Sella last_activated_at al aceptar la activación. Devuelve el timestamp."""
        conn = self._require_conn()
        ahora = utc_now_iso()
        async with conn.execute(
            "UPDATE scenes SET last_activated_at = ? WHERE id = ? AND tenant_id = ?",
            (ahora, scene_id, tenant_id),
        ) as cur:
            actualizadas = cur.rowcount
        await conn.commit()
        return ahora if actualizadas else None
