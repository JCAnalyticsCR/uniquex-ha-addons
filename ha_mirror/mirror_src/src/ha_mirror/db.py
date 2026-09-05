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

-- Preferencias KV del cliente (layout del Inicio, etc.). Single-tenant.
-- Diseñada COMPLETA de entrada: ver nota en scenes sobre migraciones.
CREATE TABLE IF NOT EXISTS preferences (
    tenant_id   INTEGER NOT NULL DEFAULT 1 REFERENCES tenants(id),
    key         TEXT NOT NULL,
    value_json  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (tenant_id, key)
);

-- Overrides de organización por entidad (habitación, nombre visible, icono,
-- orden, ocultar). El cliente los gestiona desde la app sin tocar HA.
-- Diseñada COMPLETA de entrada: ver nota en scenes sobre migraciones.
CREATE TABLE IF NOT EXISTS entity_overrides (
    tenant_id    INTEGER NOT NULL DEFAULT 1 REFERENCES tenants(id),
    entity_id    TEXT NOT NULL,
    room_id      TEXT,               -- referencia a custom_rooms.room_id o área de HA
    display_name TEXT,               -- nombre visible personalizado
    icon         TEXT,               -- icono (slug, ej: "lightbulb")
    hidden       INTEGER NOT NULL DEFAULT 0,  -- 0|1
    sort_order   INTEGER,            -- orden dentro de la habitación
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (tenant_id, entity_id)
);

-- Habitaciones custom creadas por el cliente (complementan las áreas de HA).
-- room_id siempre tiene el prefijo "custom:" para distinguirlas de las áreas HA.
CREATE TABLE IF NOT EXISTS custom_rooms (
    tenant_id   INTEGER NOT NULL DEFAULT 1 REFERENCES tenants(id),
    room_id     TEXT NOT NULL,       -- "custom:<slug>" (ej: "custom:sala-de-estar")
    name        TEXT NOT NULL,
    icon        TEXT,
    sort_order  INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (tenant_id, room_id)
);

-- Cámaras sumadas desde la app, además de las que trae la configuración del
-- add-on (`camera_stream_map`).
--
-- POR QUÉ ACÁ Y NO EN LA CONFIGURACIÓN DEL ADD-ON: cambiar una opción del
-- add-on exige permisos de Supervisor, que el token de la app no tiene. Y
-- aunque los tuviera, dejar que el cliente edite la configuración de un add-on
-- es darle una llave que no necesita: por acá solo puede sumar canales del NVR
-- que ya está configurado.
--
-- 🔪 EL `canal` SE GUARDA PORQUE LA URL NO. La dirección RTSP lleva usuario y
-- contraseña del NVR; se vuelve a derivar de un stream que funcione cada vez
-- que hace falta. Así la credencial vive en un solo lugar (la configuración de
-- go2rtc) y no se copia a una segunda base que después habría que proteger,
-- respaldar y borrar igual de bien.
CREATE TABLE IF NOT EXISTS custom_cameras (
    tenant_id   INTEGER NOT NULL DEFAULT 1 REFERENCES tenants(id),
    entity_id   TEXT NOT NULL,       -- "camera.nvr_c21_bodega"
    stream_name TEXT NOT NULL,       -- nombre del stream en go2rtc
    label       TEXT NOT NULL,       -- "Bodega de atrás"
    canal       INTEGER NOT NULL,
    created_at  TEXT NOT NULL,
    PRIMARY KEY (tenant_id, entity_id)
);

-- Registro de entidades conocidas para detección de dispositivos nuevos.
-- acknowledged_at NULL = pendiente de revisión por el cliente.
-- Alembic y CREATE TABLE IF NOT EXISTS NO agrega columnas a una tabla que ya
-- existe). Por eso las columnas de emparejamiento ya están, aunque el código que
-- las llena sea del paso siguiente: si se agregaran después, toda caja ya
-- instalada arrastraría un schema incompleto para siempre.
CREATE TABLE IF NOT EXISTS device_identity (
    id                INTEGER PRIMARY KEY CHECK (id = 1),
    device_id         TEXT NOT NULL UNIQUE,     -- uuid4().hex, aleatorio (NO derivado del hardware)
    public_key        TEXT NOT NULL,            -- base64 de los 32 bytes crudos Ed25519
    key_algorithm     TEXT NOT NULL DEFAULT 'ed25519',
    hardware_id       TEXT,                     -- inventario/soporte; nullable a propósito
    created_at        TEXT NOT NULL,            -- ISO8601 con offset, escrito desde Python

    -- --- Emparejamiento ---
    -- El código del QR se DERIVA de la llave privada (no se guarda). Esta versión
    -- entra en la derivación: subirla da un código nuevo sin tocar el par de
    -- llaves. Sirve para el caso "la calcomanía se fotografió antes de instalar":
    -- se reimprime sin re-aprovisionar la caja.
    claim_code_version INTEGER NOT NULL DEFAULT 1,
    paired_at         TEXT,                     -- cuándo se completó el claim
    paired_house_id   TEXT,                     -- el house_id que asignó el backend
    backend_base_url  TEXT,                     -- a qué plataforma quedó atada esta caja

    -- --- Alcance: el túnel que el backend provisiona AL emparejar ---
    -- Resuelve que hoy hay que averiguar la IP local de la caja y editar el
    -- add-on de Cloudflared a mano (00_INSTALAR_REMOTO.md, paso 6) — cuatro
    -- pasos que exigen a Jeyrell presente, y una IP local clavada que se rompe
    -- con cualquier renovación de DHCP.
    --
    -- La CREDENCIAL del túnel no está acá: es un secreto y va a su propio
    -- archivo, por la misma razón que la llave privada (la base se copia para
    -- depurar y se respalda; los secretos no deben viajar con ella).
    tunnel_provider   TEXT,                     -- 'cloudflare' — no clavarse a un proveedor
    tunnel_hostname   TEXT,                     -- el subdominio asignado; público, no secreto
    tunnel_ready_at   TEXT                      -- cuándo quedó operativo
);

CREATE TABLE IF NOT EXISTS known_entities (
    tenant_id       INTEGER NOT NULL DEFAULT 1 REFERENCES tenants(id),
    entity_id       TEXT NOT NULL,
    first_seen_at   TEXT NOT NULL,
    acknowledged_at TEXT,            -- NULL mientras el cliente no las confirme
    PRIMARY KEY (tenant_id, entity_id)
);
CREATE INDEX IF NOT EXISTS idx_known_entities_ack
    ON known_entities(tenant_id, acknowledged_at);
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

    # -------------------------------------------------------------------------
    # Preferencias KV (layout del Inicio, configuración del cliente)
    #
    # Misma decisión que escenas: _require_conn() en lugar de silencio. Devolver
    # None cuando la DB no está montada confundiría al router creyendo que no hay
    # preferencias guardadas y resetearía el layout del cliente — mejor un 500
    # ruidoso que resetear en silencio el orden que eligió el usuario.
    # -------------------------------------------------------------------------

    async def get_preference(self, key: str, tenant_id: int = 1) -> Any | None:
        """Lee una preferencia KV. Devuelve el objeto Python o None si no existe.

        Si el JSON guardado está corrupto devuelve None en lugar de reventar:
        el endpoint lo tratará como "sin dato" y devolverá el fallback vacío.
        """
        conn = self._require_conn()
        async with conn.execute(
            "SELECT value_json FROM preferences WHERE tenant_id = ? AND key = ?",
            (tenant_id, key),
        ) as cur:
            row = await cur.fetchone()
            if row is None:
                return None
            try:
                return json.loads(row["value_json"])
            except json.JSONDecodeError:
                logger.warning("db.preference_json_corrupt", key=key, tenant_id=tenant_id)
                return None

    async def set_preference(self, key: str, value: Any, tenant_id: int = 1) -> None:
        """Upsert de una preferencia KV. Crea la fila o actualiza sin tocar otras claves."""
        conn = self._require_conn()
        await conn.execute(
            """
            INSERT INTO preferences (tenant_id, key, value_json, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(tenant_id, key) DO UPDATE
                SET value_json = excluded.value_json,
                    updated_at = excluded.updated_at
            """,
            (tenant_id, key, json.dumps(value, separators=(",", ":")), utc_now_iso()),
        )
        await conn.commit()

    # -------------------------------------------------------------------------
    # Entity overrides (onboarding: habitación, nombre visible, icono, ocultar)
    #
    # Misma decisión que escenas: _require_conn() en lugar de silencio.
    # Devolver listas vacías cuando la DB no está montada ocultaría los overrides
    # del cliente en cada reconexión — mejor un 500 ruidoso.
    # -------------------------------------------------------------------------

    @staticmethod
    def _override_row_to_dict(row: aiosqlite.Row) -> dict[str, Any]:
        """Fila cruda de entity_overrides → dict normalizado."""
        return {
            "entity_id": row["entity_id"],
            "room_id": row["room_id"],
            "display_name": row["display_name"],
            "icon": row["icon"],
            "hidden": bool(row["hidden"]),
            "sort_order": row["sort_order"],
            "updated_at": row["updated_at"],
        }

    async def list_overrides(self, tenant_id: int = 1) -> list[dict[str, Any]]:
        """Todos los overrides del tenant, ordenados por entity_id."""
        conn = self._require_conn()
        async with conn.execute(
            "SELECT * FROM entity_overrides WHERE tenant_id = ? ORDER BY entity_id",
            (tenant_id,),
        ) as cur:
            rows = await cur.fetchall()
        return [self._override_row_to_dict(r) for r in rows]

    async def get_override(self, entity_id: str, tenant_id: int = 1) -> dict[str, Any] | None:
        """Override de una entidad, o None si no existe."""
        conn = self._require_conn()
        async with conn.execute(
            "SELECT * FROM entity_overrides WHERE tenant_id = ? AND entity_id = ?",
            (tenant_id, entity_id),
        ) as cur:
            row = await cur.fetchone()
            if row is None:
                return None
            return self._override_row_to_dict(row)

    async def save_override(
        self,
        entity_id: str,
        *,
        room_id: str | None,
        display_name: str | None,
        icon: str | None,
        hidden: bool,
        sort_order: int | None,
        tenant_id: int = 1,
    ) -> dict[str, Any]:
        """Upsert completo de un override. Devuelve la fila resultante."""
        conn = self._require_conn()
        ahora = utc_now_iso()
        await conn.execute(
            """
            INSERT INTO entity_overrides
                (tenant_id, entity_id, room_id, display_name, icon, hidden, sort_order, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tenant_id, entity_id) DO UPDATE SET
                room_id      = excluded.room_id,
                display_name = excluded.display_name,
                icon         = excluded.icon,
                hidden       = excluded.hidden,
                sort_order   = excluded.sort_order,
                updated_at   = excluded.updated_at
            """,
            (tenant_id, entity_id, room_id, display_name, icon, int(hidden), sort_order, ahora),
        )
        await conn.commit()
        resultado = await self.get_override(entity_id, tenant_id)
        if resultado is None:  # pragma: no cover
            raise RuntimeError(f"Override {entity_id} desapareció tras el INSERT")
        return resultado

    async def delete_override(self, entity_id: str, tenant_id: int = 1) -> bool:
        """True si borró algo; False si no existía (idempotente desde el router)."""
        conn = self._require_conn()
        async with conn.execute(
            "DELETE FROM entity_overrides WHERE tenant_id = ? AND entity_id = ?",
            (tenant_id, entity_id),
        ) as cur:
            borradas = cur.rowcount
        await conn.commit()
        return bool(borradas)

    # -------------------------------------------------------------------------
    # Cámaras sumadas desde la app
    # -------------------------------------------------------------------------

    async def listar_camaras_propias(self, tenant_id: int = 1) -> list[dict[str, Any]]:
        """Las cámaras que sumó el cliente, además de las del add-on."""
        conn = self._require_conn()
        async with conn.execute(
            "SELECT entity_id, stream_name, label, canal, created_at "
            "FROM custom_cameras WHERE tenant_id = ? ORDER BY canal",
            (tenant_id,),
        ) as cur:
            filas = await cur.fetchall()
        return [dict(f) for f in filas]

    async def guardar_camara_propia(
        self,
        *,
        entity_id: str,
        stream_name: str,
        label: str,
        canal: int,
        tenant_id: int = 1,
    ) -> None:
        conn = self._require_conn()
        await conn.execute(
            "INSERT OR REPLACE INTO custom_cameras "
            "(tenant_id, entity_id, stream_name, label, canal, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                tenant_id,
                entity_id,
                stream_name,
                label,
                canal,
                datetime.now(UTC).isoformat(),
            ),
        )
        await conn.commit()

    async def borrar_camara_propia(self, entity_id: str, tenant_id: int = 1) -> bool:
        conn = self._require_conn()
        cur = await conn.execute(
            "DELETE FROM custom_cameras WHERE tenant_id = ? AND entity_id = ?",
            (tenant_id, entity_id),
        )
        await conn.commit()
        return bool(cur.rowcount)

    # -------------------------------------------------------------------------
    # Custom rooms (onboarding: habitaciones propias del cliente)
    # -------------------------------------------------------------------------

    @staticmethod
    def _room_row_to_dict(row: aiosqlite.Row) -> dict[str, Any]:
        """Fila cruda de custom_rooms → dict normalizado."""
        return {
            "room_id": row["room_id"],
            "name": row["name"],
            "icon": row["icon"],
            "sort_order": row["sort_order"],
            "source": "custom",
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    async def list_rooms(self, tenant_id: int = 1) -> list[dict[str, Any]]:
        """Todas las habitaciones custom del tenant, en orden de sort_order."""
        conn = self._require_conn()
        async with conn.execute(
            "SELECT * FROM custom_rooms WHERE tenant_id = ? ORDER BY sort_order, created_at",
            (tenant_id,),
        ) as cur:
            rows = await cur.fetchall()
        return [self._room_row_to_dict(r) for r in rows]

    async def get_room(self, room_id: str, tenant_id: int = 1) -> dict[str, Any] | None:
        """Una habitación custom por room_id, o None si no existe."""
        conn = self._require_conn()
        async with conn.execute(
            "SELECT * FROM custom_rooms WHERE tenant_id = ? AND room_id = ?",
            (tenant_id, room_id),
        ) as cur:
            row = await cur.fetchone()
            if row is None:
                return None
            return self._room_row_to_dict(row)

    async def get_rooms_max_sort_order(self, tenant_id: int = 1) -> int:
        """Máximo sort_order actual entre las habitaciones custom del tenant."""
        conn = self._require_conn()
        async with conn.execute(
            "SELECT MAX(sort_order) AS mx FROM custom_rooms WHERE tenant_id = ?",
            (tenant_id,),
        ) as cur:
            row = await cur.fetchone()
            return int(row["mx"]) if row and row["mx"] is not None else -1

    async def create_room(
        self,
        *,
        room_id: str,
        name: str,
        icon: str | None,
        sort_order: int,
        tenant_id: int = 1,
    ) -> dict[str, Any]:
        """Inserta una habitación custom nueva. Lanza IntegrityError si room_id duplicado."""
        conn = self._require_conn()
        ahora = utc_now_iso()
        await conn.execute(
            """
            INSERT INTO custom_rooms
                (tenant_id, room_id, name, icon, sort_order, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (tenant_id, room_id, name, icon, sort_order, ahora, ahora),
        )
        await conn.commit()
        creada = await self.get_room(room_id, tenant_id)
        if creada is None:  # pragma: no cover
            raise RuntimeError(f"Habitación {room_id} desapareció tras el INSERT")
        return creada

    async def update_room(
        self,
        room_id: str,
        *,
        name: str | None = None,
        icon: str | None = None,
        sort_order: int | None = None,
        tenant_id: int = 1,
    ) -> dict[str, Any] | None:
        """
        Actualiza campos opcionales de una habitación.

        Solo modifica los campos que se pasan (no None). None como valor de
        icon es válido (borra el icono), así que el flag de "no tocar" se
        decide por qué argumentos se pasaron en el router.
        """
        conn = self._require_conn()
        actual = await self.get_room(room_id, tenant_id)
        if actual is None:
            return None
        nuevo_name = name if name is not None else actual["name"]
        nuevo_icon = icon  # None borra; el router decide cuándo pasar None
        nuevo_sort = sort_order if sort_order is not None else actual["sort_order"]
        ahora = utc_now_iso()
        await conn.execute(
            """
            UPDATE custom_rooms
            SET name = ?, icon = ?, sort_order = ?, updated_at = ?
            WHERE tenant_id = ? AND room_id = ?
            """,
            (nuevo_name, nuevo_icon, nuevo_sort, ahora, tenant_id, room_id),
        )
        await conn.commit()
        return await self.get_room(room_id, tenant_id)

    async def delete_room(self, room_id: str, tenant_id: int = 1) -> bool:
        """
        Borra una habitación custom y limpia los overrides que la referencian.

        Ambas operaciones ocurren en la misma transacción para garantizar
        consistencia: ningún override queda apuntando a una habitación inexistente.
        """
        conn = self._require_conn()
        # Limpiar referencias en entity_overrides ANTES de borrar la habitación
        await conn.execute(
            "UPDATE entity_overrides SET room_id = NULL WHERE tenant_id = ? AND room_id = ?",
            (tenant_id, room_id),
        )
        async with conn.execute(
            "DELETE FROM custom_rooms WHERE tenant_id = ? AND room_id = ?",
            (tenant_id, room_id),
        ) as cur:
            borradas = cur.rowcount
        await conn.commit()
        return bool(borradas)

    # -------------------------------------------------------------------------
    # Known entities (onboarding: detección de dispositivos nuevos)
    # -------------------------------------------------------------------------

    async def count_known_entities(self, tenant_id: int = 1) -> int:
        """Cuántas entidades están registradas para el tenant."""
        conn = self._require_conn()
        async with conn.execute(
            "SELECT COUNT(*) AS n FROM known_entities WHERE tenant_id = ?",
            (tenant_id,),
        ) as cur:
            row = await cur.fetchone()
            return int(row["n"]) if row else 0

    async def seed_known_entities(
        self, ids: list[str], acknowledged: bool = True, tenant_id: int = 1
    ) -> None:
        """
        Siembra las entidades como 'conocidas'.

        Se usa en la primera visita al endpoint /pending para que una
        instalación nueva no muestre cientos de dispositivos como "nuevos".
        INSERT OR IGNORE para no pisar registros que ya existan (idempotente).
        """
        conn = self._require_conn()
        ahora = utc_now_iso()
        ack = ahora if acknowledged else None
        for eid in ids:
            await conn.execute(
                """
                INSERT OR IGNORE INTO known_entities
                    (tenant_id, entity_id, first_seen_at, acknowledged_at)
                VALUES (?, ?, ?, ?)
                """,
                (tenant_id, eid, ahora, ack),
            )
        await conn.commit()

    async def insert_unknown_entities(
        self, ids: list[str], tenant_id: int = 1
    ) -> list[str]:
        """
        Inserta entidades no conocidas como pendientes de revisión.

        Las entidades ya registradas se ignoran (idempotente).
        Devuelve la lista de entity_ids efectivamente insertados.
        """
        conn = self._require_conn()
        ahora = utc_now_iso()
        nuevas: list[str] = []
        for eid in ids:
            async with conn.execute(
                """
                INSERT OR IGNORE INTO known_entities
                    (tenant_id, entity_id, first_seen_at, acknowledged_at)
                VALUES (?, ?, ?, NULL)
                """,
                (tenant_id, eid, ahora),
            ) as cur:
                if cur.rowcount > 0:
                    nuevas.append(eid)
        await conn.commit()
        return nuevas

    async def list_unacknowledged(self, tenant_id: int = 1) -> list[dict[str, Any]]:
        """Entidades pendientes de revisión (acknowledged_at IS NULL)."""
        conn = self._require_conn()
        async with conn.execute(
            """
            SELECT entity_id, first_seen_at
            FROM known_entities
            WHERE tenant_id = ? AND acknowledged_at IS NULL
            ORDER BY first_seen_at
            """,
            (tenant_id,),
        ) as cur:
            rows = await cur.fetchall()
        return [{"entity_id": r["entity_id"], "first_seen_at": r["first_seen_at"]} for r in rows]

    async def acknowledge_entities(self, ids: list[str], tenant_id: int = 1) -> int:
        """
        Marca entidades como revisadas.

        Solo actualiza las que aún no estaban confirmadas.
        Devuelve el número de filas efectivamente marcadas.
        """
        conn = self._require_conn()
        ahora = utc_now_iso()
        total = 0
        for eid in ids:
            async with conn.execute(
                """
                UPDATE known_entities
                SET acknowledged_at = ?
                WHERE tenant_id = ? AND entity_id = ? AND acknowledged_at IS NULL
                """,
                (ahora, tenant_id, eid),
            ) as cur:
                total += cur.rowcount
        await conn.commit()
        return total

    # -------------------------------------------------------------------------
    # Identidad de la caja (modo fábrica)
    #
    # Solo se usa cuando `platform_base_url` tiene valor. En modo artesanal estas
    # tablas quedan vacías y estos métodos no los llama nadie — la tabla se crea
    # igual porque `CREATE TABLE IF NOT EXISTS` no agrega columnas a una tabla ya
    # existente: un schema incompleto se arrastraría para siempre.
    # -------------------------------------------------------------------------

    @staticmethod
    def _device_identity_row_to_dict(row: aiosqlite.Row) -> dict[str, Any]:
        """
        Fila cruda → dict con las claves EXACTAS del dataclass DeviceIdentity.

        Se listan a mano en vez de hacer `dict(row)` porque la fila guarda la
        columna `id` (siempre 1) que el dataclass no tiene: un `dict(row)` haría
        reventar el constructor con un argumento inesperado.

        🔪 Este método se perdió al unificar las dos ramas del Mirror y estuvo
        AUSENTE en la 0.28.0, que llegó a instalarse en hardware real. El efecto:
        `get_device_identity` lo llamaba, tiraba AttributeError, el lifespan lo
        atrapaba con su `except Exception` y la caja arrancaba con
        `device_identity.unavailable` — o sea, sin poder activarse nunca, pero
        sin un solo traceback visible. Las 453 pruebas seguían en verde porque
        ninguna ejercitaba `ensure_identity` contra una Database de verdad.
        Ahora sí: ver `test_identidad_contra_db_real.py`.
        """
        return {
            "device_id": row["device_id"],
            "public_key": row["public_key"],
            "hardware_id": row["hardware_id"],
            "key_algorithm": row["key_algorithm"],
            "created_at": row["created_at"],
            "claim_code_version": row["claim_code_version"],
            "paired_at": row["paired_at"],
            "paired_house_id": row["paired_house_id"],
            "backend_base_url": row["backend_base_url"],
            "tunnel_provider": row["tunnel_provider"],
            "tunnel_hostname": row["tunnel_hostname"],
            "tunnel_ready_at": row["tunnel_ready_at"],
        }

    async def get_device_identity(self) -> dict[str, Any] | None:
        """La identidad de esta caja, o None si nunca arrancó (primer boot)."""
        conn = self._require_conn()
        async with conn.execute("SELECT * FROM device_identity WHERE id = 1") as cur:
            row = await cur.fetchone()
            if row is None:
                return None
            return self._device_identity_row_to_dict(row)

    async def create_device_identity(
        self,
        *,
        device_id: str,
        public_key: str,
        hardware_id: str | None,
        key_algorithm: str,
    ) -> dict[str, Any]:
        """
        Inserta la identidad única de la caja. Solo corre en el primer arranque.

        El INSERT es sin OR REPLACE a propósito: si ya hay una fila, que falle.
        Pisar la identidad de una caja en funcionamiento es justo lo que no
        queremos que pueda pasar por accidente.
        """
        conn = self._require_conn()
        await conn.execute(
            """
            INSERT INTO device_identity
                (id, device_id, public_key, key_algorithm, hardware_id, created_at,
                 claim_code_version, paired_at, paired_house_id, backend_base_url,
                 tunnel_provider, tunnel_hostname, tunnel_ready_at)
            VALUES (1, ?, ?, ?, ?, ?, 1, NULL, NULL, NULL, NULL, NULL, NULL)
            """,
            (device_id, public_key, key_algorithm, hardware_id, utc_now_iso()),
        )
        await conn.commit()
        creada = await self.get_device_identity()
        if creada is None:  # pragma: no cover — solo si algo borra en el medio
            raise RuntimeError("La identidad desapareció inmediatamente tras el INSERT")
        return creada

    async def mark_device_paired(
        self,
        *,
        house_id: str,
        backend_base_url: str,
    ) -> dict[str, Any] | None:
        """
        Sella el emparejamiento. UN SOLO USO: si ya estaba emparejada, no hace nada.

        El `WHERE paired_at IS NULL` es la garantía de un solo uso, y vive en la
        base a propósito — no en un `if` de Python. Dos requests simultáneos con
        el mismo código llegan los dos al UPDATE; solo uno encuentra la fila sin
        emparejar. Devuelve None cuando no actualizó nada, y el router lo
        traduce a "esta caja ya tiene dueño".
        """
        conn = self._require_conn()
        async with conn.execute(
            """
            UPDATE device_identity
            SET paired_at = ?, paired_house_id = ?, backend_base_url = ?
            WHERE id = 1 AND paired_at IS NULL
            """,
            (utc_now_iso(), house_id, backend_base_url),
        ) as cur:
            actualizadas = cur.rowcount
        await conn.commit()
        if not actualizadas:
            return None
        return await self.get_device_identity()

    async def unmark_device_paired(self) -> dict[str, Any] | None:
        """
        Deshace el emparejamiento local: la caja vuelve a estar disponible.

        Es el espejo exacto de `mark_device_paired` y existe porque la plataforma
        SÍ tiene forma de liberar una caja (`POST /boxes/{id}/deactivate`) y la
        caja no tenía ninguna de enterarse. El resultado era una caja convencida
        de tener dueño para siempre, callada, que ninguna calcomanía podía
        volver a activar.

        Limpia también los campos del túnel: el túnel de la casa anterior ya no
        existe del lado de Cloudflare, y dejar su hostname acá haría que la
        calcomanía y `/api/device/identity` mintieran sobre el estado real.

        NO toca `device_id` ni el par de llaves: la identidad criptográfica de la
        caja es del hardware, no de la casa. Regenerarla obligaría a reimprimir
        la calcomanía, que es justo lo que se quiere evitar — la que está pegada
        tiene que seguir sirviendo.

        `WHERE paired_at IS NOT NULL` es el simétrico del un-solo-uso del mark:
        devuelve None si no había nada que deshacer, y así quien llama distingue
        "la liberé" de "ya estaba libre" sin leer antes.
        """
        conn = self._require_conn()
        async with conn.execute(
            """
            UPDATE device_identity
            SET paired_at = NULL,
                paired_house_id = NULL,
                tunnel_provider = NULL,
                tunnel_hostname = NULL,
                tunnel_ready_at = NULL
            WHERE id = 1 AND paired_at IS NOT NULL
            """,
        ) as cur:
            actualizadas = cur.rowcount
        await conn.commit()
        if not actualizadas:
            return None
        return await self.get_device_identity()

    async def set_device_tunnel(
        self,
        *,
        provider: str,
        hostname: str,
    ) -> dict[str, Any] | None:
        """
        Registra el túnel que el backend provisionó para esta casa.

        Separado de mark_device_paired porque son dos momentos distintos: el
        claim se completa apenas la persona escanea, y el túnel puede tardar
        (crear el DNS, que propague, que cloudflared levante). Meterlos en el
        mismo UPDATE haría que un túnel lento pareciera un emparejamiento fallido.
        """
        conn = self._require_conn()
        async with conn.execute(
            """
            UPDATE device_identity
            SET tunnel_provider = ?, tunnel_hostname = ?, tunnel_ready_at = ?
            WHERE id = 1
            """,
            (provider, hostname, utc_now_iso()),
        ) as cur:
            actualizadas = cur.rowcount
        await conn.commit()
        if not actualizadas:
            return None
        return await self.get_device_identity()
