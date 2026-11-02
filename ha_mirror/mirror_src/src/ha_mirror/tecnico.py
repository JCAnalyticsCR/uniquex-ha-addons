"""
Canal técnico (0.54.1): el mantenimiento remoto que solo podemos hacer NOSOTROS.

POR QUÉ EXISTE
--------------
Hasta 0.53.x el Mirror bloquea a propósito, desde internet, actualizarse a sí
mismo, instalar integraciones y reiniciar Home Assistant: es lo que impide que
alguien con acceso a la app tome control de la caja. El costo: cada mejora
exigía a alguien en la red de la casa (casa 2, noviembre de 2026: el 0.53.0, HACS
y Xtend Tuya esperando una visita). Este canal abre ESAS operaciones, y solo
para nosotros.

QUIÉN PUEDE USARLO
------------------
Un pedido técnico necesita las DOS cosas:
  1. La `X-API-Key` de siempre (la que también usa la app web).
  2. Una firma Ed25519 de la PLATAFORMA sobre el pedido exacto, con la misma
     llave que ya firma las respuestas de activación (`LLAVES_PLATAFORMA`,
     horneada en el código) pero con OTRO prefijo de dominio: una firma de
     activación no sirve acá ni al revés.

La mitad privada vive solo en Railway (servicio `fabrica-api`) y la usa
`fabrica_api.tecnico`, que se corre por `railway ssh` — o sea, con nuestra cuenta
de Railway y nuestra llave SSH. La app web NO la tiene: si alguien le sacara la
llave del Mirror a la app, igual no podría firmar.

QUÉ FIRMA (en este orden exacto, cada campo con su largo de 4 bytes delante)
------------------------------------------------------------------------------
    b"uniquexcr/tecnico/v1"
    device_id   — la caja: una firma para casa A no sirve en casa B
    método      — "GET" / "POST"
    ruta        — path + query, crudos (sin decodificar)
    ts          — segundos Unix; hasta 120 s atrás y 30 adelante, y NUNCA
                  de antes de que arrancara este proceso
    nonce       — no se acepta dos veces (memoria de la ventana)
    cuerpo      — los bytes crudos

QUÉ HACE (y nada más)
---------------------
Ver el estado y las actualizaciones · refrescar la tienda del Supervisor ·
guardar un respaldo completo · instalar CUALQUIER actualización (Core, sistema
operativo, Supervisor, complementos, integraciones, el propio Mirror) menos
Dahua · reiniciar Home Assistant · correr formularios de integraciones de una
lista cerrada (HACS, Xtend Tuya, Smart Life, SonoffLAN) · descargar integraciones
de la comunidad de una lista cerrada de repositorios. No es una consola: no hay
paso genérico.

La administración remota ES el producto (Jeyrell, 2026-11-01: "el cliente nos
encargó este proyecto para administrarle todo remotamente"). La única excepción
es Dahua, y es técnica: su versión nueva importa `async_timeout`, que Home
Assistant ya no trae, y al reiniciar no carga — se apagan las cámaras.

🔪 TODO LO QUE TARDA SE CONTESTA ANTES DE HACERLO. Cloudflare corta a los 100 s
un pedido sin respuesta, y una actualización tarda minutos (el sistema operativo
además reinicia la caja, y el Mirror se apaga a sí mismo). El avance se sigue
con `estado`; `fabrica_api.tecnico actualizar-todo` lo hace solo.
"""

from __future__ import annotations

import asyncio
import base64
import re
import struct
import time
from typing import Any

import structlog
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from ha_mirror.announce_client import LLAVE_PENDIENTE, LLAVES_PLATAFORMA

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

DOMINIO_FIRMA = b"uniquexcr/tecnico/v1"
VENTANA_S = 120
#: Tolerancia hacia adelante: el reloj de Railway y el de la caja no son el mismo.
FUTURO_S = 30

#: Cuándo arrancó este proceso. La memoria de nonces vive en RAM y se pierde al
#: reiniciar (actualizar el Mirror lo reinicia a propósito): sin este piso, un
#: pedido capturado justo antes seguiría valiendo hasta 120 s después.
ARRANQUE_DEL_PROCESO = int(time.time())  # en segundos enteros, como `ts`
_RE_NONCE = re.compile(r"^[A-Za-z0-9_-]{16,64}$")

#: Formularios de integraciones que el canal puede correr.
HANDLERS_TECNICOS: frozenset[str] = frozenset({"hacs", "xtend_tuya", "tuya", "sonoff"})

#: Repositorios de la comunidad que se pueden descargar, con su categoría.
REPOS_PERMITIDOS: dict[str, str] = {
    "azerty9971/xtend_tuya": "integration",
    "alexxit/sonofflan": "integration",
    "rroller/dahua": "integration",
}

#: Lo único que el canal NUNCA instala (ver el docstring). Se busca en el id, en
#: el nombre visible y en el repositorio de su `release_url`: el id de un
#: complemento no siempre dice de qué es, y el nombre se puede cambiar en HA.
_NUNCA_ACTUALIZAR = re.compile(r"dahua", re.I)

#: Bit BACKUP de `supported_features` de las entidades `update.*`.
_SOPORTA_RESPALDO = 8
_RE_REPO_GITHUB = re.compile(r"^https://github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)(?:/|$)", re.I)


def _leer(attrs: Any, clave: str) -> Any:
    """Un atributo de un estado, venga como dict o como modelo."""
    return attrs.get(clave) if isinstance(attrs, dict) else getattr(attrs, clave, None)


class TecnicoNoAutorizado(Exception):
    """El pedido no trae una firma técnica válida. El motivo va al log, no al cliente."""


class TecnicoProhibido(Exception):
    """La firma es buena pero la operación pedida no está en la lista."""


def _con_largo(dato: bytes) -> bytes:
    return struct.pack(">I", len(dato)) + dato


def material_firmado(
    device_id: str, metodo: str, ruta: str, ts: str, nonce: str, cuerpo: bytes
) -> bytes:
    return (
        DOMINIO_FIRMA
        + _con_largo(device_id.encode())
        + _con_largo(metodo.upper().encode())
        + _con_largo(ruta.encode())
        + _con_largo(ts.encode())
        + _con_largo(nonce.encode())
        + _con_largo(cuerpo)
    )


class MemoriaDeNonces:
    """Los nonces vistos dentro de la ventana. Uno repetido es una repetición capturada."""

    def __init__(self, arranque: float | None = None) -> None:
        self._vistos: dict[str, float] = {}
        #: Nada firmado antes de esto vale (ver `ARRANQUE_DEL_PROCESO`).
        self.arranque = ARRANQUE_DEL_PROCESO if arranque is None else arranque

    def usar(self, nonce: str, ahora: float) -> bool:
        """True si es nuevo (y lo anota). False si ya se usó."""
        for n, vence in list(self._vistos.items()):
            if vence < ahora:
                del self._vistos[n]
        if nonce in self._vistos:
            return False
        self._vistos[nonce] = ahora + 2 * VENTANA_S
        return True


def verificar_firma(
    *,
    device_id: str | None,
    metodo: str,
    ruta: str,
    cuerpo: bytes,
    firma: str | None,
    key_id: str | None,
    ts: str | None,
    nonce: str | None,
    nonces: MemoriaDeNonces,
    ahora: float | None = None,
) -> None:
    """Revienta con `TecnicoNoAutorizado` si algo no cuadra. Nunca loguea la firma."""
    momento = time.time() if ahora is None else ahora
    if not device_id:
        raise TecnicoNoAutorizado("esta caja no tiene identidad: el canal técnico no se abre")
    if not (firma and key_id and ts and nonce):
        raise TecnicoNoAutorizado("faltan cabeceras de firma técnica")
    llave_b64 = LLAVES_PLATAFORMA.get(key_id)
    if llave_b64 is None or llave_b64 == LLAVE_PENDIENTE:
        raise TecnicoNoAutorizado(f"key_id desconocido: {key_id!r}")
    if not ts.isdigit() or not (momento - VENTANA_S <= int(ts) <= momento + FUTURO_S):
        raise TecnicoNoAutorizado("hora fuera de la ventana")
    if int(ts) < int(nonces.arranque):
        raise TecnicoNoAutorizado("firmado antes de que arrancara el proceso")
    if not _RE_NONCE.fullmatch(nonce):
        raise TecnicoNoAutorizado("nonce mal formado")
    try:
        llave = Ed25519PublicKey.from_public_bytes(base64.b64decode(llave_b64, validate=True))
        llave.verify(
            base64.b64decode(firma, validate=True),
            material_firmado(device_id, metodo, ruta, ts, nonce, cuerpo),
        )
    except (InvalidSignature, ValueError) as exc:
        raise TecnicoNoAutorizado("la firma no verifica") from exc
    # El nonce se anota DESPUÉS de verificar: si no, cualquiera podría quemar
    # nonces ajenos mandando basura.
    if not nonces.usar(nonce, momento):
        raise TecnicoNoAutorizado("nonce repetido")


def repo_permitido(repositorio: str) -> str:
    """La categoría del repositorio, o `TecnicoProhibido` si no está en la lista."""
    clave = repositorio.strip().lower().removeprefix("https://github.com/").strip("/")
    categoria = REPOS_PERMITIDOS.get(clave)
    if categoria is None:
        raise TecnicoProhibido(f"repositorio fuera de la lista: {repositorio!r}")
    return categoria


def repo_de_la_actualizacion(attrs: Any) -> str | None:
    """`owner/repo` de GitHub de una entidad `update.*`, por su `release_url`."""
    m = _RE_REPO_GITHUB.match(str(_leer(attrs, "release_url") or ""))
    return m.group(1).lower() if m else None


def _campo_de(entity_id: str, attrs: Any) -> str:
    """Id + nombre visible + repositorio: donde se busca qué es una actualización."""
    nombre = _leer(attrs, "friendly_name") or _leer(attrs, "title") or ""
    return f"{entity_id} {nombre} {repo_de_la_actualizacion(attrs) or ''}"


def es_el_mirror(entity_id: str, nombre: str) -> bool:
    """La actualización del propio Mirror: hay que contestar ANTES de lanzarla."""
    return bool(re.search(r"uniquexcr|\bmirror\b", f"{entity_id} {nombre}", re.I))


class CanalTecnico:
    """Las operaciones del canal. Recibe el upstream (WS de HA) y el store."""

    def __init__(self, upstream: Any, store: Any, flow_client: Any) -> None:
        self._upstream = upstream
        self._store = store
        self._flow_client = flow_client  # callable → HaFlowClient | None
        self._tareas: set[asyncio.Task[Any]] = set()

    async def _ws(self, msg: dict[str, Any], timeout: float = 30.0) -> Any:
        resp = await self._upstream.send_command(msg, timeout=timeout)
        return resp.get("result") if isinstance(resp, dict) else resp

    def _despues(self, fabrica: Any, demora: float) -> None:
        """Lanza algo después de contestar (actualizar el Mirror, reiniciar HA).

        Recibe una FÁBRICA de la corrutina y no la corrutina: así se crea recién al
        lanzarla, y si el proceso se apaga antes no queda una llamada a medio armar.
        """

        async def _correr() -> None:
            await asyncio.sleep(demora)
            try:
                await fabrica()
            except Exception:  # noqa: BLE001 — el proceso puede morir a mitad, es lo esperado
                logger.info("tecnico.tarea_diferida_cortada")

        tarea = asyncio.create_task(_correr())
        self._tareas.add(tarea)
        tarea.add_done_callback(self._tareas.discard)

    # ── Estado ──────────────────────────────────────────────────────────────

    async def estado(self) -> dict[str, Any]:
        entradas = await self._ws({"type": "config_entries/get"})
        actualizaciones: list[dict[str, Any]] = []
        for entity_id, st in self._store.get_all_states().items():
            if not entity_id.startswith("update."):
                continue
            attrs = getattr(st, "attributes", None)
            actualizaciones.append(
                {
                    "entity_id": entity_id,
                    "nombre": _leer(attrs, "friendly_name"),
                    "instalada": _leer(attrs, "installed_version"),
                    "disponible": _leer(attrs, "latest_version"),
                    "hay_novedad": getattr(st, "state", None) == "on",
                    "en_curso": bool(_leer(attrs, "in_progress")),
                    "nunca": bool(_NUNCA_ACTUALIZAR.search(_campo_de(entity_id, attrs))),
                }
            )
        return {
            "integraciones": [
                {
                    "domain": e.get("domain"),
                    "title": e.get("title"),
                    "state": e.get("state"),
                    "entry_id": e.get("entry_id"),
                }
                for e in (entradas or [])
            ],
            "hacs_configurado": any(
                e.get("domain") == "hacs" and e.get("state") == "loaded" for e in (entradas or [])
            ),
            "actualizaciones": sorted(
                actualizaciones, key=lambda a: (not a["hay_novedad"], a["entity_id"])
            ),
            "respaldos": await self._respaldos(),
        }

    async def _respaldos(self) -> dict[str, Any]:
        """Los últimos respaldos, para saber cuándo terminó el que se pidió."""
        try:
            datos = await self._ws({"type": "backup/info"}, timeout=15.0) or {}
        except Exception:  # noqa: BLE001 — un Core viejo no tiene `backup/*`
            return {"disponible": False, "generando": False, "recientes": []}
        lista = [
            {"nombre": b.get("name"), "fecha": b.get("date")}
            for b in datos.get("backups") or []
            if isinstance(b, dict)
        ]
        lista.sort(key=lambda b: str(b["fecha"] or ""), reverse=True)
        return {
            "disponible": True,
            "generando": bool(datos.get("generating")) or datos.get("state") not in (None, "idle"),
            "recientes": lista[:5],
        }

    # ── Supervisor ──────────────────────────────────────────────────────────

    async def recargar_tienda(self) -> dict[str, Any]:
        """La tienda cachea: sin esto, una versión recién publicada no aparece."""
        await self._ws(
            {"type": "supervisor/api", "endpoint": "/store/reload", "method": "post"}, timeout=60.0
        )
        return {"ok": True}

    def _instalando(self) -> str | None:
        """La actualización que se está instalando ahora, si hay una."""
        for entity_id, st in self._store.get_all_states().items():
            if entity_id.startswith("update.") and _leer(
                getattr(st, "attributes", None), "in_progress"
            ):
                return entity_id
        return None

    async def respaldo(self) -> dict[str, Any]:
        """Un respaldo completo. Se contesta antes: tarda minutos."""
        nombre = time.strftime("UniquexCR %Y-%m-%d %H:%M UTC", time.gmtime())
        self._despues(
            lambda: self._ws(
                {
                    "type": "call_service",
                    "domain": "hassio",
                    "service": "backup_full",
                    "service_data": {"name": nombre},
                },
                timeout=3600.0,
            ),
            demora=0.5,
        )
        logger.warning("tecnico.respaldo_programado", nombre=nombre)
        return {"programado": True, "nombre": nombre}

    async def actualizar(self, entity_id: str, respaldo: bool = True) -> dict[str, Any]:
        if not entity_id.startswith("update."):
            raise TecnicoProhibido("eso no es una actualización")
        st = self._store.get_state(entity_id)
        if st is None:
            raise TecnicoProhibido(f"no existe {entity_id!r}")
        attrs = getattr(st, "attributes", None) or {}
        if _NUNCA_ACTUALIZAR.search(_campo_de(entity_id, attrs)):
            raise TecnicoProhibido(
                "Dahua no se actualiza: la versión nueva no arranca en esta casa y apaga las cámaras"
            )
        if getattr(st, "state", None) != "on":
            raise TecnicoProhibido(f"{entity_id!r} ya está al día")
        otra = self._instalando()
        if otra is not None:
            raise TecnicoProhibido(f"ya se está instalando {otra!r}: esperá a que termine")
        datos: dict[str, Any] = {"entity_id": entity_id}
        if respaldo and int(_leer(attrs, "supported_features") or 0) & _SOPORTA_RESPALDO:
            datos["backup"] = True

        def llamada() -> Any:
            return self._ws(
                {
                    "type": "call_service",
                    "domain": "update",
                    "service": "install",
                    "service_data": datos,
                },
                timeout=3600.0,
            )

        # Siempre se contesta primero (ver el docstring del módulo): el Mirror se
        # apaga a sí mismo, el sistema operativo reinicia la caja, y lo demás tarda
        # más que los 100 s que Cloudflare espera.
        self._despues(llamada, demora=2.0)
        logger.warning(
            "tecnico.actualizacion_programada",
            entity_id=entity_id,
            el_mirror=es_el_mirror(entity_id, str(_leer(attrs, "friendly_name") or "")),
            respaldo="backup" in datos,
        )
        return {"programado": True, "respaldo": "backup" in datos}

    async def reiniciar_ha(self) -> dict[str, Any]:
        self._despues(
            lambda: self._ws(
                {"type": "call_service", "domain": "homeassistant", "service": "restart"},
                timeout=120.0,
            ),
            demora=2.0,
        )
        logger.warning("tecnico.reinicio_programado")
        return {"programado": True}

    # ── Formularios de integraciones (lista cerrada) ─────────────────────────

    def _cliente(self) -> Any:
        cliente = self._flow_client()
        if cliente is None:
            raise TecnicoProhibido("esta caja no tiene token admin en Home Assistant")
        return cliente

    async def flujo_iniciar(self, handler: str) -> dict[str, Any]:
        if handler not in HANDLERS_TECNICOS:
            raise TecnicoProhibido(f"integración fuera de la lista: {handler!r}")
        paso = await self._cliente().iniciar(handler)
        logger.warning("tecnico.flujo_iniciado", handler=handler, tipo=paso.get("type"))
        return paso

    async def _exigir_flujo(self, flow_id: str) -> None:
        for f in await self._ws({"type": "config_entries/flow/progress"}) or []:
            if f.get("flow_id") == flow_id:
                if f.get("handler") not in HANDLERS_TECNICOS:
                    raise TecnicoProhibido("ese formulario no es de una integración de la lista")
                return
        raise TecnicoProhibido("ese formulario no existe")

    async def flujo_leer(self, flow_id: str) -> dict[str, Any]:
        await self._exigir_flujo(flow_id)
        return await self._cliente().leer(flow_id)

    async def flujo_avanzar(self, flow_id: str, datos: dict[str, Any]) -> dict[str, Any]:
        await self._exigir_flujo(flow_id)
        paso = await self._cliente().avanzar(flow_id, datos)
        logger.warning("tecnico.flujo_avanzado", tipo=paso.get("type"))
        return paso

    # ── HACS (lista cerrada de repositorios) ─────────────────────────────────

    async def _buscar_repo(self, nombre: str) -> dict[str, Any] | None:
        lista = await self._ws(
            {"type": "hacs/repositories/list", "categories": ["integration"]}, timeout=60.0
        )
        for r in lista or []:
            if str(r.get("full_name", "")).lower() == nombre:
                return r
        return None

    async def hacs_descargar(self, repositorio: str) -> dict[str, Any]:
        categoria = repo_permitido(repositorio)
        nombre = repositorio.strip().lower().removeprefix("https://github.com/").strip("/")
        repo = await self._buscar_repo(nombre)
        if repo is None:
            await self._ws(
                {"type": "hacs/repositories/add", "repository": nombre, "category": categoria},
                timeout=120.0,
            )
            repo = await self._buscar_repo(nombre)
        if repo is None:
            raise TecnicoProhibido(f"HACS no encontró {repositorio!r}")
        await self._ws(
            {"type": "hacs/repository/download", "repository": str(repo["id"])}, timeout=600.0
        )
        despues = await self._buscar_repo(nombre)
        logger.warning("tecnico.hacs_descargado", repositorio=nombre)
        return {
            "repositorio": nombre,
            "instalada": (despues or {}).get("installed_version"),
            "hace_falta_reiniciar": True,
        }
