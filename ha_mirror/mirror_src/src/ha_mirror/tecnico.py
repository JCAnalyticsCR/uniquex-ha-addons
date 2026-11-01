"""
Canal técnico (0.54.0): el mantenimiento remoto que solo podemos hacer NOSOTROS.

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
actualizar el Mirror, HACS y las integraciones de la lista, con respaldo (Core,
el sistema operativo y los demás complementos, NO: se hacen a mano) · reiniciar Home
Assistant · correr formularios de integraciones de una lista cerrada (HACS,
Xtend Tuya, Smart Life, SonoffLAN) · descargar integraciones de la comunidad de
una lista cerrada de repositorios. No es una consola: no hay paso genérico.
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

#: Además del propio Mirror, lo único que el canal puede actualizar: HACS y las
#: integraciones de la lista (se reconocen por el repositorio de su `release_url`).
#: Core, el sistema operativo, el Supervisor y los demás complementos tardan
#: minutos y, si algo sale mal, hace falta alguien en la casa: van a mano.
REPOS_ACTUALIZABLES: frozenset[str] = frozenset({*REPOS_PERMITIDOS, "hacs/integration"})

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
        actualizaciones = []
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
        }

    # ── Supervisor ──────────────────────────────────────────────────────────

    async def recargar_tienda(self) -> dict[str, Any]:
        """La tienda cachea: sin esto, una versión recién publicada no aparece."""
        await self._ws(
            {"type": "supervisor/api", "endpoint": "/store/reload", "method": "post"}, timeout=60.0
        )
        return {"ok": True}

    async def actualizar(self, entity_id: str, respaldo: bool = True) -> dict[str, Any]:
        if not entity_id.startswith("update."):
            raise TecnicoProhibido("eso no es una actualización")
        st = self._store.get_state(entity_id)
        if st is None:
            raise TecnicoProhibido(f"no existe {entity_id!r}")
        attrs = getattr(st, "attributes", None) or {}
        nombre = str(_leer(attrs, "friendly_name") or entity_id)
        el_mirror = es_el_mirror(entity_id, nombre)
        if not el_mirror and repo_de_la_actualizacion(attrs) not in REPOS_ACTUALIZABLES:
            raise TecnicoProhibido(f"{entity_id!r} no se actualiza por este canal: va a mano")
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
                timeout=900.0,
            )

        if el_mirror:
            # Al actualizarse, el Supervisor apaga ESTE proceso: se contesta primero.
            self._despues(llamada, demora=2.0)
            logger.warning(
                "tecnico.actualizar_mirror_programado",
                entity_id=entity_id,
                respaldo="backup" in datos,
            )
            return {"programado": True, "respaldo": "backup" in datos}
        await llamada()
        logger.warning("tecnico.actualizado", entity_id=entity_id, respaldo="backup" in datos)
        return {"programado": False, "respaldo": "backup" in datos}

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
