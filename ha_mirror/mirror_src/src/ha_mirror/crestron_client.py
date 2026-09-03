"""Cliente de bajo nivel para la API REST local de Crestron Home (servidor "CWS").

Habla contra `https://{ip}/cws/api` de un procesador CP4-R (Crestron Home OS 4).
Solo se usan GET y POST con JSON. La sesion se autentica con un token que Crestron
canjea por un `AuthKey` de corta vida (~10 min de inactividad -> 401/511 -> re-login).

Decisiones de diseno que importan:

- **Sin hardware real todavia — pero los payloads YA NO son una suposicion.**
  (2026-08-12) Se investigo contra la doc oficial del SDK
  (sdkcon78221.crestron.com/sdk/Crestron-Home-API) y el codigo fuente real de
  dos integraciones que SI corren contra hardware (`ruudruud/ha-crestron-home`,
  `Desluca/crestron-mcp`). Lo que queda sin confirmar de verdad (y por eso sigue
  marcado `# TODO verificar contra hardware real`) es solo el SENTIDO de la
  posicion de persiana (¿0 siempre es cerrada, o depende de como el instalador
  configuro el motor?) — eso ninguna fuente lo garantiza, varia por instalacion.
  El parseo se mantiene DEFENSIVO de todos modos (tolera campos faltantes o
  tipos raros sin crashear) porque documentacion no es lo mismo que un dump real
  de esta casa puntual.

- **Anti-SSRF.** Todas las URLs se arman desde `base_url` + paths FIJOS del codigo,
  nunca desde datos externos. No se siguen redirects (`allow_redirects=False`) y
  solo se habla con el host de `base_url`.

- **Cert autofirmado.** Los CP4-R V2 exponen HTTPS con certificado autofirmado; con
  `verify_ssl=False` los requests van con `ssl=False` (aiohttp no valida el cert).

- **El token/AuthKey nunca se loguea ni aparece en repr.** `close()` lo sobrescribe.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Literal

import aiohttp
import structlog

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class CrestronError(Exception):
    """Error controlado del cliente Crestron."""

    def __init__(self, code: str, status_code: int = 502) -> None:
        super().__init__(code)
        self.code = code
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class CrestronDevice:
    """Dispositivo Crestron ya normalizado para el resto del Mirror.

    `raw` conserva el payload original completo por si algun consumidor necesita
    un campo que aun no mapeamos (util mientras el JSON real no esta confirmado).
    """

    id: int
    name: str
    device_type: Literal["light", "shade", "scene", "sensor", "unknown"]  # normalizado
    raw_type: str  # el "type"/"subType" original de Crestron
    room: str | None
    level: int | None  # 0-100 para dimmers/persianas; None si no aplica
    status: str | None  # estado crudo reportado (on/off/open/closed/etc)
    reachable: bool
    raw: dict[str, Any]  # payload original completo (para debug)


def _clamp(value: int, low: int, high: int) -> int:
    """Acota `value` al rango [low, high]."""
    return max(low, min(high, value))


class CrestronClient:
    """Acceso saliente y acotado a la API REST local de un CP4-R (Crestron Home)."""

    # 🔪 DOS headers distintos, no uno. Antes de esta correccion (2026-08-12) el
    # cliente reusaba el MISMO nombre de header para el token de login y para el
    # AuthKey de las llamadas siguientes — eso hubiera dado 401/511 en cada
    # request despues del login contra hardware real. Confirmado en la doc
    # oficial (Lights/Shades/Devices API) y en el codigo real de
    # ruudruud/ha-crestron-home (api.py), que usa esta misma separacion.
    _LOGIN_HEADER = "Crestron-RestAPI-AuthToken"  # SOLO en GET /cws/api/login
    _AUTHKEY_HEADER = "Crestron-RestAPI-AuthKey"  # en TODAS las llamadas siguientes

    # Tope de bytes por respuesta: una respuesta enorme no deberia reventar la RAM.
    _MAX_RESPONSE_BYTES = 4 * 1024 * 1024  # 4 MB
    # Tope de dispositivos parseados por sanidad (una casa real no llega a esto).
    _MAX_DEVICES = 2000

    # Escala de nivel del hardware: 0-65535, CONFIRMADO (2026-08-12) triangulando
    # 3 fuentes independientes — doc oficial (JSON Payload Fields: "Lights: level
    # Integer 0-65535"), la constante CRESTRON_MAX_LEVEL=65535 del codigo real de
    # ha-crestron-home, y la conversion equivalente en crestron-mcp. El wire
    # format de la API SIEMPRE es esta escala; herramientas que exponen 0-100
    # hacia afuera convierten antes de tocar el wire, igual que este cliente.
    _LEVEL_MAX = 65535

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        verify_ssl: bool = False,
        request_timeout: float = 10.0,
    ) -> None:
        # Aceptamos base_url con o sin el sufijo /cws/api: lo normalizamos para no
        # duplicarlo (nuestras rutas ya incluyen /cws/api). Asi funciona tanto
        # "https://<ip>" como "https://<ip>/cws/api".
        base = base_url.rstrip("/")
        if base.lower().endswith("/cws/api"):
            base = base[: -len("/cws/api")]
        self._base_url = base.rstrip("/")

        self._token = token
        # aiohttp: ssl=False desactiva la validacion del cert (autofirmado del
        # CP4-R V2); ssl=True usa la validacion por defecto.
        self._ssl: bool = verify_ssl
        self._request_timeout = request_timeout

        self._session: aiohttp.ClientSession | None = None
        self._authkey: str | None = None
        # Serializa el (re)login para que varios requests concurrentes no disparen
        # varios logins a la vez cuando la sesion expira (thundering herd).
        self._auth_lock = asyncio.Lock()

    def __repr__(self) -> str:  # nunca exponer token ni AuthKey
        return f"CrestronClient(base_url={self._base_url!r}, connected={self.connected})"

    # ── Ciclo de vida ─────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Crea la sesion HTTP reutilizable; llamar durante el lifespan de FastAPI."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._request_timeout, connect=5)
            self._session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        """Borra el token/AuthKey de memoria y cierra la sesion HTTP."""
        self._authkey = None
        self._token = ""  # el token se declara ido al cerrar
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    @property
    def connected(self) -> bool:
        """True si hay un AuthKey vigente en memoria."""
        return self._authkey is not None

    def _require_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            raise CrestronError("crestron_client_not_started", 503)
        return self._session

    def _url(self, path: str) -> str:
        """Arma la URL final desde base_url + un path FIJO del codigo (anti-SSRF)."""
        return f"{self._base_url}{path}"

    # ── Autenticacion / sesion ────────────────────────────────────────────────

    async def login(self) -> None:
        """Hace el GET /cws/api/login con el token y guarda el AuthKey resultante."""
        async with self._auth_lock:
            await self._login_locked()

    async def _login_locked(self) -> None:
        """Login efectivo; asume el `_auth_lock` tomado."""
        session = self._require_session()
        url = self._url("/cws/api/login")
        headers = {self._LOGIN_HEADER: self._token, "Accept": "application/json"}
        try:
            async with session.get(
                url, headers=headers, allow_redirects=False, ssl=self._ssl
            ) as response:
                if response.status in (401, 403, 511):
                    raise CrestronError("crestron_auth_failed", 502)
                if response.status >= 400:
                    raise CrestronError("crestron_login_failed", 502)
                data = await self._read_json(response)
        except CrestronError:
            raise
        except TimeoutError as exc:
            raise CrestronError("crestron_timeout", 504) from exc
        except aiohttp.ClientError as exc:
            raise CrestronError("crestron_connection_failed", 502) from exc

        authkey = self._extract_authkey(data)
        if not authkey:
            raise CrestronError("crestron_login_no_authkey", 502)
        self._authkey = authkey
        # Log SIN el token ni el AuthKey; solo la version que reporta el CWS.
        version = data.get("version") if isinstance(data, dict) else None
        logger.info("crestron.login_ok", version=version)

    async def _ensure_login(self, stale: str | None) -> None:
        """(Re)autentica solo si hace falta, coalesciendo llamadas concurrentes.

        `stale` es el AuthKey que quien llama vio justo antes de fallar. Bajo el
        lock, solo (re)hacemos login si nadie renovo la sesion mientras
        esperabamos (el AuthKey sigue None o sigue siendo el mismo `stale`).
        """
        async with self._auth_lock:
            if self._authkey is None or self._authkey == stale:
                await self._login_locked()

    @staticmethod
    def _extract_authkey(data: Any) -> str | None:
        """Saca el AuthKey del JSON de login, tolerante al casing de la clave.

        Crestron puede devolverlo como `authkey`/`AuthKey`/`authKey`; parseamos
        case-insensitive y aceptamos solo un string no vacio.
        """
        if not isinstance(data, dict):
            return None
        for key, value in data.items():
            if (
                isinstance(key, str)
                and key.lower() == "authkey"
                and isinstance(value, str)
                and value
            ):
                return value
        return None

    # ── Request helper con re-login automatico ────────────────────────────────

    async def _request(
        self, method: str, path: str, *, json_body: dict[str, Any] | None = None
    ) -> Any:
        """Hace un request autenticado; re-login UNA vez si la sesion expiro.

        Si no hay AuthKey, primero hace login. Ante 401/511 (sesion vencida),
        re-hace login UNA sola vez y reintenta el request UNA vez. Si vuelve a
        vencer, `CrestronError("crestron_auth_failed", 502)`.
        """
        self._require_session()  # valida temprano que start() se haya llamado
        snapshot = self._authkey
        if snapshot is None:
            await self._ensure_login(None)
            snapshot = self._authkey

        try:
            return await self._send(method, path, json_body)
        except CrestronError as exc:
            if exc.code != "crestron_session_expired":
                raise
        # La sesion expiro (401/511): UN re-login + UN reintento.
        await self._ensure_login(snapshot)
        try:
            return await self._send(method, path, json_body)
        except CrestronError as exc:
            if exc.code == "crestron_session_expired":
                raise CrestronError("crestron_auth_failed", 502) from exc
            raise

    async def _send(
        self, method: str, path: str, json_body: dict[str, Any] | None
    ) -> Any:
        """Un unico request con el AuthKey vigente (sin logica de reintento)."""
        session = self._require_session()
        url = self._url(path)
        headers = {self._AUTHKEY_HEADER: self._authkey or "", "Accept": "application/json"}
        try:
            async with session.request(
                method,
                url,
                json=json_body,
                headers=headers,
                allow_redirects=False,
                ssl=self._ssl,
            ) as response:
                if response.status in (401, 511):
                    raise CrestronError("crestron_session_expired", 502)
                if response.status == 404:
                    raise CrestronError("crestron_not_found", 404)
                if response.status >= 400:
                    raise CrestronError("crestron_request_failed", 502)
                return await self._read_json(response)
        except CrestronError:
            raise
        except TimeoutError as exc:
            raise CrestronError("crestron_timeout", 504) from exc
        except aiohttp.ClientError as exc:
            raise CrestronError("crestron_connection_failed", 502) from exc

    async def _read_json(self, response: aiohttp.ClientResponse) -> Any:
        """Lee el body con tope de tamaño y lo parsea como JSON (defensivo).

        No confia en el Content-Type: Crestron a veces manda JSON sin declararlo.
        Un body vacio se trata como {} para que quien llama no tenga que distinguir.
        """
        raw = await response.content.read(self._MAX_RESPONSE_BYTES + 1)
        if len(raw) > self._MAX_RESPONSE_BYTES:
            raise CrestronError("crestron_response_too_large", 502)
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise CrestronError("crestron_invalid_json", 502) from exc

    # ── Lectura de dispositivos ───────────────────────────────────────────────
    #
    # 🔪 REDISEÑADO 2026-08-12. `GET /cws/api/devices` (el unico que se usaba
    # antes) NO trae nivel/posicion/estado — confirmado con el ejemplo verbatim
    # de la doc oficial (Devices API): solo `id`, `name`, `type`, `subType`,
    # `roomId`. Y las escenas NO aparecen ahi en absoluto — viven solo en
    # `/scenes`. O sea que con una sola llamada, antes de este cambio, las luces
    # y persianas iban a reportarse SIEMPRE sin nivel, y las escenas nunca se
    # iban a descubrir. La solucion combina 4 llamadas — ver `get_devices()`.

    async def get_rooms(self) -> dict[int, str]:
        """Trae el mapa id->nombre de sala desde `GET /cws/api/rooms`.

        Confirmado contra la doc oficial (Rooms API):
        `{"rooms": [{"id": 1, "name": "Atrium"}, ...], "version": "..."}`.
        Hace falta porque el resto de los endpoints (`/devices`, `/lights`,
        `/shades`, `/scenes`) solo traen `roomId` (numero), nunca un nombre.

        Degrada con gracia: si esta llamada falla, se loguea y se devuelve {}
        — los dispositivos van a aparecer sin nombre de cuarto en vez de tumbar
        el ciclo de polling entero por un problema en un endpoint secundario.
        """
        try:
            data = await self._request("GET", "/cws/api/rooms")
        except CrestronError as exc:
            logger.warning(
                "crestron.rooms_fetch_failed", code=exc.code, status=exc.status_code
            )
            return {}
        items = data.get("rooms") if isinstance(data, dict) else None
        if not isinstance(items, list):
            return {}
        rooms: dict[int, str] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            room_id = item.get("id")
            name = item.get("name")
            if isinstance(room_id, int) and isinstance(name, str) and name:
                rooms[room_id] = name
        return rooms

    async def get_lights(self, rooms: dict[int, str] | None = None) -> list[CrestronDevice]:
        """Trae luces CON nivel real desde `GET /cws/api/lights`.

        Confirmado (doc oficial, Lights API):
        `{"lights": [{"id","name","type","subType","level" (0-65535),
        "connectionStatus","roomId"}], "version"}`.
        """
        data = await self._request("GET", "/cws/api/lights")
        items = data.get("lights") if isinstance(data, dict) else None
        if not isinstance(items, list):
            return []
        return [
            self._parse_device(item, rooms=rooms, known_type="light")
            for item in items
            if isinstance(item, dict)
        ]

    async def get_shades(self, rooms: dict[int, str] | None = None) -> list[CrestronDevice]:
        """Trae persianas CON posicion real desde `GET /cws/api/shades`.

        Confirmado (doc oficial, Shades API):
        `{"shades": [{"position" (0-65535),"id","name","subType",
        "connectionStatus","roomId"}], "version"}`.
        """
        data = await self._request("GET", "/cws/api/shades")
        items = data.get("shades") if isinstance(data, dict) else None
        if not isinstance(items, list):
            return []
        return [
            self._parse_device(item, rooms=rooms, known_type="shade")
            for item in items
            if isinstance(item, dict)
        ]

    async def get_scenes(self, rooms: dict[int, str] | None = None) -> list[CrestronDevice]:
        """Trae escenas desde `GET /cws/api/scenes` — NO aparecen en `/devices`.

        Confirmado (doc oficial, Scenes API):
        `{"scenes": [{"id","name","type" (categoria, ej. "Lighting"/"Shade"),
        "status" (bool),"roomId"}], "version"}`.
        """
        data = await self._request("GET", "/cws/api/scenes")
        items = data.get("scenes") if isinstance(data, dict) else None
        if not isinstance(items, list):
            return []
        return [
            self._parse_device(item, rooms=rooms, known_type="scene")
            for item in items
            if isinstance(item, dict)
        ]

    async def get_devices(self) -> list[CrestronDevice]:
        """Trae y normaliza TODOS los dispositivos que el Mirror sabe mapear.

        Combina 4 llamadas (ver el comentario de arriba de esta seccion):
          1. `/rooms`  -> mapa id->nombre, para resolver el `roomId` numerico.
          2. `/lights`, `/shades`, `/scenes` -> los 3 tipos con endpoint propio
             y estado REAL confirmado.
          3. `/devices` -> inventario generico, para no perder lo que todavia
             no tiene endpoint dedicado (sensores, thermostats, locks, media).
             Se descartan de aca los id que ya vinieron por (2), para no
             duplicar una luz/persiana/escena con una copia sin estado real.
        """
        rooms = await self.get_rooms()
        lights = await self.get_lights(rooms)
        shades = await self.get_shades(rooms)
        scenes = await self.get_scenes(rooms)

        data = await self._request("GET", "/cws/api/devices")
        items = self._extract_device_list(data)
        generic = [
            self._parse_device(item, rooms=rooms)
            for item in items
            if isinstance(item, dict)
        ]

        typed_ids = {d.id for d in lights} | {d.id for d in shades} | {d.id for d in scenes}
        devices = [*lights, *shades, *scenes, *(d for d in generic if d.id not in typed_ids)]

        if len(devices) > self._MAX_DEVICES:
            logger.warning(
                "crestron.devices_truncated", total=len(devices), max=self._MAX_DEVICES
            )
            devices = devices[: self._MAX_DEVICES]

        logger.info(
            "crestron.devices_fetched",
            total=len(devices),
            lights=len(lights),
            shades=len(shades),
            scenes=len(scenes),
            other=len(devices) - len(lights) - len(shades) - len(scenes),
        )
        return devices

    async def get_device(
        self, device_id: int, *, hint: Literal["light", "shade", "scene"] | None = None
    ) -> CrestronDevice:
        """Trae y normaliza UN dispositivo por id, con su estado real si se puede.

        Quien llama (el connector, tras ejecutar un control) suele SABER el
        dominio de la entidad — pasarlo en `hint` ahorra intentos: se prueba
        ese endpoint tipado primero. Sin `hint`, se prueban los 3 endpoints
        tipados en orden y recien despues se cae al generico `/devices/{id}`
        (que NO trae estado, confirmado — sirve para identidad nada mas, mejor
        que un 404 duro en tipos que aun no tienen endpoint propio).
        Un 404 en un endpoint tipado significa "no es de ese tipo": se prueba
        el siguiente, no se trata como error.
        """
        typed: list[Literal["light", "shade", "scene"]] = ["light", "shade", "scene"]
        if hint in typed:
            typed.remove(hint)
            typed.insert(0, hint)

        paths: dict[Literal["light", "shade", "scene"], str] = {
            "light": "/cws/api/lights",
            "shade": "/cws/api/shades",
            "scene": "/cws/api/scenes",
        }
        for known_type in typed:
            try:
                data = await self._request("GET", f"{paths[known_type]}/{device_id}")
            except CrestronError as exc:
                if exc.code == "crestron_not_found":
                    continue
                raise
            item = self._unwrap_single(data)
            if item is not None:
                return self._parse_device(item, known_type=known_type)

        # Ninguno tipado lo tenia: cae al generico (identidad sin estado real,
        # pero mejor que un 404 duro para sensores/thermostats/locks/media que
        # todavia no tienen endpoint propio en este cliente).
        data = await self._request("GET", f"/cws/api/devices/{device_id}")
        item = self._unwrap_single(data)
        if item is not None:
            return self._parse_device(item)
        raise CrestronError("crestron_device_not_found", 404)

    @staticmethod
    def _unwrap_single(data: Any) -> dict[str, Any] | None:
        """Desenvuelve la respuesta de un GET de un solo item.

        El CWS puede devolver el objeto directo, envuelto en una lista, o en
        un dict `{"lights": [...]}`/`{"devices": [...]}`. Cubre las formas sin
        crashear; `None` si no se pudo extraer nada usable.
        """
        if isinstance(data, dict):
            for key in ("lights", "shades", "scenes", "devices", "Devices", "results"):
                value = data.get(key)
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    return value[0]
            # No vino envuelto en ninguna lista conocida: puede ser el objeto
            # directo (heuristica: trae "id" o "name").
            if "id" in data or "name" in data:
                return data
            return None
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
        return None

    @staticmethod
    def _extract_device_list(data: Any) -> list[Any]:
        """Extrae la lista de dispositivos de las formas que el CWS podria usar.

        Acepta una lista pelada o un dict con la lista bajo `devices`/`Devices`/
        `results`. Cualquier otra cosa -> lista vacia (nunca crashea).
        """
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("devices", "Devices", "device", "results"):
                value = data.get(key)
                if isinstance(value, list):
                    return value
        return []

    # ── Normalizacion ─────────────────────────────────────────────────────────

    @classmethod
    def _parse_device(
        cls,
        raw: dict[str, Any],
        *,
        rooms: dict[int, str] | None = None,
        known_type: Literal["light", "shade", "scene"] | None = None,
    ) -> CrestronDevice:
        """Normaliza un dict crudo de Crestron a `CrestronDevice`.

        Todo con `.get()`/lookup tolerante y defaults: un campo faltante o de tipo
        raro nunca debe tumbar el parseo.

        `rooms` — el JSON real NO trae un nombre de cuarto, trae `roomId`
        (numero) — confirmado contra la doc oficial de Devices/Lights/Shades/
        Scenes API. Si se pasa el mapa id->nombre (de `get_rooms()`), se resuelve
        aca; si no hay mapa o el id no esta, `room` queda en `None` en vez de
        mostrar un numero pelado que no le dice nada a nadie.

        `known_type` — cuando el item viene de un endpoint TIPADO (`/lights`,
        `/shades`, `/scenes`) ya sabemos la categoria con certeza y no hace
        falta adivinarla por substring. Sin esto (ej. items de `/devices`, que
        cubre tipos sin endpoint propio todavia) se usa la heuristica de
        siempre.
        """
        raw_type = cls._as_str(cls._get_ci(raw, "type", "subType", "deviceType"), "")
        name = cls._as_str(cls._get_ci(raw, "name", "deviceName"), "")
        device_id = cls._coerce_int(cls._get_ci(raw, "id", "deviceId"))

        # Nombre de cuarto: preferir un string ya armado si el CWS algun dia lo
        # manda asi (roomName/room); si no, resolver el roomId numerico real
        # contra el mapa de /rooms.
        room = cls._get_ci(raw, "roomName", "room")
        room_name = str(room) if room is not None else None
        if room_name is None and rooms:
            room_id_raw = cls._get_ci(raw, "roomId", "RoomId")
            if room_id_raw is not None:
                room_name = rooms.get(cls._coerce_int(room_id_raw))

        level = cls._coerce_level(cls._get_ci(raw, "level", "position", "value"))
        status_val = cls._get_ci(raw, "status", "state", "powerState")
        reachable = cls._coerce_reachable(
            cls._get_ci(raw, "reachable", "available", "online", "connectionStatus")
        )
        device_type = known_type or cls._classify(raw_type, name)
        return CrestronDevice(
            id=device_id,
            name=name,
            device_type=device_type,
            raw_type=raw_type,
            room=room_name,
            level=level,
            status=str(status_val) if status_val is not None else None,
            reachable=reachable,
            raw=raw,
        )

    @staticmethod
    def _get_ci(raw: dict[str, Any], *keys: str) -> Any:
        """Busca la primera clave que exista, con fallback case-insensitive."""
        for key in keys:
            if key in raw:
                return raw[key]
        lowered = {k.lower(): v for k, v in raw.items() if isinstance(k, str)}
        for key in keys:
            hit = lowered.get(key.lower())
            if hit is not None:
                return hit
        return None

    @classmethod
    def _classify(
        cls, raw_type: str, name: str
    ) -> Literal["light", "shade", "scene", "sensor", "unknown"]:
        """Mapea el tipo crudo (y el nombre como respaldo) a la categoria normalizada.

        Heuristica por substrings porque los strings exactos de Crestron no estan
        confirmados. Se prueba primero contra el tipo y despues contra el nombre.
        """
        for text in (raw_type, name):
            result = cls._match_type(text)
            if result != "unknown":
                return result
        return "unknown"

    @staticmethod
    def _match_type(
        text: str,
    ) -> Literal["light", "shade", "scene", "sensor", "unknown"]:
        low = text.lower()
        # Orden pensado para que "sensor de puerta" no caiga en "switch" (light):
        # shade/scene/sensor se evaluan antes que light.
        if any(s in low for s in ("shade", "blind", "drape", "cover", "curtain", "roller")):
            return "shade"
        if "scene" in low:
            return "scene"
        if any(s in low for s in ("sensor", "occupancy", "door", "photo", "lux", "contact", "motion")):
            return "sensor"
        if any(s in low for s in ("dimmer", "light", "switch", "lamp")):
            return "light"
        return "unknown"

    @staticmethod
    def _as_str(value: Any, default: str) -> str:
        return str(value) if value is not None else default

    @staticmethod
    def _coerce_int(value: Any) -> int:
        """Convierte a int sin crashear; 0 si no se puede (bool NO cuenta)."""
        if isinstance(value, bool):
            return 0
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            try:
                return int(value.strip())
            except ValueError:
                return 0
        return 0

    @classmethod
    def _coerce_level(cls, value: Any) -> int | None:
        """Normaliza el nivel reportado (0-65535, confirmado) a 0-100.

        La heuristica ">100 => esta en escala 65535" se mantiene como
        salvaguarda defensiva aunque la escala ya este confirmada: si algun
        dia un endpoint reporta distinto (ej. un `subType` que resulte no
        tener nivel real y devuelva un numero chico por accidente), esto no
        rompe — simplemente no reescala algo que ya viene 0-100.
        """
        if isinstance(value, bool) or value is None:
            return None
        num: float
        if isinstance(value, (int, float)):
            num = float(value)
        elif isinstance(value, str):
            try:
                num = float(value.strip())
            except ValueError:
                return None
        else:
            return None
        if num > 100:
            num = num / cls._LEVEL_MAX * 100
        return _clamp(int(round(num)), 0, 100)

    @staticmethod
    def _coerce_reachable(value: Any) -> bool:
        """Interpreta el campo de alcance; sin dato -> True (asumimos alcanzable)."""
        if value is None:
            return True
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            return value.strip().lower() in (
                "true",
                "1",
                "online",
                "connected",
                "available",
                "reachable",
                "ok",
            )
        return True

    @classmethod
    def _scale_level(cls, pct: int) -> int:
        """Convierte un porcentaje 0-100 a la escala de nivel del hardware."""
        return round(_clamp(int(pct), 0, 100) / 100 * cls._LEVEL_MAX)

    # ── Control ───────────────────────────────────────────────────────────────

    async def set_light(
        self, device_id: int, *, on: bool, level: int | None = None
    ) -> None:
        """Enciende/apaga una luz y, si es dimmer, fija el nivel (0-100, se acota).

        Confirmado (doc oficial, Lights API + codigo real de ha-crestron-home):
        el endpoint es BATCH, `POST /cws/api/lights/SetState` con
        `{"lights": [{"id": N, "level": 0-65535, "time": ms}]}` — no
        `/cws/api/lights/{id}`. No existe un campo on/off separado: `level=0`
        es apagado, `level>0` es encendido. `time` es la duracion del fade; se
        manda `0` para cambio instantaneo (no confirmado si es obligatorio,
        pero mandarlo siempre es lo mas seguro).
        """
        if not on:
            target_pct = 0
        elif level is None:
            target_pct = 100
        else:
            target_pct = _clamp(level, 0, 100)

        payload = {
            "lights": [{"id": device_id, "level": self._scale_level(target_pct), "time": 0}]
        }
        await self._request("POST", "/cws/api/lights/SetState", json_body=payload)
        logger.info(
            "crestron.control",
            device_id=device_id,
            action="set_light",
            on=on,
            level=target_pct,
        )

    async def set_shade(
        self,
        device_id: int,
        *,
        position: int | None = None,
        action: Literal["open", "close", "stop"] | None = None,
    ) -> None:
        """Mueve una persiana a una posicion 0-100 (se acota) o ejecuta open/close/stop.

        Confirmado (doc oficial, Shades API): el endpoint es BATCH,
        `POST /cws/api/shades/SetState` con
        `{"shades": [{"id": N, "position": 0-65535}]}` — no `/cws/api/shades/{id}`.

        🔪 NO EXISTE un endpoint de "stop" en ningun lado (ni la doc oficial ni
        ninguna integracion open-source lo tiene). Confirmado leyendo el
        codigo real de `ha-crestron-home` (`cover.py`, `async_stop_cover`):
        "parar" una persiana es RELEER su posicion actual y reenviarla como
        destino al mismo `SetState` — el motor recibe la orden y, como ya esta
        ahi, no se mueve mas. Por eso `action="stop"` hace un `GET /shades/{id}`
        antes de mandar el POST.

        TODO verificar contra hardware real: el SENTIDO de la posicion (¿0
        siempre es cerrada, o depende de como el instalador configuro el
        motor?). Ninguna fuente lo garantiza — esto SI varia por instalacion.
        """
        if position is None and action is None:
            raise CrestronError("crestron_bad_request", 400)

        if action == "stop":
            current = await self.get_device(device_id, hint="shade")
            target_pct = current.level if current.level is not None else 0
            payload = {"shades": [{"id": device_id, "position": self._scale_level(target_pct)}]}
            await self._request("POST", "/cws/api/shades/SetState", json_body=payload)
            logger.info(
                "crestron.control",
                device_id=device_id,
                action="shade_stop",
                held_position=target_pct,
            )
            return

        if action is not None:
            # open -> abierta del todo; close -> cerrada del todo.
            target_pct = 100 if action == "open" else 0
        else:
            target_pct = _clamp(position if position is not None else 0, 0, 100)

        payload = {"shades": [{"id": device_id, "position": self._scale_level(target_pct)}]}
        await self._request("POST", "/cws/api/shades/SetState", json_body=payload)
        logger.info(
            "crestron.control",
            device_id=device_id,
            action="set_shade",
            position=target_pct,
        )

    async def recall_scene(self, device_id: int) -> None:
        """Dispara (recall) una escena por su id.

        Confirmado (doc oficial, Scenes API): `POST /cws/api/scenes/recall/{id}`
        sin cuerpo. Esta es la unica de las tres acciones de control que YA
        estaba bien desde el principio — coincide exacto con lo que ya usaba
        el codigo antes de esta revision.
        """
        await self._request(
            "POST", f"/cws/api/scenes/recall/{device_id}", json_body={}
        )
        logger.info("crestron.control", device_id=device_id, action="recall_scene")


if __name__ == "__main__":
    # Smoke test manual: el dia que tengamos IP+token real.
    #   CRESTRON_BASE_URL=https://<ip> CRESTRON_TOKEN=<token> \
    #       python -m ha_mirror.crestron_client
    #   o: python crestron_client.py https://<ip> <token>
    # NUNCA imprime el token ni el AuthKey.
    async def _smoke() -> None:
        base_url = os.environ.get("CRESTRON_BASE_URL") or (
            sys.argv[1] if len(sys.argv) > 1 else ""
        )
        token = os.environ.get("CRESTRON_TOKEN") or (
            sys.argv[2] if len(sys.argv) > 2 else ""
        )
        if not base_url or not token:
            print(
                "Uso: CRESTRON_BASE_URL=... CRESTRON_TOKEN=... "
                "python crestron_client.py\n"
                "  o: python crestron_client.py https://<ip> <token>"
            )
            return

        client = CrestronClient(base_url=base_url, token=token, verify_ssl=False)
        await client.start()
        try:
            await client.login()
            print(f"login OK -> connected={client.connected}")
            devices = await client.get_devices()
            print(f"devices: {len(devices)}")
            for d in devices[:10]:
                print(
                    f"  #{d.id} [{d.device_type}] {d.name!r} "
                    f"room={d.room} level={d.level} reachable={d.reachable}"
                )
        except CrestronError as exc:
            print(f"CrestronError: {exc.code} (status {exc.status_code})")
        finally:
            await client.close()

    asyncio.run(_smoke())
