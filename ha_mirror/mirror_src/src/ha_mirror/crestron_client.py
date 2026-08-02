"""Cliente de bajo nivel para la API REST local de Crestron Home (servidor "CWS").

Habla contra `https://{ip}/cws/api` de un procesador CP4-R (Crestron Home OS 4).
Solo se usan GET y POST con JSON. La sesion se autentica con un token que Crestron
canjea por un `AuthKey` de corta vida (~10 min de inactividad -> 401/511 -> re-login).

Decisiones de diseno que importan:

- **Sin hardware real todavia.** La forma EXACTA del JSON de `/devices` y de los
  cuerpos POST de control NO esta confirmada. Por eso el parseo es DEFENSIVO
  (tolera campos faltantes o tipos raros sin crashear) y los payloads de control
  quedan AISLADOS en cada metodo con un `# TODO verificar contra hardware real`
  donde la forma sea una suposicion. La idea: dejar todo listo para que el dia que
  tengamos IP+token solo haya que ajustar un puñado de literales bien marcados.

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

    # Header con el que Crestron acepta primero el token y luego el AuthKey.
    _AUTH_HEADER = "Crestron-RestAPI-AuthToken"

    # Tope de bytes por respuesta: una respuesta enorme no deberia reventar la RAM.
    _MAX_RESPONSE_BYTES = 4 * 1024 * 1024  # 4 MB
    # Tope de dispositivos parseados por sanidad (una casa real no llega a esto).
    _MAX_DEVICES = 2000

    # Escala de nivel del hardware. En varias builds de la CWS API el nivel de un
    # dimmer/persiana va de 0 a 65535 (no 0-100). Convertimos el 0-100 del contrato
    # a esta escala al mandar, y de vuelta a 0-100 al parsear.
    # TODO verificar contra hardware real: puede ser 0-100 directo. Si asi fuera,
    # poner _LEVEL_MAX = 100 y todo el resto sigue funcionando sin tocar mas nada.
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
        headers = {self._AUTH_HEADER: self._token, "Accept": "application/json"}
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
        headers = {self._AUTH_HEADER: self._authkey or "", "Accept": "application/json"}
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

    async def get_devices(self) -> list[CrestronDevice]:
        """Trae y normaliza todos los dispositivos de `/cws/api/devices`."""
        data = await self._request("GET", "/cws/api/devices")
        items = self._extract_device_list(data)
        if len(items) > self._MAX_DEVICES:
            logger.warning(
                "crestron.devices_truncated", total=len(items), max=self._MAX_DEVICES
            )
            items = items[: self._MAX_DEVICES]
        devices = [self._parse_device(item) for item in items if isinstance(item, dict)]
        logger.info("crestron.devices_fetched", count=len(devices))
        return devices

    async def get_device(self, device_id: int) -> CrestronDevice:
        """Trae y normaliza un dispositivo por id desde `/cws/api/devices/{id}`."""
        data = await self._request("GET", f"/cws/api/devices/{device_id}")
        # El CWS puede devolver el device directo, envuelto en una lista, o en un
        # dict {"devices": [...]}. Cubrimos las tres formas sin crashear.
        if isinstance(data, dict):
            items = self._extract_device_list(data)
            if items and isinstance(items[0], dict):
                return self._parse_device(items[0])
            return self._parse_device(data)
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return self._parse_device(data[0])
        raise CrestronError("crestron_device_not_found", 404)

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
    def _parse_device(cls, raw: dict[str, Any]) -> CrestronDevice:
        """Normaliza un dict crudo de Crestron a `CrestronDevice`.

        Todo con `.get()`/lookup tolerante y defaults: un campo faltante o de tipo
        raro nunca debe tumbar el parseo. Los strings exactos de "type"/"subType"
        no estan confirmados sin hardware, asi que la clasificacion es heuristica.
        """
        raw_type = cls._as_str(cls._get_ci(raw, "type", "subType", "deviceType"), "")
        name = cls._as_str(cls._get_ci(raw, "name", "deviceName"), "")
        device_id = cls._coerce_int(cls._get_ci(raw, "id", "deviceId"))
        room = cls._get_ci(raw, "roomName", "room")
        level = cls._coerce_level(cls._get_ci(raw, "level", "position", "value"))
        status_val = cls._get_ci(raw, "status", "state", "powerState")
        reachable = cls._coerce_reachable(
            cls._get_ci(raw, "reachable", "available", "online", "connectionStatus")
        )
        return CrestronDevice(
            id=device_id,
            name=name,
            device_type=cls._classify(raw_type, name),
            raw_type=raw_type,
            room=str(room) if room is not None else None,
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
        """Normaliza el nivel reportado a 0-100, o None si no aplica/no es numerico.

        TODO verificar contra hardware real: si el CWS reporta el nivel en escala
        0-65535 lo bajamos a 0-100 (heuristica: cualquier valor > 100 se asume en
        esa escala); si ya viene 0-100 se deja igual.
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
        """Enciende/apaga una luz y, si es dimmer, fija el nivel (0-100, se acota)."""
        if not on:
            target_pct = 0
        elif level is None:
            target_pct = 100
        else:
            target_pct = _clamp(level, 0, 100)

        # TODO verificar contra hardware real: la ruta y el cuerpo exactos del POST
        # de control de luces no estan confirmados sin la cajita. Suposicion:
        #   POST /cws/api/lights/{id}   body {"level": <0-65535>}
        #   (0 = apagado, 65535 = 100%). Alternativa vista en otras builds:
        #   body {"state": "on"|"off", "level": <0-100>}. Si es esa, cambiar solo
        #   este dict y la ruta de abajo.
        payload = {"level": self._scale_level(target_pct)}
        await self._request("POST", f"/cws/api/lights/{device_id}", json_body=payload)
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
        """Mueve una persiana a una posicion 0-100 (se acota) o ejecuta open/close/stop."""
        if position is None and action is None:
            raise CrestronError("crestron_bad_request", 400)

        if action == "stop":
            # TODO verificar contra hardware real: se asume un endpoint de stop
            # dedicado sin cuerpo. Alternativa: POST /cws/api/shades/{id} con
            # body {"action": "stop"}.
            await self._request(
                "POST", f"/cws/api/shades/{device_id}/stop", json_body={}
            )
            logger.info("crestron.control", device_id=device_id, action="shade_stop")
            return

        if action is not None:
            # open -> abierta del todo; close -> cerrada del todo.
            target_pct = 100 if action == "open" else 0
        else:
            target_pct = _clamp(position if position is not None else 0, 0, 100)

        # TODO verificar contra hardware real: ruta/cuerpo de posicion de persiana
        # asumidos. Suposicion:
        #   POST /cws/api/shades/{id}   body {"position": <0-65535>}
        #   (0 = cerrada, 65535 = abierta). Confirmar tambien el sentido: en
        #   algunas instalaciones 0 = abierta. Si se invierte, negar aca el pct.
        payload = {"position": self._scale_level(target_pct)}
        await self._request("POST", f"/cws/api/shades/{device_id}", json_body=payload)
        logger.info(
            "crestron.control",
            device_id=device_id,
            action="set_shade",
            position=target_pct,
        )

    async def recall_scene(self, device_id: int) -> None:
        """Dispara (recall) una escena por su id."""
        # TODO verificar contra hardware real: el recall de escena podria ser
        #   POST /cws/api/scenes/recall/{id}   (sin cuerpo) -- asumido aqui -- o
        #   GET  /cws/api/scenes/recall/{id}, o POST /cws/api/scenes/{id} con
        #   body {"action": "recall"}. Aislado para ajustar de una sola linea.
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
