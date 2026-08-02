"""
Configuración del mirror vía pydantic-settings.

Lee variables de entorno (o .env) y descifra el LLAT con Fernet.
El LLAT nunca queda expuesto en repr ni en logs gracias a SecretStr.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken
from pydantic import AliasChoices, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from ha_mirror.errors import MirrorConfigError


class Settings(BaseSettings):
    """Variables de configuración del mirror. Todas vienen de env o .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # -------------------------------------------------------------------------
    # Home Assistant upstream
    # -------------------------------------------------------------------------
    ha_url: str = Field(
        description="URL WebSocket de HA: ws://100.x.x.x:8123/api/websocket "
        "o ws://supervisor/core/websocket cuando corre como add-on de HA"
    )

    # --- Autenticación a HA: dos modos mutuamente excluyentes ---
    # Modo add-on (recomendado): token crudo del Supervisor de HA. Se lee de
    # HA_TOKEN o SUPERVISOR_TOKEN (esta última la inyecta HA al add-on cuando
    # tiene homeassistant_api: true). No requiere LLAT ni Fernet.
    ha_token: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("ha_token", "supervisor_token"),
        description=(
            "Token de acceso crudo a HA. En modo add-on es el token del "
            "Supervisor (SUPERVISOR_TOKEN). Si se define, se ignoran el LLAT "
            "cifrado y la Fernet key."
        ),
    )
    # Modo standalone (legacy): LLAT cifrado con Fernet. Solo se usa si ha_token
    # NO está definido.
    ha_llat_enc_path: Path | None = Field(
        default=None,
        description="Path al archivo LLAT cifrado con Fernet (modo standalone)",
    )
    fernet_key_path: Path | None = Field(
        default=None,
        description="Path a la Fernet master key (modo standalone)",
    )

    # -------------------------------------------------------------------------
    # Seguridad del mirror
    # -------------------------------------------------------------------------
    mirror_api_key: SecretStr = Field(
        description=(
            "API key primaria para autenticar el frontend. "
            "Longitud minima: 32 caracteres (256 bits de entropía). "
            "Si se define MIRROR_API_KEYS, este campo sirve como fallback."
        )
    )
    mirror_api_keys: SecretStr | None = Field(
        default=None,
        description=(
            "Lista de API keys activas separadas por coma para rotacion "
            "zero-downtime. Ej: MIRROR_API_KEYS=key1,key2,key3. "
            "Cuando esta definida, MIRROR_API_KEY queda como fallback ignorado. "
            "Permite hasta 3 keys simultaneas: dar de alta la nueva key, "
            "actualizar el cliente para usarla, luego revocar la vieja."
        ),
    )
    session_secret: SecretStr = Field(
        description="Secret para firmar cookies de sesión"
    )
    iframe_token_secret: SecretStr = Field(
        description="Secret para firmar URLs de iframe (itsdangerous)"
    )

    # -------------------------------------------------------------------------
    # Red
    # -------------------------------------------------------------------------
    mirror_host: str = Field(default="127.0.0.1")
    mirror_port: int = Field(default=8000)
    frontend_origin: str | None = Field(
        default=None,
        description=(
            "Origen(es) del frontend en producción para CORS, separados por coma "
            "(ej. https://fortunatta.up.railway.app)."
        ),
    )
    go2rtc_base_url: str | None = Field(
        default=None,
        description=(
            "URL interna de go2rtc (por ejemplo http://go2rtc:1984). "
            "Nunca debe apuntar a una interfaz publica."
        ),
    )
    go2rtc_username: str | None = Field(
        default=None,
        description="Usuario HTTP Basic de go2rtc, si esta habilitado.",
    )
    go2rtc_password: SecretStr | None = Field(
        default=None,
        description="Password HTTP Basic de go2rtc, si esta habilitado.",
    )
    camera_stream_map: str = Field(
        default="{}",
        description=(
            "JSON que relaciona camera.entity_id con un nombre de stream go2rtc. "
            'Ejemplo: {"camera.entrada":"entrada_sub"}'
        ),
    )
    camera_labels: str = Field(
        default="{}",
        description=(
            "JSON opcional con el nombre visible de cada camara. Si falta, se "
            "usa el friendly_name de HA o se deriva del entity_id. "
            'Ejemplo: {"camera.nvr_c21_gimnasio":"C21 GIMNASIO"}'
        ),
    )
    tailscale_serve_ha_hostname: str = Field(
        default="ha-gateway.example.ts.net",
        description="Hostname del HA en el tailnet con TLS"
    )
    tailscale_serve_mirror_hostname: str = Field(
        default="mirror.example.ts.net",
        description="Hostname del mirror en el tailnet (para CORS)"
    )

    # -------------------------------------------------------------------------
    # Storage y logs
    # -------------------------------------------------------------------------
    mirror_db_path: Path = Field(default=Path("/var/lib/ha-mirror/mirror.sqlite3"))
    log_level: str = Field(default="INFO")
    # Fix C3 — /docs, /redoc y /openapi.json NO se exponen en producción.
    # Solo se habilitan si EXPOSE_DOCS=true (útil en desarrollo).
    expose_docs: bool = Field(
        default=False,
        description="Si true, expone /docs, /redoc y /openapi.json. Default false (prod).",
    )

    # -------------------------------------------------------------------------
    # Parámetros operacionales (con defaults razonables)
    # -------------------------------------------------------------------------
    upstream_ping_interval: float = Field(default=30.0, description="Segundos entre pings")
    upstream_ping_timeout: float = Field(default=10.0, description="Timeout espera pong")
    service_call_timeout: float = Field(default=5.0, description="Timeout service call (202 fallback)")
    ws_queue_maxsize: int = Field(default=1000, description="Tamaño máx de queue por suscriptor WS")
    events_log_retention_days: int = Field(default=7, description="Retención del log de eventos")
    iframe_token_ttl_seconds: int = Field(default=900, description="TTL de tokens iframe (15 min)")

    # -------------------------------------------------------------------------
    # Tickets de WebSocket (0.21.0) — ver ha_mirror/ws_ticket.py
    # -------------------------------------------------------------------------
    ws_ticket_ttl_seconds: int = Field(
        default=30,
        ge=5,
        le=300,
        description=(
            "TTL del ticket de WebSocket. Corto a propósito: el ticket solo tiene "
            "que sobrevivir el tiempo entre que el frontend lo pide y el navegador "
            "abre el socket."
        ),
    )
    reject_legacy_ws_key: bool = Field(
        default=False,
        description=(
            "Rechaza WebSockets que lleguen con ?api_key= (el camino viejo, "
            "anterior a los tickets). ARRANCA EN False A PROPÓSITO: el add-on vive "
            "en la caja y el frontend en Railway, y se despliegan por separado — si "
            "el add-on deja de aceptar la key antes de que Railway tenga el código "
            "nuevo, las cámaras se caen. Activar solo cuando el log del add-on ya "
            "no muestre 'auth.ws_legacy_key_used'. Casa 2 puede arrancar en True: "
            "no tiene frontend viejo que respetar."
        ),
    )

    # -------------------------------------------------------------------------
    # Multi-tenant en frío: tenant_id constante
    # -------------------------------------------------------------------------
    tenant_id: int = Field(default=1, description="ID del tenant único (N=1)")

    # -------------------------------------------------------------------------
    # Crestron Home (conector directo)
    # -------------------------------------------------------------------------
    crestron_enabled: bool = Field(
        default=False,
        description=(
            "Activa el conector Crestron Home. Default off — el Mirror arranca "
            "normalmente sin CP4-R. Poné True solo cuando tengas acceso al procesador."
        ),
    )
    crestron_base_url: str | None = Field(
        default=None,
        description=(
            "URL base del CP4-R, p.ej. https://192.168.1.30. "
            "Requiere crestron_enabled=true. "
            "Nunca exponer el CP4-R directamente a internet: "
            "el Mirror debe alcanzarlo por LAN o tailnet."
        ),
    )
    crestron_token: SecretStr | None = Field(
        default=None,
        description=(
            "Token de la Web API de Crestron Home. "
            "Obtenerlo en Crestron Setup app → Installer Settings → "
            "System Control Options → Web API Settings → Update Token. "
            "NUNCA commitear el valor real — siempre vía variable de entorno o "
            "opción 'password?' del add-on."
        ),
    )
    crestron_verify_ssl: bool = Field(
        default=False,
        description=(
            "Verificar el certificado SSL del CP4-R. "
            "CP4-R V2 usa cert autofirmado → default False (no validar). "
            "Poné True solo si hay cert válido firmado por una CA reconocida."
        ),
    )
    crestron_poll_interval: float = Field(
        default=12.0,
        description=(
            "Intervalo en segundos entre polls al CP4-R. "
            "Default 12 s (rango recomendado 10-15 s)."
        ),
    )
    crestron_area_id: str = Field(
        default="crestron",
        description=(
            "ID de área virtual en el Mirror para los dispositivos Crestron. "
            "Default 'crestron'. Cambiar si el sistema tiene múltiples procesadores."
        ),
    )

    @field_validator("ha_url")
    @classmethod
    def validate_ha_url(cls, v: str) -> str:
        """Asegura que la URL sea ws:// o wss://."""
        if not v.startswith(("ws://", "wss://")):
            raise ValueError("ha_url debe comenzar con ws:// o wss://")
        return v

    @field_validator("go2rtc_base_url")
    @classmethod
    def validate_go2rtc_base_url(cls, v: str | None) -> str | None:
        if v is None or not v.strip():
            return None
        value = v.strip().rstrip("/")
        if not value.startswith(("http://", "https://")):
            raise ValueError("go2rtc_base_url debe comenzar con http:// o https://")
        return value

    @field_validator("crestron_base_url")
    @classmethod
    def validate_crestron_base_url(cls, v: str | None) -> str | None:
        if v is None or not v.strip():
            return None
        value = v.strip().rstrip("/")
        if not value.startswith(("http://", "https://")):
            raise ValueError("crestron_base_url debe comenzar con http:// o https://")
        return value

    @field_validator("crestron_token")
    @classmethod
    def validate_crestron_token(cls, v: SecretStr | None) -> SecretStr | None:
        """Convierte SecretStr vacío a None (p.ej. CRESTRON_TOKEN='' en .env)."""
        if v is None:
            return None
        if not v.get_secret_value().strip():
            return None
        return v

    @field_validator("camera_stream_map")
    @classmethod
    def validate_camera_stream_map(cls, v: str) -> str:
        try:
            parsed = json.loads(v or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError("camera_stream_map debe ser JSON valido") from exc
        if not isinstance(parsed, dict):
            raise ValueError("camera_stream_map debe ser un objeto JSON")
        entity_pattern = re.compile(r"^camera\.[a-z0-9_]+$")
        stream_pattern = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
        for entity_id, stream_name in parsed.items():
            if not isinstance(entity_id, str) or not entity_pattern.fullmatch(entity_id):
                raise ValueError(f"Entidad de camara invalida en camera_stream_map: {entity_id!r}")
            if not isinstance(stream_name, str) or not stream_pattern.fullmatch(stream_name):
                raise ValueError(f"Nombre de stream go2rtc invalido para {entity_id}")
        return json.dumps(parsed, separators=(",", ":"), sort_keys=True)

    @field_validator("camera_labels")
    @classmethod
    def validate_camera_labels(cls, v: str) -> str:
        """Valida el mapa opcional entity_id -> nombre visible de la camara."""
        try:
            parsed = json.loads(v or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError("camera_labels debe ser JSON valido") from exc
        if not isinstance(parsed, dict):
            raise ValueError("camera_labels debe ser un objeto JSON")
        entity_pattern = re.compile(r"^camera\.[a-z0-9_]+$")
        for entity_id, label in parsed.items():
            if not isinstance(entity_id, str) or not entity_pattern.fullmatch(entity_id):
                raise ValueError(f"Entidad de camara invalida en camera_labels: {entity_id!r}")
            if not isinstance(label, str) or not 1 <= len(label) <= 64:
                raise ValueError(f"Nombre de camara invalido para {entity_id}")
        return json.dumps(parsed, separators=(",", ":"), sort_keys=True)

    @field_validator("mirror_api_key")
    @classmethod
    def validate_mirror_api_key_length(cls, v: SecretStr) -> SecretStr:
        """
        Exige longitud minima de 32 bytes (256 bits) para la API key primaria.
        Genera un openssl rand -hex 32 para cumplir este requisito.
        """
        raw = v.get_secret_value()
        if len(raw.encode()) < 32:
            raise ValueError(
                "MIRROR_API_KEY debe tener al menos 32 bytes (256 bits). "
                "Generar con: openssl rand -hex 32"
            )
        return v

    @model_validator(mode="after")
    def validate_auth_config(self) -> Settings:
        """
        Valida la configuración de autenticación a HA.

        Modo add-on: ha_token definido → no se requieren archivos Fernet.
        Modo standalone: ha_token ausente → ha_llat_enc_path y fernet_key_path
        deben existir en disco.
        """
        if self.ha_token is not None:
            return self  # Modo add-on/Supervisor: token directo, sin Fernet

        # Modo standalone: exige el LLAT cifrado + Fernet key
        if self.ha_llat_enc_path is None or self.fernet_key_path is None:
            raise MirrorConfigError(
                "Sin HA_TOKEN definido: se requieren HA_LLAT_ENC_PATH y "
                "FERNET_KEY_PATH (modo standalone), o bien definir HA_TOKEN "
                "(modo add-on de HA)."
            )
        for p, label in [
            (self.ha_llat_enc_path, "LLAT cifrado"),
            (self.fernet_key_path, "Fernet key"),
        ]:
            if not p.exists():
                raise MirrorConfigError(
                    f"Archivo de {label} no encontrado: {p}. "
                    "Verificar que el agente #5 (secrets) haya corrido."
                )
        return self

    def get_ha_token(self) -> str:
        """
        Retorna el token de acceso a HA en texto plano.

        Modo add-on: devuelve ha_token (token del Supervisor) directamente.
        Modo standalone: descifra el LLAT con Fernet.
        Mantener el resultado en memoria solo el tiempo necesario.
        """
        if self.ha_token is not None:
            return self.ha_token.get_secret_value()
        return self.load_llat()

    def load_llat(self) -> str:
        """
        Descifra y retorna el LLAT en texto plano.

        El resultado solo debe mantenerse en memoria el tiempo necesario.
        Nunca loggear, nunca persistir en variables de larga vida fuera de HAUpstream.
        """
        if self.fernet_key_path is None or self.ha_llat_enc_path is None:
            raise MirrorConfigError(
                "load_llat() requiere ha_llat_enc_path y fernet_key_path (modo standalone)"
            )
        try:
            key = self.fernet_key_path.read_bytes().strip()
            f = Fernet(key)
            ciphertext = self.ha_llat_enc_path.read_bytes()
            return f.decrypt(ciphertext).decode("utf-8")
        except InvalidToken as exc:
            raise MirrorConfigError(
                "No se pudo descifrar el LLAT — Fernet key incorrecta o archivo corrupto"
            ) from exc
        except OSError as exc:
            raise MirrorConfigError(f"Error al leer archivos de secretos: {exc}") from exc

    @property
    def ha_https_url(self) -> str:
        """URL HTTP(S) base de HA derivada del WS URL."""
        return self.ha_http_url

    @property
    def ha_http_url(self) -> str:
        """URL HTTP(S) base de HA, compatible con Core y proxy del Supervisor."""
        value = self.ha_url.replace("ws://", "http://", 1).replace("wss://", "https://", 1)
        for suffix in ("/api/websocket", "/websocket"):
            if value.endswith(suffix):
                return value[: -len(suffix)]
        return value.rstrip("/")

    @property
    def camera_streams(self) -> dict[str, str]:
        """Mapa validado de entidades HA a nombres internos de go2rtc."""
        parsed = json.loads(self.camera_stream_map)
        return {str(key): str(value) for key, value in parsed.items()}

    @property
    def camera_label_map(self) -> dict[str, str]:
        """Nombres visibles opcionales por entidad de camara."""
        parsed = json.loads(self.camera_labels)
        return {str(key): str(value) for key, value in parsed.items()}

    @property
    def allowed_origins(self) -> list[str]:
        """
        Orígenes permitidos para CORS.

        Fix M2 — en producción (con frontend_origin definido) NO se permiten
        los orígenes localhost; solo el/los frontend real(es).
        """
        if self.frontend_origin:
            return [o.strip() for o in self.frontend_origin.split(",") if o.strip()]
        # Modo dev: sin frontend_origin de producción.
        return [
            f"https://{self.tailscale_serve_mirror_hostname}",
            "http://localhost:3000",
            "http://localhost:5173",
        ]

    @property
    def crestron_configured(self) -> bool:
        """True solo cuando el conector Crestron está habilitado y tiene URL + token."""
        return (
            self.crestron_enabled
            and self.crestron_base_url is not None
            and self.crestron_token is not None
        )

    def get_crestron_token(self) -> str | None:
        """
        Retorna el token Crestron en texto plano, o None si no está definido.

        El token Crestron llega por opción 'password?' del add-on (nunca cifrado
        con Fernet — no es un LLAT de HA). No loggear el resultado.
        """
        if self.crestron_token is None:
            return None
        return self.crestron_token.get_secret_value()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Instancia única de Settings (cacheada). Usar como dependencia FastAPI."""
    return Settings()  # type: ignore[call-arg]
