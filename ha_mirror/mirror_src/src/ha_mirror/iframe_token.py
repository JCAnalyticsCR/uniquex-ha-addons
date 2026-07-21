"""
Generación de URLs temporales para el iframe de HA (Opción C del frontend).

El componente <HAEmbed view="energia" /> del frontend llama a
GET /api/iframe-token?view=energia y recibe una URL firmada de corta duración
que apunta al HA real en el tailnet.

Estrategia:
  - itsdangerous.URLSafeTimedSerializer firma la URL con el IFRAME_TOKEN_SECRET.
  - La URL tiene el path de la vista Lovelace en el tailnet con HTTPS.
  - TTL configurable (default 15 min) via settings.iframe_token_ttl_seconds.

El frontend pone esa URL como src del iframe. HA acepta la petición porque
el browser ya tiene una cookie de sesión HA (establecida en la primera carga).

Para que el iframe cargue sin pedir login adicional, el usuario debe haber
autenticado en HA al menos una vez (el browser retiene la cookie de sesión HA).
Si la sesión HA expiró, el iframe mostrará la pantalla de login de HA — esto
es aceptable para N=1 donde el usuario siempre llega con sesión activa.
"""

from __future__ import annotations

import structlog
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from ha_mirror.config import Settings

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# Vistas Lovelace soportadas (whitelist para evitar open redirect)
ALLOWED_VIEWS: frozenset[str] = frozenset({
    "0",          # Vista principal / resumen
    "energia",
    "historial",
    "camaras",
    "mapa",
    "admin",
    "hacs",
    "media",
})


def _get_serializer(settings: Settings) -> URLSafeTimedSerializer:
    secret = settings.iframe_token_secret.get_secret_value()
    return URLSafeTimedSerializer(secret, salt="iframe-view")


def generate_iframe_url(view: str, settings: Settings) -> dict[str, str]:
    """
    Genera una URL firmada al HA real para el iframe.

    Retorna:
      {
        "url": "https://ha-gateway.caracara-bicolor.ts.net/lovelace/0/energia?_t=<token>",
        "view": "energia",
        "expires_in": 900
      }
    """
    # Sanitizar la vista solicitada
    clean_view = view.strip("/").lower()
    if clean_view not in ALLOWED_VIEWS:
        logger.warning("iframe.unknown_view", requested=view, fallback="0")
        clean_view = "0"

    serializer = _get_serializer(settings)
    token = serializer.dumps({"view": clean_view})

    ha_host = settings.tailscale_serve_ha_hostname
    base_url = f"https://{ha_host}/lovelace/0/{clean_view}"
    signed_url = f"{base_url}?_t={token}"

    logger.info("iframe.token_generated", view=clean_view, ha_host=ha_host)

    return {
        "url": signed_url,
        "view": clean_view,
        "expires_in": settings.iframe_token_ttl_seconds,
    }


def verify_iframe_token(token: str, settings: Settings) -> dict[str, str] | None:
    """
    Verifica un token de iframe (para validación interna si se necesita).

    Retorna el payload o None si el token expiró o es inválido.
    """
    serializer = _get_serializer(settings)
    try:
        payload: dict[str, str] = serializer.loads(
            token, max_age=settings.iframe_token_ttl_seconds
        )
        return payload
    except SignatureExpired:
        logger.info("iframe.token_expired")
        return None
    except BadSignature:
        logger.warning("iframe.token_invalid")
        return None
