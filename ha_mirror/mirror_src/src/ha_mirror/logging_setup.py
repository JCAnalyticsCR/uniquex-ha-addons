"""
Configuración de structlog con output JSON para producción.

En desarrollo (LOG_LEVEL=DEBUG) usa ConsoleRenderer legible.
En producción (LOG_LEVEL=INFO+) usa JSONRenderer compatible con journald/Loki.

Incluye el procesador ``redact_secrets`` que aplica redacción contextual y precisa
sobre los event_dict de structlog antes de serializarlos. Ver docstring de
``redact_secrets`` para la estrategia exacta.
"""

from __future__ import annotations

import logging
import re
import sys
from typing import Any

import structlog

# ---------------------------------------------------------------------------
# Claves de contexto sensibles — si aparecen como kwarg en logger.xxx()
# se reemplazan por <REDACTED> y se registran en _redacted_keys.
# ---------------------------------------------------------------------------
_SENSITIVE_KEYS: frozenset[str] = frozenset({
    "token",
    "access_token",
    "llat",
    "bearer",
    "authorization",
    "master_key",
    "private_key",
    "secret",
    "password",
    "passphrase",
    "api_key",
    "apikey",
})

# ---------------------------------------------------------------------------
# Estrategia 1 — Key-value patterns en el contenido de strings.
# Redacción contextual: solo reemplaza el VALOR cuando la CLAVE es sensible.
# Compilados una vez al importar el módulo (no por evento de log).
# ---------------------------------------------------------------------------
_KV_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # HTTP header: "Authorization: Bearer eyJ..." o "Authorization: token abc".
    # El lookahead negativo (?!Bearer\s) evita redactar "Bearer" cuando el valor
    # completo "Bearer <token>" ya fue manejado por _STRUCTURAL_PATTERNS (que se
    # aplica ANTES que _KV_PATTERNS en _redact_string).  Sin este lookahead, el
    # patrón \S+ captura solo la palabra "Bearer" y deja el token expuesto.
    (
        re.compile(r"(Authorization\s*:\s*)(?!Bearer\s)(?!<REDACTED>)\S+", re.IGNORECASE),
        r"\1<REDACTED>",
    ),
    # Parámetros en query string o cuerpo: token=xxx, secret=xxx, etc.
    (
        re.compile(
            r"(?i)((?:token|secret|password|api_key|apikey|bearer)\s*=\s*)\S+",
        ),
        r"\1<REDACTED>",
    ),
]

# ---------------------------------------------------------------------------
# Estrategia 2 — Patrones estructurales conocidos en cualquier string.
# Anclan en estructura, no en longitud genérica, para evitar falsos positivos.
# ---------------------------------------------------------------------------
_STRUCTURAL_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # JWT con estructura exacta: 3 segmentos base64url separados por puntos.
    # Requiere eyJ al inicio del segmento 1 (header JSON) + 2 puntos.
    # NO matchea strings que solo empiezan con eyJ sin los dos puntos de separación.
    (
        re.compile(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"),
        "<JWT_REDACTED>",
    ),
    # Tailscale auth/api/client keys.
    (
        re.compile(r"tskey-(?:auth|api|client)-[A-Za-z0-9\-]+"),
        "<TSKEY_REDACTED>",
    ),
    # Bearer token (header completo): Bearer + token de al menos 20 caracteres.
    # El límite de 20 evita matchear "Bearer self" u otros valores triviales.
    (
        re.compile(r"Bearer\s+[A-Za-z0-9._\-]{20,}"),
        "Bearer <REDACTED>",
    ),
]


def _redact_string(value: str) -> str:
    """
    Aplica todas las estrategias de redacción a un string.

    Orden de aplicación:
    1. _STRUCTURAL_PATTERNS primero: captura "Bearer <token_largo>" completo,
       produciendo "Bearer <REDACTED>".  Si se aplicara KV primero, el patron
       captura solo la palabra "Bearer" y deja el token expuesto.
    2. _KV_PATTERNS después: redacta valores clave-valor remanentes como
       "Authorization: <token_sin_bearer>" o "token=xxx".  El lookahead negativo
       en el patron Authorization evita redactar "Bearer" cuando ya fue procesado
       en el paso anterior.
    """
    for pattern, replacement in _STRUCTURAL_PATTERNS:
        value = pattern.sub(replacement, value)
    for pattern, replacement in _KV_PATTERNS:
        value = pattern.sub(replacement, value)
    return value


def redact_secrets(
    _logger: Any,
    _method: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """
    Procesador structlog que redacta secretos antes de serializar el evento.

    Estrategia de redacción (doble capa):

    1. **Redacción por clave** (Estrategia 1a): si algún kwarg del event_dict
       tiene una clave en ``_SENSITIVE_KEYS`` (token, llat, authorization, etc.),
       su valor se reemplaza por ``<REDACTED>`` independientemente del contenido.
       Se agrega ``_redacted_keys`` para evidenciar el intento en code review.

    2. **Redacción contextual en strings** (Estrategias 1b y 2): sobre el valor
       string de cada campo (incluyendo el campo ``event`` y ``exc_info``), se
       aplican patrones clave-valor (``Authorization: xxx``, ``token=xxx``) y
       patrones estructurales conocidos (JWT de 3 segmentos, tskey-, Bearer).

    Qué NO redacta (falsos positivos evitados intencionalmente):
    - SHA256 / SHA1 hex hashes: el regex JWT requiere ``eyJ`` al inicio + 2 puntos,
      por lo que un hash hexadecimal nunca matchea.
    - Paths del filesystem: ``/var/lib/ha-mirror/...`` no tienen estructura JWT ni
      contienen claves sensibles conocidas.
    - Base64 de payloads JSON pequeños sin estructura JWT: requiere los 3 segmentos
      base64url con ``eyJ`` al inicio del primero.
    - Identificadores cortos como git commit hashes (40 chars hex): sin estructura JWT,
      sin clave sensible de contexto.
    """
    redacted_keys: list[str] = []

    # Paso 1: redacción por clave en kwargs del event_dict
    for key in list(event_dict.keys()):
        if key.lower() in _SENSITIVE_KEYS:
            event_dict[key] = "<REDACTED>"
            redacted_keys.append(key)

    if redacted_keys:
        event_dict["_redacted_keys"] = redacted_keys

    # Paso 2: redacción contextual en valores string de todos los campos
    for key, value in event_dict.items():
        if key == "_redacted_keys":
            continue
        if isinstance(value, str):
            event_dict[key] = _redact_string(value)
        elif isinstance(value, Exception):
            # Redactar en la representación string de la excepción
            redacted_repr = _redact_string(str(value))
            if redacted_repr != str(value):
                event_dict[key] = redacted_repr

    return event_dict


def configure_logging(log_level: str = "INFO") -> None:
    """Configura structlog + stdlib logging. Llamar una sola vez al arrancar."""
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Procesadores compartidos stdlib + structlog.
    # redact_secrets se ejecuta antes de cualquier renderer para garantizar
    # que ningún secreto llegue al output final (JSON o ConsoleRenderer).
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        redact_secrets,
    ]

    if log_level.upper() == "DEBUG":
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level)

    # Silenciar uvicorn access log (uvicorn ya genera estructurado)
    logging.getLogger("uvicorn.access").propagate = False
