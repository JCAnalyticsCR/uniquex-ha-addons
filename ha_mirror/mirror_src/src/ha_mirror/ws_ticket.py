"""
Tickets de vida corta para autenticar WebSockets sin exponer la API key.

POR QUE EXISTE (0.21.0)
-----------------------
Hasta 0.20.0 el frontend le entregaba al navegador la `MIRROR_API_KEY` tal cual:
`/api/cameras/{id}/ws-ticket` devolvia `ticket: MIRROR_API_KEY` y una `ws_url`
con la misma key en el query string. El encabezado de ese archivo lo justificaba
con "seguro en Fase 1 porque el Mirror vive solo en la tailnet".

Tailscale NUNCA entro en produccion. Esa condicion no se cumplio ni un solo dia:
desde el primer despliegue el Mirror estuvo en internet detras de Cloudflare. El
efecto es que cualquiera con sesion en la app podia sacar la key maestra de las
herramientas de desarrollador y llamar al Mirror directo con X-API-Key,
saltandose el frontend por completo.

Un Service Token de Cloudflare Access no resuelve esto: es un par de headers, y
el navegador no puede mandar headers en el upgrade de un WebSocket.

DISENO
------
El Mirror emite el ticket; el navegador nunca recibe un secreto de larga vida.

  - Firmado, no almacenado. Sin estado en memoria: sobrevive un reinicio del
    add-on, no crece y no necesita limpieza.
  - Se usa `itsdangerous.URLSafeTimedSerializer`, el mismo mecanismo que ya
    usa `iframe_token.py`, en vez de inventar un formato nuevo.
  - El secreto se DERIVA de la `mirror_api_key` (via el salt del serializer),
    asi que no se agrega configuracion nueva y rotar la key invalida todos los
    tickets vivos — que es el comportamiento correcto.
  - El ticket lleva scope y entity_id: un ticket de `state` no abre
    `/ws/cameras/*`, y uno de `camera.sala` no abre `camera.dormitorio`.
"""

from __future__ import annotations

import structlog
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from ha_mirror.config import Settings

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# Alcances validos. `state` es el WS de estado; `camera` el puente MSE.
VALID_SCOPES: frozenset[str] = frozenset({"state", "camera"})


def _get_serializer(settings: Settings) -> URLSafeTimedSerializer:
    # Derivado de la API key: rotarla invalida todos los tickets emitidos.
    # NO se usa `session_secret` a proposito — es un campo muerto (hallazgo B1
    # de la auditoria) y esta marcado para eliminarse.
    secret = settings.mirror_api_key.get_secret_value()
    return URLSafeTimedSerializer(secret, salt="uniquex-ws-ticket-v1")


def issue_ticket(
    settings: Settings,
    scope: str,
    entity_id: str | None = None,
) -> tuple[str, int]:
    """
    Emite un ticket firmado para un WebSocket.

    Retorna (ticket, ttl_segundos). Lanza ValueError si el scope no es valido.
    """
    if scope not in VALID_SCOPES:
        raise ValueError(f"scope invalido: {scope!r}")

    payload = {"s": scope, "e": entity_id}
    ticket: str = _get_serializer(settings).dumps(payload)

    # entity_id no es secreto; se loguea para poder auditar el uso.
    logger.info("ws_ticket.issued", scope=scope, entity_id=entity_id)
    return ticket, settings.ws_ticket_ttl_seconds


def verify_ticket(
    settings: Settings,
    ticket: str,
    scope: str,
    entity_id: str | None = None,
) -> bool:
    """
    Valida firma, expiracion, scope y entidad. NUNCA loguea el ticket.

    Devuelve True solo si las cuatro condiciones se cumplen.
    """
    serializer = _get_serializer(settings)
    try:
        payload = serializer.loads(ticket, max_age=settings.ws_ticket_ttl_seconds)
    except SignatureExpired:
        logger.info("ws_ticket.expired", scope=scope)
        return False
    except BadSignature:
        logger.warning("ws_ticket.bad_signature", scope=scope)
        return False

    if not isinstance(payload, dict):
        logger.warning("ws_ticket.malformed_payload", scope=scope)
        return False

    if payload.get("s") != scope:
        # Un ticket de estado no debe abrir una camara y viceversa.
        logger.warning(
            "ws_ticket.scope_mismatch",
            esperado=scope,
            recibido=payload.get("s"),
        )
        return False

    if payload.get("e") != entity_id:
        # Un ticket de camera.sala no debe abrir camera.dormitorio.
        logger.warning(
            "ws_ticket.entity_mismatch",
            esperado=entity_id,
            recibido=payload.get("e"),
        )
        return False

    return True
