"""
Autenticacion del mirror — API key estatica rotable (HC9, Fase 2, N=1).

Estrategia decidida en veredicto_final.md §Contradicciones:
  - API key estatica para Fase 2 single-tenant.
  - Migracion a JWT 15min + refresh diferida a cuando se agregue el 2do usuario humano.

Mejoras sobre la implementacion original:
  1. Comparacion con hmac.compare_digest (resistente a timing attacks).
  2. MIRROR_API_KEY / MIRROR_API_KEYS leidas de env via pydantic-settings (SecretStr).
     Longitud minima 32 bytes aplicada en validador de Settings.
  3. MIRROR_API_KEYS=key1,key2,key3 permite hasta 3 keys activas simultaneas
     para rotacion zero-downtime (dar de alta la nueva antes de revocar la vieja).
  4. Header esperado: X-API-Key (NO Authorization: Bearer, para evitar confusion
     con el LLAT de HA que viaja en Authorization hacia HA).
  5. WebSocket /ws/state valida la key via query param ?api_key= en el upgrade
     (el browser no permite headers custom en WS upgrade).
  6. Auth fallida → 401 con body JSON {"detail": "Invalid API key"}, logueado en
     WARN con IP del cliente pero SIN la key en el log.
  7. Rate-limiting basico en memoria: 5 fallos desde el mismo IP en 60s bloquea
     durante 300s. Sin Redis — apropiado para N=1 single-process.
"""

from __future__ import annotations

import hmac
import time
from threading import Lock

import structlog
from fastapi import Depends, HTTPException, Query, Request, WebSocket, status
from fastapi.security import APIKeyHeader, APIKeyQuery

from ha_mirror.config import Settings, get_settings

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Security schemes — FastAPI los muestra en /docs
# ---------------------------------------------------------------------------
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
_api_key_query = APIKeyQuery(name="api_key", auto_error=False)

# ---------------------------------------------------------------------------
# Rate-limiter in-memory (thread-safe, apropiado para N=1 single-process)
# ---------------------------------------------------------------------------
# Estructura: { ip: {"count": int, "window_start": float, "blocked_until": float} }
# Fix A1 — dict plano (NO defaultdict): las entradas se crean solo al REGISTRAR
# un fallo, no en cada lectura, y se acotan con eviction. Así un atacante no
# puede inflar memoria creando una entrada por cada IP que consulta.
_rate_state: dict[str, dict] = {}
_rate_lock = Lock()

_RATE_WINDOW_SECONDS = 60       # ventana de conteo
_RATE_MAX_FAILURES = 5          # fallos permitidos en la ventana
_RATE_BLOCK_SECONDS = 300       # duracion del bloqueo (5 min)
_RATE_MAX_ENTRIES = 10_000      # cota dura de IPs rastreadas (anti-OOM)


def _evict_stale_locked(now: float) -> None:
    """Remueve entradas inactivas si el dict supera la cota. Llamar bajo _rate_lock."""
    if len(_rate_state) <= _RATE_MAX_ENTRIES:
        return
    stale = [
        ip
        for ip, s in _rate_state.items()
        if now >= s["blocked_until"] and now - s["window_start"] > _RATE_WINDOW_SECONDS
    ]
    for ip in stale:
        del _rate_state[ip]


def _get_client_ip(request: Request) -> str:
    """
    Extrae la IP real del cliente.

    Fix C4 — detrás de Cloudflare usamos CF-Connecting-IP (Cloudflare lo
    sobrescribe en cada request, no es falsificable por el cliente). NO confiamos
    en X-Forwarded-For, que sí es spoofeable y permitía DoS dirigido y bypass del
    rate-limit. Fallback: la IP de la conexión directa (acceso LAN).
    """
    cf_ip = request.headers.get("cf-connecting-ip")
    if cf_ip:
        return cf_ip.strip()
    if request.client:
        return request.client.host
    return "unknown"


def _check_rate_limit(ip: str) -> None:
    """
    Verifica el rate limit para el IP dado.
    Lanza HTTPException 429 si el IP esta en periodo de bloqueo.
    """
    now = time.monotonic()
    with _rate_lock:
        state = _rate_state.get(ip)
        if state is not None and now < state["blocked_until"]:
            remaining = int(state["blocked_until"] - now)
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Demasiados intentos fallidos. Reintente en {remaining}s.",
                headers={"Retry-After": str(remaining)},
            )


def _record_failure(ip: str) -> None:
    """
    Registra un fallo de autenticacion para el IP.
    Si se supera el limite en la ventana, activa el bloqueo.
    """
    now = time.monotonic()
    with _rate_lock:
        _evict_stale_locked(now)
        state = _rate_state.get(ip)
        if state is None:
            state = {"count": 0, "window_start": now, "blocked_until": 0.0}
            _rate_state[ip] = state
        # Reiniciar ventana si expiro
        if now - state["window_start"] > _RATE_WINDOW_SECONDS:
            state["count"] = 0
            state["window_start"] = now
        state["count"] += 1
        if state["count"] >= _RATE_MAX_FAILURES:
            state["blocked_until"] = now + _RATE_BLOCK_SECONDS
            logger.warning(
                "auth.rate_limit_triggered",
                ip=ip,
                failures=state["count"],
                block_seconds=_RATE_BLOCK_SECONDS,
            )


def _clear_rate_limit(ip: str) -> None:
    """Limpia el contador de fallos tras autenticacion exitosa."""
    with _rate_lock:
        if ip in _rate_state:
            _rate_state[ip]["count"] = 0


# ---------------------------------------------------------------------------
# Verificacion de la key
# ---------------------------------------------------------------------------

def _get_valid_keys(settings: Settings) -> list[bytes]:
    """
    Devuelve la lista de keys validas como bytes.

    Si MIRROR_API_KEYS esta configurada (lista separada por comas), usa esa.
    Si solo esta MIRROR_API_KEY, usa esa como lista de una entrada.
    Esto permite rotacion zero-downtime:
      1. Agregar nueva key a MIRROR_API_KEYS y reiniciar el servicio.
      2. Actualizar el frontend para usar la nueva key.
      3. Remover la key vieja de MIRROR_API_KEYS y reiniciar nuevamente.
    """
    # Preferir la lista multi-key si esta disponible
    multi = getattr(settings, "mirror_api_keys", None)
    if multi:
        raw: str = multi.get_secret_value()
        keys = [k.strip().encode() for k in raw.split(",") if k.strip()]
        if keys:
            return keys

    # Fallback a la key unica
    single: str = settings.mirror_api_key.get_secret_value()
    return [single.encode()]


def _is_valid_key(provided: str, settings: Settings) -> bool:
    """
    Verifica si la key provista coincide con alguna de las keys validas.
    Usa hmac.compare_digest para resistencia a timing attacks.
    """
    provided_bytes = provided.encode()
    valid_keys = _get_valid_keys(settings)
    # Evaluar TODAS las keys para no revelar cuantas hay por timing
    matches = [
        hmac.compare_digest(provided_bytes, expected)
        for expected in valid_keys
    ]
    return any(matches)


# ---------------------------------------------------------------------------
# Dependencias FastAPI
# ---------------------------------------------------------------------------

async def require_api_key(
    request: Request,
    header_key: str | None = Depends(_api_key_header),
    settings: Settings = Depends(get_settings),
) -> None:
    """
    Dependencia FastAPI para endpoints REST.

    Fix A4 — la key SOLO se acepta por header X-API-Key. Se removió el query
    param ?api_key= en REST porque las query strings quedan en logs de proxies,
    historial del navegador y Referer. (El WS sí usa query por limitación del
    browser en el upgrade — ver authenticate_ws.)
    Aplica rate-limiting por IP antes de verificar la key.
    """
    ip = _get_client_ip(request)
    _check_rate_limit(ip)

    provided = header_key
    if provided is None:
        _record_failure(ip)
        logger.warning("auth.missing_api_key", ip=ip, path=request.url.path)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    if not _is_valid_key(provided, settings):
        _record_failure(ip)
        logger.warning(
            "auth.invalid_api_key",
            ip=ip,
            path=request.url.path,
            # NO loguear la key provista — solo metadata
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    _clear_rate_limit(ip)


async def authenticate_ws(
    websocket: WebSocket,
    api_key: str | None = Query(default=None, alias="api_key"),
) -> None:
    """
    Autentica una conexion WebSocket entrante.

    El upgrade HTTP del browser no soporta headers custom arbitrarios,
    por eso la key viaja en query param ?api_key= en el WS upgrade request.
    Si la key es invalida, cierra el WS con codigo 4001 antes de aceptar.
    Aplica rate-limiting por IP.
    """
    settings = get_settings()
    # Fix A3 — detrás de Cloudflare todos los WS parecen venir del mismo IP edge;
    # usamos CF-Connecting-IP para no bloquear a todos los clientes de un saque.
    ip = websocket.headers.get("cf-connecting-ip") or (
        websocket.client.host if websocket.client else "unknown"
    )

    # Rate-limiting para WS
    try:
        _check_rate_limit(ip)
    except HTTPException:
        await websocket.close(code=4029, reason="Rate limit excedido")
        raise

    if api_key is None or not _is_valid_key(api_key, settings):
        _record_failure(ip)
        logger.warning(
            "auth.ws_rejected",
            ip=ip,
            reason="missing_or_invalid_api_key",
        )
        await websocket.close(code=4001, reason="API key invalida o ausente")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    _clear_rate_limit(ip)
