"""
FastAPI app — punto de entrada del mirror HA.

Lifespan:
  1. Configura logging estructurado
  2. Carga Settings (valida .env y archivos de secretos)
  3. Descifra LLAT con Fernet
  4. Conecta DB SQLite
  5. Inicia HAUpstream.run_forever() en asyncio task supervisado
  6. Monta routers
  7. Al shutdown: cancela el task del upstream, cierra DB

El upstream corre en background; FastAPI atiende requests independientemente.
Si el upstream falla con HaAuthError, el task termina pero FastAPI sigue
corriendo (health endpoint muestra AUTH_FAILED, servicio disponible para diagnóstico).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

import structlog
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from ha_mirror.api.areas import router as areas_router
from ha_mirror.api.camera_media import router as camera_media_router
from ha_mirror.api.entities import router as entities_router
from ha_mirror.api.health import router as health_router
from ha_mirror.api.iframe_token import router as iframe_router
from ha_mirror.api.scenes import router as scenes_router
from ha_mirror.api.service import router as service_router
from ha_mirror.api.ws_state import router as ws_router
from ha_mirror.auth import require_api_key
from ha_mirror.camera_media import CameraMediaClient
from ha_mirror.config import get_settings
from ha_mirror.correlations import CorrelationTracker
from ha_mirror.db import Database
from ha_mirror.errors import HaAuthError
from ha_mirror.ha_upstream import HAUpstream
from ha_mirror.logging_setup import configure_logging
from ha_mirror.state_store import StateStore

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class CSPMiddleware(BaseHTTPMiddleware):
    """
    Inyecta Content-Security-Policy en todas las respuestas HTTP del mirror.

    Notas de diseño:
    - ``frame-ancestors`` compensa el ``use_x_frame_options: false`` configurado
      en HA (Opción C híbrida). Sin este header cualquier origen puede embeber HA
      en un iframe y realizar clickjacking sobre las persianas u otros controles.
    - El resto del CSP (default-src, script-src, style-src, etc.) es defensa en
      profundidad contra XSS y data exfiltration en caso de compromiso del frontend.
    - ``'unsafe-inline'`` en script-src es temporal para la Fase 2 (sin frontend
      propio). TODO Fase 3 frontend: migrar a nonces generados por request para
      eliminar ``'unsafe-inline'`` de script-src y style-src.
    - El wildcard ``*.{tailnet}`` cubre todos los hostnames del tailnet
      (ha-gateway, mirror, etc.) sin necesidad de listarlos individualmente.
      Elimina redundancia respecto a listar ``app.{tailnet}`` aparte.
    """

    def __init__(self, app: FastAPI, tailnet_host: str) -> None:  # type: ignore[override]
        super().__init__(app)
        self._tailnet_host = tailnet_host
        # Derivar el tailnet (ej: "ha-gateway.mired.ts.net" -> "mired.ts.net")
        parts = tailnet_host.split(".", 1)
        self._tailnet = parts[1] if len(parts) == 2 else tailnet_host

    def _build_csp(self) -> str:
        h = self._tailnet_host
        wc = f"*.{self._tailnet}"
        directives = [
            "default-src 'self'",
            "script-src 'self' 'unsafe-inline'",   # TODO Fase 3: reemplazar con nonces
            "style-src 'self' 'unsafe-inline'",     # TODO Fase 3: reemplazar con nonces
            "img-src 'self' data: https:",
            "font-src 'self' data:",
            f"connect-src 'self' https://{h} wss://{h}",
            f"frame-src 'self' https://{h}",
            f"frame-ancestors 'self' https://{wc}",
            "object-src 'none'",
            "base-uri 'none'",
            "form-action 'self'",
        ]
        return "; ".join(directives)

    async def dispatch(self, request: Request, call_next: object) -> Response:
        response: Response = await call_next(request)  # type: ignore[operator]
        response.headers["Content-Security-Policy"] = self._build_csp()
        # Fix M3 — headers de hardening adicionales.
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Permissions-Policy"] = "geolocation=(), camera=(), microphone=()"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        return response


def _build_csp_tailnet_host(settings: object) -> str:
    """
    Extrae el hostname del tailnet desde la configuración.

    Usa ``tailscale_serve_ha_hostname`` si está disponible y no es el valor
    por defecto placeholder. Si no, registra un warning y devuelve 'self' como
    fallback para no romper la aplicación.
    """
    _log = structlog.get_logger(__name__)
    placeholder = "ha-gateway.example.ts.net"
    host: str = getattr(settings, "tailscale_serve_ha_hostname", placeholder)
    if not host or host == placeholder:
        _log.warning(
            "csp_middleware.tailnet_host_not_configured",
            msg=(
                "TAILSCALE_SERVE_HA_HOSTNAME no configurado o usa el placeholder por defecto. "
                "CSP usará 'self' como fallback — frame-ancestors no cubrirá el tailnet."
            ),
            fallback="self",
        )
        return "self"
    return host


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Lifespan: inicializa y teardown del mirror."""
    # 1. Configurar logging antes de cualquier otra cosa
    settings = get_settings()
    configure_logging(settings.log_level)

    logger.info(
        "mirror.starting",
        version="0.3.0",
        ha_url=settings.ha_url,
        db_path=str(settings.mirror_db_path),
        tenant_id=settings.tenant_id,
    )

    # 2. Obtener el token de HA — falla rápido si hay problema.
    #    Add-on: token del Supervisor (SUPERVISOR_TOKEN).
    #    Standalone: descifra el LLAT con Fernet.
    try:
        ha_token = settings.get_ha_token()
    except Exception as exc:
        logger.error("mirror.ha_token_load_failed", exc=str(exc))
        raise

    # 3. Inicializar componentes
    store = StateStore(queue_maxsize=settings.ws_queue_maxsize)
    correlations = CorrelationTracker()
    db = Database(
        db_path=settings.mirror_db_path,
        events_retention_days=settings.events_log_retention_days,
    )
    await db.connect()

    # 4. Construir HAUpstream
    upstream = HAUpstream(
        ha_ws_url=settings.ha_url,
        llat=ha_token,
        store=store,
        correlations=correlations,
        ping_interval=settings.upstream_ping_interval,
        ping_timeout=settings.upstream_ping_timeout,
        service_call_timeout=settings.service_call_timeout,
    )

    # Cliente HTTP interno para snapshots HA y señalizacion go2rtc. Las
    # credenciales viven solo en memoria dentro del add-on Mirror.
    camera_media = CameraMediaClient(
        ha_base_url=settings.ha_http_url,
        ha_token=ha_token,
        go2rtc_base_url=settings.go2rtc_base_url,
        go2rtc_username=settings.go2rtc_username,
        go2rtc_password=(
            settings.go2rtc_password.get_secret_value()
            if settings.go2rtc_password is not None
            else None
        ),
        camera_streams=settings.camera_streams,
    )
    await camera_media.start()

    # Borrar el token de la variable local lo antes posible
    # (Python no garantiza wipe de memoria, pero reducimos ventana de exposición)
    del ha_token

    # 5. Montar en app.state para que los routers accedan via request.app.state
    app.state.store = store
    app.state.upstream = upstream
    app.state.correlations = correlations
    app.state.db = db
    app.state.settings = settings
    app.state.camera_media = camera_media

    # 6. Lanzar el upstream como task supervisado
    upstream_task = asyncio.create_task(upstream.run_forever(), name="ha_upstream")

    def _on_upstream_done(task: asyncio.Task) -> None:
        """Callback para loguear si el upstream terminó inesperadamente."""
        if task.cancelled():
            logger.info("upstream.task_cancelled")
            return
        exc = task.exception()
        if exc:
            if isinstance(exc, HaAuthError):
                logger.error(
                    "upstream.auth_failed_permanent",
                    msg="Token HA inválido (LLAT o token del Supervisor) — verificar.",
                )
            else:
                logger.error("upstream.task_failed", exc=str(exc))

    upstream_task.add_done_callback(_on_upstream_done)

    logger.info("mirror.started")

    try:
        yield
    finally:
        logger.info("mirror.shutting_down")
        upstream_task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await upstream_task
        await camera_media.close()
        await db.close()
        logger.info("mirror.stopped")


def create_app() -> FastAPI:
    """Factory de la aplicación FastAPI."""
    settings = get_settings()

    # Fix C3 — no exponer /docs, /redoc ni /openapi.json en producción.
    _docs = "/docs" if settings.expose_docs else None
    _redoc = "/redoc" if settings.expose_docs else None
    _openapi = "/openapi.json" if settings.expose_docs else None
    app = FastAPI(
        title="HA Mirror",
        description="FastAPI mirror para Home Assistant — single-tenant",
        version="0.3.0",
        lifespan=lifespan,
        docs_url=_docs,
        redoc_url=_redoc,
        openapi_url=_openapi,
    )

    # CORS — solo orígenes del tailnet + localhost dev
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        # PUT/DELETE los usa el CRUD de escenas. El frontend pega vía el BFF de
        # Next (server-side, sin preflight), pero sin declararlos acá cualquier
        # llamada desde el browser al mirror moriría en el preflight.
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["X-API-Key", "Content-Type"],
    )

    # CSP — inyecta Content-Security-Policy en todas las respuestas.
    # frame-ancestors compensa use_x_frame_options: false en HA (Opción C).
    # Ver docstring de CSPMiddleware para notas sobre Fase 3 (nonces).
    tailnet_host = _build_csp_tailnet_host(settings)
    app.add_middleware(CSPMiddleware, tailnet_host=tailnet_host)

    # Routers REST
    app.include_router(health_router)
    app.include_router(entities_router)
    app.include_router(areas_router)
    app.include_router(service_router)
    app.include_router(iframe_router)
    app.include_router(camera_media_router)
    app.include_router(scenes_router)

    # Router WebSocket
    app.include_router(ws_router)

    # Métricas Prometheus en /metrics — Fix C2: PROTEGIDO con API key.
    # Antes estaba montado SIN auth ("solo vía tailnet"), pero ahora el Mirror
    # queda expuesto por el túnel Cloudflare → filtraba actividad de la casa y
    # versiones. Ahora exige X-API-Key igual que el resto de los endpoints.
    @app.get("/metrics", include_in_schema=False)
    async def metrics(_: None = Depends(require_api_key)) -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


# Instancia para uvicorn: uvicorn ha_mirror.main:app
app = create_app()
