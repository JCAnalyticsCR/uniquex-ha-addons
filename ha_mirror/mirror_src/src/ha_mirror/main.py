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
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

import structlog
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from ha_mirror.api.areas import router as areas_router
from ha_mirror.api.camera_media import router as camera_media_router
from ha_mirror.api.camera_ws import router as camera_ws_router
from ha_mirror.api.entities import router as entities_router
from ha_mirror.api.health import router as health_router
from ha_mirror.api.iframe_token import router as iframe_router
from ha_mirror.api.onboarding import router as onboarding_router
from ha_mirror.api.preferences import router as preferences_router
from ha_mirror.api.costumbres import router as costumbres_router
from ha_mirror.api.pronostico import router as pronostico_router
from ha_mirror.api.scenes import router as scenes_router
from ha_mirror.api.service import router as service_router
from ha_mirror.api.ws_state import router as ws_router
from ha_mirror.api.ws_ticket import router as ws_ticket_router
from ha_mirror.auth import require_api_key
from ha_mirror.camera_media import CameraMediaClient
from ha_mirror.config import get_settings
from ha_mirror.correlations import CorrelationTracker
from ha_mirror.db import Database
from ha_mirror.errors import HaAuthError
from ha_mirror.ha_upstream import HAUpstream
from ha_mirror.logging_setup import configure_logging
from ha_mirror.onboarding import OnboardingService
from ha_mirror.state_store import StateStore

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# Versión del paquete instalado — fuente única de verdad.
# importlib.metadata lee el METADATA del wheel/egg-info que instaló pip,
# originado en pyproject.toml (version = "x.y.z"). El Supervisor de HA
# siempre instala el paquete antes de iniciar, así que en la cajita esto
# siempre resuelve. El fallback "desconocida" protege entornos de desarrollo
# donde el paquete no está instalado (editable install pendiente).
# NUNCA levanta excepción al importar el módulo.
try:
    _MIRROR_VERSION: str = _pkg_version("ha-mirror")
except PackageNotFoundError:
    _MIRROR_VERSION = "desconocida"


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
        version=_MIRROR_VERSION,
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

    # ---------------------------------------------------------------------
    # MODO FABRICA vs MODO ARTESANAL — el interruptor es `platform_base_url`.
    #
    # Artesanal (default, vacio): no se genera identidad, no se reporta a
    # ninguna plataforma, no se levanta tunel propio y no hay calcomania. Una
    # casa instalada a mano actualiza el add-on y NO cambia absolutamente nada.
    # Esa garantia es el motivo de que la condicion sea una sola y este aca.
    #
    # Un fallo generando la identidad NO tumba el Mirror: esta caja puede estar
    # sirviendo las camaras de una familia, y que una funcion de
    # aprovisionamiento deje la casa a oscuras seria un intercambio pesimo.
    # ---------------------------------------------------------------------
    device_identity = None
    if settings.modo_fabrica:
        logger.info("mirror.modo_fabrica", platform_base_url=settings.platform_base_url)
        # Aviso AL ARRANCAR y no recien cuando alguien intente activar: si esta
        # version se publico sin la llave publica de la plataforma, la caja va a
        # rechazar toda respuesta del anuncio. Mejor que se vea en el log del
        # taller que descubrirlo con el cliente esperando.
        from ha_mirror.announce_client import hay_llave_de_plataforma

        if not hay_llave_de_plataforma():
            logger.error(
                "mirror.sin_llave_de_plataforma",
                msg=(
                    "Modo fabrica SIN llave publica horneada: la activacion no "
                    "va a poder verificarse y toda respuesta se rechaza. Esta "
                    "caja no puede activarse con esta version del add-on."
                ),
            )
        try:
            from ha_mirror.device_identity import ensure_identity

            device_identity = await ensure_identity(db, settings.device_key_path)
        except Exception as exc:
            logger.error(
                "device_identity.unavailable",
                exc=str(exc),
                msg="El Mirror arranca igual; solo la activacion queda fuera de servicio.",
            )
    else:
        logger.info(
            "mirror.modo_artesanal",
            msg="Sin platform_base_url: sin identidad, sin reporte y sin tunel propio.",
        )

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

    # 4b. Connector Crestron (opcional). Import PEREZOSO: cuando Crestron no está
    #     configurado (caso por defecto), el mirror arranca sin depender de
    #     crestron_client/crestron_connector. Solo cuando está configurado se
    #     importan y se levanta el cliente + el polling supervisado.
    crestron_client = None
    crestron_connector = None
    if settings.crestron_configured:
        from ha_mirror.crestron_client import CrestronClient
        from ha_mirror.crestron_connector import CrestronConnector

        crestron_token = settings.get_crestron_token()
        crestron_client = CrestronClient(
            base_url=settings.crestron_base_url,  # allowlist de settings, nunca del request
            token=crestron_token,
            verify_ssl=settings.crestron_verify_ssl,
        )
        # Borrar el token de la variable local ASAP (igual que con ha_token).
        del crestron_token
        await crestron_client.start()
        crestron_connector = CrestronConnector(
            client=crestron_client,
            store=store,
            correlations=correlations,
            poll_interval=settings.crestron_poll_interval,
            area_id=settings.crestron_area_id,
        )
        await crestron_connector.start()
        logger.info(
            "crestron.enabled",
            base_url=settings.crestron_base_url,
            poll_interval=settings.crestron_poll_interval,
            area_id=settings.crestron_area_id,
        )
    else:
        logger.info("crestron.disabled")

    # 5. Montar en app.state para que los routers accedan via request.app.state
    app.state.store = store
    app.state.upstream = upstream
    app.state.correlations = correlations
    app.state.db = db
    app.state.settings = settings
    app.state.camera_media = camera_media
    # None si Crestron no está configurado; api/service.py lo consulta con getattr.
    app.state.crestron = crestron_connector
    # Versión legible desde cualquier router sin reimportar importlib.
    # Permite que /api/health la exponga hacia afuera — confirmar en un segundo
    # si una actualización entró o un rollback funcionó, sin entrar a la cajita.
    app.state.mirror_version = _MIRROR_VERSION
    # None en modo artesanal; api/device.py responde 503 en ese caso.
    app.state.device_identity = device_identity

    # Servicio de onboarding (sin tareas de fondo propias).
    app.state.onboarding = OnboardingService(
        store=store,
        upstream=upstream,
        db=db,
        mirror_version=_MIRROR_VERSION,
    )

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

    # ---------------------------------------------------------------------
    # Reporte a la plataforma + tunel propio. Solo en modo fabrica y solo si la
    # identidad se resolvio. Si la caja YA esta emparejada, el loop termina solo
    # en su primer ciclo (el backend responde paired:true).
    # ---------------------------------------------------------------------
    announce_task: asyncio.Task | None = None
    tunnel_task: asyncio.Task | None = None
    if settings.modo_fabrica and device_identity is not None:
        try:
            from ha_mirror.announce_client import AnnounceClient
            from ha_mirror.device_identity import cargar_llave_privada
            from ha_mirror.tunnel_client import TunnelClient

            privada_para_anuncio = cargar_llave_privada(settings.device_key_path)
            announce_client = AnnounceClient(
                identity=device_identity,
                private_key=privada_para_anuncio,
                db=db,
                platform_base_url=settings.platform_base_url,
                tunnel_token_path=settings.tunnel_token_path,
                mirror_key_path=settings.platform_mirror_key_path,
                upstream=upstream,
                on_paired=lambda nueva: setattr(app.state, "device_identity", nueva),
                # La caja soltó la casa: hay que bajar cloudflared. El token ya
                # fue borrado por el propio cliente de anuncio, así que el
                # supervisor vuelve a quedar esperando uno nuevo.
                on_unpaired=lambda _identidad: tunnel_client.detener_por_desemparejo(),
            )
            # El tunel corre EN PARALELO al anuncio, no despues: el token puede
            # llegar en cualquier momento (cuando alguien escanee el QR) y
            # tambien puede estar ya en disco de un arranque anterior.
            tunnel_client = TunnelClient(token_path=settings.tunnel_token_path)
            tunnel_task = asyncio.create_task(
                tunnel_client.run_forever(), name="tunnel_client"
            )

            def _on_tunnel_done(task: asyncio.Task) -> None:
                if task.cancelled():
                    return
                exc = task.exception()
                if exc:
                    logger.error(
                        "tunnel.task_failed",
                        exc=str(exc),
                        msg="La casa sigue andando en red local, pero no desde afuera.",
                    )

            tunnel_task.add_done_callback(_on_tunnel_done)

            announce_task = asyncio.create_task(
                announce_client.run_forever(), name="announce_client"
            )

            def _on_announce_done(task: asyncio.Task) -> None:
                if task.cancelled():
                    logger.info("announce.task_cancelled")
                    return
                exc = task.exception()
                if exc:
                    logger.error("announce.task_failed", exc=str(exc))
                else:
                    # El loop ya no termina solo: emparejada sigue con el latido
                    # lento. Si llega acá sin excepcion es un cambio de contrato,
                    # no el final feliz que este mensaje decia antes.
                    logger.error(
                        "announce.task_finished_inesperado",
                        msg=(
                            "El loop de anuncio terminó sin error. La caja deja "
                            "de enterarse de cambios de la plataforma."
                        ),
                    )

            announce_task.add_done_callback(_on_announce_done)
        except Exception as exc:
            logger.error(
                "announce.init_failed",
                exc=str(exc),
                msg="El Mirror arranca sin el cliente de anuncio. Revisar los logs.",
            )

    logger.info("mirror.started")

    try:
        yield
    finally:
        logger.info("mirror.shutting_down")
        upstream_task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await upstream_task
        # Parar el polling Crestron y cerrar el cliente antes de la DB.
        if crestron_connector is not None:
            with suppress(Exception):
                await crestron_connector.close()
        if crestron_client is not None:
            with suppress(Exception):
                await crestron_client.close()
        for tarea in (announce_task, tunnel_task):
            if tarea is not None:
                tarea.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await tarea
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
        version=_MIRROR_VERSION,
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
    # Pronóstico: solo lectura y con caché propia. Endpoint dedicado a propósito,
    # NO un flag en el proxy de servicios — ver la cabecera de pronostico.py.
    app.include_router(pronostico_router)
    # Costumbres: crea automatizaciones a partir de RECETAS, nunca de JSON del
    # cliente, y con la barrera "solo ambiente" del lado del servidor.
    app.include_router(costumbres_router)
    app.include_router(preferences_router)
    app.include_router(onboarding_router)
    # Emisor de tickets de WebSocket (0.21.0). Lo llama el frontend desde el
    # servidor con X-API-Key; el navegador nunca recibe la key.
    app.include_router(ws_ticket_router)

    # Identidad de la caja y calcomania de activacion: SOLO modo fabrica. En
    # artesanal ni siquiera se importan (asi `segno` no se carga donde no se
    # usa) y las rutas no existen — un 404 en vez de una pantalla vacia.
    if settings.modo_fabrica:
        from ha_mirror.api.device import router as device_router

        app.include_router(device_router)

    # Routers WebSocket
    app.include_router(ws_router)
    # Puente MSE-over-WebSocket de camaras (/ws/cameras/{entity_id}) — evita el
    # buffering de Cloudflare sobre el fMP4 por HTTP que congelaba el video.
    app.include_router(camera_ws_router)

    # Métricas Prometheus en /metrics — Fix C2: PROTEGIDO con API key.
    # Antes estaba montado SIN auth ("solo vía tailnet"), pero ahora el Mirror
    # queda expuesto por el túnel Cloudflare → filtraba actividad de la casa y
    # versiones. Ahora exige X-API-Key igual que el resto de los endpoints.
    @app.get("/metrics", include_in_schema=False)
    async def metrics(_: None = Depends(require_api_key)) -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app



def create_sticker_app() -> FastAPI:
    """
    App SEPARADA que sirve unicamente la calcomania de activacion.

    POR QUE VIVE APARTE Y NO EN LA APP PRINCIPAL
    --------------------------------------------
    La app principal se publica en el puerto 8099 del host porque el tunel de
    Cloudflare de las casas artesanales rutea el frontend por ahi. Servir la
    calcomania en esa misma app significaba que **cualquiera en la red de la
    casa podia abrir `http://<ip>:8099/` y leer el codigo de activacion** — que
    es todo lo que hace falta para quedarse con la caja, porque hoy no hay forma
    de deshacer una activacion.

    Se evaluaron dos parches y ninguno cierra:
      · chequear la cabecera `X-Ingress-Path`: se falsifica con un `curl -H`.
      · chequear la IP de origen: no es concluyente. Segun si Docker publica el
        puerto con DNAT o con el proxy de usuario, el contenedor ve la IP real
        del cliente o la del gateway — y en el segundo caso no distingue una
        peticion de ingress de una de la LAN.

    La solucion no es proteger mejor la pagina: es **no servirla en el puerto
    que sale a la red**. Esta app corre en el 8001, que NO se publica al host.
    El unico que llega es el ingress de Home Assistant, que ya autentico al
    usuario antes de proxear.

    Solo se levanta en modo fabrica. Una casa artesanal no la arranca nunca.
    """
    from ha_mirror.api.sticker import router as sticker_router

    app = FastAPI(
        title="UniquexCR — Activacion",
        description="Calcomania de activacion. Solo accesible por el ingress de Home Assistant.",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.include_router(sticker_router)
    return app


# Instancia para uvicorn: uvicorn ha_mirror.main:app
app = create_app()

# Instancia de la app de la calcomania (solo modo fabrica, puerto 8001).
# Se construye siempre; el que decide si se levanta es run.sh.
sticker_app = create_sticker_app()
