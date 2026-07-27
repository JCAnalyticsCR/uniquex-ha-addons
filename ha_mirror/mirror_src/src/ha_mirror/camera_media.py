"""Clientes internos para snapshots de Home Assistant y WebRTC de go2rtc.

Las URLs y credenciales permanecen dentro del add-on Mirror. Los endpoints
publicos nunca aceptan una URL arbitraria ni un nombre de stream enviado por
el navegador; ambos se resuelven desde configuracion validada para evitar SSRF.
"""

from __future__ import annotations

import asyncio
import re
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from typing import AsyncIterator
from urllib.parse import parse_qs, parse_qsl, quote, urlencode, urljoin, urlparse

import aiohttp
import structlog

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class CameraMediaError(Exception):
    """Error controlado al obtener medios de camara."""

    def __init__(self, code: str, status_code: int = 502) -> None:
        super().__init__(code)
        self.code = code
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class SnapshotPayload:
    """Imagen validada devuelta por Home Assistant."""

    body: bytes
    content_type: str


@dataclass(frozen=True, slots=True)
class HlsSegmentPayload:
    """Bytes de un segmento HLS proxeado desde go2rtc."""

    body: bytes
    content_type: str  # "video/mp4" o "video/mp2t"


@dataclass(frozen=True, slots=True)
class _CachedSnapshot:
    """Ultimo cuadro bueno de una camara, con el instante en que se guardo."""

    payload: SnapshotPayload
    stored_at: float


# Clave de cache: la misma camara pedida con distinto ancho/calidad produce
# bytes distintos, asi que cada variante se cachea por separado.
_CacheKey = tuple[str, "int | None", "int | None"]


# ── HLS (Safari) — constantes y helpers de parseo/reescritura ───────────────
#
# Safari NO soporta MSE-over-WebSocket: el ManagedMediaSource que expone se
# congela en cuanto el stream fMP4 lleva unos segundos. En cambio, Safari
# reproduce HLS nativamente con un <video> comun. go2rtc puede emitir HLS;
# reescribimos las URIs de segmento para que el navegador nunca vea el host
# ni el stream name internos de go2rtc (anti-SSRF del lado cliente).
#
# FORMATO REAL DE go2rtc 1.9.14 — VERIFICADO en la cajita del cliente
# (2026-07-27), capturado del log del add-on. No es lo que se habia supuesto:
#
#   GET /api/stream.m3u8?src=NVR_CH01  devuelve un MASTER playlist:
#       #EXTM3U
#       #EXT-X-STREAM-INF:BANDWIDTH=192000,CODECS="avc1.4D001E"
#       hls/playlist.m3u8?id=rrEq3WNf
#
# Tres consecuencias que costaron un despliegue entender:
#   1. El master NO trae segmentos: trae un puntero a una segunda playlist.
#      Lo seguimos del lado servidor y le entregamos a Safari la MEDIA playlist
#      ya reescrita (Safari la acepta directo, y evitamos reescribir 2 niveles).
#   2. La URI de la variante es RELATIVA a /api/stream.m3u8 -> resuelve a
#      /api/hls/playlist.m3u8. Y los segmentos de ESA playlist son relativos a
#      ELLA, no al master: por eso `_resolve_seg_ref` recibe `resolve_base` en
#      vez de armarla sola. Resolver con la base equivocada rompe las rutas.
#   3. La sesion se identifica con un `id` OPACO (no con `src`). Ese `id` debe
#      sobrevivir la reescritura; `src` en cambio se remueve siempre y se
#      re-inyecta desde el allowlist.

# Referencia validada a un segmento en go2rtc: path+query sin scheme, host
# ni el param src (que viaja server-side desde el allowlist).
# Formato: /api/<seg1>[/<seg2>][?<query>]
# Cada segmento del path debe arrancar con alfanumerico -> bloquea ".." y "@".
# El query solo acepta chars URL-safe (% permitido para encoding legitimo).
_SEG_REF_RE = re.compile(
    r"^/api/[a-zA-Z0-9][a-zA-Z0-9\-_.]*(?:/[a-zA-Z0-9][a-zA-Z0-9\-_.]*)*"
    r"(?:\?[a-zA-Z0-9\-_.%=&+]{0,220})?$"
)
_SEG_REF_MAX_LEN = 256

_MAX_PLAYLIST_BYTES = 256 * 1024   # 256 KB — una playlist m3u8 nunca deberia pasar de esto
_MAX_SEGMENT_BYTES = 8 * 1024 * 1024  # 8 MB — generoso para un segmento H.264 de 2-4 s

# Tipos MIME que go2rtc puede devolver para segmentos HLS.
# application/octet-stream: fallback cuando go2rtc no pone tipo explicito.
_HLS_SEGMENT_CONTENT_TYPES = frozenset(
    {"video/mp4", "video/mp2t", "video/iso.segment", "application/octet-stream"}
)


def _resolve_seg_ref(raw_uri: str, *, go2rtc_base_url: str, resolve_base: str) -> str:
    """
    Convierte una URI de segmento go2rtc en una referencia validada.

    Pasos:
    1. Resuelve la URI (relativa o absoluta) contra `resolve_base` (la URL
       exacta de la playlist que la contiene — master o media — para que las
       rutas relativas se resuelvan correctamente: los segmentos de la media
       playlist son relativos a /api/hls/playlist.m3u8, NO a /api/stream.m3u8).
    2. Verifica que el host resultante sea el mismo go2rtc (anti-SSRF).
    3. Verifica que el path comience con /api/.
    4. Elimina el param `src` (se re-agrega server-side desde el allowlist).
    5. Valida longitud y charset del resultado.

    Lanza CameraMediaError con codigo explicito si algo falla — nunca silencia.
    """
    # urljoin resuelve correctamente: relativas, absolutas con path, y absolutas
    # con host distinto (estas ultimas quedan con netloc != go2rtc_base_url).
    abs_url = urljoin(resolve_base, raw_uri)
    parsed = urlparse(abs_url)

    # Anti-SSRF: si la URI apuntara a otro host (p.ej. via URI="http://evil.com/")
    # urljoin deja ese host en parsed.netloc. Lo bloqueamos aqui.
    expected_netloc = urlparse(go2rtc_base_url).netloc
    if parsed.netloc and parsed.netloc != expected_netloc:
        raise CameraMediaError("go2rtc_hls_uri_host_externo", 502)

    path = parsed.path
    if not path.startswith("/api/"):
        # Las URIs de go2rtc 1.9.x deben estar bajo /api/. Si llegan de otro
        # lugar, la suposicion sobre la API no se cumple — mejor error explicito
        # que proxear a ciegas a una ruta desconocida.
        raise CameraMediaError("go2rtc_hls_uri_fuera_de_api", 502)

    # Eliminar src del query: el stream_name viaja server-side desde el allowlist.
    # Ordenamos las claves restantes para producir una referencia determinista.
    params = parse_qs(parsed.query, keep_blank_values=True)
    params.pop("src", None)
    new_query = urlencode(sorted((k, v[0]) for k, v in params.items()))

    seg_ref = path + ("?" + new_query if new_query else "")

    if len(seg_ref) > _SEG_REF_MAX_LEN:
        raise CameraMediaError("go2rtc_hls_uri_demasiado_larga", 502)
    if not _SEG_REF_RE.fullmatch(seg_ref):
        # Chars inesperados en el path o query (p.ej. espacios, backslash, @@).
        raise CameraMediaError("go2rtc_hls_uri_invalida", 502)

    return seg_ref


def _extract_first_variant_uri(master_raw: str) -> str | None:
    """
    Extrae la URI de la primera variante de un master playlist HLS.

    go2rtc 1.9.14 devuelve un master con exactamente una variante. La URI va
    en la linea inmediatamente siguiente a #EXT-X-STREAM-INF (no comentario,
    no vacia). Devuelve None si no se encuentra ninguna.
    """
    take_next = False
    for line in master_raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("#EXT-X-STREAM-INF"):
            take_next = True
            continue
        if take_next and stripped and not stripped.startswith("#"):
            return stripped
    return None


def _rewrite_hls_playlist(
    raw: str,
    *,
    go2rtc_base_url: str,
    resolve_base: str,
    our_seg_prefix: str,
) -> str:
    """
    Reescribe las URIs de segmento de una MEDIA playlist HLS de go2rtc.

    `resolve_base` es la URL exacta desde la que se descargo esta media
    playlist (p.ej. http://go2rtc/api/hls/playlist.m3u8?id=X). Es crucial
    que sea la URL de la media playlist y NO la del master: los segmentos
    son relativos a /api/hls/, no a /api/; usar la base equivocada produce
    rutas mal resueltas.

    Itera linea a linea el m3u8 y reemplaza:
    - #EXT-X-MAP:URI="..." (segmento de inicializacion fMP4)
    - Lineas de segmento (no-comentario, no-vacias)

    Los tags restantes (#EXTINF, #EXT-X-TARGETDURATION, etc.) se dejan tal
    cual — no contienen URIs que necesiten reescritura.

    Lanza CameraMediaError si alguna URI falla la validacion.
    """
    lines_out: list[str] = []

    for line in raw.splitlines(keepends=True):
        stripped = line.rstrip("\r\n")
        eol = line[len(stripped):]  # preserva el terminador original (\n o \r\n)

        # Tag de segmento de inicializacion: #EXT-X-MAP:URI="<uri>"
        if stripped.startswith("#EXT-X-MAP:"):
            def _sub_map_uri(m: re.Match) -> str:
                raw_uri = m.group(1)
                seg_ref = _resolve_seg_ref(
                    raw_uri,
                    go2rtc_base_url=go2rtc_base_url,
                    resolve_base=resolve_base,
                )
                our_uri = f"{our_seg_prefix}?_seg={quote(seg_ref, safe='')}"
                return f'URI="{our_uri}"'

            rewritten = re.sub(r'URI="([^"]*)"', _sub_map_uri, stripped)
            lines_out.append(rewritten + eol)
            continue

        # Segmento de medios: linea no-vacia que NO empieza con #
        if stripped and not stripped.startswith("#"):
            seg_ref = _resolve_seg_ref(
                stripped,
                go2rtc_base_url=go2rtc_base_url,
                resolve_base=resolve_base,
            )
            our_uri = f"{our_seg_prefix}?_seg={quote(seg_ref, safe='')}"
            lines_out.append(our_uri + eol)
            continue

        # Tags (#EXTINF, #EXT-X-TARGETDURATION, etc.), comentarios y vacias
        lines_out.append(line)

    return "".join(lines_out)


class CameraMediaClient:
    """Acceso saliente y limitado a HA Core y a go2rtc."""

    _ALLOWED_IMAGE_TYPES = frozenset({"image/jpeg", "image/png", "image/webp"})
    _MAX_SNAPSHOT_BYTES = 12 * 1024 * 1024
    _MAX_SDP_BYTES = 256 * 1024
    # go2rtc devuelve un JPEG gris de relleno (~3.9KB) de forma INTERMITENTE
    # cuando el stream aún no tiene un cuadro decodable en ese instante. Un frame
    # real de una cámara (aun de noche) pesa bastante más. Si el snapshot viene
    # por debajo de este umbral lo tratamos como "gris" y reintentamos.
    # Umbral por debajo del cual tratamos el JPEG como gris de relleno y
    # reintentamos. Acotado para que sirva tanto a full-res (gris ~3.9KB, real
    # >10KB) como a las miniaturas del grid (gris ~1-2KB, real >5KB).
    _PLACEHOLDER_MAX_BYTES = 4_500
    _SNAPSHOT_RETRIES = 4
    _SNAPSHOT_RETRY_DELAY = 0.45

    # ── Cache de snapshots (stale-while-revalidate) ───────────────────────────
    #
    # POR QUE: esta MEDIDO que un solo `frame.jpeg` tarda 2,5-26 s. A diferencia
    # de `stream.mp4`, que solo retransmite el H.264 y va fluido, `frame.jpeg`
    # obliga a go2rtc a abrir una sesion RTSP contra el NVR, esperar un keyframe,
    # DECODIFICAR el cuadro y recomprimirlo a JPEG. Con 20 camaras en la
    # cuadricula eso son 20 ciclos de conectar/soltar por refresco contra un NVR
    # Dahua con sesiones simultaneas limitadas — y esa rotacion es la que le hacia
    # stutter al video en vivo que el usuario estaba mirando.
    #
    # Comprobacion frio-vs-caliente sobre la misma camara (3 pedidos seguidos):
    #   frio    2,56 / 7,89 / 11,88 s  (empeora en cada pedido)
    #   caliente 1,85 / 3,87 /  1,95 s  (con un stream.mp4 abierto en paralelo)
    #
    # COMO: se devuelve SIEMPRE el ultimo cuadro bueno al instante y, si ya paso
    # el TTL, se refresca EN SEGUNDO PLANO. Asi el pedido del usuario no espera
    # nunca a go2rtc (salvo la primera vez de cada camara) y go2rtc recibe a lo
    # sumo un pedido por camara por TTL, fuera del camino del request.
    # TTL del GRID: por debajo de esto el cuadro cacheado se sirve sin refrescar.
    # Alineado con los 8 s del warm-loop y del refresco del cliente del grid
    # (snapshotRefreshMs=8000): con el warm-loop sano el grid siempre halla un
    # cuadro < 10 s; y si se pasa (p.ej. la variante movil que el warm-loop no
    # cubre) el propio pedido dispara un refresco en segundo plano
    # (stale-while-revalidate) en vez de esperar los 30 s de antes.
    _SNAPSHOT_CACHE_TTL = 10.0
    # Hasta cuando servimos un cuadro viejo mientras se refresca por detras.
    #
    # 30 min y no 3: con 180 s, abrir la app un par de horas despues encontraba
    # TODAS las camaras frias otra vez y se volvia a pagar el arranque de 3-11 s
    # por cada una — que es exactamente lo que este cache venia a evitar. El
    # cuadro viejo se muestra AL INSTANTE y el refresco entra segundos despues,
    # asi que lo viejo dura un parpadeo.
    #
    # No es una licencia para mentir: la respuesta lleva `X-Snapshot-Age` con la
    # edad real en segundos, y la vista en vivo (video) es la fuente de verdad
    # para la camara que el usuario esta mirando. El snapshot es el telon de
    # fondo, no la evidencia.
    _SNAPSHOT_STALE_MAX = 1_800.0
    # Tope de entradas para que pedir muchas combinaciones de w/q no infle la RAM.
    _SNAPSHOT_CACHE_MAX_ENTRIES = 120
    # TTL para el VISOR EN VIVO (`live=1`). En iPhone el visor a pantalla
    # completa no usa video —Safari no trae MediaSource— sino un bucle de
    # snapshots a ~1 fps. Con el TTL normal de 30 s ese bucle recibia EL MISMO
    # cuadro 30 veces seguidas y el reloj de la camara se veia congelado: el
    # cache arreglaba la cuadricula y rompia el visor. Con 0,8 s el bucle avanza
    # y el single-flight sigue coalescando si hay varios espectadores.
    _SNAPSHOT_LIVE_TTL = 0.8

    # ── Warm-up proactivo ─────────────────────────────────────────────────────
    # El caché de arriba es REACTIVO: solo mantiene tibia una cámara que YA se
    # pidió. Cuando nadie mira el grid por un rato, go2rtc suelta el stream RTSP
    # por inactividad y la próxima apertura paga el COLD-START (5-19 s el 1er
    # cuadro). Este loop pide un cuadro de cada cámara cada _PREWARM_INTERVAL →
    # el server mantiene una foto fresca de cada cámara cada 8 s para el grid, el
    # caché nunca vence y —más importante— el stream de go2rtc queda vivo, así el
    # grid abre rápido SIEMPRE. Es UNA decodificación por cámara cada 8 s del lado
    # server, sin importar cuántos clientes ni cuántas fichas miren el grid: PC y
    # móvil se sirven de este mismo caché tibio (el vivo pesado queda solo para la
    # cámara que se abre a pantalla completa).
    _PREWARM_ENABLED = True
    _PREWARM_INTERVAL = 8.0  # < _SNAPSHOT_CACHE_TTL (10 s) → grid siempre encuentra caché fresca
    _PREWARM_CONCURRENCY = 4  # gentil con la cajita en el warm inicial en frío
    _PREWARM_WIDTH = 512  # variante del grid de ESCRITORIO; tener vivo el stream acelera todas
    _PREWARM_QUALITY = 55
    _PREWARM_INITIAL_DELAY = 8.0  # dejar bootear el add-on antes del 1er ciclo

    def __init__(
        self,
        *,
        ha_base_url: str,
        ha_token: str,
        go2rtc_base_url: str | None,
        go2rtc_username: str | None,
        go2rtc_password: str | None,
        camera_streams: dict[str, str],
    ) -> None:
        self._ha_base_url = ha_base_url.rstrip("/")
        self._ha_token = ha_token
        self._go2rtc_base_url = go2rtc_base_url.rstrip("/") if go2rtc_base_url else None
        self._go2rtc_auth = (
            aiohttp.BasicAuth(go2rtc_username, go2rtc_password)
            if go2rtc_username and go2rtc_password
            else None
        )
        self._camera_streams = dict(camera_streams)
        self._session: aiohttp.ClientSession | None = None
        self._snapshot_slots = asyncio.Semaphore(24)
        self._webrtc_slots = asyncio.Semaphore(4)
        # La cuadrícula de Fortunata muestra 20 substreams simultáneos.
        self._stream_slots = asyncio.Semaphore(24)
        # Cache de snapshots + un lock por variante (single-flight: si llegan
        # varios pedidos de la misma camara juntos, go2rtc recibe UNO solo).
        self._snapshot_cache: dict[_CacheKey, _CachedSnapshot] = {}
        self._snapshot_locks: dict[_CacheKey, asyncio.Lock] = {}
        # Refrescos en segundo plano en curso, y sus tasks (guardamos la
        # referencia para que el GC no se los lleve a medio camino).
        self._refreshing: set[_CacheKey] = set()
        self._refresh_tasks: set[asyncio.Task[None]] = set()
        # Task del loop de warm-up proactivo (ver constantes _PREWARM_*).
        self._prewarm_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Crea una sesion HTTP reutilizable; llamar durante lifespan."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15, connect=5, sock_read=12)
            self._session = aiohttp.ClientSession(timeout=timeout)
        # Warm-up proactivo: mantiene vivos los streams go2rtc para que el grid
        # abra rápido siempre (no solo la 2ª vez). Se cancela en close().
        if self._PREWARM_ENABLED and self._prewarm_task is None:
            self._prewarm_task = asyncio.create_task(self._prewarm_loop())

    async def close(self) -> None:
        # Cortar el warm-up ANTES que nada, para que no dispare pedidos nuevos
        # mientras cerramos.
        if self._prewarm_task is not None:
            self._prewarm_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._prewarm_task
            self._prewarm_task = None
        # Cortar los refrescos en segundo plano ANTES de cerrar la sesion HTTP,
        # si no quedan tasks pidiendo sobre un ClientSession ya cerrado.
        for task in list(self._refresh_tasks):
            task.cancel()
        if self._refresh_tasks:
            await asyncio.gather(*self._refresh_tasks, return_exceptions=True)
        self._refresh_tasks.clear()
        self._refreshing.clear()
        self._snapshot_cache.clear()
        self._snapshot_locks.clear()
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._ha_token = ""

    @property
    def webrtc_enabled(self) -> bool:
        return self._go2rtc_base_url is not None and bool(self._camera_streams)

    def has_stream(self, entity_id: str) -> bool:
        return self.webrtc_enabled and entity_id in self._camera_streams

    def list_stream_entities(self) -> list[str]:
        """entity_ids con stream go2rtc mapeado (allowlist), en orden estable."""
        if not self.webrtc_enabled:
            return []
        return sorted(self._camera_streams)

    def go2rtc_ws_target(
        self, entity_id: str
    ) -> tuple[str, aiohttp.BasicAuth | None] | None:
        """
        Destino del puente MSE-over-WebSocket de go2rtc para una camara.

        Devuelve (url_ws, auth) para conectar a {go2rtc}/api/ws?src=<stream>, o
        None si go2rtc no esta configurado o el entity_id no tiene stream mapeado.
        El nombre del stream sale SIEMPRE del allowlist del servidor, nunca del
        navegador (anti-SSRF, igual que el resto de camera_media).
        """
        if not self._go2rtc_base_url:
            return None
        stream_name = self._camera_streams.get(entity_id)
        if stream_name is None:
            return None
        # http->ws / https->wss (mismo reemplazo /^http/ -> ws del BFF de Next).
        ws_base = "ws" + self._go2rtc_base_url[4:]
        encoded = quote(stream_name, safe="")
        return f"{ws_base}/api/ws?src={encoded}", self._go2rtc_auth

    def _require_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            raise CameraMediaError("media_client_not_started", 503)
        return self._session

    async def get_snapshot(
        self,
        entity_id: str,
        *,
        width: int | None = None,
        quality: int | None = None,
        live: bool = False,
    ) -> SnapshotPayload:
        """
        Devuelve una imagen fija de la camara, sirviendo del cache cuando puede.

        Contrato de la CUADRICULA (`live=False`, ver constantes _SNAPSHOT_CACHE_*):
          · Hay cuadro y es reciente (< TTL)  -> se devuelve al instante.
          · Hay cuadro y esta vencido          -> se devuelve al instante IGUAL y
            se dispara un refresco en segundo plano.
          · Hay cuadro pero pasa de STALE_MAX  -> se pide uno nuevo esperando.
          · No hay cuadro (camara fria)        -> se pide uno nuevo esperando.
        O sea: solo el primer pedido de cada camara paga el costo de go2rtc.

        Contrato del VISOR EN VIVO (`live=True`): TTL de 0,8 s y NADA de servir
        vencido — el bucle de snapshots de iPhone necesita cuadros que avancen,
        no el ultimo bueno. Devolverle cache viejo se ve como imagen congelada.
        """
        key: _CacheKey = (entity_id, width, quality)
        ttl = self._SNAPSHOT_LIVE_TTL if live else self._SNAPSHOT_CACHE_TTL
        cached = self._snapshot_cache.get(key)

        if cached is not None:
            age = time.monotonic() - cached.stored_at
            if age < ttl:
                return cached.payload  # suficientemente fresco para ambos modos
            # Vencido: la cuadricula se conforma con lo viejo y refresca detras;
            # el visor NO — espera el cuadro nuevo aunque cueste.
            if not live and age < self._SNAPSHOT_STALE_MAX:
                self._schedule_refresh(key)
                return cached.payload

        return await self._refresh_snapshot(key, ttl=ttl)

    def snapshot_age(
        self,
        entity_id: str,
        *,
        width: int | None = None,
        quality: int | None = None,
    ) -> float | None:
        """Segundos desde que se capturo el cuadro cacheado (None si no hay)."""
        cached = self._snapshot_cache.get((entity_id, width, quality))
        if cached is None:
            return None
        return time.monotonic() - cached.stored_at

    def _schedule_refresh(self, key: _CacheKey) -> None:
        """Dispara un refresco en segundo plano si no hay ya uno para esta clave."""
        if key in self._refreshing:
            return
        self._refreshing.add(key)
        task = asyncio.create_task(self._background_refresh(key))
        self._refresh_tasks.add(task)
        task.add_done_callback(self._refresh_tasks.discard)

    async def _background_refresh(self, key: _CacheKey) -> None:
        """Refresco fuera del camino del request: si falla, queda el cuadro viejo."""
        try:
            await self._refresh_snapshot(key)
        except CameraMediaError:
            pass  # go2rtc no pudo ahora; el cuadro cacheado sigue sirviendo
        finally:
            self._refreshing.discard(key)

    async def _prewarm_loop(self) -> None:
        """
        Mantiene las cámaras TIBIAS de forma proactiva (ver constantes _PREWARM_*).

        Cada _PREWARM_INTERVAL pide un cuadro de cada stream mapeado con
        concurrencia acotada. `_refresh_snapshot(..., ttl=_PREWARM_INTERVAL)`
        salta el pedido si otro (real o del ciclo anterior) ya trajo un cuadro
        dentro de la ventana → durante el uso activo defiere al tráfico real, y
        durante la inactividad es el único que mantiene vivo el stream de go2rtc.
        Nunca tumba el add-on: un ciclo que falla se loguea y se reintenta.
        """
        await asyncio.sleep(self._PREWARM_INITIAL_DELAY)
        sem = asyncio.Semaphore(self._PREWARM_CONCURRENCY)

        async def _warm_one(entity_id: str) -> None:
            async with sem:
                key: _CacheKey = (entity_id, self._PREWARM_WIDTH, self._PREWARM_QUALITY)
                # go2rtc no pudo ahora; se reintenta en el próximo ciclo.
                with suppress(CameraMediaError):
                    await self._refresh_snapshot(key, ttl=self._PREWARM_INTERVAL)

        while True:
            try:
                entities = self.list_stream_entities()
                if entities:
                    await asyncio.gather(
                        *(_warm_one(e) for e in entities), return_exceptions=True
                    )
            except asyncio.CancelledError:
                raise  # cierre normal (close())
            except Exception:  # noqa: BLE001 — un loop de fondo jamás debe tumbar el add-on
                logger.exception("camera.prewarm_cycle_error")
            await asyncio.sleep(self._PREWARM_INTERVAL)

    async def _refresh_snapshot(
        self, key: _CacheKey, *, ttl: float | None = None
    ) -> SnapshotPayload:
        """
        Pide un cuadro nuevo y actualiza el cache. Single-flight por clave.

        `ttl` es la frescura que exige QUIEN LLAMA (el visor pide 0,8 s, la
        cuadricula 30 s). Importa al salir del lock: si mientras esperabamos
        otro pedido ya trajo un cuadro, lo reusamos solo si cumple ESE umbral.

        Un cuadro GRIS nunca pisa uno bueno: si go2rtc devuelve el relleno y ya
        teniamos algo real guardado, se conserva lo real.
        """
        umbral = self._SNAPSHOT_CACHE_TTL if ttl is None else ttl
        lock = self._snapshot_locks.setdefault(key, asyncio.Lock())
        async with lock:
            # Otro pedido pudo haberlo refrescado mientras esperabamos el lock.
            cached = self._snapshot_cache.get(key)
            if cached is not None:
                if time.monotonic() - cached.stored_at < umbral:
                    return cached.payload

            entity_id, width, quality = key
            payload = await self._fetch_snapshot(entity_id, width=width, quality=quality)

            es_gris = len(payload.body) < self._PLACEHOLDER_MAX_BYTES
            if es_gris and cached is not None:
                return cached.payload  # preferimos lo real viejo al gris nuevo

            self._store_snapshot(key, payload)
            return payload

    def _store_snapshot(self, key: _CacheKey, payload: SnapshotPayload) -> None:
        """Guarda el cuadro, desalojando el mas viejo si el cache se paso de tope."""
        self._snapshot_cache[key] = _CachedSnapshot(
            payload=payload, stored_at=time.monotonic()
        )
        if len(self._snapshot_cache) > self._SNAPSHOT_CACHE_MAX_ENTRIES:
            oldest = min(
                self._snapshot_cache,
                key=lambda k: self._snapshot_cache[k].stored_at,
            )
            self._snapshot_cache.pop(oldest, None)
            self._snapshot_locks.pop(oldest, None)

    async def _fetch_snapshot(
        self,
        entity_id: str,
        *,
        width: int | None = None,
        quality: int | None = None,
    ) -> SnapshotPayload:
        """
        Trae una imagen fija desde go2rtc o el proxy de camara de HA (sin cache).

        `width`/`quality` (solo para go2rtc) piden una imagen mas chica y
        comprimida — se usa en el grid de camaras, para que cargue rapido y
        parejo. El fullscreen y el proxy de HA usan calidad completa.
        """
        if self.has_stream(entity_id):
            return await self._get_go2rtc_snapshot(entity_id, width=width, quality=quality)

        session = self._require_session()
        url = f"{self._ha_base_url}/api/camera_proxy/{entity_id}"
        headers = {
            "Authorization": f"Bearer {self._ha_token}",
            "Accept": "image/jpeg,image/png,image/webp",
        }

        try:
            async with self._snapshot_slots, session.get(
                url, headers=headers, allow_redirects=False
            ) as response:
                if response.status == 404:
                    raise CameraMediaError("snapshot_not_found", 404)
                if response.status in {401, 403}:
                    raise CameraMediaError("ha_snapshot_auth_failed", 502)
                if response.status >= 400:
                    raise CameraMediaError("ha_snapshot_unavailable", 502)

                content_type = response.headers.get("Content-Type", "").split(";", 1)[0].lower()
                if content_type not in self._ALLOWED_IMAGE_TYPES:
                    raise CameraMediaError("invalid_snapshot_content_type", 502)

                declared_size = response.content_length
                if declared_size is not None and declared_size > self._MAX_SNAPSHOT_BYTES:
                    raise CameraMediaError("snapshot_too_large", 502)

                body = await response.content.read(self._MAX_SNAPSHOT_BYTES + 1)
                if not body:
                    raise CameraMediaError("empty_snapshot", 502)
                if len(body) > self._MAX_SNAPSHOT_BYTES:
                    raise CameraMediaError("snapshot_too_large", 502)
                return SnapshotPayload(body=body, content_type=content_type)
        except CameraMediaError:
            raise
        except TimeoutError as exc:
            raise CameraMediaError("ha_snapshot_timeout", 504) from exc
        except aiohttp.ClientError as exc:
            raise CameraMediaError("ha_snapshot_connection_failed", 502) from exc

    async def _get_go2rtc_snapshot(
        self,
        entity_id: str,
        *,
        width: int | None = None,
        quality: int | None = None,
    ) -> SnapshotPayload:
        """
        Extrae un JPEG del stream go2rtc allowlisted para camaras virtuales.

        go2rtc devuelve un frame gris de relleno de forma intermitente cuando el
        stream no tiene un cuadro listo en ese instante. Reintentamos hasta agarrar
        uno real; en las pruebas casi siempre el 1er o 2do intento ya trae imagen.
        Si todos vienen pequeños (stream muy frío) devolvemos el ultimo — mejor un
        gris que un error.

        `width`/`quality` se pasan a go2rtc (params `w`/`quality` de frame.jpeg)
        para pedir una imagen mas liviana en el grid del celular. Ya vienen
        validados/acotados por la capa de la API.
        """
        session = self._require_session()
        if not self._go2rtc_base_url:
            raise CameraMediaError("go2rtc_not_configured", 503)
        stream_name = self._camera_streams.get(entity_id)
        if stream_name is None:
            raise CameraMediaError("camera_stream_not_configured", 404)

        encoded = quote(stream_name, safe="")
        url = f"{self._go2rtc_base_url}/api/frame.jpeg?src={encoded}"
        if width is not None:
            url += f"&w={width}"
        if quality is not None:
            url += f"&quality={quality}"

        last: SnapshotPayload | None = None
        async with self._snapshot_slots:
            for intento in range(self._SNAPSHOT_RETRIES):
                payload = await self._fetch_go2rtc_frame(session, url)
                if len(payload.body) >= self._PLACEHOLDER_MAX_BYTES:
                    return payload  # cuadro real
                last = payload  # gris de relleno → reintentar
                if intento < self._SNAPSHOT_RETRIES - 1:
                    await asyncio.sleep(self._SNAPSHOT_RETRY_DELAY)
        if last is None:
            raise CameraMediaError("empty_snapshot", 502)
        return last

    async def _fetch_go2rtc_frame(
        self, session: aiohttp.ClientSession, url: str
    ) -> SnapshotPayload:
        """Un solo GET a go2rtc /api/frame.jpeg con validacion (sin reintentos)."""
        try:
            async with session.get(
                url,
                headers={"Accept": "image/jpeg"},
                auth=self._go2rtc_auth,
                allow_redirects=False,
            ) as response:
                if response.status == 404:
                    raise CameraMediaError("go2rtc_stream_not_found", 404)
                if response.status in {401, 403}:
                    raise CameraMediaError("go2rtc_auth_failed", 502)
                if response.status >= 400:
                    raise CameraMediaError("go2rtc_snapshot_failed", 502)

                content_type = (
                    response.headers.get("Content-Type", "").split(";", 1)[0].lower()
                )
                if content_type != "image/jpeg":
                    raise CameraMediaError("invalid_snapshot_content_type", 502)
                body = await response.content.read(self._MAX_SNAPSHOT_BYTES + 1)
                if not body:
                    raise CameraMediaError("empty_snapshot", 502)
                if len(body) > self._MAX_SNAPSHOT_BYTES:
                    raise CameraMediaError("snapshot_too_large", 502)
                return SnapshotPayload(body=body, content_type=content_type)
        except CameraMediaError:
            raise
        except TimeoutError as exc:
            raise CameraMediaError("go2rtc_snapshot_timeout", 504) from exc
        except aiohttp.ClientError as exc:
            raise CameraMediaError("go2rtc_connection_failed", 502) from exc

    async def exchange_webrtc_offer(
        self,
        entity_id: str,
        offer_sdp: str,
        user_agent: str | None = None,
    ) -> str:
        """Intercambia SDP con la API WHEP interna de go2rtc."""
        session = self._require_session()
        if not self._go2rtc_base_url:
            raise CameraMediaError("go2rtc_not_configured", 503)
        stream_name = self._camera_streams.get(entity_id)
        if stream_name is None:
            raise CameraMediaError("camera_stream_not_configured", 404)
        encoded = quote(stream_name, safe="")
        url = f"{self._go2rtc_base_url}/api/webrtc?src={encoded}"
        headers = {
            "Content-Type": "application/sdp",
            "Accept": "application/sdp",
        }
        if user_agent:
            headers["User-Agent"] = user_agent[:256]

        try:
            async with self._webrtc_slots, session.post(
                url,
                data=offer_sdp.encode("utf-8"),
                headers=headers,
                auth=self._go2rtc_auth,
                allow_redirects=False,
            ) as response:
                if response.status == 404:
                    raise CameraMediaError("go2rtc_stream_not_found", 404)
                if response.status in {401, 403}:
                    raise CameraMediaError("go2rtc_auth_failed", 502)
                if response.status >= 400:
                    raise CameraMediaError("go2rtc_offer_failed", 502)

                content_type = response.headers.get("Content-Type", "").split(";", 1)[0].lower()
                if content_type != "application/sdp":
                    raise CameraMediaError("invalid_webrtc_answer_content_type", 502)
                answer = await response.content.read(self._MAX_SDP_BYTES + 1)
                if not answer or len(answer) > self._MAX_SDP_BYTES:
                    raise CameraMediaError("invalid_webrtc_answer_size", 502)
                return answer.decode("utf-8")
        except CameraMediaError:
            raise
        except (TimeoutError, UnicodeDecodeError) as exc:
            raise CameraMediaError("go2rtc_offer_timeout", 504) from exc
        except aiohttp.ClientError as exc:
            raise CameraMediaError("go2rtc_connection_failed", 502) from exc

    @asynccontextmanager
    async def open_mp4_stream(
        self,
        entity_id: str,
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        """Abre un fMP4 continuo desde un stream go2rtc mapeado."""
        session = self._require_session()
        if not self._go2rtc_base_url:
            raise CameraMediaError("go2rtc_not_configured", 503)
        stream_name = self._camera_streams.get(entity_id)
        if stream_name is None:
            raise CameraMediaError("camera_stream_not_configured", 404)

        encoded = quote(stream_name, safe="")
        url = f"{self._go2rtc_base_url}/api/stream.mp4?src={encoded}"
        timeout = aiohttp.ClientTimeout(total=None, connect=5, sock_read=15)

        async with self._stream_slots:
            try:
                async with session.get(
                    url,
                    headers={"Accept": "video/mp4"},
                    auth=self._go2rtc_auth,
                    allow_redirects=False,
                    timeout=timeout,
                ) as response:
                    if response.status == 404:
                        raise CameraMediaError("go2rtc_stream_not_found", 404)
                    if response.status in {401, 403}:
                        raise CameraMediaError("go2rtc_auth_failed", 502)
                    if response.status >= 400:
                        raise CameraMediaError("go2rtc_stream_failed", 502)
                    content_type = (
                        response.headers.get("Content-Type", "").split(";", 1)[0].lower()
                    )
                    if content_type != "video/mp4":
                        raise CameraMediaError("invalid_stream_content_type", 502)
                    yield response
            except CameraMediaError:
                raise
            except TimeoutError as exc:
                raise CameraMediaError("go2rtc_stream_timeout", 504) from exc
            except aiohttp.ClientError as exc:
                raise CameraMediaError("go2rtc_connection_failed", 502) from exc

    async def _fetch_raw_hls(
        self,
        url: str,
        session: aiohttp.ClientSession,
        *,
        not_found_code: str,
        not_found_status: int,
    ) -> str:
        """
        Descarga y decodifica una playlist HLS (master o media) desde go2rtc.

        `not_found_code`/`not_found_status` distinguen "no hay HLS en esta
        build" (501, para el master) de "sesion de media expirada" (502).
        """
        try:
            async with session.get(
                url,
                auth=self._go2rtc_auth,
                allow_redirects=False,
                headers={"Accept": "application/vnd.apple.mpegurl"},
            ) as response:
                if response.status == 404:
                    raise CameraMediaError(not_found_code, not_found_status)
                if response.status in {401, 403}:
                    raise CameraMediaError("go2rtc_auth_failed", 502)
                if response.status >= 400:
                    raise CameraMediaError("go2rtc_hls_failed", 502)

                raw = await response.content.read(_MAX_PLAYLIST_BYTES + 1)
                if not raw:
                    raise CameraMediaError("go2rtc_hls_empty", 502)
                if len(raw) > _MAX_PLAYLIST_BYTES:
                    raise CameraMediaError("go2rtc_hls_demasiado_grande", 502)
        except CameraMediaError:
            raise
        except TimeoutError as exc:
            raise CameraMediaError("go2rtc_hls_timeout", 504) from exc
        except aiohttp.ClientError as exc:
            raise CameraMediaError("go2rtc_connection_failed", 502) from exc

        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CameraMediaError("go2rtc_hls_encoding_invalido", 502) from exc

    async def fetch_hls_playlist(
        self,
        entity_id: str,
        *,
        our_seg_prefix: str,
    ) -> str:
        """
        Descarga la playlist HLS de go2rtc y reescribe las URIs de segmento.

        go2rtc 1.9.14 devuelve un MASTER playlist con #EXT-X-STREAM-INF en
        /api/stream.m3u8?src=<nombre>. El browser (Safari) necesita una MEDIA
        playlist directa. Este metodo la sigue server-side:

        1. Fetch master de /api/stream.m3u8?src=<allowlist>.
        2. Si tiene #EXT-X-STREAM-INF: extraer primera variante, validarla
           (mismo host go2rtc, bajo /api/, sin traversal), fetch de la media.
        3. Reescribir los segmentos de la media usando la URL EXACTA de la
           media como resolve_base (los segmentos son relativos a /api/hls/,
           NO a /api/ — usar la base del master produce rutas erroneas).
        4. Devolver la media playlist reescrita.

        Si go2rtc responde 404 al master: "go2rtc_hls_no_soportado" (501) —
        distinguible de un error de red para diagnostico rapido.

        Si la reescritura falla, loguea la muestra cruda en el add-on (los
        errores 5xx de go2rtc no llegan al exterior porque Cloudflare los
        reemplaza — el log es la unica ventana de diagnostico).
        """
        session = self._require_session()
        if not self._go2rtc_base_url:
            raise CameraMediaError("go2rtc_not_configured", 503)
        stream_name = self._camera_streams.get(entity_id)
        if stream_name is None:
            raise CameraMediaError("camera_stream_not_configured", 404)

        encoded = quote(stream_name, safe="")
        master_url = f"{self._go2rtc_base_url}/api/stream.m3u8?src={encoded}"

        async with self._stream_slots:
            # ── Fetch del master playlist ────────────────────────────────────
            master_raw = await self._fetch_raw_hls(
                master_url,
                session,
                not_found_code="go2rtc_hls_no_soportado",
                not_found_status=501,
            )

            # ── Si es master, seguir a la media playlist ─────────────────────
            if "#EXT-X-STREAM-INF" in master_raw:
                variant_uri_raw = _extract_first_variant_uri(master_raw)
                if variant_uri_raw is None:
                    raise CameraMediaError("go2rtc_hls_master_sin_variante", 502)

                # La variante es relativa al master_url, no a go2rtc_base_url.
                variant_ref = _resolve_seg_ref(
                    variant_uri_raw,
                    go2rtc_base_url=self._go2rtc_base_url,
                    resolve_base=master_url,
                )
                # go2rtc 1.9.14 identifica la sesion por `id`, no por `src`:
                # la URI de variante ya trae el id (hls/playlist.m3u8?id=XXX).
                # No se agrega src — el id es suficiente y un src ajeno puede
                # desorientar a go2rtc.
                media_url = f"{self._go2rtc_base_url}{variant_ref}"
                media_raw = await self._fetch_raw_hls(
                    media_url,
                    session,
                    not_found_code="go2rtc_hls_variante_no_encontrada",
                    not_found_status=502,
                )
            else:
                # go2rtc devolvio media playlist directa (poco probable en 1.9.14,
                # pero lo cubrimos por si cambia en versiones futuras).
                media_url = master_url
                media_raw = master_raw

        # resolve_base = URL de la MEDIA playlist: los segmentos de go2rtc son
        # relativos a /api/hls/playlist.m3u8?id=X, no a /api/stream.m3u8.
        try:
            return _rewrite_hls_playlist(
                media_raw,
                go2rtc_base_url=self._go2rtc_base_url,
                resolve_base=media_url,
                our_seg_prefix=our_seg_prefix,
            )
        except CameraMediaError as exc:
            # Logueamos la muestra cruda para poder corregir el parseo sin
            # adivinar: Cloudflare reemplaza el cuerpo de 5xx y el detail
            # nunca llega al exterior.
            logger.error(
                "hls.rewrite_failed",
                code=exc.code,
                entity_id=entity_id,
                muestra=media_raw[:600],
            )
            raise

    async def fetch_hls_segment(self, entity_id: str, seg_ref: str) -> HlsSegmentPayload:
        """
        Proxea un segmento HLS desde go2rtc hacia el navegador.

        `seg_ref` es la referencia generada por _rewrite_hls_playlist:
        path+query de go2rtc sin scheme, host ni el param src. Llega aqui ya
        validado por el endpoint de la API; se re-valida por defensividad.

        Reconstruye la URL de go2rtc agregando el src del allowlist del servidor
        (nunca del request del navegador) — ese es el mecanismo anti-SSRF del
        lado de los segmentos: el navegador solo ve el entity_id en la ruta, no
        el stream name interno ni el host de go2rtc.
        """
        session = self._require_session()
        if not self._go2rtc_base_url:
            raise CameraMediaError("go2rtc_not_configured", 503)
        stream_name = self._camera_streams.get(entity_id)
        if stream_name is None:
            raise CameraMediaError("camera_stream_not_configured", 404)

        # Re-validacion defensiva: aunque ya viene validado del endpoint,
        # el cliente no deberia confiar en que su llamador siempre valido.
        if not seg_ref or len(seg_ref) > _SEG_REF_MAX_LEN or not _SEG_REF_RE.fullmatch(seg_ref):
            raise CameraMediaError("go2rtc_hls_seg_ref_invalida", 400)

        # Reconstruir la URL de go2rtc con la siguiente logica de seguridad:
        #
        # 1. SIEMPRE remover `src` del seg_ref, aunque el navegador lo inyecte.
        #    Un cliente puede fabricar `_seg=/api/x?src=EVIL&id=abc` — pasa la
        #    regex (caracteres legitimos). go2rtc (Go) resuelve Query().Get("src")
        #    con el PRIMER valor: el del navegador ganaria. Removerlo siempre
        #    garantiza que solo viaja el src del allowlist.
        #
        # 2. Agregar `src` del allowlist SOLO SI no hay `id` en el seg_ref.
        #    go2rtc 1.9.14 identifica sesiones HLS por `id` opaco — una vez que
        #    la sesion existe, go2rtc sabe a que stream pertenece. Agregar un
        #    `src` ajeno a un recurso de sesion puede confundir a go2rtc o, peor,
        #    hacer que ignore el id y abra una sesion nueva para otro stream.
        partes = urlparse(seg_ref)
        query_pairs = [
            (k, v)
            for k, v in parse_qsl(partes.query, keep_blank_values=True)
            if k != "src"
        ]
        # Agregar src del allowlist AL FINAL, siempre: aunque el segmento
        # tenga id de sesion, go2rtc lo usa para encontrar la sesion pero
        # necesita el src para autorizar la peticion contra la configuracion.
        # El src del navegador ya fue eliminado arriba.
        query_pairs.append(("src", stream_name))
        go2rtc_url = f"{self._go2rtc_base_url}{partes.path}?{urlencode(query_pairs)}"

        async with self._stream_slots:
            try:
                async with session.get(
                    go2rtc_url,
                    auth=self._go2rtc_auth,
                    allow_redirects=False,
                    headers={"Accept": "video/mp4, video/mp2t, */*"},
                ) as response:
                    if response.status == 404:
                        raise CameraMediaError("go2rtc_hls_segmento_no_encontrado", 404)
                    if response.status in {401, 403}:
                        raise CameraMediaError("go2rtc_auth_failed", 502)
                    if response.status >= 400:
                        raise CameraMediaError("go2rtc_hls_segment_failed", 502)

                    content_type = (
                        response.headers.get("Content-Type", "application/octet-stream")
                        .split(";")[0]
                        .strip()
                        .lower()
                    )
                    if content_type not in _HLS_SEGMENT_CONTENT_TYPES:
                        raise CameraMediaError("go2rtc_hls_segment_content_type_invalido", 502)

                    body = await response.content.read(_MAX_SEGMENT_BYTES + 1)
                    if len(body) > _MAX_SEGMENT_BYTES:
                        raise CameraMediaError("go2rtc_hls_segmento_demasiado_grande", 502)

                    # Normalizamos octet-stream a video/mp4 para que Safari no
                    # rechace el segmento por tipo de contenido desconocido.
                    final_ct = content_type if content_type != "application/octet-stream" else "video/mp4"
                    return HlsSegmentPayload(body=body, content_type=final_ct)
            except CameraMediaError:
                raise
            except TimeoutError as exc:
                raise CameraMediaError("go2rtc_hls_segment_timeout", 504) from exc
            except aiohttp.ClientError as exc:
                raise CameraMediaError("go2rtc_connection_failed", 502) from exc
