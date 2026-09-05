"""
Sondeo de canales del NVR — ¿hay cámaras conectadas que la app no muestra?

── QUÉ PREGUNTA RESPONDE ────────────────────────────────────────────────────
El NVR de Fortunatta es un Dahua DH-NVR5232-4KS2: **32 canales**, y la app
muestra **20**. La pregunta obvia —"¿hay cámaras en los otros 12?"— hoy solo se
puede contestar mirando el DMSS del cliente, y el cliente no siempre está.

Esto la contesta sin él: le pide a go2rtc que intente conectarse a cada canal y
mira si vuelve video.

── 🔪 EL CLIENTE NO ELIGE LA URL. NUNCA. ────────────────────────────────────
Este módulo construye la URL RTSP **derivándola de un stream que ya funciona**:
toma el de una cámara existente y le cambia el número de canal. El que llama
solo puede pasar NÚMEROS de canal, acotados.

No es una precaución teórica. Este proyecto ya se comió un bypass del allowlist
por dejar que un parámetro del cliente entrara en la URL de go2rtc: mandando un
`src` propio el navegador elegía qué stream ver (ver `fetch_hls_segment` y su
prueba de regresión). La lección que quedó: **concatenar un parámetro de
seguridad no lo impone — hay que construir la URL entera del lado servidor.**

── LAS CREDENCIALES NO SALEN DE LA CAJA ─────────────────────────────────────
La URL derivada lleva usuario y contraseña del NVR adentro. **Nunca se registra,
nunca se devuelve, nunca sale de este proceso.** Todo lo que cruza la puerta es:
número de canal, si dio video, y cuántos bytes pesó. `_sin_credenciales()` es la
única forma en que una URL puede aparecer en un log.

── POR QUÉ AGREGA Y BORRA UN STREAM ─────────────────────────────────────────
go2rtc solo sabe sacar un cuadro de un stream que tiene registrado. Para probar
un canal que nadie configuró hay que registrarlo un momento y sacarlo después.

Es un cambio EN MEMORIA de go2rtc, con nombre propio (`_sondeo_cNN`) que no
choca con ninguno real, y se borra en un `finally` — falle lo que falle. No toca
la configuración del add-on ni sobrevive a un reinicio.

── LA CAJITA Y EL NVR TIENEN LÍMITES ────────────────────────────────────────
Un NVR Dahua aguanta pocas sesiones RTSP simultáneas, y este proyecto ya midió
que un `frame.jpeg` en frío tarda entre 2,5 y 26 segundos. Por eso el sondeo va
**de a uno**, no en paralelo: apurarlo le robaría sesiones al video que el
cliente está mirando.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import aiohttp
import structlog

logger = structlog.get_logger(__name__)

#: Prefijo de los streams temporales. Elegido para no chocar con los reales
#: (`NVR_CH01`, `c01_ipc`…) y para que salte a la vista si alguno quedara vivo.
PREFIJO = "_sondeo_c"

#: Cuánto se espera a que un canal dé señal. Generoso a propósito: en frío
#: go2rtc tarda hasta 26 s medidos, y cortar antes daría falsos "no hay cámara".
TIMEOUT_CANAL = 30.0

#: Un JPEG por debajo de esto es el gris de relleno de go2rtc, no una imagen.
#: El valor sale de la misma medición que usa `camera_media` (~3930 B exactos).
MINIMO_REAL = 6000

#: Respiro entre canales. El NVR no es un servidor web: encadenar sesiones RTSP
#: sin pausa le roba las pocas que tiene al video que alguien esté mirando.
#: Es constante para que las pruebas puedan ponerla en 0 y no esperar de verdad.
PAUSA_ENTRE_CANALES = 0.4

#: Tope de canales por sondeo. Un NVR de 32 es el más grande de la flota; el
#: límite existe para que nadie pueda pedir un barrido de miles por descuido.
CANAL_MIN, CANAL_MAX = 1, 64


@dataclass
class ResultadoCanal:
    """Lo que se sabe de un canal. Sin URL, sin credenciales."""

    canal: int
    #: `con_camara` · `vacio` · `error`
    estado: str
    bytes: int = 0
    #: Qué contar cuando no se pudo decidir. Nunca lleva la URL.
    detalle: str | None = None


def _sin_credenciales(url: str) -> str:
    """`rtsp://user:pass@host/x` → `rtsp://***@host/x`. Para logs."""
    return re.sub(r"://[^/@]*@", "://***@", url)


def _cambiar_canal(url: str, canal: int) -> str | None:
    """
    Deriva la URL de otro canal a partir de una que funciona.

    Solo se toca el número de `channel=`. Si la URL no tiene ese parámetro no se
    inventa nada: se devuelve `None` y ese canal queda sin sondear. Adivinar la
    forma de la URL de un NVR ajeno es cómo se termina pidiéndole cosas raras al
    equipo de seguridad de una casa.
    """
    if not re.search(r"[?&]channel=\d+", url):
        return None
    return re.sub(r"([?&]channel=)\d+", rf"\g<1>{canal}", url)


class SondeoCanales:
    """Sondea canales del NVR reutilizando la conexión a go2rtc del Mirror."""

    def __init__(
        self,
        *,
        base_url: str,
        auth: aiohttp.BasicAuth | None,
        session: aiohttp.ClientSession,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._auth = auth
        self._session = session

    # ── go2rtc ───────────────────────────────────────────────────────────────

    async def _streams_actuales(self) -> dict[str, Any]:
        """Los streams que go2rtc tiene registrados ahora mismo."""
        async with self._session.get(
            f"{self._base}/api/streams", auth=self._auth
        ) as r:
            if r.status != 200:
                raise RuntimeError(f"go2rtc /api/streams devolvió {r.status}")
            return await r.json(content_type=None)

    async def _url_de_referencia(self) -> str | None:
        """
        Una URL RTSP que YA funciona, para derivar las demás.

        De acá salen el host, el usuario y la contraseña del NVR sin que este
        código los conozca nunca por separado ni los pida a nadie.
        """
        datos = await self._streams_actuales()
        for nombre, info in (datos or {}).items():
            if nombre.startswith(PREFIJO):
                continue
            fuentes: list[str] = []
            if isinstance(info, dict):
                prod = info.get("producers") or []
                for p in prod:
                    if isinstance(p, dict) and isinstance(p.get("url"), str):
                        fuentes.append(p["url"])
                if isinstance(info.get("source"), str):
                    fuentes.append(info["source"])
            elif isinstance(info, str):
                fuentes.append(info)
            for f in fuentes:
                if f.startswith("rtsp://") and re.search(r"[?&]channel=\d+", f):
                    return f
        return None

    async def _registrar(self, nombre: str, url: str) -> None:
        destino = f"{self._base}/api/streams?name={quote(nombre, safe='')}&src={quote(url, safe='')}"
        async with self._session.post(destino, auth=self._auth) as r:
            if r.status not in (200, 201, 204):
                # Algunas versiones de go2rtc usan PUT para lo mismo.
                async with self._session.put(destino, auth=self._auth) as r2:
                    if r2.status not in (200, 201, 204):
                        raise RuntimeError(f"go2rtc no aceptó el stream ({r.status}/{r2.status})")

    async def _borrar(self, nombre: str) -> None:
        destino = f"{self._base}/api/streams?src={quote(nombre, safe='')}"
        try:
            async with self._session.delete(destino, auth=self._auth) as r:
                await r.read()
        except Exception as exc:  # noqa: BLE001
            # Que no se pueda borrar no invalida el sondeo, pero hay que
            # enterarse: un stream temporal olvidado mantiene una sesión RTSP
            # abierta contra el NVR, y esas son escasas.
            logger.warning("sondeo.no_se_pudo_borrar", stream=nombre, error=str(exc)[:120])

    async def _cuadro(self, nombre: str) -> bytes:
        url = f"{self._base}/api/frame.jpeg?src={quote(nombre, safe='')}"
        async with self._session.get(
            url, auth=self._auth, timeout=aiohttp.ClientTimeout(total=TIMEOUT_CANAL)
        ) as r:
            if r.status != 200:
                raise RuntimeError(f"frame.jpeg devolvió {r.status}")
            return await r.read()

    # ── El sondeo ────────────────────────────────────────────────────────────

    async def sondear(self, canales: list[int]) -> tuple[list[ResultadoCanal], str | None]:
        """
        Prueba cada canal, de a uno. Devuelve (resultados, motivo_si_no_se_pudo).

        De a uno y no en paralelo: el NVR tiene pocas sesiones RTSP y este
        sondeo NO puede robarle ancho al video que alguien esté mirando.
        """
        referencia = await self._url_de_referencia()
        if referencia is None:
            return [], (
                "No se encontró ninguna cámara con URL RTSP por canal de la que "
                "derivar las demás. El sondeo necesita al menos una que funcione."
            )
        logger.info("sondeo.referencia", url=_sin_credenciales(referencia))

        salida: list[ResultadoCanal] = []
        for canal in canales:
            derivada = _cambiar_canal(referencia, canal)
            if derivada is None:
                salida.append(ResultadoCanal(canal, "error", detalle="URL sin canal"))
                continue

            nombre = f"{PREFIJO}{canal:02d}"
            try:
                await self._registrar(nombre, derivada)
                try:
                    datos = await self._cuadro(nombre)
                except TimeoutError:
                    salida.append(ResultadoCanal(canal, "vacio", detalle="no contestó en 30 s"))
                    continue
                n = len(datos)
                completo = n > 4 and datos[:2] == b"\xff\xd8" and datos[-2:] == b"\xff\xd9"
                if n >= MINIMO_REAL and completo:
                    salida.append(ResultadoCanal(canal, "con_camara", bytes=n))
                else:
                    salida.append(
                        ResultadoCanal(canal, "vacio", bytes=n, detalle="sin imagen real")
                    )
            except Exception as exc:  # noqa: BLE001
                # `_sin_credenciales` no hace falta acá porque el mensaje de
                # go2rtc no incluye la URL, pero se recorta por las dudas.
                salida.append(ResultadoCanal(canal, "vacio", detalle=str(exc)[:90]))
            finally:
                # SIEMPRE. Un stream temporal olvidado le come una sesión RTSP
                # al NVR, y de esas hay pocas.
                await self._borrar(nombre)
                # Un respiro entre canales: el NVR no es un servidor web.
                await asyncio.sleep(PAUSA_ENTRE_CANALES)

        return salida, None

    async def limpiar_huerfanos(self) -> int:
        """
        Borra streams `_sondeo_*` que hayan quedado de un sondeo interrumpido.

        Existe porque el `finally` no corre si el proceso muere en el medio, y
        un stream olvidado mantiene abierta una sesión contra el NVR.
        """
        try:
            datos = await self._streams_actuales()
        except Exception:  # noqa: BLE001
            return 0
        sobras = [n for n in (datos or {}) if n.startswith(PREFIJO)]
        for n in sobras:
            await self._borrar(n)
        if sobras:
            logger.info("sondeo.huerfanos_borrados", cuantos=len(sobras))
        return len(sobras)
