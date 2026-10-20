"""
Cámaras sumadas POR SU DIRECCIÓN desde la app.

── QUÉ RESUELVE ─────────────────────────────────────────────────────────────
Las cámaras de una casa nueva solo se podían cargar dentro de Home Assistant,
en la configuración del complemento. La segunda casa tiene una docena de
cámaras sueltas (Reolink, Dahua) y nadie con acceso a esa pantalla: sin esto,
la sección Cámaras de esa familia quedaba vacía para siempre.

Acá el cliente da, para UNA cámara: su dirección en la red de la casa, la
marca, y el usuario y la contraseña de esa cámara. El Mirror arma la dirección
de video, la PRUEBA, y solo si da imagen la guarda y la suma a go2rtc.

── 🔪 LAS REGLAS. LAS PIDIÓ EL DUEÑO Y SON DE SEGURIDAD A LA VEZ ────────────
1. **Una cámara por pedido, una prueba por cámara.** Este módulo no recorre
   listas de direcciones ni de contraseñas: recibe una cámara y una clave.

2. **Clave equivocada = se dice y se frena.** No se reintenta con otra clave ni
   se prueba esa clave en otra cámara. La app tampoco pasa a la siguiente: avisa
   y espera que la persona corrija o salte esa cámara.

3. **Tope de intentos fallidos por dirección** (`LimiteDeIntentos`). Algunas
   marcas —Reolink, medido en la segunda casa— bloquean la cámara un rato
   después de varios fallos seguidos, y entonces no entra ni la clave correcta.
   El Mirror frena ANTES de llegar a ese punto y dice cuánto esperar.

4. **Solo direcciones de la red de la casa** (`validar_direccion`): IPv4
   privada. Nunca internet, nunca la red interna de Home Assistant. Sin esto,
   este endpoint sería una forma de hacer que la cajita se conecte a donde
   alguien quiera.

5. **La contraseña no sale de la caja.** Se guarda en la base del Mirror, que
   vive en el almacenamiento privado del complemento. Nunca vuelve en una
   respuesta ni aparece en un registro: lo único que cruza la puerta es el
   nombre, la dirección, la marca y el resultado de la prueba.

── POR QUÉ ffprobe Y NO go2rtc PARA PROBAR ──────────────────────────────────
go2rtc contesta "no hay cuadro" igual para una clave mala que para una cámara
apagada, y para el cliente esas dos cosas piden acciones opuestas: corregir la
clave, o ir a ver la cámara. ffprobe devuelve el código RTSP de la cámara (401,
404) y deja distinguirlas. Viene en la imagen desde la 0.49.0 (paquete ffmpeg).

La dirección con la clave viaja como argumento de ffprobe unos segundos. Eso la
deja visible para `ps` DENTRO del contenedor del complemento —que tiene su propio
espacio de procesos— y para nadie más. Es la misma exposición que ya tiene el
archivo de configuración de go2rtc, que la lleva escrita.
"""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import re
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from urllib.parse import quote

import structlog

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

#: Dónde publica cada marca su video principal. Es la ruta de fábrica: si un
#: instalador la cambió, la marca "otra" deja escribir la ruta a mano.
RUTAS_POR_MARCA: dict[str, str] = {
    "dahua": "/cam/realmonitor?channel=1&subtype=0",
    "hikvision": "/Streaming/Channels/101",
    "reolink": "/h264Preview_01_main",
}

#: Reolink publica H.265 en otra ruta en sus modelos más nuevos. Se prueba SOLO
#: si la primera contesta 404 (la ruta no existe), nunca después de un 401: un
#: 401 dice que la clave está mal, y ahí se frena.
RUTA_ALTERNATIVA: dict[str, str] = {
    "reolink": "/Preview_01_main",
}

MARCAS = (*RUTAS_POR_MARCA, "otra")

#: Redes privadas que NO son la casa. Pasan `is_private`, pero detrás no hay
#: cámaras sino la propia cajita o algo que no le toca a este endpoint:
#: - 172.30.0.0/16: la red de los complementos de Home Assistant (Supervisor,
#:   DNS, los demás complementos).
#: - 172.17.0.0/16: el puente por defecto de Docker dentro de la cajita.
#: - 100.64.0.0/10: el rango compartido de los proveedores, que es además el de
#:   Tailscale. Una cámara de la casa nunca vive ahí.
_REDES_QUE_NO_SON_LA_CASA = tuple(
    ipaddress.ip_network(r) for r in ("172.30.0.0/16", "172.17.0.0/16", "100.64.0.0/10")
)

#: Los puertos donde una cámara publica video. Sin esta lista, el endpoint
#: dejaría probar cualquier servicio de la red y el resultado de la prueba
#: diría qué hay en cada puerto.
PUERTOS_RTSP = frozenset({554, 8554})

_RUTA_VALIDA = re.compile(r"^/[A-Za-z0-9/_.\-?=&%]{0,199}$")
_TEXTO_VALIDO = re.compile(r"^[^\x00-\x1f\x7f]+$")

#: Lo que ffprobe tiene para contestar. Una cámara que no está tarda en no
#: contestar; más de esto y la app parece colgada.
TIMEOUT_PRUEBA_S = 12.0


class Resultado(str, Enum):
    """Lo que salió de probar una cámara. El valor viaja a la app tal cual."""

    OK = "ok"
    CLAVE_INCORRECTA = "clave_incorrecta"
    RUTA_INCORRECTA = "ruta_incorrecta"
    NO_RESPONDE = "no_responde"
    SIN_VIDEO = "sin_video"
    ERROR = "error"


#: El texto que ve la persona. Dice qué pasó y qué hacer, sin términos técnicos.
MENSAJES: dict[Resultado, str] = {
    Resultado.CLAVE_INCORRECTA: (
        "La cámara rechazó el usuario o la contraseña. Revisalos antes de seguir: "
        "no vamos a probar con otra cámara hasta que la corrijas o la saltes."
    ),
    Resultado.RUTA_INCORRECTA: (
        "La cámara contestó, pero no publica video donde esa marca suele hacerlo. "
        "Revisá que la marca sea la correcta."
    ),
    Resultado.NO_RESPONDE: (
        "No hubo respuesta en esa dirección. Revisá que la cámara esté encendida "
        "y que la dirección sea la correcta."
    ),
    Resultado.SIN_VIDEO: "La cámara contestó, pero no mandó video.",
    Resultado.ERROR: "No se pudo probar la cámara. Probá de nuevo en un momento.",
}


# ── Validación ──────────────────────────────────────────────────────────────


def validar_direccion(texto: str) -> str:
    """
    La dirección, si es una de la red de la casa. Si no, ValueError.

    Los mensajes no repiten lo recibido: lo que se rechaza puede ser basura y
    termina en el registro.
    """
    try:
        ip = ipaddress.ip_address(texto.strip())
    except ValueError:
        raise ValueError("La dirección no es válida. Tiene que ser como 192.168.1.50.") from None
    if ip.version != 4:
        raise ValueError("Por ahora solo se aceptan direcciones como 192.168.1.50.")
    if ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_unspecified:
        raise ValueError("Esa dirección no es de una cámara de la casa.")
    if any(ip in red for red in _REDES_QUE_NO_SON_LA_CASA):
        raise ValueError("Esa dirección es de la red interna de la cajita, no de la casa.")
    if not ip.is_private:
        raise ValueError("La dirección tiene que ser de la red de la casa, no de internet.")
    return str(ip)


def validar_ruta(ruta: str) -> str:
    ruta = ruta.strip()
    if not _RUTA_VALIDA.fullmatch(ruta):
        raise ValueError("La ruta tiene que empezar con / y no llevar espacios.")
    return ruta


def _validar_texto(valor: str, que: str, largo: int) -> str:
    if not valor or len(valor) > largo or not _TEXTO_VALIDO.fullmatch(valor):
        raise ValueError(f"{que} no es válido.")
    return valor


def armar_fuente(
    *,
    ip: str,
    marca: str,
    usuario: str,
    clave: str,
    ruta: str | None = None,
    puerto: int = 554,
) -> str:
    """
    La dirección de video de la cámara, armada del lado de la caja.

    🔪 USUARIO Y CLAVE VAN CODIFICADOS. Las claves de los instaladores llevan
    `@`, `#`, `:` — una `@` sin codificar parte la dirección en dos y la cámara
    recibe otra clave que la que se escribió. `quote(..., safe='')` las vuelve
    `%40`, `%23`, `%3A`, que go2rtc y ffprobe decodifican igual.
    """
    ip = validar_direccion(ip)
    if marca not in MARCAS:
        raise ValueError("Marca desconocida.")
    if puerto not in PUERTOS_RTSP:
        raise ValueError("Ese puerto no es de video de una cámara.")
    usuario = _validar_texto(usuario, "El usuario", 64)
    clave = _validar_texto(clave, "La contraseña", 128)
    if marca == "otra":
        if not ruta:
            raise ValueError("Para otra marca hay que escribir la ruta del video.")
        camino = validar_ruta(ruta)
    else:
        camino = validar_ruta(ruta) if ruta else RUTAS_POR_MARCA[marca]
    return f"rtsp://{quote(usuario, safe='')}:{quote(clave, safe='')}@{ip}:{puerto}{camino}"


def con_otra_ruta(fuente: str, marca: str) -> str | None:
    """La misma fuente con la ruta alternativa de la marca, si tiene una."""
    alternativa = RUTA_ALTERNATIVA.get(marca)
    principal = RUTAS_POR_MARCA.get(marca)
    if not alternativa or not principal or not fuente.endswith(principal):
        return None
    return fuente[: -len(principal)] + alternativa


# ── La prueba ───────────────────────────────────────────────────────────────

#: Una dirección de video dentro de un mensaje de ffprobe.
_RE_DIRECCION = re.compile(r"rtsps?://\S+", re.IGNORECASE)


def clasificar(codigo: int | None, salida: str, error: str) -> Resultado:
    """
    Qué dijo ffprobe, en términos de lo que la persona tiene que hacer.

    Pura, para poder probarla sin una cámara. El orden importa: un 401 gana
    sobre todo lo demás, porque es lo único que pide corregir la clave.

    🔪 ANTES DE BUSCAR EL CÓDIGO SE BORRA LA DIRECCIÓN. ffprobe empieza cada
    error repitiendo la dirección entera, clave incluida. Buscando "401" en el
    texto crudo, una clave como `Casa401` hacía que una cámara apagada se leyera
    como "contraseña incorrecta" —y la app le pedía a la persona corregir una
    clave que estaba bien—. Y se buscan códigos enteros, no pedazos de número.
    """
    texto = _RE_DIRECCION.sub(" ", error).lower()
    if re.search(r"\b401\b", texto) or "unauthorized" in texto or "authorization failed" in texto:
        return Resultado.CLAVE_INCORRECTA
    if re.search(r"\b404\b", texto) or "not found" in texto:
        return Resultado.RUTA_INCORRECTA
    if codigo == 0:
        return Resultado.OK if "video" in salida.lower() else Resultado.SIN_VIDEO
    if any(
        señal in texto
        for señal in ("connection refused", "no route to host", "timed out", "timeout", "unreachable")
    ):
        return Resultado.NO_RESPONDE
    return Resultado.ERROR


async def probar(fuente: str, *, binario: str = "ffprobe", timeout_s: float = TIMEOUT_PRUEBA_S) -> Resultado:
    """
    Le pide a la cámara su primer stream de video. Nunca lanza.

    La salida de ffprobe NO se registra: sus errores repiten la dirección
    completa, con la clave adentro.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            binario,
            "-v", "error",
            "-rtsp_transport", "tcp",
            # Microsegundos. Tope de cada lectura del socket, no de la prueba entera.
            "-timeout", str(int(timeout_s * 1_000_000)),
            "-select_streams", "v:0",
            "-show_entries", "stream=codec_type",
            "-of", "csv=p=0",
            fuente,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except (FileNotFoundError, PermissionError):
        logger.error("camaras_directas.sin_ffprobe")
        return Resultado.ERROR

    try:
        salida, error = await asyncio.wait_for(proc.communicate(), timeout=timeout_s + 3)
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        with contextlib.suppress(Exception):
            await proc.wait()
        return Resultado.NO_RESPONDE

    return clasificar(
        proc.returncode,
        salida.decode("utf-8", errors="replace"),
        error.decode("utf-8", errors="replace"),
    )


# ── Tope de intentos ────────────────────────────────────────────────────────


@dataclass
class LimiteDeIntentos:
    """
    Cuántas veces se puede fallar la clave de UNA dirección antes de frenar.

    `maximo` fallos dentro de `ventana_s` y la dirección queda en pausa hasta que
    el más viejo salga de la ventana. Después de cualquier fallo hay además una
    pausa corta (`respiro_s`): dos clics seguidos en "Agregar" no cuentan como
    dos intentos contra la cámara.

    En memoria a propósito: si el Mirror se reinicia la cuenta vuelve a cero,
    que es más o menos lo que tarda la cámara en desbloquearse sola.
    """

    maximo: int = 3
    ventana_s: float = 15 * 60
    respiro_s: float = 20
    _fallos: dict[str, deque[float]] = field(default_factory=dict)

    def _vigentes(self, ip: str, ahora: float) -> deque[float]:
        fallos = self._fallos.setdefault(ip, deque())
        while fallos and ahora - fallos[0] >= self.ventana_s:
            fallos.popleft()
        return fallos

    def espera(self, ip: str, ahora: float | None = None) -> int:
        """Segundos que faltan para poder probar esta dirección. 0 = ya."""
        ahora = time.monotonic() if ahora is None else ahora
        fallos = self._vigentes(ip, ahora)
        if not fallos:
            return 0
        if len(fallos) >= self.maximo:
            return max(1, int(self.ventana_s - (ahora - fallos[0]) + 0.999))
        resto = self.respiro_s - (ahora - fallos[-1])
        return max(0, int(resto + 0.999)) if resto > 0 else 0

    def fallo(self, ip: str, ahora: float | None = None) -> None:
        ahora = time.monotonic() if ahora is None else ahora
        self._vigentes(ip, ahora).append(ahora)

    def acierto(self, ip: str) -> None:
        self._fallos.pop(ip, None)


@dataclass
class TopeDePruebas:
    """
    Cuántas pruebas hace la caja en total por hora, salga lo que salga.

    `LimiteDeIntentos` cuida a UNA cámara de las claves malas. Esto cuida a la
    casa: sin un tope general, el endpoint podría recorrer la red de a una
    dirección por vez. Cuarenta alcanza de sobra para sumar una docena de
    cámaras equivocándose varias veces.
    """

    maximo: int = 40
    ventana_s: float = 60 * 60
    _pruebas: deque[float] = field(default_factory=deque)

    def espera(self, ahora: float | None = None) -> int:
        ahora = time.monotonic() if ahora is None else ahora
        while self._pruebas and ahora - self._pruebas[0] >= self.ventana_s:
            self._pruebas.popleft()
        if len(self._pruebas) < self.maximo:
            return 0
        return max(1, int(self.ventana_s - (ahora - self._pruebas[0]) + 0.999))

    def anotar(self, ahora: float | None = None) -> None:
        self._pruebas.append(time.monotonic() if ahora is None else ahora)


def slug(texto: str) -> str:
    """`Portón de atrás` → `porton_de_atras`. Para armar el entity_id."""
    import unicodedata

    limpio = unicodedata.normalize("NFD", texto).encode("ascii", "ignore").decode()
    limpio = re.sub(r"[^a-zA-Z0-9]+", "_", limpio).strip("_").lower()
    return limpio[:28] or "camara"
