"""
go2rtc integrado — el motor de video vive adentro de la cajita.

── POR QUÉ ──────────────────────────────────────────────────────────────────
Hasta acá, cada casa con cámaras necesitaba un SEGUNDO complemento de Home
Assistant: go2rtc, instalado y configurado a mano, con el Mirror apuntando a su
dirección. Eso costaba tres cosas:

  1. Un paso manual más por casa, en un producto que se vende por réplicas.
  2. Una configuración distinta en cada casa.
  3. La más seria: el Mirror NO puede administrar otro complemento
     (`hassio_role: default`), y el túnel de una casa de plataforma solo llega
     al Mirror. Si go2rtc se colgaba, nadie podía reiniciarlo a distancia.

Integrado, go2rtc es un subproceso del Mirror —igual que cloudflared, ver
`tunnel_client.py`—: el Mirror lo arranca, lo vigila y lo reinicia si se cae.

── CUÁNDO SE ACTIVA ─────────────────────────────────────────────────────────
Ver `debe_integrarse()`. En corto:

  go2rtc_base_url con valor     → go2rtc EXTERNO, como siempre.
                                  Fortunatta queda idéntica.
  vacía + sin go2rtc_streams     → no corre nada. Una casa sin cámaras no paga
                                  ni un proceso.
  vacía + con go2rtc_streams     → integrado.

── 🔪 LA TRAMPA DE SEGURIDAD QUE DEFINE ESTE ARCHIVO ────────────────────────
go2rtc, por diseño, NO le pide clave a lo que llega desde localhost, "aunque la
tengas configurada" — textual de su documentación. En una máquina propia eso es
cómodo. Adentro de ESTE contenedor es un agujero: cloudflared corre al lado, y
todo lo que entra por el túnel le llega a go2rtc como localhost.

Las reglas del túnel viven en Cloudflare, no en la caja. Alguien con la cuenta o
el token de Cloudflare podría agregar una ruta hacia `localhost:1984` sin tocar
el hardware — y tendría las cámaras de la familia. Con un stream `exec:`, además,
ejecutaría comandos en la cajita.

Las defensas, en orden de importancia:

  1. `local_auth: true`. Exige la clave también desde localhost. Verificado en
     el código de go2rtc v1.9.14, `internal/api/api.go`:
     `if localAuth || !isLoopback(r.RemoteAddr)`.
  2. Clave aleatoria nueva en cada arranque del Mirror. Solo la conoce este
     proceso: no se guarda en opciones, ni en la base, ni en un log.
  3. API atada a 127.0.0.1: ni la red de la casa ni los otros complementos
     llegan.
  4. RTSP, RTMP, SRTP y WebRTC APAGADOS (`listen: ""`, verificado módulo por
     módulo en v1.9.14). El servidor RTSP de go2rtc TAMBIÉN se saltea la clave
     desde localhost (`internal/rtsp/rtsp.go`, línea 73): con él prendido, la
     defensa 1 no alcanzaría. La app no lo usa — el video viaja por la API.
  5. Solo se aceptan fuentes de cámara conocidas —rtsp, rtsps, rtspx, http,
     https, onvif, dvrip— al leer la configuración (ver `config.py`). La
     primera versión prohibía `exec:`/`echo:`/`expr:` y una auditoría la
     esquivó con `ffmpeg:`, que go2rtc reescribe a `exec:` por dentro.

── SI FALLA, EL MIRROR SIGUE ────────────────────────────────────────────────
Mismo criterio que cloudflared: esta caja le da luces y portones a una familia.
Que las cámaras no levanten es un problema; que por eso se caiga la casa entera
sería mucho peor.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import random
import re
import secrets
import shutil
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

import aiohttp
import structlog

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

#: Solo loopback. Ver la defensa 3 en la cabecera.
DIRECCION = "127.0.0.1"
PUERTO = 1984

# Mismo esquema de reintento que `tunnel_client.py`.
_BACKOFF_BASE = 5.0
_BACKOFF_CAP = 300.0
_BACKOFF_JITTER = 0.5
_SESION_SANA_S = 60.0

#: Cuánto se espera a que la API conteste después de arrancar. go2rtc levanta en
#: menos de un segundo; el margen es para una Raspberry recién encendida.
_ESPERA_LISTO_S = 30.0

# 🔪 Las direcciones de las cámaras llevan usuario y contraseña adentro, y go2rtc
# las escribe en su registro cuando una conexión falla. Todo lo que sale de
# go2rtc pasa por acá antes de llegar a un log.
#
# 🔪 SE CORTA HASTA LA ÚLTIMA `@`, NO LA PRIMERA. La primera versión frenaba en
# la primera `@` y una auditoría la rompió con una clave real: los NVR Dahua
# aceptan `@` en la contraseña sin codificarla, así que
# `rtsp://tecnico:Casa@2026@192.168.1.50` salía `rtsp://***@2026@192.168.1.50`
# — con media clave a la vista. `[^/\s]*` es voraz y se queda con todo lo que
# hay antes del host, hasta la última `@` que aparezca antes de la ruta.
_CREDENCIAL_EN_URL = re.compile(r"(?i)([a-z][a-z0-9+.-]*://)[^/\s]*@")
_CREDENCIAL_EN_QUERY = re.compile(r"(?i)\b(password|passwd|pass|pwd|user|username)=[^&\s]+")


def redactar(texto: str) -> str:
    """Borra usuario y contraseña de cualquier dirección que aparezca en `texto`."""
    texto = _CREDENCIAL_EN_URL.sub(r"\1***@", texto)
    return _CREDENCIAL_EN_QUERY.sub(r"\1=***", texto)


def debe_integrarse(*, go2rtc_base_url: str | None, streams: dict[str, object]) -> bool:
    """
    ¿Hay que levantar go2rtc adentro del Mirror?

    🔪 LA PRIMERA CONDICIÓN PROTEGE A FORTUNATTA. Esa casa tiene go2rtc como
    complemento aparte y `go2rtc_base_url` apuntándole. Si esta función mirara
    solo los streams, una actualización del Mirror le levantaría un segundo
    go2rtc peleándole las sesiones RTSP al NVR — que tiene pocas, y ya se midió
    que perderlas es lo que le congela el video.
    """
    if go2rtc_base_url:
        return False
    return bool(streams)


@dataclass(frozen=True)
class Credenciales:
    usuario: str
    clave: str

    @classmethod
    def nuevas(cls) -> Credenciales:
        # El usuario también es aleatorio: una clave fuerte con usuario "admin"
        # le regala la mitad del trabajo a quien pruebe.
        return cls(usuario=f"uq_{secrets.token_hex(6)}", clave=secrets.token_urlsafe(32))


def armar_config(streams: dict[str, object], credenciales: Credenciales) -> dict[str, object]:
    """
    La configuración de go2rtc. Pura: no toca disco, para poder probarla.

    Se escribe como JSON y no como YAML: YAML es un superconjunto de JSON, así
    que go2rtc la lee igual, y no hace falta sumar una dependencia para esto.
    """
    return {
        "api": {
            "listen": f"{DIRECCION}:{PUERTO}",
            "username": credenciales.usuario,
            "password": credenciales.clave,
            # Defensa 1. Sin esta línea, las dos de arriba no protegen nada.
            "local_auth": True,
        },
        # Defensa 4: apagados.
        "rtsp": {"listen": ""},
        "rtmp": {"listen": ""},
        "srtp": {"listen": ""},
        "webrtc": {"listen": ""},
        # `warn`: a nivel `info` go2rtc anota cada conexión de cliente, y en una
        # Raspberry con tarjeta eso es escritura constante para nada.
        "log": {"level": "warn", "format": "text", "output": "stdout"},
        "streams": streams,
    }


def escribir_config(ruta: Path, config: dict[str, object]) -> None:
    """
    Escribe la configuración con permisos 0600, de forma atómica.

    Atómica porque si la cajita se apaga a mitad de la escritura, go2rtc no puede
    arrancar con un archivo cortado a la mitad. 0600 porque adentro van las
    contraseñas de las cámaras.
    """
    ruta.parent.mkdir(parents=True, exist_ok=True)
    # 🔪 LA CARPETA EN 0700, NO SOLO EL ARCHIVO. go2rtc REESCRIBE este archivo
    # cada vez que alguien registra un stream por su API —las cámaras sumadas
    # desde la app lo hacen— y lo deja en 0644 (`os.WriteFile(..., 0644)` en
    # su código). El 0600 de abajo dura hasta la primera cámara nueva. Con la
    # carpeta cerrada, ningún otro usuario llega al archivo, tenga el permiso
    # que tenga.
    os.chmod(ruta.parent, 0o700)

    contenido = json.dumps(config, ensure_ascii=False)
    # Si ya está igual no se toca el disco. Si go2rtc entrara en un bucle de
    # caídas, reescribir en cada intento serían cientos de escrituras por día
    # sobre la tarjeta de una Raspberry.
    try:
        if ruta.read_text(encoding="utf-8") == contenido:
            return
    except (FileNotFoundError, UnicodeDecodeError):
        pass

    fd, temporal = tempfile.mkstemp(dir=ruta.parent, prefix=".go2rtc-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(contenido)
        os.chmod(temporal, 0o600)
        os.replace(temporal, ruta)
    except BaseException:
        # No dejar el temporal: lleva contraseñas adentro.
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporal)
        raise


class Go2rtcIntegrado:
    """Corre go2rtc como subproceso del Mirror y lo reinicia si se cae."""

    def __init__(
        self,
        *,
        ruta_config: Path,
        streams: dict[str, object],
        binario: str = "go2rtc",
    ) -> None:
        self._ruta_config = ruta_config
        self._streams = dict(streams)
        self._binario = binario
        self.credenciales = Credenciales.nuevas()
        self._proc: asyncio.subprocess.Process | None = None
        #: Se llama CADA VEZ que go2rtc queda listo, no solo la primera. Ver
        #: `run_forever` para por qué importa.
        self.al_estar_listo: Callable[[], Awaitable[None]] | None = None

    @property
    def base_url(self) -> str:
        return f"http://{DIRECCION}:{PUERTO}"

    def binario_disponible(self) -> bool:
        return shutil.which(self._binario) is not None

    async def run_forever(self) -> None:
        """
        Arranca go2rtc, espera a que conteste, y lo vuelve a levantar si se cae.

        🔪 CADA ARRANQUE LLAMA A `al_estar_listo`, NO SOLO EL PRIMERO. Las
        cámaras sumadas desde la app se registran por la API de go2rtc y viven en
        su MEMORIA. Si go2rtc se cae y este supervisor lo levanta de nuevo, esas
        cámaras ya no están — y como el Mirror nunca se reinició, nadie las
        vuelve a poner. Desaparecerían de la app sin un solo error.
        """
        ruta = shutil.which(self._binario)
        if ruta is None:
            logger.error(
                "go2rtc.binario_ausente",
                binario=self._binario,
                msg="go2rtc no está en la imagen. La casa funciona, pero sin cámaras.",
            )
            return

        backoff = _BACKOFF_BASE
        while True:
            escribir_config(self._ruta_config, armar_config(self._streams, self.credenciales))

            arrancado = asyncio.get_running_loop().time()
            try:
                codigo = await self._correr_una_vez(ruta)
            except asyncio.CancelledError:
                await self._terminar()
                raise

            vivio = asyncio.get_running_loop().time() - arrancado
            if vivio >= _SESION_SANA_S:
                backoff = _BACKOFF_BASE

            jitter = 1.0 + _BACKOFF_JITTER * (2 * random.random() - 1)
            espera = min(backoff * jitter, _BACKOFF_CAP)
            logger.warning(
                "go2rtc.caido",
                codigo_salida=codigo,
                vivio_s=round(vivio, 1),
                reintenta_en_s=round(espera, 1),
            )
            await asyncio.sleep(espera)
            backoff = min(backoff * 2, _BACKOFF_CAP)

    async def _correr_una_vez(self, ruta: str) -> int | None:
        self._proc = await asyncio.create_subprocess_exec(
            ruta,
            "-config",
            str(self._ruta_config),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        logger.info("go2rtc.arrancado", pid=self._proc.pid)

        lector = asyncio.create_task(self._leer_salida(self._proc))
        preparacion = asyncio.create_task(self._avisar_cuando_este_listo())
        try:
            codigo = await self._proc.wait()
        finally:
            preparacion.cancel()
            await asyncio.gather(lector, preparacion, return_exceptions=True)
            self._proc = None
        return codigo

    async def _leer_salida(self, proc: asyncio.subprocess.Process) -> None:
        """
        Vuelca lo que dice go2rtc al registro del Mirror, SIN credenciales.

        Hay que leerlo aunque no interese: si nadie vacía el caño, cuando se
        llena go2rtc se bloquea escribiendo y deja de servir video.
        """
        if proc.stdout is None:
            return
        async for crudo in proc.stdout:
            linea = redactar(crudo.decode("utf-8", errors="replace").rstrip())
            if linea:
                logger.warning("go2rtc.salida", linea=linea[:500])

    async def _avisar_cuando_este_listo(self) -> None:
        if not await self.esperar_listo():
            logger.error(
                "go2rtc.no_contesto",
                espera_s=_ESPERA_LISTO_S,
                msg="go2rtc arrancó pero su API no contesta. Las cámaras no van a cargar.",
            )
            return
        logger.info("go2rtc.listo", streams=len(self._streams))
        if self.al_estar_listo is not None:
            try:
                await self.al_estar_listo()
            except Exception as exc:  # noqa: BLE001 — restaurar cámaras no puede tumbar el supervisor
                logger.warning("go2rtc.al_estar_listo_fallo", error=redactar(str(exc))[:200])

    async def esperar_listo(self, espera_s: float = _ESPERA_LISTO_S) -> bool:
        """True cuando la API contesta CON la clave. Nunca lanza."""
        auth = aiohttp.BasicAuth(self.credenciales.usuario, self.credenciales.clave)
        limite = asyncio.get_running_loop().time() + espera_s
        timeout = aiohttp.ClientTimeout(total=3)
        async with aiohttp.ClientSession(timeout=timeout) as sesion:
            while asyncio.get_running_loop().time() < limite:
                try:
                    async with sesion.get(f"{self.base_url}/api/streams", auth=auth) as r:
                        if r.status == 200:
                            return True
                except (aiohttp.ClientError, TimeoutError):
                    pass
                await asyncio.sleep(0.5)
        return False

    async def _terminar(self) -> None:
        proc = self._proc
        if proc is None or proc.returncode is not None:
            return
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except TimeoutError:
            logger.warning("go2rtc.no_cerro_solo", msg="Enviando SIGKILL.")
            proc.kill()
            await proc.wait()
        logger.info("go2rtc.detenido")

    async def close(self) -> None:
        await self._terminar()
