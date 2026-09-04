"""
Supervisor de cloudflared — levanta el túnel de esta casa y lo mantiene vivo.

QUÉ RESUELVE
------------
Sin esto, la casa queda registrada pero incomunicada: la plataforma le entregó
su credencial de túnel y nadie la usa. Este módulo es el último eslabón entre
"la caja se activó" y "la familia puede ver sus cámaras desde afuera".

POR QUÉ UN PROCESO Y NO UNA BIBLIOTECA
--------------------------------------
cloudflared es un binario de Go. Se corre como subproceso y se supervisa, igual
que el Mirror ya supervisa su conexión a Home Assistant.

TÚNEL "REMOTELY-MANAGED"
------------------------
`cloudflared tunnel run --token <token>` y nada más: las reglas de ruteo viven
del lado de Cloudflare, puestas por el backend al activar. La caja no decide a
dónde va el tráfico ni guarda un archivo de configuración que pueda quedar
desincronizado. Si mañana hay que cambiar el puerto de destino de una casa, se
hace desde la plataforma sin tocar el hardware del cliente.

EL TOKEN NUNCA VA EN LA LÍNEA DE COMANDOS
-----------------------------------------
Se pasa por la variable de entorno `TUNNEL_TOKEN`, que cloudflared lee. Un
argumento de línea de comandos es visible para cualquier proceso del sistema con
un simple `ps`, y queda en los logs de auditoría del kernel. Una variable de
entorno de un hijo es bastante más difícil de leer desde afuera.

SI FALLA, EL MIRROR SIGUE
-------------------------
Mismo criterio que el resto del aprovisionamiento: esta caja le da luces y
cámaras a una familia por la red local. Que el acceso remoto no levante es un
problema; que la casa entera se caiga por eso sería mucho peor.
"""

from __future__ import annotations

import asyncio
import os
import random
import shutil
from pathlib import Path

import structlog

from ha_mirror.device_identity import cargar_token_tunel

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# Reintento ante caídas. Cloudflared ya reconecta solo ante cortes de red; que
# el PROCESO muera es otra cosa (OOM, un bug, una señal) y ahí entra esto.
_BACKOFF_BASE = 5.0
_BACKOFF_CAP = 300.0
_BACKOFF_JITTER = 0.5

# Cuánto tiene que sobrevivir el proceso para considerar la sesión sana y
# reiniciar el backoff. Sin esto, un cloudflared que arranca y muere en dos
# segundos reintentaría cada 5s para siempre sin que el backoff crezca nunca.
_SESION_SANA_S = 60.0

# Cada cuánto se revisa si ya llegó el token, mientras todavía no hay ninguno.
# La caja se anuncia cada 120s, así que el token puede aparecer en cualquier
# momento después de que alguien escanee el QR.
_ESPERA_TOKEN_S = 20.0


class TunnelClient:
    """Corre `cloudflared tunnel run` y lo reinicia si se cae."""

    def __init__(self, token_path: Path, binario: str = "cloudflared") -> None:
        self._token_path = token_path
        self._binario = binario
        self._proc: asyncio.subprocess.Process | None = None
        # Se levanta cuando alguien baja el túnel a propósito (des-emparejamiento).
        # Sin esta distinción, la salida del proceso se leería como una caída:
        # warning de alarma y backoff creciente para algo que hicimos nosotros.
        self._parada_intencional = False

    async def detener_por_desemparejo(self) -> None:
        """
        Baja el túnel porque esta caja dejó de tener casa.

        No termina el supervisor: el loop sigue vivo sondeando, así que cuando la
        caja se vuelva a activar y llegue un token nuevo, el túnel levanta solo.
        Matar el task obligaría a reiniciar el add-on para volver a tener acceso
        remoto — exactamente el trabajo manual que este arreglo elimina.
        """
        self._parada_intencional = True
        await self._terminar()

    async def run_forever(self) -> None:
        """
        Espera a que exista el token, levanta el túnel y lo supervisa.

        No termina nunca por su cuenta: si el token todavía no llegó, sondea; si
        el proceso muere, reintenta con backoff. Solo sale por cancelación.
        """
        ruta = shutil.which(self._binario)
        if ruta is None:
            # No es fatal: el Mirror funciona igual en la red local. Pero hay que
            # gritarlo, porque significa que esta caja NO va a ser alcanzable.
            logger.error(
                "tunnel.binario_ausente",
                binario=self._binario,
                msg=(
                    "cloudflared no está en la imagen. La casa va a funcionar en "
                    "la red local pero no desde afuera."
                ),
            )
            return

        backoff = _BACKOFF_BASE
        while True:
            token = cargar_token_tunel(self._token_path)
            if token is None:
                # Todavía sin activar. Es el estado normal de una caja recién
                # salida del taller, así que no se loguea cada vez.
                await asyncio.sleep(_ESPERA_TOKEN_S)
                continue

            arrancado = asyncio.get_running_loop().time()
            try:
                codigo = await self._correr_una_vez(ruta, token)
            except asyncio.CancelledError:
                await self._terminar()
                raise

            vivio = asyncio.get_running_loop().time() - arrancado

            if self._parada_intencional:
                # Lo bajamos nosotros. Ni warning ni backoff: se vuelve al
                # sondeo del token, que ya no está, y el loop queda esperando
                # tranquilo a que una activación nueva entregue otro.
                self._parada_intencional = False
                backoff = _BACKOFF_BASE
                logger.info(
                    "tunnel.detenido_por_desemparejo",
                    vivio_s=round(vivio, 1),
                    msg="La caja dejó de tener casa. Esperando una activación nueva.",
                )
                continue

            if vivio >= _SESION_SANA_S:
                # Estuvo arriba un rato: el problema fue puntual, no de config.
                backoff = _BACKOFF_BASE

            jitter = 1.0 + _BACKOFF_JITTER * (2 * random.random() - 1)
            espera = min(backoff * jitter, _BACKOFF_CAP)
            logger.warning(
                "tunnel.caido",
                codigo_salida=codigo,
                vivio_s=round(vivio, 1),
                reintenta_en_s=round(espera, 1),
            )
            await asyncio.sleep(espera)
            backoff = min(backoff * 2, _BACKOFF_CAP)

    async def _correr_una_vez(self, ruta: str, token: str) -> int | None:
        """Lanza cloudflared y espera a que termine. Devuelve su código de salida."""
        entorno = os.environ.copy()
        # Por entorno y NO como argumento: un argumento lo ve cualquiera con `ps`.
        entorno["TUNNEL_TOKEN"] = token

        self._proc = await asyncio.create_subprocess_exec(
            ruta,
            "--no-autoupdate",  # la versión la fija el Dockerfile, no el binario
            "tunnel",
            "run",
            env=entorno,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        logger.info("tunnel.arrancado", pid=self._proc.pid)
        codigo = await self._proc.wait()
        self._proc = None
        return codigo

    async def _terminar(self) -> None:
        """Baja cloudflared con SIGTERM y, si se resiste, lo mata."""
        proc = self._proc
        if proc is None or proc.returncode is not None:
            return
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except TimeoutError:
            logger.warning("tunnel.no_cerro_solo", msg="Enviando SIGKILL.")
            proc.kill()
            await proc.wait()
        logger.info("tunnel.detenido")

    async def close(self) -> None:
        await self._terminar()
