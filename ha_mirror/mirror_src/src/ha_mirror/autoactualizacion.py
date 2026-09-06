"""
La caja se mantiene sola.

── QUÉ PROBLEMA RESUELVE ────────────────────────────────────────────────────
Una caja instalada hace un año tiene que tener las funciones de hoy sin que
nadie viaje a la casa. El interruptor que lo consigue es `auto_update` del
Supervisor — pero vive en la INSTALACIÓN, no en el `config.yaml` del add-on:
una caja recién quemada nace con él apagado, y encenderlo a mano es un paso
que se olvida. Por eso lo enciende el propio Mirror, en cada arranque.

── POR QUÉ ALCANZA CON EL PERMISO MÁS BAJO ──────────────────────────────────
El Supervisor tiene un bypass explícito para que un add-on toque LO SUYO::

    /addons/self/(?!security|update)[^/]+

Con `hassio_api: true` y `hassio_role: default` el Mirror puede escribir sus
propias opciones y **nada más**. No puede forzarse una actualización (`update`
está excluido del bypass), no puede tocar su seguridad, y no puede ver ni
modificar otros add-ons: eso exige el rol `manager`, que a propósito NO se
pide. `ROLE_DEFAULT` por sí solo apenas permite leer `/.+/info`.

Esto importa porque el Mirror está expuesto a internet por el túnel. Darle rol
`manager` le daría a un atacante que lo comprometa la capacidad de instalar
add-ons y reiniciar la máquina. El bypass de `self` no.

── 🔪 NUNCA ES FATAL ────────────────────────────────────────────────────────
Si esto falla, el Mirror arranca igual y no se entera el cliente. Su trabajo es
servirle la casa; mantenerse al día es deseable, no un requisito para
funcionar. Fuera de HA (desarrollo, pruebas) no hay Supervisor y la función
simplemente no hace nada.

── 🔪 SE VERIFICA LEYENDO DE VUELTA ─────────────────────────────────────────
Que el POST conteste `result: ok` NO prueba que el valor quedó puesto. Se
relee siempre. Esta lección salió cara el 2026-09-05: dos vigías distintos
dieron éxito sin que hubiera pasado nada, porque preguntaban "¿contestó?" en
vez de "¿cambió a lo que espero?".
"""

from __future__ import annotations

import os
from typing import Any, Final

import aiohttp
import structlog

logger = structlog.get_logger(__name__)

URL_SUPERVISOR: Final = "http://supervisor"
INFO_PROPIA: Final = "/addons/self/info"
OPCIONES_PROPIAS: Final = "/addons/self/options"

# Corto a propósito: esto corre en el arranque y no puede demorar el momento en
# que la casa queda disponible. Si el Supervisor no contesta en 10 s, se deja.
ESPERA: Final = aiohttp.ClientTimeout(total=10)


def _token() -> str | None:
    """El Supervisor lo inyecta en el entorno del add-on. Afuera no existe."""
    return os.environ.get("SUPERVISOR_TOKEN")


async def _leer_auto_update(sesion: aiohttp.ClientSession, cab: dict[str, str]) -> bool | None:
    async with sesion.get(URL_SUPERVISOR + INFO_PROPIA, headers=cab, timeout=ESPERA) as r:
        if r.status != 200:
            logger.warning("autoactualizacion.info_rechazada", status=r.status)
            return None
        cuerpo: dict[str, Any] = await r.json()
    datos = cuerpo.get("data", cuerpo)
    valor = datos.get("auto_update")
    return valor if isinstance(valor, bool) else None


async def asegurar_auto_update() -> bool | None:
    """
    Deja `auto_update` encendido en esta instalación.

    Devuelve True si quedó encendido (ya lo estaba o se encendió), False si se
    intentó y no quedó, y None si acá no hay Supervisor o no se pudo saber —
    que no es un fallo, es "esto no corre como add-on".
    """
    token = _token()
    if not token:
        logger.debug("autoactualizacion.sin_supervisor")
        return None

    cab = {"Authorization": f"Bearer {token}"}
    try:
        async with aiohttp.ClientSession() as sesion:
            actual = await _leer_auto_update(sesion, cab)
            if actual is None:
                return None
            if actual:
                logger.info("autoactualizacion.ya_encendida")
                return True

            async with sesion.post(
                URL_SUPERVISOR + OPCIONES_PROPIAS,
                headers=cab,
                json={"auto_update": True},
                timeout=ESPERA,
            ) as r:
                if r.status != 200:
                    logger.warning("autoactualizacion.no_se_pudo", status=r.status)
                    return False

            # No se le cree al POST: se relee.
            quedo = await _leer_auto_update(sesion, cab)
            if quedo:
                logger.info("autoactualizacion.encendida")
                return True
            logger.warning("autoactualizacion.contesto_ok_pero_no_quedo", releido=quedo)
            return False

    except Exception as e:  # noqa: BLE001 — jamás debe tumbar el arranque
        logger.warning("autoactualizacion.fallo", error=str(e), tipo=type(e).__name__)
        return None
