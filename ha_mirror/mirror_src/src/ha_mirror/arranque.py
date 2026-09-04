"""
Arranque del Mirror: UN proceso, hasta DOS servidores.

    python3 -m ha_mirror.arranque

POR QUÉ EXISTE ESTE MÓDULO
--------------------------
Hasta la 0.28.0, `run.sh` levantaba **dos procesos de uvicorn**: uno para la app
principal en el 8000 y otro para la calcomanía en el 8001. Eso rompía la
calcomanía de una forma que no daba ningún error.

Solo `create_app()` tiene `lifespan=`. El lifespan es lo que puebla
`app.state` — la identidad de la caja, la base, el store. El proceso del 8001
servía `sticker_app`, que no tiene lifespan, así que **su `app.state` no lo
poblaba nadie, nunca**. La calcomanía leía `getattr(state, "device_identity",
None)`, encontraba `None` y respondía, con un 200 impecable, "La caja todavía no
tiene identidad" — aunque la identidad existiera perfectamente en el otro
proceso.

La firma del defecto quedó en el log de la instalación real: `mirror.starting`
una sola vez y el warning del middleware de CSP **dos** veces. El módulo se
importaba en los dos procesos (por eso el middleware se construía dos veces),
pero el lifespan corría en uno solo.

POR QUÉ NO SE ARREGLÓ DÁNDOLE UN LIFESPAN A LA CALCOMANÍA
---------------------------------------------------------
Porque seguirían siendo dos procesos: se abriría una **segunda** conexión a la
SQLite y una **segunda** sesión WebSocket contra Home Assistant, solo para poder
contar dispositivos en una pantalla. Y dejaría dos caminos de arranque que hay
que mantener sincronizados a mano — que es exactamente el tipo de duplicación
que produjo el otro defecto de la 0.28.0.

Un solo proceso con los dos servidores en el mismo event loop conserva la
propiedad de seguridad que motivó la separación (el 8001 sigue sin publicarse al
host), corre un solo lifespan y no duplica ninguna conexión.
"""

from __future__ import annotations

import asyncio
import os
import sys

import structlog
import uvicorn

logger = structlog.get_logger(__name__)

PUERTO_API = 8000
PUERTO_CALCOMANIA = 8001


def _construir() -> list[uvicorn.Server]:
    """Arma los servidores que corresponden según el modo de la caja."""
    # Se importan las instancias que ya crea main.py al importarse, en vez de
    # construir otras: si se llamara create_app() de nuevo, habría dos apps
    # distintas y `uvicorn ha_mirror.main:app` dejaría de ser la misma cosa.
    from ha_mirror.config import get_settings
    from ha_mirror.main import app, sticker_app

    settings = get_settings()
    nivel = os.environ.get("LOG_LEVEL", "info").lower()

    servidores = [
        uvicorn.Server(
            uvicorn.Config(app, host="0.0.0.0", port=PUERTO_API, log_level=nivel)
        )
    ]

    if settings.modo_fabrica:
        # 🔪 EL PUNTO DE TODO ESTE MÓDULO.
        #
        # Las dos apps comparten el MISMO objeto de estado. No es una copia: es
        # la misma referencia, así que cuando el lifespan de `app` le escriba la
        # identidad, la calcomanía la ve.
        #
        # Esto depende de que el lifespan MUTE el estado (`app.state.x = ...`) y
        # no lo REASIGNE (`app.state = State()`). Una reasignación rompería el
        # vínculo en silencio y volveríamos a la calcomanía vacía. Hay un test
        # que corre el lifespan de verdad y comprueba que la calcomanía ve la
        # identidad: test_dos_servidores_un_estado.py.
        sticker_app.state = app.state

        servidores.append(
            uvicorn.Server(
                uvicorn.Config(
                    sticker_app,
                    host="0.0.0.0",
                    port=PUERTO_CALCOMANIA,
                    log_level=nivel,
                )
            )
        )
        logger.info(
            "arranque.dos_servidores",
            api=PUERTO_API,
            calcomania=PUERTO_CALCOMANIA,
            msg="Calcomania en :8001 (no se publica al host: solo ingress).",
        )
    else:
        logger.info(
            "arranque.un_servidor",
            api=PUERTO_API,
            msg="Modo artesanal: sin calcomania.",
        )

    return servidores


async def _servir(servidores: list[uvicorn.Server]) -> int:
    """
    Corre los servidores hasta que UNO se caiga, y ahí termina el proceso.

    No se usa `gather`: con gather, si el servidor de la calcomanía muere, el
    proceso sigue vivo sirviendo la API y nadie se entera de que la activación
    dejó de existir. Un add-on a medias es peor que uno caído — el Supervisor
    reinicia lo que muere, pero no puede reiniciar lo que no sabe que falló.
    """
    tareas = [asyncio.create_task(s.serve()) for s in servidores]
    listas, pendientes = await asyncio.wait(tareas, return_when=asyncio.FIRST_COMPLETED)

    for s in servidores:
        s.should_exit = True
    for t in pendientes:
        t.cancel()
    await asyncio.gather(*pendientes, return_exceptions=True)

    for t in listas:
        exc = t.exception()
        if exc is not None:
            logger.error("arranque.servidor_murio", exc=str(exc))
            return 1

    # Salida limpia de un servidor con el otro todavía arriba: tampoco es normal.
    if pendientes:
        logger.error(
            "arranque.servidor_termino_solo",
            msg="Un servidor terminó y el otro seguía arriba. Se corta el proceso.",
        )
        return 1
    return 0


def main() -> int:
    return asyncio.run(_servir(_construir()))


if __name__ == "__main__":
    sys.exit(main())
