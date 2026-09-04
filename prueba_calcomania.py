"""
Se ejecuta DENTRO de la imagen: pide la calcomanía y mira lo que devuelve.

Lo monta `verificar_imagen.py` en el contenedor. No usa TestClient porque httpx
no viaja en la imagen — habla ASGI a mano, que además es exactamente lo que hace
uvicorn.

POR QUÉ ESTA PRUEBA
-------------------
La 0.28.0 salió con la calcomanía rota y CI la dio por buena. Los chequeos que
había eran "la app se construye" y "los endpoints existen por nombre", y los dos
pasaban: la app se construía perfecto y `/` existía. Lo que estaba roto era lo
que `/` CONTESTABA.

El detalle que hace inútil el chequeo obvio: la página de fallo responde **200**.
Un `assert status == 200` se pone verde contra el bug. Por eso acá hay dos patas:

  · POSITIVA — que aparezca el `<svg` del QR, que solo existe cuando funciona.
  · NEGATIVA — que NO aparezca el texto de "todavía no tiene identidad".

Es el espejo del error que ya pagamos con un assert de ausencia contra una lista
vacía: allá se verificaba contra la nada, acá se verificaría contra algo que
está siempre.
"""

from __future__ import annotations

import asyncio
import os
import sys

TEXTO_DE_FALLO = "todavía no tiene identidad"
CABECERA_INGRESS = (b"x-ingress-path", b"/api/hassio_ingress/prueba")


async def _lifespan(app):
    """Corre el arranque de la app por el protocolo ASGI, igual que uvicorn."""
    recibidos: asyncio.Queue = asyncio.Queue()
    await recibidos.put({"type": "lifespan.startup"})
    salida: list[dict] = []

    async def receive():
        return await recibidos.get()

    async def send(msg):
        salida.append(msg)

    tarea = asyncio.create_task(app({"type": "lifespan"}, receive, send))
    for _ in range(200):  # hasta 20 s
        if any(m["type"].startswith("lifespan.startup.") for m in salida):
            break
        await asyncio.sleep(0.1)
    else:
        raise SystemExit("el lifespan no termino de arrancar en 20 s")

    fallo = [m for m in salida if m["type"] == "lifespan.startup.failed"]
    if fallo:
        raise SystemExit(f"el lifespan fallo al arrancar: {fallo[0].get('message')}")
    return tarea, recibidos


async def _pedir(app, camino: str) -> tuple[int, str]:
    """Un GET por ASGI. Devuelve (status, cuerpo)."""
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.1"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": camino,
        "raw_path": camino.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"localhost"), CABECERA_INGRESS],
        # La calcomanía exige venir de la red del Supervisor o de loopback.
        "client": ("127.0.0.1", 50000),
        "server": ("localhost", 8001),
    }
    estado = {"status": 0, "cuerpo": b""}

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(msg):
        if msg["type"] == "http.response.start":
            estado["status"] = msg["status"]
        elif msg["type"] == "http.response.body":
            estado["cuerpo"] += msg.get("body", b"")

    await app(scope, receive, send)
    return estado["status"], estado["cuerpo"].decode("utf-8", "replace")


async def main() -> int:
    from ha_mirror.arranque import _construir
    from ha_mirror.main import app, sticker_app

    # Los mismos dos servidores que levantaría la caja.
    servidores = _construir()
    puertos = sorted(s.config.port for s in servidores)
    if puertos != [8000, 8001]:
        print(f"ROTO: en modo fabrica se esperaban los puertos 8000 y 8001, y hay {puertos}")
        return 1
    print(f"OK  levanta los dos servidores {puertos} en un solo proceso")

    # _construir() ya dejó el estado compartido. Verificarlo explícitamente:
    # si algún día alguien reasigna app.state en vez de mutarlo, se rompe acá y
    # no en la casa de un cliente.
    if sticker_app.state is not app.state:
        print("ROTO: la calcomania no comparte el estado con la app principal")
        return 1
    print("OK  las dos apps comparten el mismo app.state")

    tarea, _ = await _lifespan(app)
    try:
        identidad = getattr(app.state, "device_identity", None)
        if identidad is None:
            print("ROTO: el lifespan arranco pero no dejo identidad. La caja no puede activarse.")
            return 1
        print(f"OK  el lifespan genero la identidad ({identidad.device_id[:8]}...)")

        status, cuerpo = await _pedir(sticker_app, "/")
        if status != 200:
            print(f"ROTO: la calcomania devolvio {status}")
            return 1

        # Pata NEGATIVA: la que agarra el defecto de la 0.28.0.
        if TEXTO_DE_FALLO in cuerpo:
            print("ROTO: la calcomania dice que no hay identidad, pero SI hay.")
            print("      Es el defecto de la 0.28.0: el estado no llega a esa app.")
            return 1
        # Pata POSITIVA: algo que solo existe cuando de verdad funciona.
        if "<svg" not in cuerpo:
            print("ROTO: la calcomania no trae el QR. Sin QR no se activa nada.")
            print(f"      Primeros 300 caracteres: {cuerpo[:300]!r}")
            return 1

        print(f"OK  la calcomania renderiza el QR ({len(cuerpo)} bytes, sin el aviso de 'sin identidad')")
        return 0
    finally:
        tarea.cancel()


if __name__ == "__main__":
    faltan = [v for v in ("MIRROR_DB_PATH", "DEVICE_KEY_PATH") if not os.environ.get(v)]
    if faltan:
        print(f"falta configurar {faltan} para no escribir fuera de /tmp")
        sys.exit(2)
    sys.exit(asyncio.run(main()))
