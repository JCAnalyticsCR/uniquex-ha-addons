"""
Verifica que una imagen YA PUBLICADA del add-on realmente arranque.

Se corre en CI, despues de publicar:

    python3 verificar_imagen.py ghcr.io/.../uniquex-mirror-amd64:0.27.0 linux/amd64

POR QUE EXISTE (aparte de verificar_paquete.py)
----------------------------------------------
`verificar_paquete.py` revisa los ARCHIVOS EN DISCO antes de publicar. Esto
revisa lo que quedo ADENTRO DE LA IMAGEN, que es lo unico que llega a la caja
del cliente.

La diferencia no es teorica. Los dos defectos que rompieron la primera
instalacion en hardware real —CRLF en run.sh y continuaciones rotas en el
Dockerfile— producian imagenes que se CONSTRUIAN BIEN y morian al arrancar. Un
build verde no prueba que el contenedor viva.

Desde la 0.27.0 las cajas no construyen nada: bajan esta imagen. Si sale rota,
sale rota para toda la flota a la vez.

POR QUE ESTA EN PYTHON Y NO EN EL YAML
--------------------------------------
El chequeo central busca un retorno de carro. Escribirlo dentro del workflow
significa escribir un `\\r` que varias capas de comillas (YAML, bash, docker,
sh) pueden convertir en un retorno de carro DE VERDAD dentro del archivo — o
sea, meter el defecto que se esta buscando. Aca es un byte, `b"\\r"`, y no pasa
por ninguna comilla.

No necesita Docker Desktop: corre en el runner de GitHub.
"""

from __future__ import annotations

import subprocess
import sys

# El shebang y el resto de run.sh tienen que terminar en LF. Con CRLF el kernel
# busca un interprete llamado "/usr/bin/bashi\r" y el contenedor muere con
# "bad interpreter" apenas arranca.
RETORNO_DE_CARRO = b"\r"


class Roto(Exception):
    """Un problema que hace que la imagen no sirva. Corta la publicacion."""


def _correr(args: list[str], *, binario: bool = False):
    r = subprocess.run(args, capture_output=True)
    if r.returncode != 0:
        salida = (r.stderr or r.stdout).decode("utf-8", "replace").strip()
        raise Roto(f"fallo `{' '.join(args[:6])} ...`:\n{salida}")
    return r.stdout if binario else r.stdout.decode("utf-8", "replace")


def _en_la_imagen(imagen: str, plataforma: str, entrypoint: str, *args: str, binario=False):
    return _correr(
        ["docker", "run", "--rm", "--platform", plataforma,
         "--entrypoint", entrypoint, imagen, *args],
        binario=binario,
    )


def verificar(imagen: str, plataforma: str) -> list[str]:
    ok: list[str] = []

    print(f"Bajando {imagen} ({plataforma})...", flush=True)
    _correr(["docker", "pull", "--platform", plataforma, imagen])

    # --- 1. run.sh, leido como bytes crudos desde adentro de la imagen ---
    print("Revisando /run.sh...", flush=True)
    contenido = _en_la_imagen(imagen, plataforma, "/bin/cat", "/run.sh", binario=True)

    if not contenido:
        raise Roto("/run.sh esta vacio o no existe dentro de la imagen")

    if RETORNO_DE_CARRO in contenido:
        cuantos = contenido.count(RETORNO_DE_CARRO)
        raise Roto(
            f"/run.sh tiene {cuantos} retorno(s) de carro (CRLF).\n"
            "El contenedor va a arrancar y morir con 'bad interpreter'.\n"
            "Es exactamente el defecto que rompio la instalacion del 2 de septiembre."
        )
    ok.append("run.sh sin CRLF")

    primera = contenido.split(b"\n", 1)[0]
    if not primera.startswith(b"#!"):
        raise Roto(f"/run.sh no empieza con shebang. Primera linea: {primera[:60]!r}")
    ok.append(f"shebang {primera.decode('utf-8', 'replace')}")

    # `sh -n` parsea sin ejecutar: detecta continuaciones rotas y comillas sin cerrar.
    _en_la_imagen(imagen, plataforma, "/bin/sh", "-n", "/run.sh")
    ok.append("run.sh parsea (sh -n)")

    # --- 2. Las piezas sin las que el arranque no llega a ninguna parte ---
    print("Revisando las piezas del arranque...", flush=True)
    faltan = _en_la_imagen(
        imagen, plataforma, "/bin/sh", "-c",
        # Un solo docker run: cada uno bajo QEMU cuesta segundos.
        "for f in /run.sh /usr/bin/tini /usr/local/bin/cloudflared /usr/bin/cloudflared; do "
        "  [ -e \"$f\" ] && { [ -x \"$f\" ] || echo \"NO-EJECUTABLE:$f\"; } ; "
        "done; "
        "[ -x /run.sh ] || echo FALTA:/run.sh; "
        "[ -x /usr/bin/tini ] || echo FALTA:/usr/bin/tini; "
        "command -v cloudflared >/dev/null || echo FALTA:cloudflared",
    ).strip()
    if faltan:
        raise Roto(
            "faltan piezas del arranque:\n  " + "\n  ".join(faltan.splitlines()) +
            "\n(tini es el ENTRYPOINT; sin cloudflared el tunel no levanta)"
        )
    ok.append("run.sh ejecutable, tini y cloudflared presentes")

    version_cf = _en_la_imagen(imagen, plataforma, "/bin/sh", "-c",
                               "cloudflared --version 2>&1 | head -1").strip()
    ok.append(f"cloudflared responde: {version_cf}")

    # --- 3. El Mirror importa de verdad ---
    print("Revisando que el Mirror importe...", flush=True)
    _en_la_imagen(
        imagen, plataforma, "/usr/bin/env", "python3", "-c",
        "import ha_mirror; from ha_mirror.main import create_app, create_sticker_app",
    )
    # Las dos apps son separadas a proposito: la calcomania vive en su propia app
    # (puerto 8001, solo ingress) para que no se pueda leer desde la red de la casa.
    ok.append("ha_mirror importa; create_app y create_sticker_app existen")

    return ok


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        print("Uso: verificar_imagen.py <imagen> <plataforma>")
        return 2

    imagen, plataforma = sys.argv[1], sys.argv[2]
    print(f"=== Verificando {imagen} ===\n")

    try:
        ok = verificar(imagen, plataforma)
    except Roto as e:
        print(f"\nIMAGEN ROTA\n\n{e}\n")
        print("NO publicar esta version. Una imagen rota rompe la flota entera,")
        print("porque desde la 0.27.0 todas las cajas bajan la misma.")
        return 1
    except FileNotFoundError:
        print("\nNo encontre el comando `docker`. Este script corre en CI.")
        return 1

    print("\n=== La imagen arranca ===")
    for linea in ok:
        print(f"  OK  {linea}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
