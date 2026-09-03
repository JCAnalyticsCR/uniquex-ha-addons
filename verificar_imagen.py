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


# El Mirror construye su configuracion al importarse, asi que sin variables de
# entorno ni siquiera se puede importar. En una caja se las da run.sh desde las
# opciones del add-on; aca hay que darle valores de mentira.
#
# Son deliberadamente obvios: si alguno apareciera en un log de produccion, se
# reconoce al instante que esa caja arranco sin configurar.
ENTORNO_DE_MENTIRA = {
    # ha_url es un WebSocket, no HTTP: el Mirror lo valida y rechaza http://.
    "HA_URL": "ws://ha-que-no-existe:8123/api/websocket",
    "MIRROR_API_KEY": "valor-de-mentira-solo-para-la-prueba-de-ci-no-sirve-para-nada",
    "SESSION_SECRET": "valor-de-mentira-solo-para-la-prueba-de-ci-no-sirve-para-nada",
    "IFRAME_TOKEN_SECRET": "valor-de-mentira-solo-para-la-prueba-de-ci-no-sirve-para-nada",
    # Esta NO la exporta run.sh: la inyecta Home Assistant al arrancar el add-on.
    # Se usa el nombre real (SUPERVISOR_TOKEN, que el Mirror acepta como alias de
    # ha_token) y no HA_TOKEN, para que la prueba se parezca a una caja de verdad.
    # Sin ella el Mirror cae al modo standalone y exige archivos Fernet en disco.
    "SUPERVISOR_TOKEN": "valor-de-mentira-solo-para-la-prueba-de-ci-no-sirve-para-nada",
}


def _en_la_imagen(imagen: str, plataforma: str, entrypoint: str, *args: str,
                  binario=False, entorno: dict[str, str] | None = None):
    env: list[str] = []
    for clave, valor in (entorno or {}).items():
        env += ["-e", f"{clave}={valor}"]
    return _correr(
        ["docker", "run", "--rm", "--platform", plataforma, *env,
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

    # --- 3. El Mirror se construye de verdad ---
    #
    # No alcanza con importar: construir las apps es lo que recorre routers,
    # dependencias y modelos. Un import roto en un router que nadie toca no
    # aparece hasta que la caja arranca en la casa del cliente.
    print("Revisando que el Mirror se construya...", flush=True)
    # Se piden los ENDPOINTS del esquema OpenAPI, no `len(app.routes)`.
    #
    # Esa distincion costo una investigacion: `app.routes` cuenta lo que FastAPI
    # guarda adentro, y eso CAMBIA entre versiones. La imagen trae FastAPI
    # 0.141.1, que guarda un envoltorio por router incluido (13 objetos); una
    # version anterior guardaba las rutas ya aplanadas (37). O sea que contar
    # `app.routes` mide la version de FastAPI, no el Mirror. El esquema OpenAPI
    # es el contrato publico y no depende de como FastAPI acomode sus objetos.
    salida = _en_la_imagen(
        imagen, plataforma, "/usr/bin/env", "python3", "-c",
        "from ha_mirror.main import create_app, create_sticker_app; "
        "from ha_mirror.config import get_settings; "
        "import fastapi; "
        "a = create_app(); s = create_sticker_app(); "
        # El Mirror escribe sus propios logs al construirse, asi que cada dato va
        # marcado: se buscan las lineas, no se asume que sean las unicas.
        "print('FASTAPI', fastapi.__version__); "
        "print('MODO_FABRICA', get_settings().modo_fabrica); "
        "[print('API', p) for p in sorted(a.openapi()['paths'])]; "
        "[print('CALCO', p) for p in sorted(s.openapi()['paths'])]",
        entorno=ENTORNO_DE_MENTIRA,
    )

    def _marcadas(etiqueta: str) -> list[str]:
        pref = etiqueta + " "
        return [l[len(pref):] for l in salida.splitlines() if l.startswith(pref)]

    def _unica(etiqueta: str) -> str:
        v = _marcadas(etiqueta)
        if len(v) != 1:
            raise Roto(
                f"esperaba una linea {etiqueta} y encontre {len(v)}.\n"
                "Salida completa:\n" + salida.strip()
            )
        return v[0]

    version_fastapi = _unica("FASTAPI")
    modo_fabrica = _unica("MODO_FABRICA")
    endpoints_api = _marcadas("API")
    endpoints_calco = _marcadas("CALCO")

    print(f"  endpoints de la app principal ({len(endpoints_api)}):")
    for e in endpoints_api:
        print(f"    {e}")
    print(f"  endpoints de la calcomania ({len(endpoints_calco)}): "
          f"{', '.join(endpoints_calco) or 'ninguno'}")

    # Endpoints sin los cuales la app del cliente no funciona. Se nombran uno por
    # uno en vez de exigir "al menos N": un numero se cumple aunque falte
    # justamente el que importa.
    IMPRESCINDIBLES = [
        "/api/health",
        "/api/entities",
        "/api/areas",
        "/api/cameras",
        "/api/scenes",
        "/api/preferences/home-layout",
        "/api/ws-ticket",
        # Organizar dispositivos: lo que permite que el cliente no entre a HA.
        "/api/onboarding/capabilities",
        "/api/onboarding/rooms",
        "/api/onboarding/overrides",
    ]
    faltantes = [e for e in IMPRESCINDIBLES if e not in endpoints_api]
    if faltantes:
        raise Roto(
            "a la imagen le faltan endpoints imprescindibles:\n  " +
            "\n  ".join(faltantes) +
            "\nLa copia del Mirror que quedo adentro no es la que se espera."
        )
    ok.append(f"{len(endpoints_api)} endpoints, con los {len(IMPRESCINDIBLES)} "
              "imprescindibles presentes")

    # Las dos apps son separadas a proposito: la calcomania vive en el puerto
    # 8001, que no se publica al host, para que no se pueda leer desde la red de
    # la casa. Si alguna ruta de la API apareciera ahi, esa separacion se rompio.
    coladas = [e for e in endpoints_calco if e.startswith("/api/")]
    if coladas:
        raise Roto(
            "la app de la calcomania expone rutas de la API: " + ", ".join(coladas) +
            "\nEsa app existe justamente para NO exponer nada mas que la calcomania."
        )
    ok.append(f"la calcomania sigue aislada ({len(endpoints_calco)} endpoint/s, "
              "ninguno de la API)")

    if modo_fabrica != "False":
        raise Roto(
            "sin platform_base_url la caja tendria que arrancar en modo artesanal, "
            f"y modo_fabrica dio {modo_fabrica}. El interruptor maestro esta al reves."
        )
    ok.append("sin platform_base_url arranca en modo artesanal")

    # No es una condicion de fallo, pero conviene tenerlo escrito en el log: las
    # pruebas automaticas corren contra la version de FastAPI del entorno de
    # desarrollo, que puede no ser esta.
    ok.append(f"FastAPI dentro de la imagen: {version_fastapi}")

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
