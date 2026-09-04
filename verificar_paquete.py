#!/usr/bin/env python3
"""
Gate de empaquetado — UniquexCR Mirror add-on.

Corre ANTES de publicar el repositorio. Falla con código distinto de cero
si detecta algún defecto que haya roto una instalación real en el pasado.

Uso:
    python verificar_paquete.py
    python verificar_paquete.py --dir ruta/alternativa/ha_mirror

Historial de defectos que este script previene:
  1. run.sh llegó con CRLF  → "bad interpreter" al arrancar el contenedor.
     Causa raíz: el zip se arma desde disco (CRLF) en vez del repo git (LF).
  2. Dockerfile perdió barras de continuación  → "unknown instruction: ARCH=..."
     Causa raíz: edición manual que truncó la línea RUN multi-línea.
  3. config.yaml con version desincronizada de pyproject.toml → el Supervisor
     de HA no ofrece la actualización (o la ofrece con la versión equivocada).
"""

import sys
import re
import os
import tomllib
import argparse
from pathlib import Path

# Forzar UTF-8 en stdout para que los mensajes no fallen en consolas cp1252
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

try:
    import yaml
except ImportError:
    print("ERROR: falta pyyaml. Instalalo con:  pip install pyyaml")
    sys.exit(2)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def falla(mensaje: str, solucion: str = "") -> None:
    print(f"\n[FALLO] {mensaje}")
    if solucion:
        print(f"  Cómo arreglarlo: {solucion}")

# ---------------------------------------------------------------------------
# Verificaciones
# ---------------------------------------------------------------------------

def check_crlf_sh(addon_dir: Path) -> list[str]:
    """
    Ningún archivo que Linux vaya a interpretar debe tener CRLF.

    En los `.sh` el CRLF causa 'bad interpreter' y mata el contenedor al
    arrancar. Es el defecto que rompió la primera instalación en hardware real.

    El Dockerfile entra en la misma revisión aunque su parser tolere CRLF: el
    retorno de carro se cuela dentro de los valores (un ENV queda con un \\r
    invisible al final) y adentro de cualquier heredoc de un RUN, donde vuelve a
    ser 'bad interpreter' pero recién en tiempo de build. Es la misma familia de
    defecto y no hay ninguna razón para tenerlo.
    """
    errores = []
    candidatos = sorted(addon_dir.rglob("*.sh"))
    dockerfile = addon_dir / "Dockerfile"
    if dockerfile.exists():
        candidatos.append(dockerfile)

    for archivo in candidatos:
        data = archivo.read_bytes()
        count = data.count(b"\r\n")
        if count:
            errores.append(
                f"{archivo.relative_to(addon_dir)} tiene {count} líneas CRLF.\n"
                f"  Arreglo: python3 -c \""
                f"p='{archivo}'; open(p,'wb').write(open(p,'rb').read().replace(b'\\r\\n',b'\\n'))\""
            )
    return errores


def check_dockerfile_continuations(addon_dir: Path) -> list[str]:
    """
    Parsea el Dockerfile buscando instrucciones RUN multi-línea con
    continuaciones rotas.

    Una continuación rota se ve así (la barra está pero la línea siguiente
    empieza con un token que Dockerfile interpreta como instrucción nueva):

        RUN apt-get update \\
        ARCH="$(dpkg ...)"    ← Docker lo lee como instrucción ARCH=...

    Lo que verificamos:
    - Cada línea de un bloque RUN multi-línea que NO sea la última debe
      terminar en '\' (ignorando espacios/CRLF al final).
    - Si una línea dentro de un bloque RUN termina sin '\' pero hay más
      líneas del bloque, la continuación está rota.
    """
    dockerfile = addon_dir / "Dockerfile"
    if not dockerfile.exists():
        return [f"No se encontró Dockerfile en {addon_dir}"]

    errores = []
    lines = dockerfile.read_text(encoding="utf-8", errors="replace").splitlines()

    in_run = False
    run_start_lineno = 0
    accumulated: list[tuple[int, str]] = []  # (lineno, stripped_line)

    def check_block(block: list[tuple[int, str]], start: int) -> list[str]:
        """Verifica que cada línea excepto la última termine en backslash."""
        errs = []
        for i, (lineno, raw) in enumerate(block):
            # Quitar CRLF y espacios del final, luego verificar si termina en \
            stripped = raw.rstrip("\r\n").rstrip()
            is_last = (i == len(block) - 1)
            if not is_last and not stripped.endswith("\\"):
                errs.append(
                    f"Dockerfile línea {lineno}: continuación rota en bloque RUN "
                    f"que empieza en línea {start}.\n"
                    f"  Línea:      {raw.rstrip()!r}\n"
                    f"  Arreglo:    agregá '\\' al final de esa línea."
                )
        return errs

    for lineno, line in enumerate(lines, start=1):
        stripped = line.rstrip("\r\n").rstrip()

        if not in_run:
            # ¿Empieza un bloque RUN?
            if re.match(r"^\s*RUN\b", stripped):
                in_run = True
                run_start_lineno = lineno
                accumulated = [(lineno, line)]
                # Si la propia línea RUN NO termina en \, es un bloque de 1 línea
                if not stripped.endswith("\\"):
                    in_run = False
                    accumulated = []
        else:
            accumulated.append((lineno, line))
            if not stripped.endswith("\\"):
                # Fin del bloque multi-línea
                errores.extend(check_block(accumulated, run_start_lineno))
                in_run = False
                accumulated = []

    # Si el archivo termina estando dentro de un bloque (raro pero posible)
    if in_run and accumulated:
        errores.extend(check_block(accumulated, run_start_lineno))

    return errores


def check_yaml_version(addon_dir: Path) -> list[str]:
    """config.yaml debe ser YAML válido y su version debe coincidir con pyproject.toml."""
    errores = []

    config_path = addon_dir / "config.yaml"
    if not config_path.exists():
        return [f"No se encontró {config_path}"]

    # Validar YAML
    try:
        with config_path.open(encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        return [f"config.yaml no es YAML válido: {exc}\n  Arreglo: corregí la sintaxis del archivo."]

    yaml_version = str(config.get("version", "")).strip()
    if not yaml_version:
        errores.append("config.yaml no tiene campo 'version'.")

    # Buscar pyproject.toml
    pyproject_path = addon_dir / "mirror_src" / "pyproject.toml"
    if not pyproject_path.exists():
        errores.append(
            f"No se encontró {pyproject_path}.\n"
            f"  Arreglo: corré package.ps1 para vendorizar el código antes de verificar."
        )
        return errores

    with pyproject_path.open("rb") as f:
        pyproject = tomllib.load(f)

    toml_version = str(pyproject.get("project", {}).get("version", "")).strip()
    if not toml_version:
        errores.append("pyproject.toml no tiene [project].version.")
        return errores

    if yaml_version != toml_version:
        errores.append(
            f"Versión desincronizada:\n"
            f"  config.yaml   → {yaml_version}\n"
            f"  pyproject.toml → {toml_version}\n"
            f"  Arreglo: actualizá la que esté desactualizada para que coincidan."
        )

    return errores


def check_dockerfile_copies(addon_dir: Path) -> list[str]:
    """
    Verifica que cada archivo/directorio referenciado en COPY del Dockerfile
    exista en el contexto de build (el directorio del add-on).
    """
    dockerfile = addon_dir / "Dockerfile"
    if not dockerfile.exists():
        return []

    errores = []
    text = dockerfile.read_text(encoding="utf-8", errors="replace")

    # Extraer todas las instrucciones COPY (pueden estar en múltiples líneas)
    # Normalizar continuaciones primero
    text_norm = re.sub(r"\\\s*\n", " ", text)

    for lineno, line in enumerate(text_norm.splitlines(), start=1):
        m = re.match(r"^\s*COPY\s+(.+)", line)
        if not m:
            continue
        parts = m.group(1).split()
        if len(parts) < 2:
            continue
        # El último argumento es el destino; los anteriores son fuentes
        sources = parts[:-1]
        # Ignorar flags tipo --chown o --from
        sources = [s for s in sources if not s.startswith("--")]
        for src in sources:
            src_path = addon_dir / src
            if not src_path.exists():
                errores.append(
                    f"Dockerfile COPY: '{src}' no existe en {addon_dir}.\n"
                    f"  Arreglo: corré package.ps1 para copiar los fuentes antes de verificar,\n"
                    f"           o verificá que el nombre del archivo/directorio sea correcto."
                )

    return errores


def check_no_pycache(addon_dir: Path) -> list[str]:
    """No debe haber __pycache__ ni .pyc en el paquete."""
    errores = []

    pycache_dirs = list(addon_dir.rglob("__pycache__"))
    if pycache_dirs:
        lista = "\n".join(f"  - {p.relative_to(addon_dir)}" for p in pycache_dirs)
        errores.append(
            f"Se encontraron directorios __pycache__ (no deben ir en el paquete):\n{lista}\n"
            f"  Arreglo: eliminá esos directorios antes de publicar."
        )

    pyc_files = list(addon_dir.rglob("*.pyc"))
    if pyc_files:
        lista = "\n".join(f"  - {p.relative_to(addon_dir)}" for p in pyc_files)
        errores.append(
            f"Se encontraron archivos .pyc compilados (no deben ir en el paquete):\n{lista}\n"
            f"  Arreglo: borrá esos archivos antes de publicar."
        )

    return errores


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Gate de empaquetado — verifica el add-on antes de publicar."
    )
    parser.add_argument(
        "--dir",
        default=None,
        help="Directorio raíz del add-on (default: ha_mirror/ junto a este script).",
    )
    args = parser.parse_args()

    # Directorio del add-on: argumento > carpeta por defecto junto al script
    if args.dir:
        addon_dir = Path(args.dir).resolve()
    else:
        addon_dir = (Path(__file__).parent / "ha_mirror").resolve()

    if not addon_dir.exists():
        print(f"ERROR: directorio del add-on no encontrado: {addon_dir}")
        return 1

    print(f"Verificando paquete en: {addon_dir}\n")

    todos_errores: list[str] = []

    checks = [
        ("Saltos de linea en .sh y Dockerfile (CRLF)",  check_crlf_sh),
        ("Continuaciones del Dockerfile (barras rotas -> build muerto)", check_dockerfile_continuations),
        ("YAML valido + version sincronizada con pyproject.toml", check_yaml_version),
        ("Archivos referenciados por COPY en el Dockerfile", check_dockerfile_copies),
        ("Archivos de cache Python (__pycache__, .pyc)", check_no_pycache),
    ]

    for nombre, fn in checks:
        print(f"  [{nombre}]")
        errores = fn(addon_dir)
        if errores:
            for e in errores:
                falla(e)
            todos_errores.extend(errores)
        else:
            print("    OK")

    print()
    if todos_errores:
        print(f"RESULTADO: {len(todos_errores)} problema(s) encontrado(s). NO publicar hasta corregirlos.")
        return 1
    else:
        print("RESULTADO: todo OK — el paquete está listo para publicar.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
