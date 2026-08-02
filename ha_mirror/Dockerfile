# UniquexCR Mirror — add-on de Home Assistant
# Base Debian slim multi-arch (amd64/aarch64): todas las dependencias tienen
# wheels manylinux → sin compilación en la caja, build rápido y confiable.
#
# Imagen base FIJADA POR DIGEST (0.21.0 / hallazgo B6 de la auditoría).
# El digest es el del índice OCI multi-arch, así que sirve igual para amd64 y
# arm64 — Docker elige el manifiesto de la plataforma donde se compile.
# Fijarlo también congela la versión de pip que trae la imagen: por eso más
# abajo NO se hace `pip install --upgrade pip`, que reintroduciría deriva.
#   python:3.12-slim-bookworm  →  3.12.13, publicada 2026-07-14
# Para actualizarla a propósito:
#   docker buildx imagetools inspect python:3.12-slim-bookworm
FROM python:3.12-slim-bookworm@sha256:d50fb7611f86d04a3b0471b46d7557818d88983fc3136726336b2a4c657aa30b

# tini para manejo correcto de señales (init: false en config.yaml)
RUN apt-get update \
 && apt-get install -y --no-install-recommends tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Código del Mirror. Se "vendoriza" con package.ps1 antes de publicar el repo:
#   copia build/06_fastapi_mirror/{pyproject.toml,src} → ha_mirror/mirror_src/
COPY mirror_src/pyproject.toml /app/pyproject.toml
COPY mirror_src/requirements.txt /app/requirements.txt
COPY mirror_src/src /app/src

# Dependencias FIJADAS (0.21.0 / hallazgo B6).
#
# Antes esto era `pip install .`, que resolvía los `>=` del pyproject EN CADA
# BUILD. Como el add-on se compila en la caja de cada cliente, dos casas
# instaladas en meses distintos terminaban con árboles de dependencias
# distintos corriendo el mismo tag — y una reinstalación futura podía traer
# una versión mayor sin que nadie tocara una línea de código.
#
# Caso concreto que esto evita: aiohttp deprecó el parámetro `auth=` (que el
# Mirror usa contra go2rtc en camera_media.py y api/camera_ws.py) y lo elimina
# en la v4. Con `aiohttp>=3.10` suelto, un rebuild futuro rompía las cámaras.
#
# requirements.txt se regenera a propósito con:
#   uv pip compile mirror_src/pyproject.toml --universal --python-version 3.12 \
#     -o mirror_src/requirements.txt
RUN pip install --no-cache-dir --disable-pip-version-check -r /app/requirements.txt \
 && pip install --no-cache-dir --no-deps .

# Entrypoint del add-on
COPY run.sh /run.sh
RUN chmod a+x /run.sh

ENTRYPOINT [ "/usr/bin/tini", "--" ]
CMD [ "/run.sh" ]
