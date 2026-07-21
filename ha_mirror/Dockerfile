# UniquexCR Mirror — add-on de Home Assistant
# Base Debian slim multi-arch (amd64/aarch64): todas las dependencias tienen
# wheels manylinux → sin compilación en la caja, build rápido y confiable.
FROM python:3.12-slim-bookworm

# tini para manejo correcto de señales (init: false en config.yaml)
RUN apt-get update \
 && apt-get install -y --no-install-recommends tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Código del Mirror. Se "vendoriza" con package.ps1 antes de publicar el repo:
#   copia build/06_fastapi_mirror/{pyproject.toml,src} → ha_mirror/mirror_src/
COPY mirror_src/pyproject.toml /app/pyproject.toml
COPY mirror_src/src /app/src

# Instala el Mirror + dependencias (fastapi, uvicorn, aiohttp, pydantic, etc.)
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir .

# Entrypoint del add-on
COPY run.sh /run.sh
RUN chmod a+x /run.sh

ENTRYPOINT [ "/usr/bin/tini", "--" ]
CMD [ "/run.sh" ]
