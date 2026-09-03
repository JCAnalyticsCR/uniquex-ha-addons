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
 && apt-get install -y --no-install-recommends tini curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# cloudflared — el tunel que vuelve alcanzable la casa desde internet.
#
# SOLO lo usa el MODO FABRICA (`platform_base_url` con valor): la caja recibe su
# token al activarse y `tunnel_client.py` levanta este binario. Una casa
# artesanal NUNCA lo ejecuta — su tunel lo maneja el add-on Cloudflared aparte.
# Viaja en la imagen para que una caja de producto no dependa de que alguien
# instale nada en el momento de la activacion, que es justo lo que el flujo del
# QR viene a eliminar.
#
# POR QUE SE DESCARGA ASI Y NO CON `curl | sh`: esto corre dentro de la casa de
# un cliente. Un binario que se baja sin verificar es una puerta abierta a que
# un mirror comprometido o un DNS envenenado metan otra cosa. Por eso version
# FIJADA + checksum VERIFICADO: si GitHub sirviera un binario distinto bajo el
# mismo tag, el build FALLA en vez de instalarlo.
#
# PARA ACTUALIZAR: cambiar CLOUDFLARED_VERSION, bajar los dos binarios, calcular
# su sha256sum y reemplazar los dos ARG. Los tres se cambian juntos o falla.
#
# LAS BARRAS `\` DE CONTINUACION SON CRITICAS. En la primera instalacion real
# este bloque llego con las continuaciones perdidas por el script de
# empaquetado: cada linea paso a ser una instruccion suelta y el build murio con
# `unknown instruction: ARCH="$(DPKG`. Se ve como "la instalacion falla a mitad"
# y cuesta un rato entenderlo. `verificar_paquete.py` ahora lo chequea antes de
# publicar.
# ---------------------------------------------------------------------------
ARG CLOUDFLARED_VERSION=2026.8.2
ARG CLOUDFLARED_SHA256_AMD64=fcfb02b575a52ca1af2e3267af4e1517bcdeb30ac48c834c69abaed3c0576ad2
ARG CLOUDFLARED_SHA256_ARM64=7747d94570fb390cf47dcb4f9555c193c6355cda9793f0d878d9049e5d6a7790

RUN set -eu; \
    arch="$(dpkg --print-architecture)"; \
    case "${arch}" in \
      amd64) sha="${CLOUDFLARED_SHA256_AMD64}" ;; \
      arm64) sha="${CLOUDFLARED_SHA256_ARM64}" ;; \
      *) echo "cloudflared: arquitectura no soportada: ${arch}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL --retry 3 -o /usr/local/bin/cloudflared \
      "https://github.com/cloudflare/cloudflared/releases/download/${CLOUDFLARED_VERSION}/cloudflared-linux-${arch}"; \
    echo "${sha}  /usr/local/bin/cloudflared" | sha256sum -c -; \
    chmod +x /usr/local/bin/cloudflared; \
    cloudflared --version; \
    apt-get purge -y --auto-remove curl; \
    rm -rf /var/lib/apt/lists/*

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
