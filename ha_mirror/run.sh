#!/usr/bin/env bash
# UniquexCR Mirror — entrypoint del add-on de Home Assistant.
# Lee las opciones del add-on, conecta a HA LOCAL por el proxy del Supervisor
# (sin túnel, sin LLAT) y arranca el Mirror.
#
# NOTA: este archivo DEBE tener saltos de línea LF (no CRLF). Si el add-on falla
# con "bad interpreter" o "no such file", convertí el archivo a LF.
set -euo pipefail

OPT=/data/options.json

# Lee una opción del add-on desde /data/options.json (vacío si no existe)
opt() {
  python3 -c "import json,os,sys; p='$OPT'; print(json.load(open(p)).get('$1','') if os.path.exists(p) else '')"
}
gen_secret() { python3 -c "import secrets; print(secrets.token_hex(32))"; }

API_KEY="$(opt mirror_api_key)"
FRONTEND_ORIGIN="$(opt frontend_origin)"
PLATFORM_BASE_URL="$(opt platform_base_url)"
LOG_LEVEL="$(opt log_level)"; LOG_LEVEL="${LOG_LEVEL:-INFO}"
GO2RTC_BASE_URL="$(opt go2rtc_base_url)"
GO2RTC_USERNAME="$(opt go2rtc_username)"
GO2RTC_PASSWORD="$(opt go2rtc_password)"
CAMERA_STREAM_MAP="$(opt camera_stream_map)"
if [ -z "${CAMERA_STREAM_MAP}" ] || [ "${CAMERA_STREAM_MAP}" = "null" ]; then
  CAMERA_STREAM_MAP='{}'
fi
CAMERA_LABELS="$(opt camera_labels)"
if [ -z "${CAMERA_LABELS}" ] || [ "${CAMERA_LABELS}" = "null" ]; then
  CAMERA_LABELS='{}'
fi

# --- MIRROR_API_KEY (se comparte con el frontend) ---
# Fix C5/A2/M6 — la key es OBLIGATORIA (config.yaml la exige) y NUNCA se
# autogenera ni se imprime en el Log: los logs del add-on son visibles a
# cualquier admin de HA y persisten. Fail-closed con mensaje claro.
if [ -z "${API_KEY}" ] || [ "${API_KEY}" = "null" ]; then
  echo "[mirror] ERROR: falta 'mirror_api_key' en la configuración del add-on."
  echo "[mirror] Generala en tu laptop con:  openssl rand -hex 32"
  echo "[mirror] Pegala en Configuración -> mirror_api_key y reiniciá el add-on."
  exit 1
fi
if [ "${#API_KEY}" -lt 32 ]; then
  echo "[mirror] ERROR: 'mirror_api_key' muy corta (${#API_KEY} chars, mínimo 32)."
  echo "[mirror] Generá una fuerte con:  openssl rand -hex 32"
  exit 1
fi

# --- Secrets internos (NO se comparten): generar+persistir una vez ---
persist_secret() {  # $1 = nombre de archivo en /data
  if [ -f "/data/$1" ]; then cat "/data/$1"
  else s="$(gen_secret)"; printf '%s' "$s" > "/data/$1"; printf '%s' "$s"; fi
}
SESSION_SECRET="$(persist_secret session_secret)"
IFRAME_TOKEN_SECRET="$(persist_secret iframe_token_secret)"

# --- Entorno para el Mirror ---
# HA LOCAL vía el proxy del Supervisor. SUPERVISOR_TOKEN ya está en el entorno
# (homeassistant_api: true) y config.py lo lee como ha_token automáticamente.
export HA_URL="ws://supervisor/core/websocket"
export MIRROR_API_KEY="${API_KEY}"
export SESSION_SECRET="${SESSION_SECRET}"
export IFRAME_TOKEN_SECRET="${IFRAME_TOKEN_SECRET}"
export MIRROR_HOST="0.0.0.0"
export MIRROR_PORT="8000"
export MIRROR_DB_PATH="/data/mirror.sqlite3"
export LOG_LEVEL="${LOG_LEVEL}"
if [ -n "${FRONTEND_ORIGIN}" ] && [ "${FRONTEND_ORIGIN}" != "null" ]; then
  export FRONTEND_ORIGIN="${FRONTEND_ORIGIN}"
fi
# Interruptor maestro del modo fabrica. Si la opcion viene vacia (o "null",
# que es lo que devuelve el parser cuando no esta puesta) NO se exporta nada:
# el Mirror arranca en modo artesanal y no genera identidad, no reporta a
# ninguna plataforma y no levanta tunel propio. Una casa instalada a mano
# nunca se une a una flota por accidente.
if [ -n "${PLATFORM_BASE_URL}" ] && [ "${PLATFORM_BASE_URL}" != "null" ]; then
  export PLATFORM_BASE_URL="${PLATFORM_BASE_URL}"
  echo "[mirror] Modo FABRICA: reportando a ${PLATFORM_BASE_URL}"
else
  echo "[mirror] Modo ARTESANAL: sin plataforma, sin identidad de caja, sin tunel propio."
fi
if [ -n "${GO2RTC_BASE_URL}" ] && [ "${GO2RTC_BASE_URL}" != "null" ]; then
  export GO2RTC_BASE_URL="${GO2RTC_BASE_URL}"
fi
if [ -n "${GO2RTC_USERNAME}" ] && [ "${GO2RTC_USERNAME}" != "null" ]; then
  export GO2RTC_USERNAME="${GO2RTC_USERNAME}"
fi
if [ -n "${GO2RTC_PASSWORD}" ] && [ "${GO2RTC_PASSWORD}" != "null" ]; then
  export GO2RTC_PASSWORD="${GO2RTC_PASSWORD}"
fi
export CAMERA_STREAM_MAP="${CAMERA_STREAM_MAP}"
export CAMERA_LABELS="${CAMERA_LABELS}"

echo "[mirror] Arrancando → HA local (ws://supervisor/core/websocket) en :8000"
exec python3 -m uvicorn ha_mirror.main:app \
  --host 0.0.0.0 --port 8000 \
  --log-level "$(printf '%s' "${LOG_LEVEL}" | tr '[:upper:]' '[:lower:]')"
