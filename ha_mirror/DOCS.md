# UniquexCR Mirror

Mirror del frontend de lujo **UniquexCR**. Se conecta a Home Assistant de forma **local** (por el
proxy del Supervisor, sin túnel ni LLAT) y expone el estado y el control de los dispositivos por
WebSocket para la app remota.

## Cómo conecta a HA

Usa el **token del Supervisor** (el add-on tiene `homeassistant_api: true`), conectándose a
`ws://supervisor/core/websocket`. No necesita un Long-Lived Access Token.

## Opciones

| Opción | Requerido | Descripción |
|---|---|---|
| `mirror_api_key` | Sí | Clave que autentica al frontend. Debe ser la **misma** que configurás en Railway. Mínimo 32 bytes (`openssl rand -hex 32`). El add-on no arranca si está vacía. |
| `frontend_origin` | Opcional | Origen del frontend en producción para CORS, ej. `https://fortunatta.up.railway.app`. Separá varios con coma. |
| `log_level` | Opcional | `DEBUG` \| `INFO` \| `WARNING` \| `ERROR`. Default `INFO`. |
| `go2rtc_base_url` | Opcional | URL interna del API go2rtc, nunca una URL pública. |
| `go2rtc_username` | Opcional | Usuario HTTP Basic configurado en go2rtc. |
| `go2rtc_password` | Opcional | Password HTTP Basic configurado en go2rtc. |
| `go2rtc_streams` | Opcional | **go2rtc integrado.** JSON nombre → fuente de cada cámara, ej. `{"NVR_CH01":"rtsp://usuario:clave@IP_NVR:554/cam/realmonitor?channel=1&subtype=1"}`. Solo se usa si `go2rtc_base_url` está **vacía**. Es secreta: las fuentes llevan la clave de las cámaras. No acepta fuentes `exec:`, `echo:` ni `expr:`. |
| `camera_stream_map` | Opcional | JSON que relaciona cada `camera.entity_id` con su stream go2rtc. |
| `camera_labels` | Opcional | JSON con el nombre visible de cada cámara, ej. `{"camera.nvr_c21_gimnasio":"C21 GIMNASIO"}`. Si falta, se usa el nombre de HA o se deriva del `entity_id`. |

`GET /api/cameras` devuelve la lista de cámaras (mapa go2rtc + entidades `camera.*` de HA).
La app la consume tal cual: **agregar una cámara = agregarla a go2rtc y a `camera_stream_map`**,
sin volver a desplegar el frontend.

Sin las opciones go2rtc, snapshots funciona normalmente y WebRTC queda deshabilitado.

### go2rtc integrado (sin segundo complemento)

Desde esta versión el Mirror trae go2rtc adentro. Para una casa nueva **no hace falta
instalar el complemento go2rtc**:

1. Dejá `go2rtc_base_url` **vacía**.
2. En `go2rtc_streams` cargá las cámaras (o una sola cámara del NVR, la "semilla": las
   demás se suman después desde la app con **Cámaras → Agregar cámara**).
3. `camera_stream_map` es opcional: si queda vacío, cada stream aparece como una cámara
   con su propio nombre.

Cómo elige el Mirror:

| `go2rtc_base_url` | `go2rtc_streams` | Qué pasa |
|---|---|---|
| con valor | (se ignora) | go2rtc **externo**, como siempre. Una casa que ya lo usa no cambia en nada. |
| vacía | vacía | No corre ningún go2rtc. Una casa sin cámaras no paga ni un proceso. |
| vacía | con cámaras | go2rtc **integrado**: el Mirror lo arranca, lo vigila y lo reinicia si se cae. |

Seguridad del modo integrado: la API de go2rtc queda atada a `127.0.0.1` con usuario y
clave aleatorios nuevos en cada arranque y **`local_auth` activado** (sin eso, go2rtc no
le pide clave a nada que llegue desde localhost, y el túnel de Cloudflare corre al lado).
Los servidores RTSP, RTMP, SRTP y WebRTC de go2rtc quedan **apagados**. Las contraseñas
de las cámaras se borran de todo lo que go2rtc escribe en el registro.

**Raspberry Pi:** el video en vivo es barato (go2rtc solo retransmite). Lo que pesa son
las fotos de la cuadrícula: el Mirror mantiene una foto fresca de cada cámara y cada una
obliga a FFmpeg a decodificar video. Con muchas cámaras, medir la CPU antes de entregar.

## Escenas custom (desde 0.5.0)

Las **escenas** son listas ordenadas de acciones que el usuario arma desde la app ("Buenas noches" =
apagar switches + bajar persianas). Se guardan en la SQLite del add-on (`/data`, sobreviven
reinicios y actualizaciones) y **no** son las entidades `scene.*` de Home Assistant: son un concepto
paralelo, propio de UniquexCR. No hay ninguna opción de configuración nueva que tocar.

Todos los endpoints exigen el header `X-API-Key`, igual que el resto del Mirror.

| Método | Ruta | Respuesta |
|---|---|---|
| `GET` | `/api/scenes` | `200 {"scenes":[...],"count":N}` |
| `POST` | `/api/scenes` | `201` con la escena creada (el `id` lo genera el Mirror) |
| `GET` | `/api/scenes/{scene_id}` | `200` con la escena, o `404` |
| `PUT` | `/api/scenes/{scene_id}` | `200` con la escena reemplazada, o `404` |
| `DELETE` | `/api/scenes/{scene_id}` | `204`, o `404` |
| `POST` | `/api/scenes/{scene_id}/activate` | `202 {"scene_id":...,"steps":N,"correlation_ids":[...]}` |

Cuerpo de `POST` / `PUT`:

```json
{
  "name": "Buenas noches",
  "icon": "moon",
  "accent": "warm",
  "description": "Apaga todo y baja las persianas",
  "confirm_required": false,
  "steps": [
    {"domain": "switch", "service": "turn_off", "entity_id": "switch.10016f4c4b", "data": {}},
    {"domain": "cover", "service": "set_cover_position", "entity_id": "cover.terraza_1",
     "data": {"position": 0}}
  ],
  "cameras": ["camera.nvr_c08_garaje"]
}
```

Límites validados por el add-on (devuelve `422` si no se cumplen):

| Campo | Regla |
|---|---|
| `name` | 1 a 60 caracteres |
| `icon` | `moon` \| `sun` \| `home` \| `away` \| `movie` \| `gym` \| `party` \| `sleep` \| `shield` \| `sparkles` |
| `accent` | `warm` \| `cool` \| `gold` \| `green` \| `neutral` |
| `description` | hasta 160 caracteres |
| `steps` | 1 a 64 pasos; el prefijo del `entity_id` tiene que coincidir con el `domain` del paso |
| `steps[].data` | hasta 12 claves; valores `str`, `int`, `float`, `bool` o listas planas de esos |
| `cameras` | hasta 12 entidades `camera.*`, sin repetidos |
| Total | hasta 60 escenas guardadas |

**Seguridad.** Cada paso pasa por la misma lista negra de servicios administrativos que
`/api/service/...` (`hassio`, `supervisor`, `backup`, `host`, `addon`, `homeassistant.restart`,
`recorder.purge`, etc.), y la revisa **dos veces**: al guardar y otra vez al activar. Si algún paso
está en la lista, el Mirror responde `403 {"detail":"service not allowed"}` y no guarda ni ejecuta
nada. Una API key comprometida sigue sin poder administrar el gateway.

**Activación.** `POST .../activate` responde `202` de inmediato con un `correlation_id` por paso; la
ejecución sigue en background, en el orden guardado. Cada paso confirma por `/ws/state` con
`service_complete` o `service_timeout`, igual que un service call suelto. Un paso que falla queda
registrado y **no** aborta los siguientes. Si el upstream de HA está caído, responde `502` sin
ejecutar nada.

## Puerto

El Mirror escucha en el **8000** interno, publicado en el **8099** del host. Exponelo por el túnel
Cloudflare (ver `00_INSTALAR_REMOTO.md`, Paso 6) para que el frontend lo alcance.

## Datos persistentes

Guarda su base SQLite y sus secretos internos en `/data` (sobrevive reinicios y actualizaciones).

## Verificación

En el **Log**, buscá `ha.hydration_complete states=...` — si aparece con el número de entidades, el
Mirror está leyendo la casa en vivo. También: `GET /api/health` debe devolver
`upstream_connected: true`.
