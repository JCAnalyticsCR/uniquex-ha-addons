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
| `camera_stream_map` | Opcional | JSON que relaciona cada `camera.entity_id` con su stream go2rtc. |

Sin las opciones go2rtc, snapshots funciona normalmente y WebRTC queda deshabilitado.

## Puerto

El Mirror escucha en el **8000** interno, publicado en el **8099** del host. Exponelo por el túnel
Cloudflare (ver `00_INSTALAR_REMOTO.md`, Paso 6) para que el frontend lo alcance.

## Datos persistentes

Guarda su base SQLite y sus secretos internos en `/data` (sobrevive reinicios y actualizaciones).

## Verificación

En el **Log**, buscá `ha.hydration_complete states=...` — si aparece con el número de entidades, el
Mirror está leyendo la casa en vivo. También: `GET /api/health` debe devolver
`upstream_connected: true`.
