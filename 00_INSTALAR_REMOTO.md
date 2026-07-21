# Instalar el Mirror como add-on de HA — 100% remoto

> **Qué logra esto:** el Mirror queda corriendo **dentro de la caja de Fortunatta**, pegado a
> Home Assistant (le habla por `localhost`, sin túnel) → **señal impecable, $0 de nube, y no hay
> que tocar la caja ni molestar al cliente.** Todo se hace desde tu laptop + la web de HA.
>
> **Cómo se autentica (importante):** el add-on usa el **token del Supervisor** de HA. **Ya no se
> usa el LLAT de 10 años** — un problema de seguridad menos.

---

## Antes de empezar (lo que necesitás)

- [ ] Acceso **admin** a HA por el túnel (`https://casa-fortunata.uniquexcr.com`).
- [ ] Una cuenta de **GitHub** (para el repo del add-on) — o, si preferís, el add-on de **Samba**
      instalado en HA (método local, ver Opción B).
- [ ] `git` en tu laptop (para subir el repo).

---

## Paso 1 — Vendorizar el código (en tu laptop, 10 segundos)

Esto mete el código del Mirror dentro del add-on:

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\memo-\OneDrive\Desktop\UniquexCR\build\_produccion\05_addon_mirror\package.ps1"
```

> Repetí este paso cada vez que cambie el código del Mirror, antes de re-publicar.

---

## Paso 2 — Poner el add-on en la caja

### Opción A — Repo de GitHub (recomendada, permite actualizar fácil)

1. Creá un repo **privado** en GitHub, ej. `uniquex-ha-addons`.
2. Editá `05_addon_mirror/repository.yaml` y `ha_mirror/config.yaml`: reemplazá `USUARIO` por tu
   usuario de GitHub en las URLs.
3. Subí **el contenido de** `build\_produccion\05_addon_mirror\` a la **raíz** del repo:

   ```bash
   cd "C:/Users/memo-/OneDrive/Desktop/UniquexCR/build/_produccion/05_addon_mirror"
   git init
   # Agregá SOLO lo del add-on (nunca `git add .` — evita subir secretos por accidente)
   git add ha_mirror/ repository.yaml README.md 00_INSTALAR_REMOTO.md .gitattributes .gitignore
   git commit -m "UniquexCR Mirror add-on"
   git branch -M main
   git remote add origin https://github.com/USUARIO/uniquex-ha-addons.git
   git push -u origin main
   ```

4. En HA (por el túnel): **Ajustes → Complementos → Tienda de complementos** → menú **⋮** (arriba
   derecha) → **Repositorios** → pegá `https://github.com/USUARIO/uniquex-ha-addons` → **Añadir**.
   *(Si el repo es privado, usá una URL con token de acceso de GitHub: `https://<TOKEN>@github.com/USUARIO/uniquex-ha-addons`.)*

### Opción B — Add-on local (sin GitHub, vía Samba)

1. Instalá el add-on **Samba share** en HA y activalo (te comparte las carpetas de HA por red).
2. Desde tu laptop, entrá al recurso compartido `\\casa-fortunata\addons` (o la IP de la caja).
   *(Requiere estar en la misma red o VPN; por eso la Opción A suele ser más cómoda en remoto.)*
3. Copiá la carpeta `ha_mirror\` (la que tiene `config.yaml`, `Dockerfile`, `run.sh`, `mirror_src\`)
   dentro de `addons\`.
4. En HA: **Ajustes → Complementos → Tienda** → menú **⋮** → **Buscar actualizaciones**. Aparecerá
   en "Local add-ons".

---

## Paso 3 — Instalar

1. En la Tienda de complementos, abrí **UniquexCR Mirror**.
2. Clic en **Instalar**. HA **construye el add-on dentro de la caja** — tarda **3–8 minutos** la
   primera vez (descarga Python + dependencias). Es normal que se quede un rato en "Installing".

---

## Paso 4 — Configurar y arrancar

1. Pestaña **Configuración** del add-on:
   - **`mirror_api_key`**: pegá una clave que generes vos (ej. en tu laptop:
     `openssl rand -hex 32`). **Guardala** — la vas a poner igual en el frontend (Railway).
     *(Si la dejás vacía, el add-on genera una sola y la muestra en el Log — copiala de ahí.)*
   - **`frontend_origin`**: dejala vacía por ahora; la llenás con la URL de Railway cuando el
     frontend esté arriba (ej. `https://fortunatta.up.railway.app`).
   - **`log_level`**: `INFO`.
   - **Guardar**.
2. Pestaña **Información** → **Iniciar**.
3. Activá **"Iniciar al arrancar"** y **"Watchdog"** (para que vuelva solo si se cae).

---

## Paso 5 — Verificar (pestaña Log del add-on)

Deberías ver algo así:

```
[mirror] Arrancando → HA local (ws://supervisor/core/websocket) en :8000
... mirror.starting ...
... ha.auth_ok ...
... ha.hydration_complete states=415 entities=... areas=...
... ha.subscribed_state_changed
... mirror.started
```

Si ves `hydration_complete` con el número de entidades → **el Mirror ya está leyendo Fortunatta en
vivo, local y estable.** 🎉

---

## Paso 6 — Exponer el Mirror por el túnel (para que el frontend lo alcance)

El frontend (en Railway) necesita llegar al Mirror. Lo publicamos por el **mismo túnel Cloudflare**
que ya tenés, en un subdominio nuevo.

1. Averiguá la **IP local de la caja** en tu red: HA → **Ajustes → Sistema → Red** (ej.
   `192.168.4.50`).
2. Abrí la configuración del add-on **Cloudflared** y agregá un host adicional apuntando al Mirror:

   ```yaml
   external_hostname: casa-fortunata.uniquexcr.com
   tunnel_name: casa-jeyrell        # (no lo cambies)
   additional_hosts:
     - hostname: mirror-fortunata.uniquexcr.com
       service: http://192.168.4.50:8099     # IP local de la caja + puerto 8099
   ```

3. **Reiniciá** el add-on Cloudflared.
4. Probá desde tu laptop (fuera de la casa):

   ```bash
   curl -s -H "X-API-Key: <tu_mirror_api_key>" https://mirror-fortunata.uniquexcr.com/api/health
   ```

   Esperado: `{"upstream_connected": true, "upstream_state": "READY", ...}`

> Si `mirror-fortunata.uniquexcr.com` no responde, el DNS del subdominio puede tardar unos minutos,
> o revisá que la IP y el puerto 8099 sean correctos. Este es el paso que conviene probar en vivo.

---

## Paso 7 — Conectar el frontend (Railway)

Cuando despleguemos el frontend, sus variables apuntarán al Mirror:

```
MIRROR_BASE_URL = https://mirror-fortunata.uniquexcr.com
MIRROR_API_KEY  = <la misma clave del Paso 4>
```

Y en el add-on, poné `frontend_origin = https://<tu-app>.up.railway.app` (Paso 4) y reiniciá.

---

## Si algo falla

| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| El add-on no arranca, "bad interpreter" | `run.sh` quedó con saltos CRLF | Convertí `run.sh` a **LF** y re-subí (el `.gitattributes` ya ayuda) |
| Log muestra `auth` que falla | Permiso del Supervisor | Confirmá que `homeassistant_api: true` está en `config.yaml` |
| Build falla al instalar | Sin internet en la caja o base image | Revisá que la caja tenga internet; reintentá la instalación |
| `hydration_complete` no aparece | HA no respondió a tiempo | Reiniciá el add-on; mirá si HA está saturado |
| `mirror-fortunata...` da 502 | IP/puerto del `additional_hosts` mal | Corregí la IP local de la caja y el puerto `8099` |

---

## Por qué esto es seguro (nota de entrega)

- **Sin LLAT**: usa el token del Supervisor, que vive solo dentro de la caja y rota con ella.
- **La API key** del Mirror se guarda como *password* en las opciones del add-on (no en el repo).
- **El repo del add-on no lleva ningún secreto** — ni tokens, ni datos del cliente.
- **HA queda intacto**: no se reinstala nada; solo se *agrega* el Mirror al lado.
