# Instalar el Mirror como add-on de HA — 100% remoto

> **Qué logra esto:** el Mirror queda corriendo **dentro de la caja de Fortunatta**, pegado a
> Home Assistant (le habla por `localhost`, sin túnel) → **señal impecable, $0 de nube, y no hay
> que tocar la caja ni molestar al cliente.** Todo se hace desde tu laptop + la web de HA.
>
> **Cómo se autentica (importante):** el add-on usa el **token del Supervisor** de HA. **Ya no se
> usa el LLAT de 10 años** — un problema de seguridad menos.

---

## ⚠ LEER ANTES DE PUBLICAR — de dónde se publica

Hay **CUATRO carpetas** en la laptop con `origin` apuntando al mismo repo de GitHub
(`JCAnalyticsCR/uniquex-ha-addons`):

| Carpeta | version en `config.yaml` | |
|---|---|---|
| `build/_deploy_addon_repo` | la actual | ✅ **el ÚNICO desde el que se publica** |
| `build/_produccion/05_addon_mirror` | 0.3.2 | ❌ marcado `_NO_PUBLICAR_DESDE_AQUI.md` |
| `build/_release_uniquex_addons` | 0.2.0 | ❌ marcado `_NO_PUBLICAR_DESDE_AQUI.md` |
| `build/_release_uniquex_addons_clean` | 0.3.0 | ❌ marcado `_NO_PUBLICAR_DESDE_AQUI.md` |

**Un `git push` desde cualquiera de los otros tres pisa la historia con una versión de hace
~18 releases** y la casa del cliente queda ofrecida a "actualizar" hacia atrás.

Esta es **la carpeta correcta**: estás leyendo el `00_INSTALAR_REMOTO.md` de `_deploy_addon_repo`.

> **Corrección 2026-08-04.** Los pasos 1 y 2 de este documento mandaban a vendorizar **y publicar**
> desde `_produccion/05_addon_mirror`. El `package.ps1` ya se corrigió (hoy escribe hacia
> `_deploy_addon_repo`), pero el bloque de `git init` + `git push` de este documento **sí apuntaba
> al clon equivocado**. Quedó corregido abajo.

### Para actualizar una casa que YA tiene el add-on instalado

Este documento es para la **primera** instalación. Para publicar una versión nueva en una casa
que ya lo tiene corriendo (respaldo, `update_entity`, `update.install`, verificación y plan de
reversa), usá el runbook de release:

`build/_produccion/13_release_0_22_0/RUNBOOK_INSTALACION.md`

---

## Antes de empezar (lo que necesitás)

- [ ] Acceso **admin** a HA por el túnel (`https://casa-fortunata.uniquexcr.com`).
- [ ] Una cuenta de **GitHub** (para el repo del add-on) — o, si preferís, el add-on de **Samba**
      instalado en HA (método local, ver Opción B).
- [ ] `git` en tu laptop (para subir el repo).

---

## Paso 1 — Vendorizar el código (en tu laptop)

Esto copia el código del Mirror desde el árbol de desarrollo (`build/06_fastapi_mirror`) hacia
adentro del add-on, regenera el lockfile de dependencias con versiones exactas y corre `pip-audit`
como gate (si alguna versión fijada tiene un CVE conocido, el empaquetado **falla** y no se publica).

```powershell
powershell -ExecutionPolicy Bypass -File "C:\Users\memo-\OneDrive\Desktop\UniquexCR\build\_produccion\05_addon_mirror\package.ps1"
```

> **El script vive en `05_addon_mirror` pero ESCRIBE en `_deploy_addon_repo`** (corregido en
> 0.21.0). Es correcto correrlo desde esa ruta; lo que **no** hay que hacer es publicar desde ahí.
>
> **Cuidado:** vendoriza el árbol de desarrollo **tal cual esté**. Si `06_fastapi_mirror` tiene
> trabajo a medias, eso es exactamente lo que van a instalar las casas. Revisá `git diff` en
> `_deploy_addon_repo` después de correrlo.
>
> Repetí este paso cada vez que cambie el código del Mirror, antes de re-publicar.

---

## Paso 2 — Poner el add-on en la caja

### Opción A — Repo de GitHub (recomendada, permite actualizar fácil)

El repo ya existe: **`https://github.com/JCAnalyticsCR/uniquex-ha-addons`**, y el clon que lo
publica es **`build/_deploy_addon_repo`** (esta carpeta). No hay que crear nada ni correr
`git init`.

1. Subí la `version:` en `ha_mirror/config.yaml` **y** en `mirror_src/pyproject.toml` (tienen que
   coincidir: `config.yaml` es lo único que mira el Supervisor, y `pyproject.toml` es de donde
   `/api/health` saca la versión que reporta).
2. Publicá **desde `_deploy_addon_repo`, nunca desde otro clon**:

   ```bash
   cd "C:/Users/memo-/OneDrive/Desktop/UniquexCR/build/_deploy_addon_repo"
   git status                     # confirmá que estás en el clon correcto y en main
   # Agregá SOLO lo del add-on (nunca `git add .` — evita subir secretos por accidente)
   git add ha_mirror/ repository.yaml README.md 00_INSTALAR_REMOTO.md .gitattributes .gitignore
   git commit -m "feat(mirror): <qué cambia> (X.Y.Z)"
   git push origin main
   ```

3. En HA (por el túnel): **Ajustes → Complementos → Tienda de complementos** → menú **⋮** (arriba
   derecha) → **Repositorios** → pegá `https://github.com/JCAnalyticsCR/uniquex-ha-addons` →
   **Añadir**. *(Si el repo es privado, usá una URL con token de acceso de GitHub:
   `https://<TOKEN>@github.com/JCAnalyticsCR/uniquex-ha-addons`.)*
   **Esto es solo para la primera instalación** — si la casa ya tiene el repo agregado, saltealo.

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
