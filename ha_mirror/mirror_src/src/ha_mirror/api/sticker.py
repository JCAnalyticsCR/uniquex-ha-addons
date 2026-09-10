"""
GET / — la calcomanía de activación, lista para imprimir.

PARA QUIÉN ES
-------------
Para quien prepara la caja en el taller. No es programador, no va a correr un
`curl`, y no tiene por qué saber la IP de la caja ni su API key.

Por eso vive en `/` y se abre por INGRESS de Home Assistant: el add-on aparece en
la barra lateral, se hace clic, y ahí está el QR. Home Assistant ya autenticó a
esa persona antes de dejarla entrar — no hace falta una segunda credencial.

QUÉ IMPRIME Y POR QUÉ LOS TRES DATOS
------------------------------------
El QR alcanza el 95% de las veces. El otro 5% —cámara sucia, poca luz, la
calcomanía rayada tras seis meses en un armario— necesita que alguien pueda
teclear. Y para reclamar hacen falta DOS datos: el código y el número de equipo.
Si solo se imprimiera el QR, un escaneo fallido dejaría la caja inservible sin
forma de recuperarla.

Por eso la calcomanía lleva los tres: QR, número de equipo y código.

CUÁNDO DEJA DE FUNCIONAR
------------------------
Cuando la caja ya tiene dueño. Una calcomanía para una caja emparejada no sirve
—el emparejamiento es de un solo uso— y mostrar su código sería regalar
información sin ninguna razón.

SEGURIDAD Y POR QUÉ NO SE USA X-API-KEY
----------------------------------------
La calcomanía entra por el PROXY de ingress de Home Assistant: cuando el
complemento aparece en la barra lateral de HA, el Supervisor autentica al usuario
y re-envía la petición al add-on. Pedir además X-API-Key obliga a quien prepara la
caja a escribirla a mano, sin un cliente que la tenga configurada.

Para distinguir las peticiones legítimas (pasaron por el Supervisor) de las directas
al puerto 8099 del host, verificamos la cabecera X-Ingress-Path, que el Supervisor
inyecta en cada petición que procesa. Las peticiones que llegan directamente al
puerto del host no pasan por ese proxy y por tanto no traen esa cabecera.

DECISIÓN DE SEGURIDAD ABIERTA — LEER ANTES DE MODIFICAR
---------------------------------------------------------
Se implementan DOS capas de defensa en profundidad, pero NINGUNA CIERRA el agujero
por sí sola. Se documentan aquí para que quien opere la caja entienda el estado real
y tome la decisión final.

CAPA 1 — X-Ingress-Path (necesaria, no suficiente):
  El Supervisor inyecta esta cabecera en cada petición que procesa. Un cliente que
  accede directamente al puerto 8099 del host NO pasa por el Supervisor y no trae
  la cabecera. Pero ES FALSIFICABLE: cualquiera en la LAN puede agregar la cabecera
  a mano con `curl -H "X-Ingress-Path: /" http://<ip>:8099/`.

CAPA 2 — IP de origen (incierta):
  Las peticiones de ingress llegan desde 172.30.32.2 (IP del Supervisor en la red
  interna del Supervisor en HAOS). Las peticiones directas al puerto 8099 desde la
  LAN deberían llegar con la IP real del cliente LAN (192.168.x.x) SI Docker usa
  iptables/DNAT para publicar el puerto — que es el mecanismo habitual en HAOS.

  PERO: si Docker usa su proxy de userland (docker-proxy), TODAS las conexiones al
  puerto publicado aparecen con la IP del gateway del bridge (172.17.0.1 o similar),
  que puede estar en el mismo rango que el Supervisor. En ese caso, este check NO
  distingue ingress de acceso directo de la LAN y sería igual de falso que la
  cabecera sola.

  No es posible determinar desde el código cuál de los dos mecanismos usa el runtime
  sin inspeccionarlo en vivo. Esta es la decisión abierta que hay que escalar.

ALTERNATIVA COMPLETA (no implementada acá):
  Binding a 127.0.0.1 en run.sh o eliminar el puerto de `ports` en config.yaml.
  Eso deja al puerto 8099 accesible SOLO desde la red interna del Supervisor (ingress),
  sin depender de la cabecera ni de la IP de origen. Es el único cierre real. Está
  fuera del alcance de este archivo — requiere cambios en run.sh o config.yaml.

Lo que tenemos cierra el acceso casual (un navegador en la LAN que llega directo a
la IP) y requiere conocimiento específico del protocolo de ingress de HA para
bypassearlo. No es seguridad, es elevación del esfuerzo. El operador debe decidir si
eso alcanza para el período de riesgo (entre "conectar a la red del cliente" y
"activar la caja").
"""

from __future__ import annotations

import html
from typing import Any

import segno
import structlog
from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse

from ha_mirror.device_identity import (
    DeviceIdentity,
    DeviceIdentityError,
    cargar_llave_privada,
    construir_url_qr,
    derivar_claim_code,
)
from ha_mirror.errors import UpstreamNotReadyError

# _PLATAFORMAS_INTERNAS: mismo conjunto que usa StateStore para distinguir hardware
# real de entidades internas de HA. Se importa para que la consulta fresca aplique
# exactamente el mismo criterio que la foto del store — cualquier divergencia
# causaría que un mismo dispositivo bloqueara o no la calcomanía según qué camino
# se tomara, que es el tipo de inconsistencia más difícil de depurar en el taller.
from ha_mirror.state_store import _PLATAFORMAS_INTERNAS

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

router = APIRouter()

# Cabecera que el Supervisor de HA inyecta en peticiones que pasan por su proxy
# de ingress. Su presencia indica que la petición fue autenticada y reenviada por
# el Supervisor; su ausencia indica acceso directo al puerto del host.
# Falsificable desde la LAN — ver docstring del módulo (DECISIÓN ABIERTA).
_INGRESS_HEADER = "X-Ingress-Path"

# Red interna del Supervisor de HA en HAOS (Docker network 172.30.32.0/23).
# Las peticiones de ingress llegan desde 172.30.32.2 (Supervisor).
# Las peticiones LAN directas aparecen con IP real del cliente SI Docker usa
# iptables/DNAT, o con la IP del gateway del bridge SI usa docker-proxy (userland).
# Ver docstring del módulo (DECISIÓN ABIERTA) para la limitación de este check.
_SUPERVISOR_NET_PREFIX = "172.30."

# Timeout para la consulta fresca al registry de HA.
# Esta página se abre 2 o 3 veces en la vida de una caja; el costo de 5 s
# de espera en ese contexto es irrelevante. Si el upstream no responde en ese
# tiempo, se cae al fallback del store sin dejar la pantalla en blanco.
_TIMEOUT_CONSULTA_FRESCA = 5.0


def _analizar_entidades_frescas(
    entities: list[dict[str, Any]],
) -> list[tuple[str, str]]:
    """
    Extrae (nombre_visible, plataforma) de dispositivos no-internos.

    Opera sobre la respuesta cruda de config/entity_registry/list_for_display
    (claves cortas: ei=entity_id, en=entity_name, pl=platform, di=device_id).

    Tres entidades del mismo dispositivo (di igual) cuentan como uno: se
    conserva el primer nombre visto. Misma lógica de filtrado que
    StateStore.contar_dispositivos_configurados(), pero sobre datos frescos
    del upstream en lugar de la foto tomada al hidratar.
    """
    # di → (nombre, plataforma); la inserción en el dict mantiene el primero.
    visto: dict[str, tuple[str, str]] = {}
    for entry in entities:
        di: str | None = entry.get("di")
        pl: str = entry.get("pl") or ""
        if not di:
            # Sin device_id: entidad virtual, helper o integración de servicio.
            continue
        if pl in _PLATAFORMAS_INTERNAS:
            # Infraestructura interna de HA — no es hardware del taller.
            continue
        if di not in visto:
            nombre: str = entry.get("en") or entry.get("ei") or di
            visto[di] = (nombre, pl)
    return list(visto.values())


def _pagina(titulo: str, cuerpo: str) -> HTMLResponse:
    """
    Envoltura mínima, con estilos de impresión.

    Sin dependencias de red: la caja puede estar en un taller sin internet y la
    página tiene que verse igual. Todo el CSS va en línea.
    """
    return HTMLResponse(f"""<!doctype html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(titulo)}</title>
<style>
  body {{ font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
         background: #f4ede1; color: #2a1d14; margin: 0; padding: 24px;
         display: flex; justify-content: center; }}
  .hoja {{ width: 100%; max-width: 420px; }}
  .etiqueta {{ background: #fff; border: 2px solid #2a1d14; border-radius: 14px;
               padding: 20px; text-align: center; }}
  .marca {{ font-size: 13px; font-weight: 700; letter-spacing: .14em;
            text-transform: uppercase; color: #c4593a; margin-bottom: 14px; }}
  .qr svg {{ width: 200px; height: 200px; }}
  .dato {{ margin-top: 14px; }}
  .dato .rotulo {{ font-size: 10px; font-weight: 700; letter-spacing: .1em;
                   text-transform: uppercase; color: #7a6a58; }}
  .dato .valor {{ font-family: ui-monospace, "SF Mono", Consolas, monospace;
                  font-size: 20px; font-weight: 700; letter-spacing: .1em; }}
  .equipo .valor {{ font-size: 13px; letter-spacing: .06em; word-break: break-all; }}
  .pie {{ font-size: 11px; color: #7a6a58; margin-top: 14px; line-height: 1.45; }}
  .nota {{ margin-top: 20px; font-size: 13px; color: #5a4a3a; line-height: 1.55; }}
  .aviso {{ background: #fff; border: 1px solid #d8c9b4; border-radius: 12px;
            padding: 18px; line-height: 1.6; }}
  /* Aviso de bloqueo: borde rojo para que sea imposible de ignorar. */
  .bloqueo {{ border-color: #c4593a; border-width: 2px; background: #fff8f6; }}
  .bloqueo .titulo {{ font-size: 16px; font-weight: 700; color: #c4593a;
                      margin-bottom: 12px; }}
  .bloqueo .pasos {{ margin-top: 12px; padding-left: 18px; }}
  .bloqueo .pasos li {{ margin-bottom: 4px; }}
  .bloqueo .dispositivos {{ margin: 10px 0; padding-left: 18px; }}
  .bloqueo .dispositivos li {{ font-family: ui-monospace, "SF Mono", Consolas, monospace;
                                font-size: 13px; margin-bottom: 2px; }}
  .nota-fallback {{ margin-top: 14px; padding: 10px 14px; background: #fffbe6;
                    border: 1px solid #e6c84a; border-radius: 8px;
                    font-size: 12px; color: #5a4a10; }}
  /* Al imprimir: solo la etiqueta, sin fondos ni instrucciones. */
  .boton-rotar {{ margin-top: 10px; padding: 10px 18px; border-radius: 10px;
                  border: 1px solid #c4593a; background: #c4593a; color: #fff;
                  font: inherit; font-weight: 700; cursor: pointer; }}
  .boton-rotar:hover {{ background: #a94830; }}
  @media print {{
    body {{ background: #fff; padding: 0; }}
    .nota, .no-imprimir {{ display: none; }}
    .etiqueta {{ border-width: 1px; }}
  }}
</style></head><body><div class="hoja">{cuerpo}</div></body></html>""")


def _cuerpo_bloqueo(
    dispositivos: list[tuple[str, str]],
    *,
    datos_frescos: bool,
) -> str:
    """
    Genera el cuerpo HTML del aviso de bloqueo.

    Incluye la lista de dispositivos detectados (nombre — plataforma) y las
    instrucciones para limpiar la caja, incluyendo el paso de reiniciar el
    complemento. Si los datos son de la foto del store (no frescos), agrega
    una nota de advertencia visible.
    """
    n = len(dispositivos)
    plural = "s" if n != 1 else ""

    # Lista de dispositivos con nombre y plataforma para que quien esté en el
    # taller identifique en dos segundos qué hay que quitar. Ejemplo: el
    # adaptador Bluetooth interno (hci0 — bluetooth) que se contaba a sí mismo.
    items_html = "".join(
        f"<li>{html.escape(nombre)} &#8212; {html.escape(plataforma)}</li>"
        for nombre, plataforma in dispositivos
    )

    nota_fallback = ""
    if not datos_frescos:
        nota_fallback = (
            '<p class="nota-fallback">'
            "&#9888; No se pudo verificar en tiempo real (el upstream no estaba "
            "disponible). Estos datos son del &#250;ltimo arranque del complemento. "
            "Si ya eliminaste los dispositivos, reinici&#225; el complemento para "
            "que esta p&#225;gina lo confirme."
            "</p>"
        )

    return (
        f'<div class="aviso bloqueo">'
        f'<div class="titulo">'
        f"&#9888; Esta caja tiene {n} dispositivo{plural} configurado{plural} "
        f"y todav&#237;a no est&#225; activada."
        f"</div>"
        f"Si los agregaste probando en tu casa, quit&#225;los antes de "
        f"despachar. Van a aparecer en la app del cliente apuntando a equipos "
        f"que est&#225;n en otra casa &#8212; el error es silencioso y no da "
        f"ninguna se&#241;al hasta que alguien lo nota d&#237;as despu&#233;s."
        f"<br><br>"
        f"<b>Dispositivos detectados:</b>"
        f'<ul class="dispositivos">{items_html}</ul>'
        f"<b>C&#243;mo limpiar:</b>"
        f'<ol class="pasos">'
        f"<li>Home Assistant &#8594; Configuraci&#243;n &#8594; "
        f"Dispositivos y servicios</li>"
        f"<li>Entr&#225; a cada integraci&#243;n que no sea del cliente</li>"
        f"<li>Elimin&#225; los dispositivos de tu red o quit&#225; "
        f"la integraci&#243;n entera</li>"
        f"</ol>"
        f"Cuando termines, <b>reinici&#225; el complemento</b> y volv&#233; a "
        f"esta p&#225;gina. La p&#225;gina verificar&#225; en tiempo real que "
        f"la caja est&#225; limpia antes de mostrar la calcoman&#237;a."
        f"{nota_fallback}"
        f"</div>"
    )


def _rechazo_no_ingress(request: Request) -> HTMLResponse | None:
    """
    La guarda de ingress. `None` cuando la petición es legítima.

    🔪 SE EXTRAJO PORQUE AHORA HAY DOS RUTAS, Y LA SEGUNDA ESCRIBE. Estaba
    incrustada en el handler de la calcomanía, que solo lee. Copiarla y pegarla
    en la ruta que rota el código es cómo se termina arreglando un día una sola
    de las dos copias — y la que quedaría sin arreglar es justamente la que muta
    estado.

    Ver el docstring del módulo para por qué hacen falta las dos capas y cuál es
    su límite conocido.
    """
    tiene_header = _INGRESS_HEADER in request.headers
    ip_origen = request.client.host if request.client else ""
    desde_red_interna = (
        ip_origen.startswith(_SUPERVISOR_NET_PREFIX)
        or ip_origen in ("127.0.0.1", "::1", "")  # loopback y tests ASGI
    )
    if tiene_header and desde_red_interna:
        return None

    logger.warning(
        "sticker.acceso_no_ingress",
        tiene_header=tiene_header,
        ip_origen=ip_origen,
    )
    return HTMLResponse(
        '<!doctype html><html lang="es"><head><meta charset="utf-8">'
        "<title>Acceso no permitido</title></head><body>"
        "<p><b>Esta p&#225;gina solo se puede abrir desde la barra lateral "
        "de Home Assistant.</b></p>"
        "<p>Abrila desde el men&#250; del complemento en HA, no ingresando "
        "directamente a la IP de la caja.</p>"
        "</body></html>",
        status_code=403,
    )


@router.post("/rotar-codigo", include_in_schema=False)
async def rotar_codigo(request: Request) -> Response:
    """
    Emite un código de activación nuevo. La misma caja, otra calcomanía.

    ── CUÁNDO SE USA ──────────────────────────────────────────────────────
    Cuando el código ya impreso se vio: una foto de la etiqueta que viajó por
    WhatsApp, una calcomanía que se pegó y después se despegó, un equipo que
    volvió del taller. Quien tenga ese código puede reclamar la caja.

    Antes de esto la única salida era re-aprovisionar el equipo entero.

    ── LAS TRES COPIAS DE LA IDENTIDAD ────────────────────────────────────
    🔪 Rotar el código toca TRES lugares, y olvidarse de uno rompe distinto:

      1. **La base** — la que sobrevive al reinicio.
      2. **`app.state.device_identity`** — de donde lee esta misma página. Sin
         actualizarla, la calcomanía sigue mostrando el código viejo y en el
         taller se reimprime la misma etiqueta creyendo que se rotó.
      3. **`AnnounceClient._identity`** — la peligrosa. De ahí sale el
         `claim_code_hash` de cada anuncio. Sin avisarle, la plataforma sigue
         esperando el hash anterior y **rechaza el código recién impreso**. Todo
         se ve bien y nada en el registro lo explica.

    ── POST-REDIRECT-GET ──────────────────────────────────────────────────
    Contesta 303 hacia la calcomanía. Sin eso, un F5 después de rotar vuelve a
    rotar, y cada recarga invalida la etiqueta que se acaba de imprimir.

    El código —viejo o nuevo— NUNCA entra en un registro. La versión sí: es lo
    único que hace falta para leer el historial de una caja.
    """
    rechazo = _rechazo_no_ingress(request)
    if rechazo is not None:
        return rechazo

    db = getattr(request.app.state, "db", None)
    if db is None:
        return _pagina(
            "No se pudo emitir",
            '<div class="aviso"><b>La caja no está lista todavía.</b></div>',
        )

    fila = await db.rotar_claim_code()
    if fila is None:
        # Ya tiene dueño. Ver `rotar_claim_code` para por qué se rechaza.
        return _pagina(
            "Esta caja ya está activada",
            '<div class="aviso"><b>No se puede emitir un código nuevo.</b><br>'
            "Esta caja ya fue reclamada por una cuenta, así que su código de "
            "activación no sirve para nada y cambiarlo solo haría que la "
            "plataforma dejara de reconocerla.<br><br>"
            "Si hay que devolverla al estado inicial, eso se hace desde la "
            "plataforma.</div>",
        )

    identidad = DeviceIdentity(**fila)
    # Copia 2: la que lee esta página.
    request.app.state.device_identity = identidad
    # Copia 3: la del bucle de anuncio. Ver arriba.
    anuncio = getattr(request.app.state, "announce_client", None)
    if anuncio is not None:
        await anuncio.rotar_identidad(identidad)

    logger.info(
        "sticker.codigo_rotado",
        device_id=identidad.device_id,
        claim_code_version=identidad.claim_code_version,
    )

    # El destino se arma con la cabecera del proxy: una URL absoluta se saldría
    # del ingress y daría 404.
    base = request.headers.get(_INGRESS_HEADER, "")
    return RedirectResponse(url=f"{base}/", status_code=303)


@router.post("/revalidar", include_in_schema=False)
async def revalidar(request: Request) -> Response:
    """
    "Volver a preguntar a la plataforma": destraba una caja que ya fue liberada.

    ── EL CASO REAL ────────────────────────────────────────────────────────
    El 2026-09-10 se liberó una caja desde la plataforma —casa dada de baja,
    túnel borrado, DNS caído— y **diez horas después seguía diciendo "Este
    equipo ya está activado"**. La caja se entera sola en su próximo latido,
    pero cuando eso no ocurre no había NADA que hacer desde el taller salvo
    esperar. Esperar frente a un cliente no es un plan, y el QR no vuelve hasta
    que la caja se entera.

    ── NO SUELTA LA CASA POR SU CUENTA ─────────────────────────────────────
    Este botón no habla con la plataforma: le pide al bucle de anuncio que
    pregunte YA, y adelanta su contador de confirmaciones. La decisión la sigue
    tomando el mismo camino de siempre, con la firma de la respuesta verificada.
    Ver `AnnounceClient.revalidar_ahora` para por qué se hizo así y no con una
    consulta propia.

    Si la plataforma contesta que la caja sí tiene dueño, no pasa nada.
    """
    rechazo = _rechazo_no_ingress(request)
    if rechazo is not None:
        return rechazo

    anuncio = getattr(request.app.state, "announce_client", None)
    if anuncio is None:
        # Caja artesanal (sin plataforma) o el bucle todavía no arrancó.
        return _pagina(
            "No se puede revalidar",
            '<div class="aviso"><b>Esta caja no está conectada a la plataforma.</b><br>'
            "No hay a quién preguntarle si sigue activada.</div>",
        )

    if not await anuncio.revalidar_ahora():
        return _pagina(
            "No hacía falta",
            '<div class="aviso"><b>Esta caja ya se considera libre.</b><br>'
            "Volvé a la página de activación y actualizá: el código debería estar ahí."
            "</div>",
        )

    # 🔪 NO se espera el resultado. La ronda de anuncio tarda lo que tarde la
    # red, y dejar el navegador colgado esperándola convierte un botón en una
    # pantalla trabada. Se contesta enseguida diciendo qué va a pasar y en
    # cuánto — que es lo que la persona necesita para decidir si esperar.
    base = request.headers.get(_INGRESS_HEADER, "")
    return _pagina(
        "Preguntando a la plataforma",
        '<div class="aviso"><b>Le preguntamos a la plataforma.</b><br><br>'
        "Si esta caja ya fue liberada, en menos de un minuto vuelve a mostrar su "
        "código de activación. Si sigue teniendo dueño, esta página no va a "
        "cambiar — y eso también es una respuesta.<br><br>"
        f'<a href="{html.escape(base)}/">Volver a la activación</a></div>',
    )


@router.get("/", include_in_schema=False, response_class=HTMLResponse)
async def sticker(request: Request) -> HTMLResponse:
    """
    La calcomanía. Sin API key: entra por ingress, que HA ya autenticó.

    Verificamos que la petición viene por el proxy de ingress del Supervisor
    (cabecera X-Ingress-Path presente). Ver el docstring del módulo para la
    limitación de seguridad de este enfoque y la alternativa completa.
    """

    # ── 0. Defensa en profundidad: cabecera de ingress + IP de origen ────────
    #
    # CAPA 1: X-Ingress-Path debe estar presente.
    # El Supervisor inyecta esta cabecera en todas las peticiones que pasan por
    # su proxy. Las que llegan directamente al puerto 8099 del host no la traen.
    # Falsificable con `curl -H "X-Ingress-Path: /" http://<ip>:8099/`.
    #
    # CAPA 2: la IP de origen debe ser de la red interna del Supervisor o
    # loopback. En HAOS con iptables/DNAT, las peticiones LAN directas conservan
    # la IP real del cliente (192.168.x.x), que no pasa este check.
    #
    # LIMITACIÓN CONOCIDA: si Docker usa docker-proxy (userland), todas las
    # conexiones al puerto publicado aparecen con la IP del bridge (puede ser
    # 172.30.x.x), haciendo ambas capas INSUFICIENTES juntas. Ver docstring del
    # módulo y el bloque DECISIÓN ABIERTA. La protección real requiere no exponer
    # el puerto en el host (run.sh / config.yaml).
    rechazo = _rechazo_no_ingress(request)
    if rechazo is not None:
        return rechazo

    # ── 1. Identidad de la caja ───────────────────────────────────────────────
    identidad = getattr(request.app.state, "device_identity", None)
    if identidad is None:
        return _pagina(
            "Sin identidad",
            '<div class="aviso"><b>La caja todavía no tiene identidad.</b><br>'
            "Revisá el registro del add-on: el arranque falló en un punto que hay "
            "que mirar, y reintentar desde acá no lo va a resolver.</div>",
        )

    # ── 2. Caja ya emparejada — este check va ANTES del conteo ───────────────
    #
    # Si la caja ya tiene dueño, no hay nada que bloquear: el emparejamiento
    # fue exitoso. Garantiza que una caja activa nunca queda atrapada detrás
    # del aviso de dispositivos, sin importar lo que devuelva el upstream.
    if identidad.paired:
        # 🔪 EL BOTÓN NO ES UN EXTRA. Sin él, una caja que la plataforma ya
        # liberó y que —por lo que sea— no se enteró, se queda acá para siempre:
        # sin QR, sin explicación y sin nada que hacer desde el taller. Pasó el
        # 2026-09-10 y costó diez horas de una entrega.
        revalidar_url = html.escape(request.headers.get(_INGRESS_HEADER, "")) + "/revalidar"
        return _pagina(
            "Equipo ya activado",
            '<div class="aviso"><b>Este equipo ya está activado.</b><br>'
            f"Casa: <code>{html.escape(str(identidad.paired_house_id))}</code><br><br>"
            "No se genera una calcomanía nueva: la activación es de un solo uso, y "
            "mostrar el código de un equipo en servicio no tendría ningún propósito."
            "</div>"
            '<div class="nota no-imprimir">'
            "<p><b>¿Esta caja ya se liberó y sigue diciendo que está activada?</b></p>"
            "<p>Pasa cuando la cuenta se dio de baja desde la plataforma y la caja "
            "todavía no se enteró. Normalmente se entera sola en un par de minutos; "
            "si no, preguntale ahora.</p>"
            "<p>Es seguro: si la plataforma dice que la caja sigue teniendo dueño, "
            "no cambia nada.</p>"
            f'<form method="post" action="{revalidar_url}">'
            '<button type="submit" class="boton-rotar">Volver a preguntar a la plataforma</button>'
            "</form></div>",
        )

    # ── 3. Verificar dispositivos del taller — consulta FRESCA ───────────────
    #
    # POR QUÉ CONSULTA FRESCA Y NO LA FOTO DEL STORE
    # ------------------------------------------------
    # El store es una foto tomada al hidratar: ha_upstream.py se suscribe solo a
    # state_changed, nunca a entity_registry_updated. Si alguien borra un
    # dispositivo y recarga esta página sin reiniciar el complemento, la foto
    # sigue mostrando el dispositivo eliminado y la página sigue bloqueada,
    # aunque la caja ya esté limpia. El remedio documentado (eliminar + refrescar)
    # no funciona — eso es peor que el falso positivo original.
    #
    # La solución es pedir el registro fresco al upstream en cada apertura de esta
    # página (que ocurre 2 o 3 veces en la vida de una caja, en el taller).
    # Si la consulta falla, se cae al store con una nota visible — nunca 500.
    upstream = getattr(request.app.state, "upstream", None)
    store = getattr(request.app.state, "store", None)

    dispositivos: list[tuple[str, str]] = []
    datos_frescos = False

    if upstream is not None:
        try:
            resultado = await upstream.send_command(
                {"type": "config/entity_registry/list_for_display"},
                timeout=_TIMEOUT_CONSULTA_FRESCA,
            )
            dispositivos = _analizar_entidades_frescas(resultado.get("entities", []))
            datos_frescos = True
            logger.debug("sticker.consulta_fresca_ok", n_dispositivos=len(dispositivos))
        except (UpstreamNotReadyError, Exception) as exc:
            # Cualquier fallo (upstream caído, timeout, permisos) → fallback al store.
            # Nunca dejar la pantalla en blanco: alguien parado en el taller necesita
            # una respuesta, aunque sea "no pude verificar, esto es lo que sé".
            logger.warning("sticker.consulta_fresca_fallida", exc=str(exc))

    # Fallback: foto del store.
    # Se usa cuando el upstream no estaba disponible O no está montado en
    # app.state (entornos de test muy reducidos).
    if not datos_frescos and store is not None:
        dispositivos = store.listar_dispositivos_configurados()
        # Si tampoco hay store, dispositivos queda [] y la calcomanía se muestra.

    if dispositivos:
        logger.warning(
            "sticker.dispositivos_taller",
            n=len(dispositivos),
            frescos=datos_frescos,
        )
        return _pagina(
            "Dispositivos del taller detectados",
            _cuerpo_bloqueo(dispositivos, datos_frescos=datos_frescos),
        )

    # ── 4. Calcomanía ─────────────────────────────────────────────────────────
    settings = request.app.state.settings
    try:
        privada = cargar_llave_privada(settings.device_key_path)
    except DeviceIdentityError as exc:
        logger.error("sticker.sin_llave", exc=str(exc))
        return _pagina(
            "Error de identidad",
            f'<div class="aviso"><b>No se pudo leer la identidad.</b><br>'
            f"{html.escape(str(exc))}</div>",
        )

    codigo = derivar_claim_code(privada, identidad.claim_code_version)
    url = construir_url_qr(settings.platform_base_url, identidad.device_id, codigo)

    # SVG en línea: se imprime nítido a cualquier tamaño y no depende de que la
    # caja tenga internet para renderizar la imagen.
    #
    # 🔪 EL `viewBox` NO ES COSMETICO: SIN EL, EL QR SE RECORTA Y NO SE LEE.
    #
    # `svg_inline` emite `<svg width="245" height="245">` y NINGUN `viewBox`. Un
    # SVG sin `viewBox` no escala cuando se le cambia el tamaño: su sistema de
    # coordenadas queda fijo y lo que sobra se RECORTA. La hoja de estilos de
    # esta misma página lo fuerza a 200x200, asi que se perdian 45 unidades
    # —un 18%— del lado derecho y de abajo, zona quieta incluida.
    #
    # El resultado es una calcomanía que se ve bien y no escanea nunca. Se
    # descubrió el 2026-09-09 con el socio intentando leer la etiqueta de la
    # caja de casa 2, que ya estaba impresa.
    #
    # Con `viewBox`, cambiarle el tamaño lo ESCALA, que es lo que siempre se
    # quiso.
    #
    # 🔪 Y LA CORRECCION SUBE DE M A Q (15% -> 25%). Esto no es un QR de
    # pantalla: es una etiqueta pegada a un equipo que va a vivir años en una
    # casa. El propio instructivo contempla "cámara sucia, poca luz, etiqueta
    # rayada" como el motivo de imprimir el código en texto. Un cuarto de
    # tolerancia al daño cuesta unos módulos más y compra justamente eso.
    simbolo = segno.make(url, error="q")
    lado = simbolo.symbol_size(border=4)[0] * 5
    qr = simbolo.svg_inline(scale=5, dark="#2a1d14").replace(
        "<svg ", f'<svg viewBox="0 0 {lado} {lado}" ', 1
    )

    # El destino se arma con la cabecera del proxy: una URL absoluta se saldría
    # del ingress y daría 404.
    accion = html.escape(request.headers.get(_INGRESS_HEADER, "")) + "/rotar-codigo"

    return _pagina(
        "Calcomanía de activación",
        f"""
    <div class="etiqueta">
      <div class="marca">UniquexCR</div>
      <div class="qr">{qr}</div>
      <div class="dato equipo">
        <div class="rotulo">Número de equipo</div>
        <div class="valor">{html.escape(identidad.device_id)}</div>
      </div>
      <div class="dato">
        <div class="rotulo">Código de activación</div>
        <div class="valor">{html.escape(codigo)}</div>
      </div>
      <div class="pie">Escaneá el código para activar tu casa</div>
    </div>

    <div class="nota">
      <p><b>Imprimí esta etiqueta y pegala en el equipo antes de despacharlo.</b></p>
      <p>Van los tres datos a propósito. Si el QR no escanea —cámara sucia, poca luz,
      la calcomanía rayada— el cliente puede escribir el número de equipo y el código
      a mano. Con solo uno de los dos no se puede activar.</p>
      <p>Esta página deja de mostrar el código apenas alguien active el equipo.</p>
    </div>

    <div class="nota no-imprimir">
      <p><b>¿Este código quedó expuesto?</b></p>
      <p>Si le sacaste una foto a la etiqueta y la mandaste por mensaje, si la
      calcomanía se pegó y se despegó, o si el equipo volvió del taller, cualquiera
      con ese código puede activar esta caja.</p>
      <p>Podés emitir uno nuevo. <b>Es la misma caja</b> —el número de equipo no
      cambia— pero hay que <b>reimprimir y pegar la etiqueta nueva</b>: la anterior
      deja de servir en ese momento.</p>
      <form method="post" action="{accion}"
            onsubmit="return confirm('El código actual va a dejar de servir y hay que reimprimir la etiqueta. ¿Seguimos?');">
        <button type="submit" class="boton-rotar">Emitir un código nuevo</button>
      </form>
    </div>
    """,
    )
