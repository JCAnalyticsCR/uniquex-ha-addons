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
"""

from __future__ import annotations

import html

import segno
import structlog
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from ha_mirror.device_identity import (
    DeviceIdentityError,
    cargar_llave_privada,
    construir_url_qr,
    derivar_claim_code,
)

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

router = APIRouter()


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
  /* Al imprimir: solo la etiqueta, sin fondos ni instrucciones. */
  @media print {{
    body {{ background: #fff; padding: 0; }}
    .nota, .no-imprimir {{ display: none; }}
    .etiqueta {{ border-width: 1px; }}
  }}
</style></head><body><div class="hoja">{cuerpo}</div></body></html>""")


@router.get("/", include_in_schema=False, response_class=HTMLResponse)
async def sticker(request: Request) -> HTMLResponse:
    """La calcomanía. Sin API key: entra por ingress, que HA ya autenticó."""
    identidad = getattr(request.app.state, "device_identity", None)
    if identidad is None:
        return _pagina(
            "Sin identidad",
            '<div class="aviso"><b>La caja todavía no tiene identidad.</b><br>'
            "Revisá el registro del add-on: el arranque falló en un punto que hay "
            "que mirar, y reintentar desde acá no lo va a resolver.</div>",
        )

    if identidad.paired:
        return _pagina(
            "Equipo ya activado",
            '<div class="aviso"><b>Este equipo ya está activado.</b><br>'
            f"Casa: <code>{html.escape(str(identidad.paired_house_id))}</code><br><br>"
            "No se genera una calcomanía nueva: la activación es de un solo uso, y "
            "mostrar el código de un equipo en servicio no tendría ningún propósito."
            "</div>",
        )

    # Caja sin activar: verificar que no quedaron dispositivos del taller.
    #
    # EL PROBLEMA QUE RESUELVE
    # ------------------------
    # El socio prepara la caja EN SU CASA. HA descubre solo los dispositivos de
    # esa red: su televisor, su impresora, sus luces. Si acepta alguno sin querer,
    # ESE dispositivo viaja a la casa del cliente y aparece en la app apuntando a
    # un equipo que está en otro lugar. El error es silencioso — nada se rompe —
    # y no hay forma de descubrirlo después de despachar.
    #
    # POR QUÉ BLOQUEAR Y NO SOLO ADVERTIR
    # -------------------------------------
    # La activación ocurre en la casa del cliente, DESPUÉS del despacho. Todo lo
    # que se configuró estando sin activar salió del taller. Ese es el único
    # momento en que se puede corregir, y esta página es la última oportunidad.
    # Un aviso que se puede ignorar no cumple esa función: en dos días nadie lo
    # lee y la caja sale igualmente. El costo de un falso positivo (la calcomanía
    # se traba cuando la caja sí está limpia) es mínimo y corregible. El costo
    # de un falso negativo (dispositivos del taller en la app de un cliente) es
    # un viaje de soporte a la casa del cliente.
    store = getattr(request.app.state, "store", None)
    if store is not None:
        n = store.contar_dispositivos_configurados()
        if n > 0:
            plural = "s" if n != 1 else ""
            logger.warning("sticker.dispositivos_taller", n=n)
            return _pagina(
                "Dispositivos del taller detectados",
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
                f"<b>C&#243;mo limpiar:</b>"
                f'<ol class="pasos">'
                f"<li>Home Assistant &#8594; Configuraci&#243;n &#8594; "
                f"Dispositivos y servicios</li>"
                f"<li>Entr&#225; a cada integraci&#243;n que no sea del cliente</li>"
                f"<li>Elimin&#225; los dispositivos de tu red o quit&#225; "
                f"la integraci&#243;n entera</li>"
                f"</ol>"
                f"Cuando la caja no tenga dispositivos configurados, "
                f"esta p&#225;gina muestra la calcoman&#237;a."
                f"</div>",
            )

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
    qr = segno.make(url, error="m").svg_inline(scale=5, dark="#2a1d14")

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
    """,
    )
