"""
Actualizaciones y respaldos del sistema, desde la app.

── POR QUÉ ES UN ENDPOINT PROPIO Y NO UN PERMISO EN /api/service ─────────────
El proxy genérico de servicios prohíbe los dominios `update`, `hassio`,
`backup`, `supervisor`, `addon` y `host`, y esa lista negra no se toca. Es la
misma decisión que tomó `pronostico.py`: en este proyecto ya se coló capacidad
sensible CUATRO veces por relajar ese proxy, y aflojarlo para instalar una
actualización convertiría un canal de "prender la luz" en uno de "administrar
el gateway". Un token comprometido pasaría de encender lámparas a reiniciar la
casa.

Acá el cliente no elige un dominio ni un servicio: elige una entidad de una
lista que este archivo ya decidió que se puede tocar.

── LA POLÍTICA ES EL PRODUCTO ────────────────────────────────────────────────
Todas las actualizaciones se MUESTRAN — esconder que existen sería peor que no
tener la pantalla. Pero solo algunas se pueden instalar desde el teléfono, y
cada bloqueo dice su motivo con palabras que el dueño entiende.

El caso que obliga a que esto exista así: la integración Dahua. Actualizarla
**apaga las 20 cámaras** — importa `async_timeout`, que Home Assistant ya no
trae, así que al reiniciar no carga y las cámaras desaparecen sin un solo aviso
previo. Por eso la casa corre una copia parcheada. Si esta pantalla se hubiera
hecho con la lista completa y un botón por renglón, la primera actualización
que el cliente tocaría sería justamente la que deja la casa sin vigilancia.

El segundo grupo es distinto y también importa: Core, el sistema operativo, el
Supervisor, el túnel Cloudflared, el puente eWeLink y go2rtc no son peligrosos
por estar rotos — son peligrosos porque si uno falla, **la casa queda
incomunicada o ciega y el dueño no tiene cómo recuperarla desde el teléfono**.
Esos los instala quien pueda llegar a la cajita.

── EL RESPALDO VA ANTES, SIEMPRE ─────────────────────────────────────────────
`update.install` acepta `backup: true` y acá no es opcional. Una actualización
sin respaldo previo es una apuesta, y el que la paga no es quien la programó.
"""

from __future__ import annotations

import re
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from ha_mirror.auth import require_api_key
from ha_mirror.errors import UpstreamNotReadyError
from ha_mirror.models import leer_atributo

logger = structlog.get_logger(__name__)

router = APIRouter()

#: Instalar puede tardar: baja una imagen y reinicia el complemento.
TIMEOUT_INSTALAR = 600.0
TIMEOUT_RESPALDO = 900.0


class Politica(BaseModel):
    """Qué se puede hacer con esta actualización, y por qué."""

    puede_instalar: bool
    #: `null` cuando se puede. Cuando no, la frase que se muestra tal cual.
    motivo: str | None = None


#: Nunca, ni aunque lo pida el dueño. El motivo se muestra en la pantalla.
_PROHIBIDAS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"dahua", re.I),
        "Actualizar esto apaga las 20 cámaras. La versión nueva no arranca en "
        "esta casa y no hay forma de volver atrás desde el teléfono.",
    ),
)

#: Se muestran, pero las instala quien pueda llegar a la cajita. Si una de
#: estas falla, la casa queda incomunicada o ciega y la app no puede rescatarla.
_SOLO_TECNICO: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"cloudflared|t[úu]nel", re.I),
        "Es la única puerta de entrada a la casa. Si la actualización sale mal, "
        "la app deja de alcanzarla y hay que ir en persona.",
    ),
    (
        re.compile(r"home_assistant_core|_core_actualizar|\bcore\b", re.I),
        "Es el motor de la casa. Un salto de versión puede dejar integraciones "
        "sin cargar; lo hace el técnico con un respaldo verificado a mano.",
    ),
    (
        re.compile(r"operating_system|supervisor", re.I),
        "Toca el sistema de la cajita. Si algo sale mal no arranca, y desde el "
        "teléfono no hay cómo recuperarla.",
    ),
    (
        re.compile(r"ewelink", re.I),
        "Es el puente de los 245 apagadores de la casa. Si no vuelve a levantar, "
        "se quedan todos sin responder hasta que alguien lo reinicie.",
    ),
    (
        re.compile(r"go2rtc", re.I),
        "Es lo que convierte las cámaras en video para la app. Si falla, se "
        "apaga la sección Cámaras entera.",
    ),
    (
        re.compile(r"\bhacs\b|get_hacs", re.I),
        "Administra las integraciones de la comunidad. Lo actualiza el técnico "
        "junto con lo que dependa de él.",
    ),
    (
        re.compile(r"uniquexcr|\bmirror\b", re.I),
        "Es el servidor que conecta esta app con la casa. Si la actualización "
        "falla, la app deja de ver la casa y no puede arreglarse a sí misma.",
    ),
    (
        re.compile(r"matter", re.I),
        "Es lo que empareja los aparatos nuevos por código QR. Se actualiza "
        "cuando no haya un alta en curso.",
    ),
)


def _politica(entity_id: str, nombre: str) -> Politica:
    """
    La decisión, con su motivo. Mira el id Y el nombre visible.

    🔪 LOS DOS, NO UNO. El id de la integración Dahua es `update.dahua_update`,
    pero el de un add-on es `update.<slug>_actualizar` y el slug no siempre dice
    de qué es. Mirar solo el id deja pasar cosas por el nombre del paquete, y
    mirar solo el nombre las deja pasar cuando alguien lo renombra en HA.
    """
    campo = f"{entity_id} {nombre}"
    for patron, motivo in _PROHIBIDAS:
        if patron.search(campo):
            return Politica(puede_instalar=False, motivo=motivo)
    for patron, motivo in _SOLO_TECNICO:
        if patron.search(campo):
            return Politica(puede_instalar=False, motivo=motivo)
    return Politica(puede_instalar=True, motivo=None)


class Actualizacion(BaseModel):
    entity_id: str
    nombre: str
    instalada: str | None = None
    disponible: str | None = None
    #: `true` cuando hay algo que instalar. Las que están al día también se
    #: listan: saber que TODO está al día es una respuesta, no un vacío.
    hay_novedad: bool
    puede_instalar: bool
    motivo: str | None = None


class Actualizaciones(BaseModel):
    disponible: bool
    actualizaciones: list[Actualizacion] = Field(default_factory=list)
    pendientes: int = 0


class PedidoInstalar(BaseModel):
    entity_id: str


class Resultado(BaseModel):
    ok: bool
    mensaje: str


def _texto(valor: Any) -> str | None:
    return valor if isinstance(valor, str) and valor.strip() else None


@router.get(
    "/api/sistema/actualizaciones",
    response_model=Actualizaciones,
    summary="Qué se puede actualizar en la casa, y qué no",
)
async def listar(
    request: Request,
    _: None = Depends(require_api_key),
) -> Actualizaciones:
    """
    Lee las entidades `update.*` que el Mirror ya tiene espejadas.

    No pregunta nada a Home Assistant: el estado ya está en memoria porque el
    WebSocket lo trae. Devuelve SIEMPRE 200 — que no haya nada que actualizar no
    es un error, y una casa sin la información tampoco.
    """
    store = getattr(request.app.state, "store", None)
    if store is None:
        return Actualizaciones(disponible=False)

    salida: list[Actualizacion] = []
    estados: dict[str, Any] = store.get_all_states()
    for entity_id, estado in estados.items():
        if not entity_id.startswith("update."):
            continue
        nombre = _texto(leer_atributo(estado, "friendly_name")) or entity_id
        politica = _politica(entity_id, nombre)
        salida.append(
            Actualizacion(
                entity_id=entity_id,
                nombre=nombre,
                instalada=_texto(leer_atributo(estado, "installed_version")),
                disponible=_texto(leer_atributo(estado, "latest_version")),
                hay_novedad=getattr(estado, "state", None) == "on",
                puede_instalar=politica.puede_instalar,
                motivo=politica.motivo,
            )
        )

    salida.sort(key=lambda a: (not a.hay_novedad, a.nombre.lower()))
    return Actualizaciones(
        disponible=True,
        actualizaciones=salida,
        pendientes=sum(1 for a in salida if a.hay_novedad),
    )


@router.post(
    "/api/sistema/actualizaciones/instalar",
    response_model=Resultado,
    summary="Instalar una actualización permitida, con respaldo previo",
)
async def instalar(
    pedido: PedidoInstalar,
    request: Request,
    _: None = Depends(require_api_key),
) -> Resultado:
    """
    🔪 LA POLÍTICA SE VUELVE A EVALUAR ACÁ, NO SE CONFÍA EN LA PANTALLA.

    El frontend ya esconde el botón de las bloqueadas, y eso no alcanza: quien
    llame a esta ruta puede no ser el frontend. Es la misma cicatriz de las
    cuatro veces que se coló capacidad sensible por filtrar en el cliente —
    ahí lo delicado pasaba por un filtro de dominio hecho en la pantalla.
    """
    entity_id = pedido.entity_id.strip()
    if not entity_id.startswith("update."):
        raise HTTPException(status_code=400, detail="Eso no es una actualización.")

    store = getattr(request.app.state, "store", None)
    estado = store.get_state(entity_id) if store is not None else None
    if estado is None:
        raise HTTPException(status_code=404, detail="Esa actualización no existe.")

    nombre = _texto(leer_atributo(estado, "friendly_name")) or entity_id
    politica = _politica(entity_id, nombre)
    if not politica.puede_instalar:
        logger.warning(
            "instalacion_bloqueada", entity_id=entity_id, motivo=politica.motivo
        )
        # 403 y no 400: no está mal pedida, está prohibida.
        raise HTTPException(status_code=403, detail=politica.motivo)

    upstream = getattr(request.app.state, "upstream", None)
    if upstream is None:
        raise UpstreamNotReadyError("Sin conexión con la casa.")

    logger.info("instalando_actualizacion", entity_id=entity_id, nombre=nombre)
    await upstream.send_service_call(
        "update",
        "install",
        {
            # El respaldo NO es opcional. Ver la cabecera.
            "service_data": {"backup": True},
            "target": {"entity_id": entity_id},
        },
    )
    return Resultado(
        ok=True,
        mensaje=(
            f"{nombre} se está actualizando. La casa guardó un respaldo antes de "
            "empezar. Puede tardar unos minutos."
        ),
    )


@router.post(
    "/api/sistema/respaldo",
    response_model=Resultado,
    summary="Guardar un respaldo completo de la casa",
)
async def respaldar(
    request: Request,
    _: None = Depends(require_api_key),
) -> Resultado:
    """
    Dispara `hassio.backup_full`.

    🔪 ESTA RUTA SIGUE SIN PODER CONFIRMAR NADA: `hassio.backup_full` no
    devuelve resultado. Lo que cambió es que ya no hace falta que lo confirme
    ella — `GET /api/sistema/respaldos` lee `backup/info` del Core y ahí está la
    lista con fechas. Antes se creía que eso exigía la API de respaldos del
    Supervisor, que el add-on no puede tocar (`hassio_role: default`, a propósito,
    porque está expuesto por el túnel). Medido el 2026-09-07: el Core lo publica
    por el mismo WebSocket que ya usamos.

    Se deja `hassio.backup_full` para CREAR y no `backup/generate`: el servicio
    ya está probado en producción y el comando nuevo pide elegir agentes y
    contraseña, decisiones que nadie tomó todavía.

    Y hay algo más grave que conviene recordar acá: el destino del respaldo
    diario de esta casa es la propia cajita. Un respaldo que vive en el mismo
    aparato que protege no es un respaldo.
    """
    upstream = getattr(request.app.state, "upstream", None)
    if upstream is None:
        raise UpstreamNotReadyError("Sin conexión con la casa.")

    logger.info("respaldo_manual_solicitado")
    await upstream.send_service_call("hassio", "backup_full", {"service_data": {}})
    return Resultado(
        ok=True,
        mensaje=(
            "La casa empezó a guardar un respaldo. Tarda varios minutos; cuando "
            "termine va a aparecer acá arriba como el más reciente."
        ),
    )


# ---------------------------------------------------------------------------
# Estado de los respaldos
# ---------------------------------------------------------------------------
#
# 🔪 ESTO EXISTE PORQUE `POST /api/sistema/respaldo` NO PUEDE CONFIRMAR NADA.
# Dispara `hassio.backup_full` y ahí se acaba su historia: el servicio no
# devuelve resultado. La pantalla lo decía con todas las letras —"no hay forma
# de avisarte cuando termine: se confirma desde Home Assistant"— que es
# exactamente la frase que este proyecto existe para no tener que escribir.
#
# La creencia era que para saberlo hacía falta la API de respaldos del
# Supervisor, y el add-on pide `hassio_role: default` a propósito (está expuesto
# a internet por el túnel), así que se dio por imposible.
#
# Medido contra la casa el 2026-09-07: **el Core lo publica por su cuenta**. El
# comando `backup/info` del WebSocket devuelve la lista completa con fechas y
# cuál fue el último automático que terminó bien. No hace falta ningún permiso
# nuevo: es el mismo canal por el que ya viaja todo lo demás.
#
# ── LO QUE ESTA CASA CONTESTÓ, Y POR QUÉ SE REPORTA ─────────────────────────
# Los 11 respaldos tienen UN solo destino: `hassio.local`, la propia cajita. Un
# respaldo que vive en el mismo aparato que protege no es un respaldo, y esa es
# una respuesta que el dueño merece ver en su pantalla y no en una auditoría.
# Por eso `solo_en_la_caja` es un campo y no una nota al pie.


class Respaldo(BaseModel):
    nombre: str
    fecha: str | None = None
    #: `true` cuando lo hizo la casa sola, `false` cuando alguien lo pidió.
    automatico: bool = False


class EstadoRespaldos(BaseModel):
    disponible: bool
    total: int = 0
    #: `true` mientras la casa está guardando uno ahora mismo.
    generando: bool = False
    ultimo: Respaldo | None = None
    #: Cuándo terminó bien el último automático. Es LA respuesta a "¿y funciona?".
    ultimo_automatico_ok: str | None = None
    #: Cuándo se intentó por última vez. Si es más nuevo que el de arriba, el
    #: último intento FALLÓ — y eso es justo lo que nadie se enteraba.
    ultimo_automatico_intento: str | None = None
    #: Todos los respaldos viven únicamente en la cajita.
    solo_en_la_caja: bool = False


@router.get(
    "/api/sistema/respaldos",
    response_model=EstadoRespaldos,
    summary="Qué respaldos tiene la casa y si el último salió bien",
)
async def estado_respaldos(
    request: Request,
    _: None = Depends(require_api_key),
) -> EstadoRespaldos:
    """
    SIEMPRE 200. Una casa que no puede contestar devuelve `disponible:false` y
    la pantalla no muestra la sección — igual que `capabilities` y que la
    bandeja de encontrados. Esto se pinta en Ajustes, que el cliente abre todos
    los días: no puede reventar porque la casa esté durmiendo.
    """
    upstream = getattr(request.app.state, "upstream", None)
    if upstream is None:
        return EstadoRespaldos(disponible=False)

    try:
        resp = await upstream.send_command({"type": "backup/info"}, timeout=15.0)
    except UpstreamNotReadyError:
        logger.debug("respaldos.upstream_caido")
        return EstadoRespaldos(disponible=False)
    except Exception:
        # Un Core viejo no tiene `backup/*` (llegó con la integración de
        # respaldos nueva). No es un error: es una casa que no sabe contestar.
        logger.info("respaldos.no_soportado")
        return EstadoRespaldos(disponible=False)

    datos = resp.get("result") or {}
    crudos = datos.get("backups") or []

    lista: list[Respaldo] = []
    for b in crudos:
        if not isinstance(b, dict):
            continue
        lista.append(
            Respaldo(
                nombre=_texto(b.get("name")) or "Respaldo sin nombre",
                fecha=_texto(b.get("date")),
                automatico=bool(b.get("with_automatic_settings")),
            )
        )

    # El más nuevo por fecha. Se ordena acá y no se confía en el orden de HA:
    # viene ordenado por slug, no por cuándo se hizo.
    lista.sort(key=lambda r: r.fecha or "", reverse=True)

    # Un respaldo cuyo único agente es `hassio.local` está guardado en la propia
    # cajita. Si TODOS lo están, la casa no tiene copia afuera.
    destinos: set[str] = set()
    for b in crudos:
        if isinstance(b, dict):
            destinos.update((b.get("agents") or {}).keys())

    return EstadoRespaldos(
        disponible=True,
        total=len(lista),
        generando=bool(datos.get("generating")),
        ultimo=lista[0] if lista else None,
        ultimo_automatico_ok=_texto(datos.get("last_completed_automatic_backup")),
        ultimo_automatico_intento=_texto(datos.get("last_attempted_automatic_backup")),
        solo_en_la_caja=bool(destinos) and destinos <= {"hassio.local"},
    )
