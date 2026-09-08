"""
Salud de la casa — qué está roto, desde cuándo, y qué se puede hacer.

── POR QUÉ EXISTE ───────────────────────────────────────────────────────────
El 2026-09-07 se auditó a mano una casa en producción. Llevaba MESES rota en
varios lugares a la vez, en silencio:

  · las persianas caídas desde marzo, con la pregunta de Home Assistant hecha
    desde el primer día y nadie que la viera;
  · 906 caídas de apagadores en tres días;
  · los once respaldos viviendo DENTRO de la cajita que protegen;
  · cuatro aparatos con el nombre corrompido;
  · el único dato eléctrico real de la casa, tirándose.

Ninguna la descubrió la app. Las descubrió una persona descifrando un token de
administración y abriendo el WebSocket a mano. Una casa que se degrada en
silencio durante meses, con el dueño mirando la app todos los días, es el
problema central del producto — no una función que falta.

── LA DIFERENCIA CON UNA PANTALLA DE ESTADO ─────────────────────────────────
Un panel de estado dice "3 aparatos sin conexión". Esto tiene que decir QUÉ
pasó, DESDE CUÁNDO, y ofrecer el arreglo. Por eso cada hallazgo trae una acción
concreta, y las que no tienen arreglo desde el teléfono lo dicen con todas las
letras en vez de dejar un botón que no hace nada.

── 🔪 NADA DE ESTO CONOCE ESTA CASA ─────────────────────────────────────────
Ni un `entity_id`, ni una marca, ni un nombre. Todos los hallazgos salen de lo
que la casa REPORTA: sus integraciones, sus formularios en curso, sus avisos de
reparación, sus respaldos, sus entidades. Una casa nueva —sin Somfy, sin eWeLink,
con otro hardware— se diagnostica igual el día que se enciende, sin tocar este
archivo. Es la condición que pidió Jeyrell y es también lo que hace que esto
sirva como producto y no como parche de una instalación.

── 🔪 NO ABRE NINGUNA PUERTA NUEVA ──────────────────────────────────────────
Las acciones que ofrece son rutas que YA existen y que ya tienen su política
adentro: contestar un formulario, instalar una actualización permitida,
reconciliar, respaldar. Esta ruta solo LEE y decide qué mostrar. Si mañana
alguien la compromete, no gana ni una capacidad que no tuviera antes — que es
justo lo contrario de lo que pasaría con un endpoint "arreglar" genérico.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field

from ha_mirror.auth import require_api_key
from ha_mirror.errors import HaProtocolError, UpstreamNotReadyError
from ha_mirror.models import leer_atributo

logger = structlog.get_logger(__name__)

router = APIRouter()

CACHE_SEGUNDOS = 60.0

#: Un aparato que no responde hace más que esto ya no es un parpadeo.
DIAS_PARA_ABANDONADO = 3.0

#: Si esta fracción de un dominio está caída, falló el SISTEMA y no los
#: aparatos. Avisar de 150 apagadores por separado es no avisar de nada.
#: (La misma regla que usa el aviso de mantenimiento de la app.)
FRACCION_DOMINIO_CAIDO = 0.6

#: Por debajo de esto no se habla de un dominio: tres aparatos caídos de cuatro
#: es 75% y no significa nada.
MINIMO_DOMINIO = 8


class Accion(BaseModel):
    """
    Qué puede hacer el dueño, si puede algo.

    `tipo` es lo único que la app interpreta; el resto son los datos que esa
    acción necesita. `ninguna` significa que hace falta alguien en la casa, y
    entonces `motivo` dice por qué.
    """

    tipo: str  # contestar_formulario | instalar | reconciliar | respaldar | ninguna
    rotulo: str
    flow_id: str | None = None
    entity_id: str | None = None
    motivo: str | None = None


class Hallazgo(BaseModel):
    #: Estable entre corridas: la app lo usa para no repetir avisos ya vistos.
    id: str
    #: critico = algo que funcionaba dejó de funcionar.
    #: atencion = algo va a fallar, o ya falla sin que se note.
    #: nota     = apareció algo nuevo, o hay algo mejorable.
    severidad: str
    titulo: str
    detalle: str
    #: ISO8601 cuando se puede saber desde cuándo. `None` cuando no.
    desde: str | None = None
    accion: Accion


class SaludResponse(BaseModel):
    disponible: bool
    #: `true` cuando no hay ni un hallazgo crítico ni de atención.
    todo_bien: bool = False
    hallazgos: list[Hallazgo] = Field(default_factory=list)
    revisado_en: str | None = None


# ---------------------------------------------------------------------------
# Chequeos. Cada uno degrada SOLO: uno que falla no puede dejar la pantalla en
# blanco, porque entonces el único aviso que se pierde es el del día que algo
# se rompió de verdad.
# ---------------------------------------------------------------------------


async def _formularios(svc: Any) -> list[Hallazgo]:
    """
    Lo que Home Assistant preguntó y nadie contestó.

    Se apoya en `encontrados()`, que ya distingue los dos tonos: un `reauth` es
    algo que FUNCIONABA y se rompió; un descubrimiento es una oferta.
    """
    datos = await svc.encontrados()
    if not datos.get("disponible"):
        return []
    fuera: list[Hallazgo] = []
    for e in datos.get("encontrados") or []:
        falla = bool(e.get("es_falla"))
        nombre = e.get("nombre") or e.get("family_label") or "un aparato"
        if falla:
            fuera.append(
                Hallazgo(
                    id=f"flujo:{e.get('flow_id')}",
                    severidad="critico",
                    titulo=f"{nombre} dejó de responder y pide reconectarse",
                    detalle=(
                        "Esto funcionaba antes. La casa perdió la sesión con el "
                        "servicio y necesita que alguien la vuelva a autorizar. "
                        "Hasta que se conteste, esos aparatos no responden."
                    ),
                    accion=Accion(
                        tipo="contestar_formulario",
                        rotulo="Reconectar ahora",
                        flow_id=e.get("flow_id"),
                    ),
                )
            )
        else:
            fuera.append(
                Hallazgo(
                    id=f"flujo:{e.get('flow_id')}",
                    severidad="nota",
                    titulo=f"Apareció {nombre} en la red",
                    detalle=(
                        "La casa lo encontró sola y está esperando que alguien "
                        "confirme si querés sumarlo."
                    ),
                    accion=Accion(
                        tipo="contestar_formulario",
                        rotulo="Ver y agregar",
                        flow_id=e.get("flow_id"),
                    ),
                )
            )
    return fuera


async def _integraciones(upstream: Any) -> list[Hallazgo]:
    """Integraciones que no cargaron. Una caída se lleva TODOS sus aparatos."""
    resp = await upstream.send_command({"type": "config_entries/get"}, timeout=10.0)
    fuera: list[Hallazgo] = []
    for e in resp.get("result") or []:
        estado = e.get("state")
        if estado in (None, "loaded", "not_loaded"):
            continue
        titulo = e.get("title") or e.get("domain") or "una integración"
        fuera.append(
            Hallazgo(
                id=f"entry:{e.get('entry_id')}",
                severidad="critico",
                titulo=f"{titulo} no está funcionando",
                detalle=(
                    "La casa no pudo cargarla al arrancar, así que todos los "
                    "aparatos que dependen de ella están fuera de servicio."
                ),
                accion=Accion(
                    tipo="ninguna",
                    rotulo="",
                    motivo="Lo revisa el técnico: hace falta ver el registro de la casa.",
                ),
            )
        )
    return fuera


def _dominios_caidos(store: Any) -> list[Hallazgo]:
    """
    Familias enteras sin responder.

    🔪 LA REGLA QUE EVITA EL RUIDO: si la mayoría de un dominio está caída, no
    fallaron 150 aparatos — falló el puente que los conecta. Avisar de cada uno
    por separado es la forma más segura de que nadie lea ningún aviso.
    """
    estados: dict[str, Any] = store.get_all_states()
    por_dominio: dict[str, list[str]] = {}
    caidos: dict[str, int] = {}
    for eid, st in estados.items():
        dom = eid.split(".", 1)[0]
        por_dominio.setdefault(dom, []).append(eid)
        if getattr(st, "state", None) in ("unavailable", "unknown"):
            caidos[dom] = caidos.get(dom, 0) + 1

    fuera: list[Hallazgo] = []
    for dom, todos in por_dominio.items():
        n = len(todos)
        mal = caidos.get(dom, 0)
        if n < MINIMO_DOMINIO or mal / n < FRACCION_DOMINIO_CAIDO:
            continue
        fuera.append(
            Hallazgo(
                id=f"dominio:{dom}",
                severidad="critico",
                titulo=f"{mal} de {n} aparatos del grupo «{dom}» no responden",
                detalle=(
                    "Cuando cae casi todo un grupo de golpe, lo que falló no son "
                    "los aparatos: es el puente que los conecta con la casa."
                ),
                accion=Accion(
                    tipo="ninguna",
                    rotulo="",
                    motivo="Suele resolverse reiniciando el puente. Lo hace el técnico.",
                ),
            )
        )
    return fuera


def _abandonados(store: Any) -> list[Hallazgo]:
    """Aparatos sueltos que llevan días sin dar señales."""
    ahora = datetime.now(UTC)
    viejos: list[tuple[str, float, str]] = []
    for eid, st in store.get_all_states().items():
        if getattr(st, "state", None) not in ("unavailable", "unknown"):
            continue
        crudo = getattr(st, "last_changed", None) or getattr(st, "last_updated", None)
        if crudo is None:
            continue
        # 🔪 El store tipa esto como `datetime`, pero por acá también pasan
        # estados venidos de un volcado crudo donde es texto. Se aceptan los
        # dos: un `str()` sobre un datetime y después `fromisoformat` funciona
        # de casualidad —el separador es un espacio y Python lo tolera desde
        # 3.11— y de casualidad no se construye nada.
        if isinstance(crudo, datetime):
            cuando = crudo
        else:
            try:
                cuando = datetime.fromisoformat(str(crudo).replace("Z", "+00:00"))
            except ValueError:
                continue
        if cuando.tzinfo is None:
            cuando = cuando.replace(tzinfo=UTC)
        dias = (ahora - cuando).total_seconds() / 86400
        if dias >= DIAS_PARA_ABANDONADO:
            nombre = leer_atributo(st, "friendly_name") or eid
            viejos.append((eid, dias, str(nombre)))
    if not viejos:
        return []
    viejos.sort(key=lambda x: -x[1])
    # UN hallazgo con el conteo, no uno por aparato. Ver `_dominios_caidos`.
    peor = viejos[0]
    resto = len(viejos) - 1
    cola = f" y {resto} más" if resto > 0 else ""
    return [
        Hallazgo(
            id="abandonados",
            severidad="atencion",
            titulo=f"«{peor[2]}»{cola} no responden hace días",
            detalle=(
                f"El más viejo lleva {peor[1]:.0f} días sin dar señales. Puede "
                "estar desenchufado, sin corriente o fuera del alcance de la red."
            ),
            accion=Accion(
                tipo="ninguna",
                rotulo="",
                motivo="Hay que mirar el aparato en persona.",
            ),
        )
    ]


async def _respaldos(upstream: Any) -> list[Hallazgo]:
    """El respaldo que no salió, y el que no protege de nada."""
    resp = await upstream.send_command({"type": "backup/info"}, timeout=15.0)
    datos = resp.get("result") or {}
    copias = datos.get("backups") or []
    fuera: list[Hallazgo] = []

    ok = datos.get("last_completed_automatic_backup")
    intento = datos.get("last_attempted_automatic_backup")
    if intento and (not ok or str(intento) > str(ok)):
        fuera.append(
            Hallazgo(
                id="respaldo:fallo",
                severidad="critico",
                titulo="El último respaldo automático no terminó",
                detalle=(
                    "La casa lo intentó y no pudo completarlo. Si algo sale mal "
                    "ahora, lo último que se puede recuperar es más viejo de lo "
                    "que debería."
                ),
                desde=str(intento),
                accion=Accion(tipo="respaldar", rotulo="Intentar uno ahora"),
            )
        )

    if not copias:
        fuera.append(
            Hallazgo(
                id="respaldo:ninguno",
                severidad="critico",
                titulo="La casa no tiene ningún respaldo guardado",
                detalle=(
                    "Si la cajita falla, se pierde toda la configuración: "
                    "aparatos, habitaciones, escenas y costumbres."
                ),
                accion=Accion(tipo="respaldar", rotulo="Guardar uno ahora"),
            )
        )
    else:
        destinos: set[str] = set()
        for b in copias:
            if isinstance(b, dict):
                destinos.update((b.get("agents") or {}).keys())
        # Un destino que empieza con `hassio.` es la propia cajita.
        if destinos and all(d.startswith("hassio.") for d in destinos):
            fuera.append(
                Hallazgo(
                    id="respaldo:solo_local",
                    severidad="atencion",
                    titulo=f"Los {len(copias)} respaldos viven dentro de la cajita",
                    detalle=(
                        "Un respaldo guardado en el mismo aparato que protege no "
                        "protege de que ese aparato se dañe, se moje o se lo "
                        "roben. Conviene que una copia salga de la casa."
                    ),
                    accion=Accion(
                        tipo="ninguna",
                        rotulo="",
                        motivo="Configurar un destino externo lo hace el técnico.",
                    ),
                )
            )
    return fuera


async def _actualizaciones(request: Request) -> list[Hallazgo]:
    """Lo que espera y el dueño SÍ puede instalar."""
    from ha_mirror.api.mantenimiento import listar

    datos = await listar(request, None)  # type: ignore[arg-type]
    fuera: list[Hallazgo] = []
    for a in datos.actualizaciones:
        if not a.hay_novedad or not a.puede_instalar:
            continue
        fuera.append(
            Hallazgo(
                id=f"update:{a.entity_id}",
                severidad="nota",
                titulo=f"{a.nombre} tiene una versión nueva",
                detalle=(
                    f"Está en {a.instalada or '?'} y hay {a.disponible or '?'}. "
                    "La casa guarda un respaldo antes de instalarla."
                ),
                accion=Accion(
                    tipo="instalar", rotulo="Instalar", entity_id=a.entity_id
                ),
            )
        )
    return fuera


async def _desacuerdos(svc: Any) -> list[Hallazgo]:
    """Lo que la app dice y la casa no. Ver `cotejar`."""
    datos = await svc.cotejar()
    if not datos.get("disponible"):
        return []
    difs = datos.get("diferencias") or []
    if not difs:
        return []
    return [
        Hallazgo(
            id="cotejo",
            severidad="atencion",
            titulo=f"{len(difs)} cosas se ven distinto en la app y en la casa",
            detalle=(
                "Un nombre o una habitación que cambiaste no llegó a guardarse "
                "en la casa. Se puede empujar de nuevo sin perder nada."
            ),
            accion=Accion(tipo="reconciliar", rotulo="Poner la casa al día"),
        )
    ]


async def _reparaciones(upstream: Any, svc: Any) -> list[Hallazgo]:
    """
    Los avisos que Home Assistant genera solo.

    El texto sale de las traducciones de HA, igual que los motivos de aborto:
    escribir a mano los avisos de 900 integraciones no es una tabla, es una
    carrera perdida.
    """
    resp = await upstream.send_command({"type": "repairs/list_issues"}, timeout=10.0)
    crudos = (resp.get("result") or {}).get("issues") or []
    fuera: list[Hallazgo] = []
    for it in crudos:
        if it.get("dismissed_version"):
            continue
        dominio = str(it.get("domain") or "")
        issue = str(it.get("issue_id") or "")
        # Un `reauth` ya lo cubre `_formularios` con mejor texto y con acción.
        if "config_entry_reauth" in issue:
            continue
        textos = await svc._textos_de(dominio)
        titulo = textos.get(f"component.{dominio}.issues.{issue}.title")
        if not titulo:
            continue  # sin traducción no se muestra un identificador crudo
        fuera.append(
            Hallazgo(
                id=f"repair:{dominio}:{issue}",
                severidad="atencion" if it.get("severity") == "error" else "nota",
                titulo=titulo,
                detalle=(
                    "Home Assistant lo detectó solo y espera que alguien lo "
                    "revise."
                ),
                accion=Accion(
                    tipo="ninguna",
                    rotulo="",
                    motivo="Lo resuelve el técnico desde la casa.",
                ),
            )
        )
    return fuera


ORDEN = {"critico": 0, "atencion": 1, "nota": 2}


@router.get(
    "/api/salud",
    response_model=SaludResponse,
    summary="Qué está roto en la casa, desde cuándo, y qué se puede hacer",
)
async def salud(
    request: Request,
    _: None = Depends(require_api_key),
) -> SaludResponse:
    """SIEMPRE 200. Sin casa, `disponible:false` — no saber no es estar bien."""
    store = getattr(request.app.state, "store", None)
    upstream = getattr(request.app.state, "upstream", None)
    svc = getattr(request.app.state, "onboarding", None)
    if store is None or upstream is None or svc is None:
        return SaludResponse(disponible=False)

    cache = getattr(request.app.state, "_salud_cache", None)
    if cache is not None and time.monotonic() - cache[0] < CACHE_SEGUNDOS:
        return cache[1]  # type: ignore[no-any-return]

    hallazgos: list[Hallazgo] = []

    # 🔪 CADA CHEQUEO POR SEPARADO. Envolver todo en un try deja la pantalla en
    # blanco el día que UNA comprobación falla — y ese es exactamente el día en
    # que la casa tiene algo raro y el dueño necesita ver el resto.
    for nombre, corrutina in (
        ("formularios", _formularios(svc)),
        ("integraciones", _integraciones(upstream)),
        ("respaldos", _respaldos(upstream)),
        ("actualizaciones", _actualizaciones(request)),
        ("desacuerdos", _desacuerdos(svc)),
        ("reparaciones", _reparaciones(upstream, svc)),
    ):
        try:
            hallazgos.extend(await corrutina)
        except (UpstreamNotReadyError, HaProtocolError) as exc:
            logger.info("salud.chequeo_no_disponible", chequeo=nombre, detalle=str(exc)[:120])
        except Exception:
            logger.warning("salud.chequeo_error", chequeo=nombre, exc_info=True)

    for nombre, fn in (("dominios", _dominios_caidos), ("abandonados", _abandonados)):
        try:
            hallazgos.extend(fn(store))
        except Exception:
            logger.warning("salud.chequeo_error", chequeo=nombre, exc_info=True)

    hallazgos.sort(key=lambda h: ORDEN.get(h.severidad, 9))
    salida = SaludResponse(
        disponible=True,
        todo_bien=not any(h.severidad in ("critico", "atencion") for h in hallazgos),
        hallazgos=hallazgos,
        revisado_en=datetime.now(UTC).isoformat(),
    )
    request.app.state._salud_cache = (time.monotonic(), salida)
    logger.info(
        "salud.revisada",
        total=len(hallazgos),
        criticos=sum(1 for h in hallazgos if h.severidad == "critico"),
    )
    return salida
