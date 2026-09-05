"""
Costumbres — crear y borrar automatizaciones de Home Assistant, acotadas.

── LA DECISIÓN QUE SOSTIENE TODA LA SEGURIDAD DE ESTE MÓDULO ─────────────────
**El cliente NO manda una automatización. Manda el nombre de una RECETA.**

La forma obvia habría sido aceptar el JSON de la automatización y reenviarlo a
Home Assistant. Eso convierte este endpoint en "ejecutá lo que quieras, para
siempre, sin nadie presente": cualquiera con una sesión podría escribir un
disparador a las 3 de la mañana que abra un portón. Con recetas, la superficie
de ataque no es YAML arbitrario — son las dos recetas de abajo, y nada más.

El cliente elige `aires-noche` y manda dos parámetros. El Mirror construye la
automatización con SUS propios objetivos, sacados de su propio registro de
entidades. El cliente nunca dice qué aparato tocar.

── LA BARRERA: SOLO AMBIENTE ─────────────────────────────────────────────────
Decisión del dueño (2026-09-04): una costumbre puede tocar **luces, clima y
sonido**. NO portones, NO cocheras, NO cerraduras, NO el NVR.

El motivo no es teórico. La receta `amanecer-apagar` apaga circuitos al
amanecer, y en esta casa los circuitos son `switch.*` — el MISMO dominio que
`switch.nvr_motion_detection` (la detección de movimiento, que se acababa de
encender) y que los tres relés de las cocheras. Sin barrera, "apago las luces
que quedaron prendidas" habría apagado la vigilancia de la casa todas las
mañanas.

La barrera vive ACÁ, en el servidor, y no solo en el frontend: el frontend se
puede saltar, este endpoint no. Que además el frontend filtre es defensa en
profundidad, no un reemplazo.

── POR QUÉ REST Y NO WEBSOCKET ───────────────────────────────────────────────
La configuración de una automatización (disparador, condiciones, acciones) no
vive en el estado sino en los archivos de HA, y la única API que los escribe es
`POST /api/config/automation/config/{id}` — REST. El WebSocket no tiene comando
para esto. Después hay que llamar `automation.reload`, que sí es un servicio
normal y va por el camino de siempre.
"""

from __future__ import annotations

import re
from typing import Any

import aiohttp
import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from ha_mirror.auth import require_api_key
from ha_mirror.config import Settings
from ha_mirror.models import leer_atributo

logger = structlog.get_logger(__name__)

router = APIRouter()

#: Prefijo de los ids que crea UniquexCR. Sirve para dos cosas: que el frontend
#: sepa cuáles son nuestras (la entidad `automation.*` publica su `id` en los
#: atributos), y que crear dos veces la misma receta SOBREESCRIBA en vez de
#: dejar dos automatizaciones haciendo lo mismo.
PREFIJO = "uniquexcr_"

TIMEOUT = aiohttp.ClientTimeout(total=20)


# ── La barrera ────────────────────────────────────────────────────────────────

#: Dominios que una costumbre puede tocar. Ambiente y nada más.
#:
#: `cover` y `lock` quedan fuera por decisión explícita del dueño: una costumbre
#: que abre algo es un agujero físico que nadie está mirando.
DOMINIOS_DE_AMBIENTE = frozenset({"light", "switch", "climate", "media_player", "fan"})

#: Lo que NUNCA es ambiente, aunque su dominio lo parezca.
#:
#: Se mira el `entity_id` Y el nombre visible, porque en esta casa las dos cosas
#: delatan por caminos distintos: `switch.nvr_disarming` se delata por el id, y
#: `switch.1002375306_3` ("Entrada Garage-Cochera 2") SOLO por el nombre.
#:
#: Es deliberadamente generosa. Un falso positivo cuesta que una luz llamada
#: "luz del garage" quede fuera de una costumbre; un falso negativo cuesta que
#: la casa se abra sola. No son comparables.
PATRON_PROHIBIDO = re.compile(
    r"nvr|alarm|siren|sirena|disarm|armado|"
    r"gate|port[oó]n|garage|garaje|cochera|"
    r"lock|cerradura|chapa|"
    r"camara|c[aá]mara",
    re.IGNORECASE,
)


def es_de_ambiente(entity_id: str, nombre: str) -> bool:
    """¿Esta entidad puede formar parte de una costumbre?"""
    dominio = entity_id.split(".", 1)[0]
    if dominio not in DOMINIOS_DE_AMBIENTE:
        return False
    return not PATRON_PROHIBIDO.search(f"{entity_id} {nombre}")


def _objetivos(store: Any, dominio: str) -> list[str]:
    """
    Las entidades vivas de un dominio que pasan la barrera.

    Se descartan las que no responden: meter en una costumbre un aparato sin
    señal no hace daño, pero infla el objetivo con cosas que no van a obedecer y
    hace que el conteo que ve el dueño mienta.
    """
    salida: list[str] = []
    for entity_id, estado in store.get_all_states().items():
        if not entity_id.startswith(f"{dominio}."):
            continue
        crudo = getattr(estado, "state", None)
        if crudo in ("unavailable", "unknown", None):
            continue
        # `leer_atributo` y NO `.attributes.get(...)`: `HaState.attributes` es un
        # modelo de Pydantic. Acá importa el doble — el nombre visible es lo
        # ÚNICO que delata a los relés de cochera (`switch.1002375306_3` se
        # llama "Entrada Garage-Cochera 2"), así que leerlo mal deja pasar la
        # barrera entera.
        nombre = str(leer_atributo(estado, "friendly_name") or "")
        if es_de_ambiente(entity_id, nombre):
            salida.append(entity_id)
    return sorted(salida)


# ── Las recetas ───────────────────────────────────────────────────────────────


class ParametrosCostumbre(BaseModel):
    """Lo único que el cliente puede elegir. Todo lo demás lo pone el Mirror."""

    #: "23:00". Solo se usa en las recetas de hora.
    hora: str | None = Field(default=None, pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    #: Grados del clima. El rango es el de los CoolMaster de esta casa.
    grados: int | None = Field(default=None, ge=16, le=30)


class CrearCostumbre(BaseModel):
    receta: str
    parametros: ParametrosCostumbre = ParametrosCostumbre()


class CostumbreCreada(BaseModel):
    id: str
    alias: str
    #: Cuántos aparatos quedaron dentro. El frontend lo muestra: una costumbre
    #: que dice "todos los aires" y toca 15 es distinta de una que toca 2.
    aparatos: int


def _receta_aires_noche(store: Any, p: ParametrosCostumbre) -> dict[str, Any]:
    hora = p.hora or "23:00"
    grados = p.grados if p.grados is not None else 24
    objetivos = _objetivos(store, "climate")
    if not objetivos:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ningún aire está respondiendo, así que la costumbre no tocaría nada.",
        )
    return {
        "alias": f"A las {hora} dejo los aires en {grados} grados",
        "description": "Creada desde UniquexCR.",
        "trigger": [{"platform": "time", "at": f"{hora}:00"}],
        "condition": [],
        "action": [
            {
                "service": "climate.set_temperature",
                "target": {"entity_id": objetivos},
                "data": {"temperature": grados},
            }
        ],
        "mode": "single",
        "_aparatos": len(objetivos),
    }


def _receta_amanecer_apagar(store: Any, _p: ParametrosCostumbre) -> dict[str, Any]:
    interruptores = _objetivos(store, "switch")
    luces = _objetivos(store, "light")
    if not interruptores and not luces:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ningún circuito de ambiente está respondiendo.",
        )
    acciones: list[dict[str, Any]] = []
    if interruptores:
        acciones.append(
            {"service": "switch.turn_off", "target": {"entity_id": interruptores}}
        )
    if luces:
        acciones.append({"service": "light.turn_off", "target": {"entity_id": luces}})
    return {
        "alias": "Cuando amanece, apago las luces que quedaron prendidas",
        "description": "Creada desde UniquexCR. No toca portones, cocheras ni el grabador.",
        "trigger": [{"platform": "sun", "event": "sunrise", "offset": "00:00:00"}],
        "condition": [],
        "action": acciones,
        "mode": "single",
        "_aparatos": len(interruptores) + len(luces),
    }


#: Las recetas que existen. Agregar una es agregar una función acá — no hay
#: forma de que el cliente invente una.
RECETAS = {
    "aires-noche": _receta_aires_noche,
    "amanecer-apagar": _receta_amanecer_apagar,
}


# ── Escritura contra HA ───────────────────────────────────────────────────────


async def _escribir(settings: Settings, id_config: str, cuerpo: dict[str, Any]) -> None:
    token = settings.get_ha_token()
    url = f"{settings.ha_http_url}/api/config/automation/config/{id_config}"
    async with aiohttp.ClientSession(timeout=TIMEOUT) as sesion, sesion.post(
        url,
        json=cuerpo,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    ) as r:
        if r.status >= 400:
            texto = (await r.text())[:300]
            logger.warning("costumbre.escritura_fallo", status=r.status, cuerpo=texto)
            # 401/403 casi siempre = el token del add-on no es de
            # administración. Se dice claro para no mandar a nadie a
            # depurar la receta cuando el problema es el permiso.
            if r.status in (401, 403):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Home Assistant no permite escribir automatizaciones con este token.",
                )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Home Assistant rechazó la costumbre.",
            )


async def _borrar(settings: Settings, id_config: str) -> bool:
    token = settings.get_ha_token()
    url = f"{settings.ha_http_url}/api/config/automation/config/{id_config}"
    async with aiohttp.ClientSession(timeout=TIMEOUT) as sesion, sesion.delete(
        url, headers={"Authorization": f"Bearer {token}"}
    ) as r:
        if r.status == 404:
            return False
        if r.status >= 400:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Home Assistant rechazó el borrado.",
            )
    return True


async def _recargar(request: Request) -> None:
    """
    `automation.reload` — sin esto, lo escrito no corre hasta el próximo
    reinicio de Home Assistant.

    Si falla, NO se deshace la escritura ni se devuelve error: la costumbre
    quedó guardada y va a empezar a andar sola. Decirle al dueño que falló
    cuando en realidad se creó sería peor que el silencio.
    """
    upstream = getattr(request.app.state, "upstream", None)
    if upstream is None:
        return
    try:
        await upstream.send_service_call("automation", "reload", {})
    except Exception as exc:  # noqa: BLE001
        logger.warning("costumbre.reload_fallo", error=str(exc))


# ── Endpoints ─────────────────────────────────────────────────────────────────


@router.post(
    "/api/costumbres",
    response_model=CostumbreCreada,
    status_code=status.HTTP_201_CREATED,
    summary="Crea una costumbre a partir de una receta conocida",
)
async def crear_costumbre(
    request: Request,
    cuerpo: CrearCostumbre,
    _: None = Depends(require_api_key),
) -> CostumbreCreada:
    # La configuración sale de `app.state`, como en el resto de los routers de
    # este Mirror. Con `Depends(get_settings)` en el default del argumento, ruff
    # marca B008 con razón: la llamada se evalúa una sola vez al importar.
    settings: Settings = request.app.state.settings
    constructor = RECETAS.get(cuerpo.receta)
    if constructor is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"No existe la receta '{cuerpo.receta}'.",
        )

    plan = constructor(request.app.state.store, cuerpo.parametros)
    aparatos = int(plan.pop("_aparatos"))
    id_config = f"{PREFIJO}{cuerpo.receta}"

    await _escribir(settings, id_config, plan)
    await _recargar(request)

    logger.info(
        "costumbre.creada", receta=cuerpo.receta, id=id_config, aparatos=aparatos
    )
    return CostumbreCreada(id=id_config, alias=str(plan["alias"]), aparatos=aparatos)


@router.delete(
    "/api/costumbres/{id_config}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Borra una costumbre creada por UniquexCR",
)
async def borrar_costumbre(
    request: Request,
    id_config: str,
    _: None = Depends(require_api_key),
) -> None:
    """
    Solo borra lo que creó UniquexCR.

    El prefijo no es cosmético: sin él, este endpoint podría borrar cualquier
    automatización que el instalador haya escrito a mano en Home Assistant.
    """
    if not id_config.startswith(PREFIJO):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Esta costumbre no la creó UniquexCR.",
        )
    settings: Settings = request.app.state.settings
    existia = await _borrar(settings, id_config)
    if not existia:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Esa costumbre ya no está."
        )
    await _recargar(request)
    logger.info("costumbre.borrada", id=id_config)
