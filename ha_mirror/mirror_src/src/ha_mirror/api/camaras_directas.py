"""
/api/cameras/directas — sumar una cámara por su dirección, desde la app.

La lógica y las reglas están en `ha_mirror/camaras_directas.py`. Acá solo se
aplican en orden: validar, respetar la pausa por fallos, probar, y recién si dio
video guardar y registrar.

── QUÉ CONTESTA ─────────────────────────────────────────────────────────────
- 201: la cámara dio video y quedó agregada.
- 400 `datos_invalidos`: la dirección, la marca o el texto no sirven.
- 409 `ya_agregada`: esa cámara ya está.
- 422 con el `codigo` de la prueba (`clave_incorrecta`, `no_responde`, …) y un
  `mensaje` para mostrar tal cual. Nada se guardó.
- 429 `en_pausa`: esa dirección falló la clave varias veces; `espera_s` dice
  cuánto falta. Es para que la cámara no se bloquee.
- 503: la casa no tiene el servicio de video encendido.

El error va en `detail` como objeto ({codigo, mensaje}) para que la app pueda
decidir qué hacer —frenar la fila, ofrecer corregir— sin leer el texto.
"""

from __future__ import annotations

import asyncio
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from ha_mirror.auth import require_api_key
from ha_mirror.camaras_directas import (
    MARCAS,
    MENSAJES,
    PUERTOS_RTSP,
    LimiteDeIntentos,
    Resultado,
    TopeDePruebas,
    armar_fuente,
    con_otra_ruta,
    probar,
    slug,
    validar_direccion,
)
from ha_mirror.sondeo_canales import SondeoCanales

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/cameras/directas", tags=["cameras"])


class AltaDirecta(BaseModel):
    """Una cámara. Nunca una lista: ver la regla 1 de `camaras_directas.py`."""

    nombre: str = Field(min_length=1, max_length=60)
    ip: str = Field(min_length=7, max_length=15)
    marca: str = Field(min_length=1, max_length=20)
    usuario: str = Field(default="admin", min_length=1, max_length=64)
    clave: str = Field(min_length=1, max_length=128)
    ruta: str | None = Field(default=None, max_length=200)
    puerto: int = Field(default=554, description=f"Uno de {sorted(PUERTOS_RTSP)}.")


class CamaraDirecta(BaseModel):
    """Lo que la app ve de una cámara. Sin usuario ni clave, a propósito."""

    entity_id: str
    label: str
    ip: str
    marca: str


def _error(codigo_http: int, codigo: str, mensaje: str, **extra: Any) -> HTTPException:
    return HTTPException(
        status_code=codigo_http, detail={"codigo": codigo, "mensaje": mensaje, **extra}
    )


def _limite(request: Request) -> LimiteDeIntentos:
    estado = request.app.state
    if getattr(estado, "limite_camaras_directas", None) is None:
        estado.limite_camaras_directas = LimiteDeIntentos()
    return estado.limite_camaras_directas


def _tope(request: Request) -> TopeDePruebas:
    estado = request.app.state
    if getattr(estado, "tope_camaras_directas", None) is None:
        estado.tope_camaras_directas = TopeDePruebas()
    return estado.tope_camaras_directas


def _candado(request: Request) -> asyncio.Lock:
    """Una prueba a la vez en toda la caja: nunca dos cámaras en paralelo."""
    estado = request.app.state
    if getattr(estado, "candado_camaras_directas", None) is None:
        estado.candado_camaras_directas = asyncio.Lock()
    return estado.candado_camaras_directas


def _etiquetas(request: Request) -> dict[str, str]:
    estado = request.app.state
    if getattr(estado, "etiquetas_camaras", None) is None:
        estado.etiquetas_camaras = {}
    return estado.etiquetas_camaras


def _sondeo(cliente: Any) -> SondeoCanales:
    return SondeoCanales(
        base_url=cliente._go2rtc_base_url,  # noqa: SLF001
        auth=cliente._go2rtc_auth,  # noqa: SLF001
        session=cliente._require_session(),  # noqa: SLF001
    )


@router.get("", response_model=list[CamaraDirecta], summary="Cámaras sumadas por dirección")
async def listar(request: Request, _: None = Depends(require_api_key)) -> list[CamaraDirecta]:
    db = request.app.state.db
    tenant = int(getattr(request.app.state.settings, "tenant_id", 1))
    return [
        CamaraDirecta(entity_id=c["entity_id"], label=c["label"], ip=c["ip"], marca=c["marca"])
        for c in await db.listar_camaras_directas(tenant)
    ]


@router.post(
    "",
    response_model=CamaraDirecta,
    status_code=status.HTTP_201_CREATED,
    summary="Prueba una cámara por su dirección y, si da video, la agrega",
)
async def agregar(
    request: Request, cuerpo: AltaDirecta, _: None = Depends(require_api_key)
) -> CamaraDirecta:
    cliente = getattr(request.app.state, "camera_media", None)
    if cliente is None or not getattr(cliente, "webrtc_enabled", False):
        raise _error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "sin_video",
            "Esta casa todavía no tiene el servicio de video encendido.",
        )

    marca = cuerpo.marca.strip().lower()
    if marca not in MARCAS:
        raise _error(status.HTTP_400_BAD_REQUEST, "datos_invalidos", "Marca desconocida.")
    try:
        ip = validar_direccion(cuerpo.ip)
        fuente = armar_fuente(
            ip=ip,
            marca=marca,
            usuario=cuerpo.usuario,
            clave=cuerpo.clave,
            ruta=cuerpo.ruta,
            puerto=cuerpo.puerto,
        )
    except ValueError as exc:
        raise _error(status.HTTP_400_BAD_REQUEST, "datos_invalidos", str(exc)) from None

    db = request.app.state.db
    tenant = int(getattr(request.app.state.settings, "tenant_id", 1))

    async with _candado(request):
        # Todo lo que decide adentro del candado: leído afuera, dos pedidos
        # seguidos con la misma dirección pasaban los dos el chequeo de
        # repetidas y la cámara quedaba agregada dos veces.
        return await _probar_y_agregar(request, cuerpo, cliente, db, tenant, ip, marca, fuente)


async def _probar_y_agregar(
    request: Request,
    cuerpo: AltaDirecta,
    cliente: Any,
    db: Any,
    tenant: int,
    ip: str,
    marca: str,
    fuente: str,
) -> CamaraDirecta:
    actuales = await db.listar_camaras_directas(tenant)
    repetida = next((c for c in actuales if c["ip"] == ip), None)
    if repetida is not None:
        raise _error(
            status.HTTP_409_CONFLICT,
            "ya_agregada",
            f"Esa cámara ya está agregada como «{repetida['label']}».",
        )

    tope = _tope(request)
    espera_tope = tope.espera()
    if espera_tope:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "codigo": "demasiadas_pruebas",
                "mensaje": (
                    "Se probaron muchas cámaras en poco rato. Esperá "
                    f"{max(1, round(espera_tope / 60))} minutos y seguí con las que faltan."
                ),
                "espera_s": espera_tope,
            },
            headers={"Retry-After": str(espera_tope)},
        )

    limite = _limite(request)
    espera = limite.espera(ip)
    if espera:
        minutos = max(1, round(espera / 60))
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "codigo": "en_pausa",
                "mensaje": (
                    "Esa cámara rechazó la contraseña varias veces. Esperá "
                    f"{'un minuto' if minutos == 1 else f'{minutos} minutos'} antes de "
                    "volver a probar: algunas cámaras se bloquean si se insiste."
                ),
                "espera_s": espera,
            },
            headers={"Retry-After": str(espera)},
        )

    tope.anotar()
    resultado = await probar(fuente)
    # Solo ante "esa ruta no existe" se prueba la otra ruta de la misma
    # marca. Nunca después de un 401: ahí la clave está mal y se frena.
    if resultado is Resultado.RUTA_INCORRECTA:
        alternativa = con_otra_ruta(fuente, marca)
        if alternativa is not None:
            tope.anotar()
            segundo = await probar(alternativa)
            if segundo in (Resultado.OK, Resultado.CLAVE_INCORRECTA):
                resultado = segundo
                if segundo is Resultado.OK:
                    fuente = alternativa

    if resultado is Resultado.CLAVE_INCORRECTA:
        limite.fallo(ip)
        logger.info("camara_directa.clave_incorrecta", ip=ip, marca=marca)
        raise _error(422, resultado.value, MENSAJES[resultado])
    if resultado is not Resultado.OK:
        logger.info("camara_directa.sin_video", ip=ip, marca=marca, resultado=resultado.value)
        raise _error(422, resultado.value, MENSAJES[resultado])
    limite.acierto(ip)

    # Nombres únicos: no pisar una cámara que ya exista, de ningún origen.
    ocupados = {c["entity_id"] for c in actuales} | set(cliente.list_stream_entities())
    base = slug(cuerpo.nombre)
    entity_id = f"camera.{base}"
    n = 2
    while entity_id in ocupados:
        entity_id = f"camera.{base}_{n}"
        n += 1
    stream = f"uq_dir_{entity_id.split('.', 1)[1]}"
    label = cuerpo.nombre.strip()

    try:
        await _sondeo(cliente).registrar_fuente(stream, fuente)
    except Exception:  # noqa: BLE001 — el texto del error puede traer la dirección
        logger.warning("camara_directa.go2rtc_no_registro", stream=stream)
        raise _error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "sin_video",
            "La cámara funciona, pero el servicio de video de la caja no la aceptó. Probá de nuevo.",
        ) from None

    await db.guardar_camara_directa(
        entity_id=entity_id,
        stream_name=stream,
        label=label,
        ip=ip,
        marca=marca,
        fuente=fuente,
        tenant_id=tenant,
    )
    cliente.agregar_stream(entity_id, stream)
    _etiquetas(request)[entity_id] = label
    logger.info("camara_directa.alta", entity_id=entity_id, ip=ip, marca=marca)
    return CamaraDirecta(entity_id=entity_id, label=label, ip=ip, marca=marca)


@router.delete(
    "/{entity_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Quita una cámara sumada por dirección",
)
async def quitar(entity_id: str, request: Request, _: None = Depends(require_api_key)) -> None:
    db = request.app.state.db
    tenant = int(getattr(request.app.state.settings, "tenant_id", 1))
    borrada = await db.borrar_camara_directa(entity_id, tenant)
    if borrada is None:
        raise _error(status.HTTP_404_NOT_FOUND, "no_existe", "Esa cámara no está agregada.")
    cliente = getattr(request.app.state, "camera_media", None)
    if cliente is not None:
        cliente.quitar_stream(entity_id)
        if getattr(cliente, "webrtc_enabled", False):
            await _sondeo(cliente).borrar_stream(borrada["stream_name"])
    _etiquetas(request).pop(entity_id, None)
    logger.info("camara_directa.baja", entity_id=entity_id)
