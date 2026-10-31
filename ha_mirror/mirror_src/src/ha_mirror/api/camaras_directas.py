"""
/api/cameras/directas — sumar cámaras por su dirección, desde la app.

La lógica y las reglas están en `ha_mirror/camaras_directas.py`. Acá solo se
aplican en orden: validar, respetar la pausa por fallos, probar, y recién si dio
video guardar y registrar.

── DOS FORMAS ───────────────────────────────────────────────────────────────
- `POST ""` — UNA cámara suelta. Contesta cuando terminó de probarla.
- `POST /grabador` — un GRABADOR entero. Sus canales se prueban de a uno, en
  segundo plano, porque 16 o 32 canales no caben en lo que Cloudflare deja
  esperar (100 s). Contesta 202 con un `trabajo`; el avance se pregunta en
  `GET /grabador/{trabajo}`.

── QUÉ CONTESTA ─────────────────────────────────────────────────────────────
- 201 / 202: la cámara entró / el recorrido arrancó.
- 400 `datos_invalidos`: la dirección, la marca o el texto no sirven.
- 409 `ya_agregada`: esa cámara ya está. 409 `ocupado`: la casa está
  recorriendo un grabador; una prueba a la vez, siempre.
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
import re
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from ha_mirror.auth import require_api_key
from ha_mirror.camaras_directas import (
    CANALES_POR_GRABADOR,
    MARCAS,
    MARCAS_GRABADOR,
    MENSAJES,
    PUERTOS_RTSP,
    LimiteDeIntentos,
    Resultado,
    TopeDePruebas,
    armar_fuente,
    con_otra_ruta,
    destino,
    probar,
    ruta_de_canal,
    slug,
    validar_direccion,
)
from ha_mirror.sondeo_canales import SondeoCanales

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/cameras/directas", tags=["cameras"])

#: Lo que se espera a cada canal de un grabador. Menos que una cámara suelta:
#: el grabador está en la misma red y un canal vacío contesta enseguida.
TIMEOUT_CANAL_S = 8.0

#: Un recorrido terminado se puede consultar este rato; después se olvida.
_RECUERDO_S = 60 * 60

_RE_TRABAJO = re.compile(r"^[A-Za-z0-9_-]{8,32}$")

MENSAJE_CLAVE_GRABADOR = (
    "El grabador rechazó el usuario o la contraseña. No se probó ningún otro "
    "canal: revisala y volvé a intentar."
)
MENSAJE_GRABADOR_NO_RESPONDE = (
    "El grabador no respondió en esa dirección. Revisá que esté encendido y que la "
    "dirección sea la correcta."
)
MENSAJE_GRABADOR_ERROR = (
    "Algo falló mientras se recorría el grabador. Las cámaras que ya entraron "
    "quedaron agregadas; probá de nuevo para sumar el resto."
)


class AltaDirecta(BaseModel):
    """Una cámara. Nunca una lista: ver la regla 1 de `camaras_directas.py`."""

    nombre: str = Field(min_length=1, max_length=60)
    ip: str = Field(min_length=7, max_length=15)
    marca: str = Field(min_length=1, max_length=20)
    usuario: str = Field(default="admin", min_length=1, max_length=64)
    clave: str = Field(min_length=1, max_length=128)
    ruta: str | None = Field(default=None, max_length=200)
    puerto: int = Field(default=554, description=f"Uno de {sorted(PUERTOS_RTSP)}.")


class AltaGrabador(BaseModel):
    """Un grabador. Una dirección y una clave; los canales los recorre la casa."""

    nombre: str = Field(min_length=1, max_length=50)
    ip: str = Field(min_length=7, max_length=15)
    marca: str = Field(min_length=1, max_length=20)
    usuario: str = Field(default="admin", min_length=1, max_length=64)
    clave: str = Field(min_length=1, max_length=128)
    canales: int = Field(default=16, description=f"Uno de {list(CANALES_POR_GRABADOR)}.")


class CamaraDirecta(BaseModel):
    """Lo que la app ve de una cámara. Sin usuario ni clave, a propósito."""

    entity_id: str
    label: str
    ip: str
    marca: str


class CamaraDeGrabador(BaseModel):
    entity_id: str
    label: str
    canal: int


class AvanceGrabador(BaseModel):
    """Cómo va un recorrido. Sin la dirección de video ni la clave."""

    trabajo: str
    estado: str  # probando | listo | clave_incorrecta | no_responde | error
    nombre: str
    canal: int
    total: int
    agregadas: list[CamaraDeGrabador]
    vacios: list[int]
    ya_estaban: list[int]
    mensaje: str | None = None


@dataclass
class _Trabajo:
    id: str
    nombre: str
    total: int
    estado: str = "probando"
    canal: int = 0
    agregadas: list[CamaraDeGrabador] = field(default_factory=list)
    vacios: list[int] = field(default_factory=list)
    ya_estaban: list[int] = field(default_factory=list)
    mensaje: str | None = None
    terminado: float | None = None
    # Se guarda la tarea para que el recolector no la mate a mitad de camino.
    tarea: asyncio.Task[None] | None = None

    def avance(self) -> AvanceGrabador:
        return AvanceGrabador(
            trabajo=self.id,
            estado=self.estado,
            nombre=self.nombre,
            canal=self.canal,
            total=self.total,
            agregadas=list(self.agregadas),
            vacios=list(self.vacios),
            ya_estaban=list(self.ya_estaban),
            mensaje=self.mensaje,
        )


def _error(codigo_http: int, codigo: str, mensaje: str, **extra: Any) -> HTTPException:
    return HTTPException(
        status_code=codigo_http, detail={"codigo": codigo, "mensaje": mensaje, **extra}
    )


def _en_pausa(espera: int) -> HTTPException:
    minutos = max(1, round(espera / 60))
    return HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail={
            "codigo": "en_pausa",
            "mensaje": (
                "Esa dirección rechazó la contraseña varias veces. Esperá "
                f"{'un minuto' if minutos == 1 else f'{minutos} minutos'} antes de "
                "volver a probar: algunos equipos se bloquean si se insiste."
            ),
            "espera_s": espera,
        },
        headers={"Retry-After": str(espera)},
    )


def _demasiadas(espera: int) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail={
            "codigo": "demasiadas_pruebas",
            "mensaje": (
                "Se probaron muchas cámaras en poco rato. Esperá "
                f"{max(1, round(espera / 60))} minutos y seguí con las que faltan."
            ),
            "espera_s": espera,
        },
        headers={"Retry-After": str(espera)},
    )


def _de_estado(estado: Any, nombre: str, fabrica: Any) -> Any:
    if getattr(estado, nombre, None) is None:
        setattr(estado, nombre, fabrica())
    return getattr(estado, nombre)


def _limite(estado: Any) -> LimiteDeIntentos:
    return _de_estado(estado, "limite_camaras_directas", LimiteDeIntentos)  # type: ignore[no-any-return]


def _tope(estado: Any) -> TopeDePruebas:
    return _de_estado(estado, "tope_camaras_directas", TopeDePruebas)  # type: ignore[no-any-return]


def _candado(estado: Any) -> asyncio.Lock:
    """Una prueba a la vez en toda la caja: nunca dos cámaras en paralelo."""
    return _de_estado(estado, "candado_camaras_directas", asyncio.Lock)  # type: ignore[no-any-return]


def _etiquetas(estado: Any) -> dict[str, str]:
    return _de_estado(estado, "etiquetas_camaras", dict)  # type: ignore[no-any-return]


def _trabajos(estado: Any) -> dict[str, _Trabajo]:
    return _de_estado(estado, "trabajos_grabador", dict)  # type: ignore[no-any-return]


def _recorriendo(estado: Any) -> bool:
    return any(t.estado == "probando" for t in _trabajos(estado).values())


def _sondeo(cliente: Any) -> SondeoCanales:
    return SondeoCanales(
        base_url=cliente._go2rtc_base_url,  # noqa: SLF001
        auth=cliente._go2rtc_auth,  # noqa: SLF001
        session=cliente._require_session(),  # noqa: SLF001
    )


def _cliente_con_video(estado: Any) -> Any:
    cliente = getattr(estado, "camera_media", None)
    if cliente is None or not getattr(cliente, "webrtc_enabled", False):
        raise _error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "sin_video",
            "Esta casa todavía no tiene el servicio de video encendido.",
        )
    return cliente


def _tenant(estado: Any) -> int:
    return int(getattr(estado.settings, "tenant_id", 1))


async def _registrar(
    estado: Any,
    cliente: Any,
    *,
    nombre: str,
    ip: str,
    marca: str,
    fuente: str,
    ocupados: set[str],
) -> CamaraDirecta:
    """Guarda y registra una cámara que YA dio video. `ocupados` se actualiza."""
    base = slug(nombre)
    entity_id = f"camera.{base}"
    n = 2
    while entity_id in ocupados:
        entity_id = f"camera.{base}_{n}"
        n += 1
    stream = f"uq_dir_{entity_id.split('.', 1)[1]}"
    label = nombre.strip()

    try:
        await _sondeo(cliente).registrar_fuente(stream, fuente)
    except Exception:  # noqa: BLE001 — el texto del error puede traer la dirección
        logger.warning("camara_directa.go2rtc_no_registro", stream=stream)
        raise _error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "sin_video",
            "La cámara funciona, pero el servicio de video de la caja no la aceptó. Probá de nuevo.",
        ) from None

    await estado.db.guardar_camara_directa(
        entity_id=entity_id,
        stream_name=stream,
        label=label,
        ip=ip,
        marca=marca,
        fuente=fuente,
        tenant_id=_tenant(estado),
    )
    cliente.agregar_stream(entity_id, stream)
    _etiquetas(estado)[entity_id] = label
    ocupados.add(entity_id)
    logger.info("camara_directa.alta", entity_id=entity_id, ip=ip, marca=marca)
    return CamaraDirecta(entity_id=entity_id, label=label, ip=ip, marca=marca)


async def _ya_cargadas(estado: Any, cliente: Any) -> tuple[dict[str, str], set[str]]:
    """`destino → label` de lo agregado, y los entity_id que ya existen."""
    actuales = await estado.db.listar_camaras_directas(_tenant(estado), con_fuente=True)
    destinos = {destino(c["fuente"]): c["label"] for c in actuales}
    ocupados = {c["entity_id"] for c in actuales} | set(cliente.list_stream_entities())
    return destinos, ocupados


# ── Una cámara ──────────────────────────────────────────────────────────────


@router.get("", response_model=list[CamaraDirecta], summary="Cámaras sumadas por dirección")
async def listar(request: Request, _: None = Depends(require_api_key)) -> list[CamaraDirecta]:
    estado = request.app.state
    return [
        CamaraDirecta(entity_id=c["entity_id"], label=c["label"], ip=c["ip"], marca=c["marca"])
        for c in await estado.db.listar_camaras_directas(_tenant(estado))
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
    estado = request.app.state
    cliente = _cliente_con_video(estado)

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

    if _recorriendo(estado):
        raise _error(
            status.HTTP_409_CONFLICT,
            "ocupado",
            "La casa está recorriendo un grabador. Esperá a que termine para sumar más cámaras.",
        )

    async with _candado(estado):
        # Todo lo que decide adentro del candado: leído afuera, dos pedidos
        # seguidos con la misma dirección pasaban los dos el chequeo de
        # repetidas y la cámara quedaba agregada dos veces.
        return await _probar_y_agregar(estado, cliente, cuerpo, ip, marca, fuente)


async def _probar_y_agregar(
    estado: Any,
    cliente: Any,
    cuerpo: AltaDirecta,
    ip: str,
    marca: str,
    fuente: str,
) -> CamaraDirecta:
    destinos, ocupados = await _ya_cargadas(estado, cliente)
    # 🔪 Repetida es "misma dirección Y misma ruta", no "misma dirección": un
    # grabador tiene una dirección y muchas cámaras.
    repetida = destinos.get(destino(fuente))
    if repetida is not None:
        raise _error(
            status.HTTP_409_CONFLICT,
            "ya_agregada",
            f"Esa cámara ya está agregada como «{repetida}».",
        )

    tope = _tope(estado)
    espera_tope = tope.espera()
    if espera_tope:
        raise _demasiadas(espera_tope)

    limite = _limite(estado)
    espera = limite.espera(ip)
    if espera:
        raise _en_pausa(espera)

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

    return await _registrar(
        estado,
        cliente,
        nombre=cuerpo.nombre,
        ip=ip,
        marca=marca,
        fuente=fuente,
        ocupados=ocupados,
    )


# ── Un grabador entero ──────────────────────────────────────────────────────


@router.post(
    "/grabador",
    response_model=AvanceGrabador,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Recorre los canales de un grabador y agrega los que dan video",
)
async def agregar_grabador(
    request: Request, cuerpo: AltaGrabador, _: None = Depends(require_api_key)
) -> AvanceGrabador:
    """
    🔪 LAS MISMAS REGLAS QUE UNA CÁMARA, Y UNA MÁS.
    - Si el canal 1 rechaza la clave, se frena ahí: no se prueba ningún otro
      canal. Un grabador con la clave mal probada 32 veces se bloquea.
    - Si el canal 1 no contesta, tampoco se sigue: es el grabador entero el que
      no está, y recorrer 32 canales sería esperar 32 veces lo mismo.
    - El recorrido cuenta como UNA prueba en el tope por hora: es una dirección.
      El tope existe para que nadie recorra la red, no los canales de un equipo.
    """
    estado = request.app.state
    cliente = _cliente_con_video(estado)

    marca = cuerpo.marca.strip().lower()
    if marca not in MARCAS_GRABADOR:
        raise _error(
            status.HTTP_400_BAD_REQUEST,
            "datos_invalidos",
            "Por ahora se pueden recorrer grabadores Dahua, Hikvision y Reolink.",
        )
    if cuerpo.canales not in CANALES_POR_GRABADOR:
        raise _error(
            status.HTTP_400_BAD_REQUEST, "datos_invalidos", "Cantidad de canales inválida."
        )
    try:
        ip = validar_direccion(cuerpo.ip)
        # Valida usuario y clave con las mismas reglas que una cámara.
        armar_fuente(
            ip=ip,
            marca=marca,
            usuario=cuerpo.usuario,
            clave=cuerpo.clave,
            ruta=ruta_de_canal(marca, 1),
        )
    except ValueError as exc:
        raise _error(status.HTTP_400_BAD_REQUEST, "datos_invalidos", str(exc)) from None

    if _recorriendo(estado):
        raise _error(
            status.HTTP_409_CONFLICT,
            "ocupado",
            "La casa ya está recorriendo un grabador. Esperá a que termine.",
        )

    limite = _limite(estado)
    espera = limite.espera(ip)
    if espera:
        raise _en_pausa(espera)
    tope = _tope(estado)
    espera_tope = tope.espera()
    if espera_tope:
        raise _demasiadas(espera_tope)
    tope.anotar()

    trabajos = _trabajos(estado)
    ahora = time.monotonic()
    for clave_vieja in [
        k for k, t in trabajos.items() if t.terminado and ahora - t.terminado > _RECUERDO_S
    ]:
        trabajos.pop(clave_vieja, None)

    t = _Trabajo(id=secrets.token_urlsafe(12), nombre=cuerpo.nombre.strip(), total=cuerpo.canales)
    trabajos[t.id] = t
    t.tarea = asyncio.create_task(
        _recorrer(
            estado, cliente, t, ip=ip, marca=marca, usuario=cuerpo.usuario, clave=cuerpo.clave
        )
    )
    logger.info("grabador.recorrido_arranca", trabajo=t.id, ip=ip, marca=marca, canales=t.total)
    return t.avance()


@router.get(
    "/grabador/{trabajo}",
    response_model=AvanceGrabador,
    summary="Cómo va el recorrido de un grabador",
)
async def avance_grabador(
    trabajo: str, request: Request, _: None = Depends(require_api_key)
) -> AvanceGrabador:
    t = _trabajos(request.app.state).get(trabajo) if _RE_TRABAJO.fullmatch(trabajo) else None
    if t is None:
        raise _error(
            status.HTTP_404_NOT_FOUND, "no_existe", "Ese recorrido no existe o ya se olvidó."
        )
    return t.avance()


async def _recorrer(
    estado: Any,
    cliente: Any,
    t: _Trabajo,
    *,
    ip: str,
    marca: str,
    usuario: str,
    clave: str,
) -> None:
    """
    Prueba los canales de a uno y agrega los que dan video.

    🔪 La dirección de video lleva la clave: no aparece en ningún registro ni en
    `AvanceGrabador`. Un error inesperado se anota sin `exc_info` por lo mismo.
    """
    limite = _limite(estado)
    try:
        async with _candado(estado):
            destinos, ocupados = await _ya_cargadas(estado, cliente)
            for canal in range(1, t.total + 1):
                t.canal = canal
                fuente = armar_fuente(
                    ip=ip,
                    marca=marca,
                    usuario=usuario,
                    clave=clave,
                    ruta=ruta_de_canal(marca, canal),
                )
                if destino(fuente) in destinos:
                    t.ya_estaban.append(canal)
                    continue
                resultado = await probar(fuente, timeout_s=TIMEOUT_CANAL_S)
                if resultado is Resultado.CLAVE_INCORRECTA:
                    limite.fallo(ip)
                    t.estado = "clave_incorrecta"
                    t.mensaje = MENSAJE_CLAVE_GRABADOR
                    logger.info("grabador.clave_incorrecta", trabajo=t.id, ip=ip, canal=canal)
                    return
                if resultado is Resultado.NO_RESPONDE and canal == 1:
                    t.estado = "no_responde"
                    t.mensaje = MENSAJE_GRABADOR_NO_RESPONDE
                    return
                if resultado is not Resultado.OK:
                    t.vacios.append(canal)
                    continue
                limite.acierto(ip)
                cam = await _registrar(
                    estado,
                    cliente,
                    nombre=f"{t.nombre} {canal}",
                    ip=ip,
                    marca=marca,
                    fuente=fuente,
                    ocupados=ocupados,
                )
                destinos[destino(fuente)] = cam.label
                t.agregadas.append(
                    CamaraDeGrabador(entity_id=cam.entity_id, label=cam.label, canal=canal)
                )
            t.estado = "listo"
    except Exception:  # noqa: BLE001 — sin exc_info: el error puede citar la dirección
        logger.warning("grabador.recorrido_fallo", trabajo=t.id, canal=t.canal)
        t.estado = "error"
        t.mensaje = MENSAJE_GRABADOR_ERROR
    finally:
        t.terminado = time.monotonic()
        logger.info(
            "grabador.recorrido_termina",
            trabajo=t.id,
            estado=t.estado,
            agregadas=len(t.agregadas),
            vacios=len(t.vacios),
        )


# ── Baja ────────────────────────────────────────────────────────────────────


@router.delete(
    "/{entity_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Quita una cámara sumada por dirección",
)
async def quitar(entity_id: str, request: Request, _: None = Depends(require_api_key)) -> None:
    estado = request.app.state
    borrada = await estado.db.borrar_camara_directa(entity_id, _tenant(estado))
    if borrada is None:
        raise _error(status.HTTP_404_NOT_FOUND, "no_existe", "Esa cámara no está agregada.")
    cliente = getattr(estado, "camera_media", None)
    if cliente is not None:
        cliente.quitar_stream(entity_id)
        if getattr(cliente, "webrtc_enabled", False):
            await _sondeo(cliente).borrar_stream(borrada["stream_name"])
    _etiquetas(estado).pop(entity_id, None)
    logger.info("camara_directa.baja", entity_id=entity_id)
