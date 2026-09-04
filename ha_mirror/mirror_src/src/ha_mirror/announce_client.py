"""
Cliente de anuncio saliente — la caja avisa a la plataforma que existe y está lista.

POR QUÉ EXISTE
--------------
Durante el emparejamiento el túnel todavía no existe. La plataforma NO puede
llamar a la caja: el flujo tiene que arrancar desde la caja hacia afuera.

Este módulo implementa ese anuncio periódico: un POST firmado al endpoint
`/api/boxes/announce` de la plataforma. El backend lo usa para saber qué cajas
están encendidas y disponibles para ser reclamadas. Cuando alguien escanea el
QR e ingresa el código, el backend responde `paired: true` en el próximo anuncio.

El loop NO se detiene ahí: baja a un latido lento y sigue. Es lo que permite que
la caja se entere si la plataforma la libera y vuelva sola al estado activable,
sin que nadie tenga que entrar a Home Assistant a reiniciar el add-on. Ver
_HEARTBEAT_INTERVAL y _DESEMPAREJOS_PARA_CONFIRMAR.

El módulo corre como task de background supervisado en el lifespan, igual al
patrón de HAUpstream.run_forever(). Si falla (por red caída, servicio de la
plataforma caído, error de código), la caja sigue dando luces y cámaras —
el mismo criterio que ensure_identity() en el lifespan.

INTERVALO NORMAL: 120 SEGUNDOS
-------------------------------
La plataforma considera "viva" a una caja que se anunció en los últimos
600 segundos (announce_freshness_seconds). Con un intervalo de 120s, el backend
vería al menos un anuncio exitoso en cualquier ventana de 600s — incluso si 4
anuncios seguidos fallan (4 × 120 = 480s todavía dentro del límite). Un quinto
fallo consecutivo cruzaría el umbral, pero para ese momento la red tendría que
estar rota durante ~10 minutos, lo cual ya es un evento mayor.

BACKOFF ANTE ERRORES: base=5s, cap=120s, jitter ±50%
-----------------------------------------------------
Misma fórmula que ha_upstream.py: sleep = min(backoff × uniform(0.5, 1.5), cap).
Los parámetros difieren porque los timescales son distintos:

  - Base 5s (vs 1s en ha_upstream): el anuncio no es tiempo-real. No hay ninguna
    ventaja en reintentar en 1s si la plataforma está caída o la red no da.

  - Cap 120s = el intervalo normal sano. No tiene sentido esperar MÁS que eso
    entre reintentos, porque 120s ya es lo que la caja espera cuando todo va bien.

  - El backoff se reinicia en cada anuncio exitoso (no por "sesión sana" como en
    ha_upstream). Acá no hay sesión: cada anuncio es un HTTP request puntual,
    independiente del anterior.

LLAVE PRIVADA
-------------
La llave privada NUNCA sale de la caja. Este módulo la recibe en memoria como
parámetro y la usa solo para `firmar()` localmente. Lo que viaja al backend es
la firma Ed25519 y la llave PÚBLICA — nunca la privada.

ERRORES ESPECIALES
------------------
  - 401: firma inválida — bug local (codificación o derivación). Se loguea fuerte
    pero el loop continúa: podría ser un problema transitorio del backend. Si el
    error persiste hay que revisar _calcular_claim_code_hash() y firmar().

  - 409: la llave pública no coincide con la registrada para este device_id.
    Posible clonación de caja. El loop se DETIENE sin reintentar: una caja
    clonada que sigue anunciando genera ruido sin poder emparejarse, y el
    incidente requiere intervención humana.
"""

from __future__ import annotations

import asyncio
import base64
import json as _json
import random
import secrets
import struct
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import aiohttp
import structlog
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from ha_mirror.device_identity import (
    DeviceIdentity,
    borrar_secreto_de_plataforma,
    cargar_clave_mirror,
    cargar_token_tunel,
    derivar_claim_code,
    firmar,
    guardar_clave_mirror,
    guardar_token_tunel,
    hash_claim_code,
)

if TYPE_CHECKING:
    from pathlib import Path

    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    from ha_mirror.db import Database
    from ha_mirror.ha_upstream import HAUpstream

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# --- Constantes de timing ---

# Intervalo entre anuncios cuando la caja está sana (sin errores de red).
# Ver sección "INTERVALO NORMAL" en el docstring del módulo.
_ANNOUNCE_INTERVAL = 120.0

# Intervalo una vez EMPAREJADA. El loop ya no termina al emparejarse: baja a un
# latido lento y sigue.
#
# Antes el loop hacía `return` en cuanto el backend respondía `paired: true`, y
# eso dejaba a la caja sorda para siempre. La plataforma sí puede liberar una
# caja (`POST /boxes/{id}/deactivate`), pero la caja no tenía forma de
# enterarse: quedaba convencida de tener dueño, con su túnel arriba, y ninguna
# calcomanía podía reactivarla porque el claim exige un anuncio reciente y ya no
# había ninguno. Recuperarla exigía que alguien entrara a Home Assistant a
# reiniciar el add-on — justo lo que no se puede pedir en la casa de un cliente.
#
# 600s y no 120s: emparejada, el anuncio ya no es lo que destraba una activación
# (eso ya pasó). Solo sirve para enterarse de un cambio de estado y para que la
# plataforma sepa que la casa sigue viva. Con 600s son 144 peticiones por caja
# por día — nada para el backend, y una caja liberada se recupera sola en menos
# de diez minutos sin que nadie viaje.
_HEARTBEAT_INTERVAL = 600.0

# Cuántas respuestas seguidas tienen que decir "esta caja no está emparejada"
# antes de que la caja se libere de verdad.
#
# NO es 1 a propósito. Des-emparejarse implica bajar el túnel y borrar los
# secretos de la casa: es destructivo y de cara al cliente. Un pico raro del
# backend —una réplica con la base vieja, un despliegue a mitad de camino— que
# devolviera un `paired: false` aislado dejaría a una familia sin acceso remoto
# sin que nadie hubiera pedido nada.
#
# Con 3 confirmaciones separadas por el latido, hacen falta ~20 minutos de
# respuestas coherentes para soltar la casa. Un desliz momentáneo no alcanza;
# una desactivación de verdad sí, porque es un estado permanente en la base.
# La respuesta viene FIRMADA (Ed25519, ver _DOMINIO_FIRMA), así que además
# nadie puede falsificar un `paired: false` desde la red.
_DESEMPAREJOS_PARA_CONFIRMAR = 3

# Backoff exponencial ante fallos de red o respuestas HTTP inesperadas.
# Misma fórmula que ha_upstream.py: sleep = min(backoff × jitter_factor, CAP).
_BACKOFF_BASE = 5.0
_BACKOFF_CAP = 120.0
_BACKOFF_JITTER = 0.5  # ±50% del intervalo actual

# Timeout total del HTTP request. No es tiempo-real; 15s absorbe latencias
# altas de red sin bloquear el loop indefinidamente.
_HTTP_TIMEOUT = 15.0

# --- Llaves públicas de la plataforma (horneadas en el código) ---

# Justificación del horneado: si vinieran de la configuración, un atacante que
# pueda escribir `platform_base_url` también podría escribir la llave pública,
# vaciando el candado. Horneada acá, solo un cambio de código (y su proceso de
# revisión y despliegue) puede modificarla.
#
# El diccionario existe desde el primer día para rotación sin dejar cajas afuera:
# durante una rotación conviven la llave vieja y la nueva bajo distintos key_id.
# Cuando todas las cajas hayan actualizado, se retira la entrada vieja.
#
# TODO: reemplazar el valor "<pendiente...>" con la llave pública real en base64
#       estándar (32 bytes crudos de Ed25519) cuando el agente del servidor de la
#       plataforma la genere y comparta. Los tests usan su propio par generado en
#       memoria — NUNCA esta constante.
# Marcador de que todavía no se horneó ninguna llave real. Se compara contra
# esto para poder fallar con un mensaje que diga QUÉ falta, en vez de con un
# error de base64 que manda a buscar el problema al lugar equivocado.
LLAVE_PENDIENTE = "PENDIENTE-GENERAR-LLAVE-DE-PLATAFORMA"

LLAVES_PLATAFORMA: dict[str, str] = {
    # key_id -> llave pública Ed25519, base64 estándar, 32 bytes crudos.
    #
    # ES UN DICCIONARIO Y NO UNA SOLA LLAVE A PROPÓSITO: durante una rotación
    # conviven la vieja y la nueva, y así ninguna caja queda afuera mientras la
    # plataforma cambia de llave.
    #
    # VA HORNEADA EN EL CÓDIGO, NO EN LA CONFIGURACIÓN. Si viniera de las
    # opciones del add-on, quien pudiera escribir `platform_base_url` también
    # podría escribir la llave — y el candado no serviría para nada, porque el
    # atacante firmaría con la suya. Cambiarla exige publicar una versión.
    #
    # CÓMO SE OBTIENE: se corre `scripts/generar_llave_firma.py` del backend de
    # la plataforma, EN UNA TERMINAL LIMPIA. Ese script imprime la privada (va a
    # Railway, nunca a un archivo ni a un chat) y la pública (va acá).
    # Generada el 2026-09-03 en una terminal limpia. Su mitad privada vive SOLO
    # en las variables del servicio de la plataforma en Railway
    # (ANNOUNCE_SIGNING_KEY) y nunca paso por un archivo, un chat ni un commit.
    #
    # OJO — hubo un par ANTERIOR cuya privada quedo expuesta en una captura de
    # pantalla. Se descarto sin usarlo. Si alguna vez ves la publica
    # "H477NrsxwCGyd424N3+uUJ5IOR8LJ9jUOg1uH2f3lYM=" en algun lado, es esa:
    # NO la horneés, esta quemada.
    "k1": "PGK+7ml81qyh3uIkvM7LYoApbc1KI8PFHFtfiS24I5s=",
}


def hay_llave_de_plataforma() -> bool:
    """True si alguna llave real fue horneada (o sea, si la firma puede verificarse)."""
    return any(v != LLAVE_PENDIENTE for v in LLAVES_PLATAFORMA.values())

# Prefijo de dominio que separa el material firmado de otros usos de Ed25519 en
# el proyecto. Sin este prefijo, una firma construida para otro propósito podría
# (en teoría) reutilizarse acá.
_DOMINIO_FIRMA = b"uniquexcr/announce-response/v1"


def _verificar_firma_respuesta(
    body_bytes: bytes,
    device_id: str,
    nonce: str,
    sig_header: str | None,
    key_id_header: str | None,
) -> None:
    """
    Verifica la firma Ed25519 de la respuesta de la plataforma.

    POR QUÉ VERIFICAR ANTES DE PARSEAR EL JSON
    -------------------------------------------
    Sin verificación, quien logre responder el anuncio (p. ej. un dominio mal
    escrito con un certificado Let's Encrypt perfectamente válido) puede proveer
    un tunnel_token y un mirror_api_key falsos. HTTPS solo prueba que hay un
    certificado válido para ESE dominio, no que ese dominio sea el nuestro.

    MATERIAL FIRMADO (en este orden exacto)
    ----------------------------------------
        b"uniquexcr/announce-response/v1"
        + len(device_id_utf8) como 4 bytes big-endian + device_id UTF-8
        + len(nonce_utf8)     como 4 bytes big-endian + nonce UTF-8
        + len(body_bytes)     como 4 bytes big-endian + body_bytes

    Los largos de 4 bytes evitan ambigüedad: sin ellos, dos combinaciones
    distintas de (device_id, nonce) podrían concatenar igual y una firma
    valdría para dos mensajes distintos.

    Se firma sobre los bytes crudos del cuerpo (no sobre JSON re-serializado)
    para que ninguna diferencia mínima de serialización —orden de claves,
    espacios, escape de no-ASCII— cause un falso "firma inválida".

    El nonce es el que la caja mandó en ESE pedido: una respuesta capturada
    no sirve para el siguiente anuncio porque el nonce habrá cambiado.

    RESULTADO
    ---------
    Retorna None si la firma es válida.
    Lanza RuntimeError con motivo legible si falla.
    Nunca incluye el cuerpo ni la firma completa en el mensaje de error.
    """
    motivo: str | None = None

    if not sig_header:
        motivo = "cabecera X-Uniquex-Signature ausente o vacía"
    elif not key_id_header:
        motivo = "cabecera X-Uniquex-Key-Id ausente o vacía"
    elif key_id_header not in LLAVES_PLATAFORMA:
        motivo = f"key_id desconocido: {key_id_header!r}"
    elif LLAVES_PLATAFORMA[key_id_header] == LLAVE_PENDIENTE:
        # Sin esto el sintoma seria un error de base64, que manda a depurar la
        # firma cuando lo que falta es hornear la llave.
        motivo = (
            "esta version del add-on se publico SIN la llave publica de la "
            "plataforma: la activacion no puede verificarse. Generala con "
            "scripts/generar_llave_firma.py y publica una version con la "
            "llave horneada en LLAVES_PLATAFORMA."
        )
    else:
        # Reconstruir exactamente el mismo material que firmó la plataforma
        device_id_bytes = device_id.encode("utf-8")
        nonce_bytes = nonce.encode("utf-8")
        material = (
            _DOMINIO_FIRMA
            + struct.pack(">I", len(device_id_bytes))
            + device_id_bytes
            + struct.pack(">I", len(nonce_bytes))
            + nonce_bytes
            + struct.pack(">I", len(body_bytes))
            + body_bytes
        )

        try:
            sig_bytes = base64.b64decode(sig_header, validate=True)
        except Exception:
            motivo = "X-Uniquex-Signature no es base64 válido"
        else:
            try:
                pub_bytes = base64.b64decode(LLAVES_PLATAFORMA[key_id_header], validate=True)
                pub_key = Ed25519PublicKey.from_public_bytes(pub_bytes)
                pub_key.verify(sig_bytes, material)
                return  # Verificación exitosa — el flujo continúa
            except InvalidSignature:
                motivo = "firma Ed25519 no verifica"
            except Exception as exc:
                motivo = f"llave pública inválida o error interno: {type(exc).__name__}"

    raise RuntimeError(motivo)


def _calcular_claim_code_hash(privada: Ed25519PrivateKey, version: int) -> str:
    """
    SHA-256 del código de emparejamiento, en base64.

    La normalización vive en `hash_claim_code()` y NO se repite acá. Antes había
    dos implementaciones del mismo hash en el Mirror (esta y la de
    `api/device.py`) — coincidían por casualidad, y el día que una cambiara el
    emparejamiento habría fallado con "código inválido", un síntoma que no apunta
    a la causa.

    NUNCA se loguea el código derivado — solo el hash, que es público por diseño.
    """
    return hash_claim_code(derivar_claim_code(privada, version))


class _SecurityHalt(Exception):
    """
    Señal interna: el loop debe detenerse sin reintentar.

    Solo se usa para el 409 (posible clonación). Es una clase propia en vez de
    un flag booleano para que el `except Exception` del loop no la atrape y la
    trate como un error recuperable — necesitamos que propague hasta el `except
    _SecurityHalt` explícito del loop.
    """


class AnnounceClient:
    """
    Anuncia la caja a la plataforma periódicamente hasta que quede emparejada.

    Uso:
        client = AnnounceClient(identity, privada, db, platform_base_url)
        task = asyncio.create_task(client.run_forever(), name="announce_client")
        task.add_done_callback(_on_announce_done)
        # ...
        task.cancel()
    """

    def __init__(
        self,
        identity: DeviceIdentity,
        private_key: Ed25519PrivateKey,
        db: Database,
        platform_base_url: str,
        tunnel_token_path: Path,
        mirror_key_path: Path,
        upstream: HAUpstream | None = None,
        announce_interval: float = _ANNOUNCE_INTERVAL,
        on_paired: Callable[[DeviceIdentity], None] | None = None,
        on_unpaired: Callable[[DeviceIdentity], Awaitable[None]] | None = None,
        heartbeat_interval: float = _HEARTBEAT_INTERVAL,
    ) -> None:
        """
        Parámetros
        ----------
        identity
            La identidad resuelta al arrancar. Se usa para armar el payload;
            no se muta (DeviceIdentity es frozen).
        private_key
            Llave privada Ed25519 de ESTA caja, para firmar el anuncio.
            Nunca viaja al backend — solo se usa localmente.
        db
            Base de datos de la caja. Se llama a mark_device_paired() cuando
            el backend confirma el emparejamiento.
        platform_base_url
            URL base de la plataforma (sin trailing slash). Por ejemplo
            "https://app.uniquexcr.com". NO es un secreto — viene en la imagen.
        tunnel_token_path
            Dónde escribir el token de cloudflared cuando el backend lo entregue.
            El backend lo BORRA de su base en el mismo commit en que lo manda: si
            no se persiste en ese instante, se perdió para siempre y esa casa no
            vuelve a ser alcanzable sin re-aprovisionar la caja.
        upstream
            Conexion a Home Assistant, para aplicar la zona horaria de la
            casa al activarse. Opcional: sin ella el emparejamiento funciona
            igual y la zona queda como la haya dejado quien armo la caja.
        mirror_key_path
            Donde escribir la credencial con la que la app le habla a este
            Mirror. La emite la plataforma al emparejar, porque tiene que
            conocerla para poder enrutar a esta casa.
        announce_interval
            Segundos entre anuncios cuando todo va bien. Default: 120s.
        on_paired
            Callback opcional invocado al completar el emparejamiento, con la
            DeviceIdentity actualizada (leída de DB). El lifespan lo usa para
            actualizar app.state.device_identity en memoria y que el endpoint
            /api/device/identity refleje el estado real sin necesitar un reinicio.
        on_unpaired
            Callback opcional (async) invocado cuando la plataforma deja de
            reconocer esta caja y el des-emparejamiento local ya se persistió.
            El lifespan lo usa para bajar cloudflared: el túnel de la casa
            anterior ya no existe del otro lado.
        heartbeat_interval
            Segundos entre anuncios una vez emparejada. Default: 600s.
        """
        self._identity = identity
        self._privada = private_key
        self._db = db
        self._platform_base_url = platform_base_url.rstrip("/")
        self._tunnel_token_path = tunnel_token_path
        self._mirror_key_path = mirror_key_path
        self._upstream = upstream
        self._announce_interval = announce_interval
        self._heartbeat_interval = heartbeat_interval
        self._on_paired = on_paired
        self._on_unpaired = on_unpaired
        # Cuántas respuestas seguidas dijeron "no la conozco" mientras esta caja
        # se cree emparejada. Ver _DESEMPAREJOS_PARA_CONFIRMAR.
        self._desemparejos_seguidos = 0

    async def run_forever(self) -> None:
        """
        Loop principal de anuncio con backoff exponencial.

        NO termina al emparejarse: cambia de cadencia (120s → 600s) y sigue.
        Ese latido es lo que le permite a la caja enterarse de que la plataforma
        la liberó y volver sola al estado activable. Ver _HEARTBEAT_INTERVAL.

        Termina limpiamente en un solo caso:
          - asyncio.CancelledError (shutdown del lifespan).

        Termina con error sin reintentar:
          - 409 (posible clonación): _SecurityHalt propaga y el task queda
            "done" con esa excepción. El lifespan la loguea en el callback.

        Nunca termina por errores de red o HTTP 401/5xx: se aplica backoff y
        se reintenta. No tumbar el Mirror por algo que todavía nadie usa.
        """
        backoff = _BACKOFF_BASE
        endpoint = f"{self._platform_base_url}/api/boxes/announce"
        # El PRIMER anuncio exitoso se loguea en INFO; los siguientes no.
        #
        # Quien prepara la caja en el taller necesita confirmar que llegó a la
        # plataforma antes de despacharla, y solo tiene el registro del add-on
        # para saberlo. Hasta ahora el éxito se logueaba en DEBUG: con el nivel
        # normal, una caja sana y una caja que no alcanza la plataforma se veían
        # EXACTAMENTE igual — silencio las dos.
        #
        # Solo el primero, porque uno cada 2 minutos para siempre ahogaría el
        # registro donde después hay que buscar un problema real.
        primer_anuncio_ok = True

        logger.info(
            "announce.starting",
            device_id=self._identity.device_id,
            endpoint=endpoint,
            interval_s=self._announce_interval,
        )

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_HTTP_TIMEOUT)
        ) as session:
            while True:
                try:
                    paired = await self._announce_once(session, endpoint)

                except asyncio.CancelledError:
                    raise  # Shutdown limpio — no loguear como error

                except _SecurityHalt:
                    # 409: el loop se para sin reintentar. Ya se logueó en
                    # _announce_once con nivel error. Propagar para que el
                    # callback del lifespan lo registre como task failed.
                    raise

                except Exception as exc:
                    # Error recuperable (red, timeout, HTTP 4xx/5xx inesperado).
                    jitter = 1.0 + _BACKOFF_JITTER * (2 * random.random() - 1)
                    sleep_time = min(backoff * jitter, _BACKOFF_CAP)
                    logger.warning(
                        "announce.error",
                        device_id=self._identity.device_id,
                        exc=str(exc),
                        retry_in_s=round(sleep_time, 1),
                    )
                    await asyncio.sleep(sleep_time)
                    backoff = min(backoff * 2, _BACKOFF_CAP)
                    continue

                if paired:
                    # Emparejada. El loop NO termina: baja al latido lento y
                    # sigue escuchando, que es lo único que permite enterarse de
                    # una desactivación sin que un humano reinicie el add-on.
                    if primer_anuncio_ok:
                        primer_anuncio_ok = False
                        logger.info(
                            "announce.ok",
                            device_id=self._identity.device_id,
                            msg="Caja emparejada. Sigue el latido lento.",
                        )
                    backoff = _BACKOFF_BASE
                    await asyncio.sleep(self._heartbeat_interval)
                    continue

                # Anuncio exitoso, caja todavía disponible para reclamar.
                if primer_anuncio_ok:
                    primer_anuncio_ok = False
                    logger.info(
                        "announce.ok",
                        device_id=self._identity.device_id,
                        msg=(
                            "La caja alcanzó la plataforma y está lista para ser "
                            "activada. (Solo se registra el primero; los siguientes "
                            "van cada "
                            f"{int(self._announce_interval)}s en silencio.)"
                        ),
                    )

                # Reiniciar el backoff a su base: el request salió bien.
                backoff = _BACKOFF_BASE
                await asyncio.sleep(self._announce_interval)

    async def _announce_once(self, session: aiohttp.ClientSession, endpoint: str) -> bool:
        """
        Envía un anuncio firmado y procesa la respuesta.

        Devuelve True si la caja quedó emparejada y ya persistió en DB.
        Devuelve False si todavía no está emparejada (seguir el loop).
        Lanza _SecurityHalt en 409 (parar sin reintentar).
        Lanza Exception ante cualquier otro error recuperable.
        """
        device_id = self._identity.device_id

        # Nonce distinto en cada anuncio: 32 hex chars = 128 bits de entropía.
        # Rango requerido 8..128 chars, 32 está bien en el centro.
        nonce = secrets.token_hex(16)

        # La firma cubre device_id y nonce — no el body completo del request.
        # Así el backend puede verificar la identidad sin depender de cómo
        # serializa el JSON el cliente (orden de claves, espaciado, etc.).
        mensaje = f"{device_id}|{nonce}".encode()
        signature = firmar(self._privada, mensaje)

        claim_code_hash = _calcular_claim_code_hash(
            self._privada, self._identity.claim_code_version
        )

        payload: dict[str, Any] = {
            "device_id": device_id,
            "public_key": self._identity.public_key,
            "claim_code_hash": claim_code_hash,
            "hardware_id": self._identity.hardware_id,
            "nonce": nonce,
            "signature": signature,
        }

        # QUÉ LE FALTA A ESTA CAJA
        # ------------------------
        # La plataforma entrega sus secretos UNA vez y da por hecho que
        # llegaron: borra el token del túnel de su base en el mismo commit en
        # que lo manda, y marca la credencial como entregada. Si la respuesta se
        # pierde en la red, o el disco no deja escribirla, el secreto se perdió
        # para los dos lados y la casa nunca levanta.
        #
        # No es hipotético: es un commit que ocurre ANTES de que la caja
        # confirme nada. Y no había recuperación — el endpoint de reintento del
        # túnel contesta "ya_provisionado" porque el tunnel_id existe, aunque el
        # token no lo tenga nadie.
        #
        # Con esto la caja dice en cada anuncio qué le falta de verdad, mirando
        # el disco y no lo que cree recordar. Solo tiene sentido preguntarlo si
        # está emparejada: sin casa, no hay nada que entregarle.
        if self._identity.paired:
            payload["necesita_clave_mirror"] = cargar_clave_mirror(self._mirror_key_path) is None
            payload["necesita_token_tunel"] = cargar_token_tunel(self._tunnel_token_path) is None

        async with session.post(endpoint, json=payload) as resp:
            if resp.status == 200:
                # Leer los bytes CRUDOS antes de cualquier parseo.
                # La verificación ocurre sobre los bytes tal como llegaron por
                # la red — no sobre un JSON re-serializado — para que ninguna
                # diferencia mínima de serialización cause un falso negativo.
                body_bytes = await resp.read()
                sig_header = resp.headers.get("X-Uniquex-Signature")
                key_id_header = resp.headers.get("X-Uniquex-Key-Id")

                try:
                    _verificar_firma_respuesta(
                        body_bytes,
                        device_id,
                        nonce,
                        sig_header,
                        key_id_header,
                    )
                except RuntimeError as exc:
                    # Loguear solo key_id y el motivo — NUNCA el cuerpo ni la
                    # firma completa. El loop sigue reintentando (puede ser un
                    # problema transitorio del backend).
                    logger.error(
                        "announce.firma_invalida",
                        device_id=device_id,
                        key_id=key_id_header,
                        motivo=str(exc),
                        msg=(
                            "La respuesta de la plataforma no pasó verificación "
                            "de firma. Nada se escribe en disco ni en DB. "
                            "El loop sigue reintentando."
                        ),
                    )
                    raise RuntimeError(f"Firma de respuesta inválida: {exc}") from exc

                # Parsear el JSON solo DESPUÉS de la verificación exitosa
                body = _json.loads(body_bytes)
                return await self._handle_ok(body)

            if resp.status == 401:
                # Firma rechazada: bug de derivación o codificación en este lado.
                # Se loguea con error (no warning) porque es siempre un bug nuestro,
                # no un problema de la red. El loop sigue reintentando por si el
                # backend tiene un error transitorio de su lado.
                body = await resp.text()
                logger.error(
                    "announce.signature_rejected",
                    device_id=device_id,
                    status=401,
                    body=body[:256],
                    msg=(
                        "El backend rechazó la firma Ed25519. "
                        "Revisar _calcular_claim_code_hash() y la codificación de firmar(). "
                        "El loop sigue reintentando."
                    ),
                )
                raise RuntimeError("Firma rechazada (401) — bug local, ver logs")

            if resp.status == 409:
                # La llave pública de este device_id no coincide con la registrada.
                # Posible clonación: otra caja usó el mismo device_id con otra llave.
                # DETENER el anuncio — no reintentar en loop.
                body = await resp.text()
                logger.error(
                    "announce.key_conflict",
                    device_id=device_id,
                    status=409,
                    body=body[:256],
                    msg=(
                        "ALERTA DE SEGURIDAD: el backend tiene registrada una llave "
                        "pública distinta para este device_id. Posible clonación de caja. "
                        "El loop de anuncio SE DETIENE — requiere intervención humana."
                    ),
                )
                raise _SecurityHalt("409 key conflict — anuncio detenido")

            # Cualquier otro status (5xx, 429, etc.) es recuperable con backoff.
            body = await resp.text()
            raise RuntimeError(
                f"Respuesta inesperada del backend: HTTP {resp.status} — {body[:256]}"
            )

    async def _quizas_desemparejar(self) -> None:
        """
        La plataforma no reconoce una caja que localmente se cree emparejada.

        Cuenta confirmaciones consecutivas y, al llegar al umbral, suelta la casa
        de verdad. Ver _DESEMPAREJOS_PARA_CONFIRMAR para por qué no alcanza una.

        El orden importa y no es arbitrario:

          1. Se borran los secretos de la plataforma (token de túnel y clave del
             Mirror). Van PRIMERO porque son lo peligroso: mientras existan, la
             caja sigue publicando la casa anterior y la credencial del dueño
             anterior sigue abriendo este Mirror.
          2. Se limpia el emparejamiento en la base local. Es lo que hace que la
             calcomanía vuelva a mostrar el QR.
          3. Se baja cloudflared (callback del lifespan).

        Si el paso 2 fallara después del 1, la caja queda sin secretos y con la
        base diciendo "emparejada": inofensivo, y el siguiente latido lo
        reintenta. Al revés —base limpia y secretos vivos— dejaría un túnel
        publicando la casa de alguien sin que nada lo registre.
        """
        self._desemparejos_seguidos += 1
        faltan = _DESEMPAREJOS_PARA_CONFIRMAR - self._desemparejos_seguidos

        if faltan > 0:
            logger.warning(
                "announce.desemparejo_sospechado",
                device_id=self._identity.device_id,
                house_id=self._identity.paired_house_id,
                confirmaciones=self._desemparejos_seguidos,
                faltan=faltan,
                msg=(
                    "La plataforma no reconoce esta caja. Se espera confirmación "
                    "antes de soltar la casa."
                ),
            )
            return

        logger.error(
            "announce.desemparejando",
            device_id=self._identity.device_id,
            house_id=self._identity.paired_house_id,
            msg=(
                "La plataforma liberó esta caja. Bajando el túnel y borrando los "
                "secretos de la casa anterior. La calcomanía vuelve a servir."
            ),
        )

        borrar_secreto_de_plataforma(self._tunnel_token_path)
        borrar_secreto_de_plataforma(self._mirror_key_path)

        nueva = await self._db.unmark_device_paired()
        self._desemparejos_seguidos = 0

        if nueva is None:
            # La base ya lo tenía limpio. Puede pasar si un intento anterior
            # murió entre el borrado y el UPDATE.
            #
            # HAY QUE RELEER LA IDENTIDAD IGUAL. Si nos quedáramos con la de
            # memoria —que sigue diciendo "emparejada"— `_quizas_desemparejar`
            # volvería a dispararse cada tres latidos para siempre: mataría
            # cloudflared, reintentaría los borrados y llenaría el log, sin
            # converger nunca. El UPDATE no actualizó nada justamente porque el
            # estado deseado YA está en la base; lo único desincronizado es la
            # copia en memoria.
            nueva = await self._db.get_device_identity()
            logger.info(
                "announce.ya_desemparejada_en_db",
                device_id=self._identity.device_id,
            )

        if nueva is not None:
            identidad = DeviceIdentity(**nueva)
            self._identity = identidad
            if self._on_paired is not None:
                # Mismo callback que al emparejar: refresca app.state para que
                # /api/device/identity y la calcomanía dejen de mentir sin
                # necesidad de reiniciar.
                self._on_paired(identidad)
            logger.info(
                "announce.desemparejada",
                device_id=identidad.device_id,
                msg="Caja liberada. Vuelve a anunciarse cada 120s, lista para activar.",
            )

        if self._on_unpaired is not None:
            try:
                await self._on_unpaired(self._identity)
            except Exception as exc:
                # Que no se pueda bajar cloudflared no puede impedir el resto:
                # sin token, el túnel se cae solo en cuanto Cloudflare lo cierre.
                logger.error(
                    "announce.on_unpaired_fallo",
                    exc=str(exc),
                    msg="No se pudo bajar el túnel. Sin token ya no va a poder reconectar.",
                )

    async def _handle_ok(self, body: dict[str, Any]) -> bool:
        """
        Procesa una respuesta 200 del endpoint de anuncio.

        Devuelve True si la caja quedó emparejada y ya persistió en DB.
        Devuelve False si todavía no está emparejada.
        """
        paired: bool = body.get("paired", False)
        house_id: str | None = body.get("house_id")
        tunnel_provider: str | None = body.get("tunnel_provider")
        tunnel_hostname: str | None = body.get("tunnel_hostname")
        tunnel_token: str | None = body.get("tunnel_token")
        clave_mirror: str | None = body.get("mirror_api_key")
        zona_horaria: str | None = body.get("house_timezone")

        # ---------------------------------------------------------------------
        # LO PRIMERO DE TODO, antes de cualquier cosa que pueda fallar.
        #
        # El backend borra el token de su base en el MISMO commit en que lo
        # devuelve: para cuando llega acá, la plataforma ya no lo tiene. No hay a
        # quién volver a pedírselo. Si esta función revienta antes de escribirlo
        # —la DB local trabada, un house_id raro, lo que sea— el token se pierde
        # y la casa queda incomunicada hasta re-aprovisionar la caja entera.
        #
        # Por eso va antes del `if not paired`, antes de mark_device_paired, y
        # fuera de cualquier condición que no sea "vino un token".
        # ---------------------------------------------------------------------
        # La credencial con la que la app le habla a este Mirror. Va junto al
        # token del tunel y por la misma razon: se persiste ANTES de cualquier
        # cosa que pueda fallar. Sin ella, la app no puede llegar a esta casa.
        if clave_mirror:
            try:
                if guardar_clave_mirror(self._mirror_key_path, clave_mirror):
                    logger.info(
                        "announce.clave_mirror_recibida",
                        device_id=self._identity.device_id,
                        msg=(
                            "Credencial de la plataforma guardada. El Mirror la "
                            "acepta desde la proxima peticion."
                        ),
                    )
            except OSError as exc:
                logger.error(
                    "announce.clave_mirror_no_persistida",
                    device_id=self._identity.device_id,
                    exc=str(exc),
                    msg="La app no va a poder alcanzar esta casa hasta resolverlo.",
                )

        if tunnel_token:
            try:
                guardar_token_tunel(self._tunnel_token_path, tunnel_token)
            except OSError as exc:
                # Sin disco escribible esto no se puede recuperar solo. Se grita
                # con todo lo necesario para que un humano lo resuelva.
                logger.error(
                    "announce.tunnel_token_no_persistido",
                    device_id=self._identity.device_id,
                    path=str(self._tunnel_token_path),
                    exc=str(exc),
                    msg=(
                        "El token del túnel llegó y NO se pudo guardar. El backend "
                        "ya lo borró: hay que re-aprovisionar esta caja."
                    ),
                )
            else:
                logger.info(
                    "announce.tunnel_token_recibido",
                    device_id=self._identity.device_id,
                    hostname=tunnel_hostname,
                    msg="Token persistido. Falta que cloudflared lo use.",
                )

        logger.debug(
            "announce.response",
            device_id=self._identity.device_id,
            paired=paired,
            house_id=house_id,
            has_tunnel=bool(tunnel_provider and tunnel_hostname),
        )

        if not paired or not house_id:
            # Caja viva pero sin casa del lado de la plataforma. Hay dos formas
            # muy distintas de llegar acá y confundirlas sería grave:
            #
            #   · nunca se activó → estado normal del taller, nada que hacer.
            #   · SÍ estaba activada y la plataforma la liberó → hay que soltar
            #     la casa: bajar el túnel y borrar los secretos que emitió.
            #
            # Lo que las distingue es la identidad LOCAL, no la respuesta.
            if self._identity.paired:
                await self._quizas_desemparejar()
            return False

        # La plataforma la reconoce: cualquier sospecha previa se cancela. Sin
        # este reset, tres respuestas raras espaciadas por horas —con la caja
        # sana en el medio— se sumarían hasta soltar una casa que nunca dejó de
        # estar emparejada. Solo cuentan las CONSECUTIVAS.
        self._desemparejos_seguidos = 0

        # La caja fue reclamada. Persistir en DB antes de notificar al callback:
        # si la app muere aquí, en el próximo arranque ensure_identity leerá de DB
        # y el emparejamiento ya estará ahí.
        updated = await self._db.mark_device_paired(
            house_id=house_id,
            backend_base_url=self._platform_base_url,
        )

        if updated is None:
            # Ya tenía paired_at en DB — race condition (dos anuncios casi
            # simultáneos) o restart rápido donde el anuncio llegó dos veces.
            # El resultado es el mismo: está emparejada. Seguir.
            logger.info(
                "announce.already_paired_in_db",
                device_id=self._identity.device_id,
                house_id=house_id,
            )
        else:
            logger.info(
                "announce.paired",
                device_id=self._identity.device_id,
                house_id=house_id,
                msg="Emparejamiento persistido en la DB local.",
            )

        # Zona horaria de la casa REAL, que la plataforma conoce y esta caja no.
        #
        # Se aplica despues de persistir el emparejamiento y NUNCA es fatal: el
        # comando de HA que usa no es parte de su API publica (ver
        # `aplicar_zona_horaria`), asi que si un dia cambia, la casa tiene que
        # seguir funcionando. Peor la zona corrida que la casa caida.
        if zona_horaria and self._upstream is not None:
            try:
                await self._upstream.aplicar_zona_horaria(zona_horaria)
            except Exception as exc:
                logger.warning(
                    "announce.zona_horaria_no_aplicada",
                    zona=zona_horaria,
                    exc=str(exc),
                    msg=(
                        "Hay que ponerla a mano en Home Assistant. Sin esto, las "
                        "escenas por atardecer quedan corridas."
                    ),
                )

        # Si el backend ya provisionó un túnel en el mismo response, guardarlo.
        # (Normalmente el túnel llega después, en un anuncio posterior, pero el
        # contrato no lo prohíbe en el primero.)
        if tunnel_provider and tunnel_hostname:
            await self._db.set_device_tunnel(
                provider=tunnel_provider, hostname=tunnel_hostname
            )
            logger.info(
                "announce.tunnel_registered",
                provider=tunnel_provider,
                hostname=tunnel_hostname,
            )

        # Notificar al lifespan para que actualice app.state.device_identity en
        # memoria. Sin esto, el endpoint /api/device/identity seguiría mostrando
        # paired=false hasta el próximo reinicio. Se hace DESPUÉS de persistir en
        # DB porque si el callback explota no queremos perder el emparejamiento.
        if self._on_paired is not None:
            try:
                nuevo_row = await self._db.get_device_identity()
                if nuevo_row is not None:
                    self._on_paired(DeviceIdentity(**nuevo_row))
            except Exception as exc:
                # No es crítico: el emparejamiento ya está en DB. El endpoint
                # mostrará el estado viejo hasta el próximo reinicio, que es
                # aceptable — la caja ya no va a necesitar el endpoint de taller.
                logger.warning(
                    "announce.on_paired_callback_error",
                    exc=str(exc),
                )

        return True
