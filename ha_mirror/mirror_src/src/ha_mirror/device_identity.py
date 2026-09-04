"""
Identidad criptográfica de la caja — se genera sola, al primer arranque.

REEMPLAZA A `identity.py` + `registro.py` (borrados en 0.25.0)
--------------------------------------------------------------
Hubo un intento anterior que derivaba el device_id de las MAC del hardware.
Se descartó por dos razones: exigía `hassio_api: true` (subirle privilegios al
add-on en la casa de un cliente por una función que nadie usaba todavía), y
ataba la identidad al equipo — el banco de pruebas es una mini PC x86 y el
producto va a ser una Raspberry Pi 5, así que lo que se probaba no era lo que
se iba a enviar. Acá el device_id es aleatorio y el hardware_id queda solo como
dato de inventario para soporte.

POR QUÉ EXISTE
--------------
Hasta hoy el Mirror no tiene ninguna noción de "quién soy yo". La `MIRROR_API_KEY`
la escribe Jeyrell a mano en cada instalación, lo cual funciona cuando hay dos
casas y él está presente en las dos. No funciona para vender la caja.

El problema concreto de escalar así es la imagen dorada: si se clona una caja ya
instalada para hacer la siguiente, las cien cajas comparten dueño y token, y una
comprometida compromete la flota entera. Por eso la identidad **la genera cada
caja, sola, la primera vez que arranca** — nunca viene horneada en la imagen.

DISEÑO
------
  - Par de llaves Ed25519. La PRIVADA nunca sale de la caja: no viaja al backend,
    no se muestra en ningún endpoint, no entra en un log. La PÚBLICA es la que
    el backend guarda para reconocer a esta caja.

  - `device_id` ALEATORIO, no derivado del hardware. Casa 2 corre en una mini PC
    x86 y el producto va a ser Raspberry Pi 5: un Pi tiene serial de fábrica y una
    mini PC no. Si la identidad dependiera del hardware, lo que probamos no sería
    lo que enviamos. El `hardware_id` se guarda aparte, como dato de inventario
    para soporte ("¿cuál caja es la que falla?"), nunca como ancla de identidad.

  - La llave privada vive en un ARCHIVO propio (0600), no dentro del SQLite.
    La base se copia para depurar, se respalda y la leen otros caminos de código;
    un `SELECT *` distraído terminaría con la llave en un log. Un archivo que
    nada más toca tiene un radio de explosión mucho menor.

  - **La llave privada NO está cifrada, a propósito.** Cifrarla exigiría una clave
    que también tendría que vivir en la misma caja para poder arrancar sin que
    nadie escriba una contraseña — o sea, cifrado cuya llave está al lado del
    candado. Eso es teatro, no seguridad, y decirlo en voz alta es mejor que
    prometer una protección que no existe. Lo que sí protege el archivo son los
    permisos del sistema y el hecho de que la caja está en la casa del cliente.

  - Este módulo NO empareja nada. Genera, persiste, carga y firma. El código de
    emparejamiento y el intercambio con el backend son el paso siguiente.

FORMATO
-------
Ambas llaves se guardan en base64 de sus 32 bytes crudos (Ed25519 `Raw`), no en
PEM. Es el mismo formato que va a viajar al backend, así que no hay conversión
en el medio ni dudas sobre qué se está comparando cuando el backend valida que
dos cajas no presenten la misma llave pública.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import uuid
from dataclasses import dataclass
from pathlib import Path

import structlog
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# Queda escrito en la fila de identidad. Si algún día se rota a otra curva, la
# columna ya existe y las cajas viejas siguen siendo legibles.
KEY_ALGORITHM = "ed25519"


class DeviceIdentityError(RuntimeError):
    """Error irrecuperable de identidad — requiere intervención humana."""


@dataclass(frozen=True)
class DeviceIdentity:
    """
    La identidad de esta caja, lista para exponer.

    Nunca incluye la llave privada: si algún día alguien serializa este objeto
    hacia un log o un endpoint, no hay nada secreto adentro que se pueda filtrar.
    """

    device_id: str
    public_key: str
    hardware_id: str | None
    key_algorithm: str
    created_at: str
    claim_code_version: int = 1
    paired_at: str | None = None
    paired_house_id: str | None = None
    backend_base_url: str | None = None
    tunnel_provider: str | None = None
    tunnel_hostname: str | None = None
    tunnel_ready_at: str | None = None

    @property
    def paired(self) -> bool:
        return self.paired_at is not None


# -----------------------------------------------------------------------------
# Hardware ID — dato de inventario, best effort, nunca revienta
# -----------------------------------------------------------------------------


def leer_hardware_id() -> str | None:
    """
    Identificador del hardware para soporte e inventario. NO es la identidad.

    Se intenta en orden: serial del Raspberry Pi → machine-id de systemd. Si nada
    responde (Windows de desarrollo, contenedor pelado), devuelve None y la caja
    arranca igual. Que este dato falte nunca puede impedir que la caja funcione.
    """
    # Raspberry Pi: /proc/cpuinfo trae una línea "Serial : 100000001234abcd"
    try:
        cpuinfo = Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="ignore")
        for linea in cpuinfo.splitlines():
            if linea.lower().startswith("serial"):
                _, _, valor = linea.partition(":")
                serial = valor.strip()
                if serial and set(serial) != {"0"}:
                    return f"rpi:{serial}"
    except OSError:
        pass

    # Linux genérico (la mini PC de casa 2 cae acá). Cambia si se reinstala el
    # sistema, y está bien: es inventario, no identidad.
    try:
        machine_id = Path("/etc/machine-id").read_text(encoding="utf-8").strip()
        if machine_id:
            return f"machine-id:{machine_id}"
    except OSError:
        pass

    return None


# -----------------------------------------------------------------------------
# Llaves: generar, guardar, cargar
# -----------------------------------------------------------------------------


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _privada_a_bytes(llave: Ed25519PrivateKey) -> bytes:
    return llave.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _publica_a_bytes(llave: Ed25519PublicKey) -> bytes:
    return llave.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )


def _escribir_secreto(path: Path, contenido: bytes) -> None:
    """
    Escribe un secreto a disco con permisos 0600, de forma atómica.

    Atómica importa: un corte de luz a mitad de la escritura dejaría un archivo
    truncado, y un secreto truncado es peor que ninguno — parece existir pero no
    sirve. Se escribe a un temporal y se renombra; el rename sí es atómico.

    Los permisos se aplican en el `os.open`, no después, para que el archivo
    nunca exista ni un instante siendo legible por otros. En Windows (desarrollo)
    el modo se ignora silenciosamente; en la caja, que es Linux, se respeta.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")

    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(contenido)
            fh.flush()
            os.fsync(fh.fileno())
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise

    os.replace(tmp, path)


def guardar_llave_privada(path: Path, llave: Ed25519PrivateKey) -> None:
    """Persiste la llave privada de la caja. Ver `_escribir_secreto`."""
    _escribir_secreto(path, _b64(_privada_a_bytes(llave)).encode("ascii"))
    # Nunca se loguea la llave ni el contenido — solo que existe.
    logger.info("device_identity.private_key_written", path=str(path))


# -----------------------------------------------------------------------------
# Token del túnel — el único secreto que la plataforma le manda a la caja
# -----------------------------------------------------------------------------


def guardar_token_tunel(path: Path, token: str) -> None:
    """
    Persiste el token de cloudflared.

    ESTO NO ES OPCIONAL Y NO SE PUEDE POSPONER. El backend borra el token de su
    base en el mismo commit en que lo entrega: para cuando esta función corre, la
    plataforma YA no lo tiene. Si no se escribe acá, se perdió para siempre y esa
    casa no vuelve a ser alcanzable sin re-aprovisionar la caja entera.

    Por eso va a un archivo propio con 0600 y no a la SQLite: la base se copia
    para depurar y se respalda, y este token levanta el túnel de una casa de un
    cliente.
    """
    _escribir_secreto(path, token.encode("utf-8"))
    logger.info("device_identity.tunnel_token_written", path=str(path))


def guardar_clave_mirror(path: Path, clave: str) -> str | None:
    """
    Persiste la credencial con la que la APP le habla a este Mirror.

    La emite la plataforma en el emparejamiento, porque tiene que conocerla para
    poder enrutar a esta casa — la key que el socio escribe a mano al instalar
    nunca sale de la caja y por eso no sirve para eso.

    Devuelve la clave si cambió respecto de la guardada, o None si ya era la
    misma. Sirve para no reiniciar el proceso por una entrega repetida.
    """
    if cargar_clave_mirror(path) == clave:
        return None
    _escribir_secreto(path, clave.encode("utf-8"))
    logger.info("device_identity.clave_mirror_guardada", path=str(path))
    return clave


def cargar_clave_mirror(path: Path) -> str | None:
    """La credencial emitida por la plataforma, o None si esta caja no la tiene."""
    try:
        return path.read_text(encoding="utf-8").strip() or None
    except OSError:
        return None


def cargar_token_tunel(path: Path) -> str | None:
    """El token guardado, o None si esta caja todavía no tiene túnel."""
    try:
        return path.read_text(encoding="utf-8").strip() or None
    except OSError:
        return None


def borrar_secreto_de_plataforma(path: Path) -> bool:
    """
    Borra un secreto que emitió la plataforma (token de túnel o clave del Mirror).

    Existe para el des-emparejamiento: cuando la plataforma deja de reconocer a
    esta caja, los secretos que emitió para la casa anterior tienen que
    desaparecer del disco. Si se quedaran, la caja seguiría levantando el túnel
    de una casa que ya no existe, y la credencial del dueño anterior seguiría
    abriendo este Mirror.

    Devuelve True si había algo y se borró. `missing_ok=True` porque el caso
    normal —no había nada— no es un error: esta función se llama sin saber si el
    archivo existe.

    No revienta ante un OSError: si el disco no deja borrar, es mucho mejor
    seguir corriendo y gritarlo que tumbar el Mirror de una familia.
    """
    try:
        existia = path.exists()
        path.unlink(missing_ok=True)
    except OSError as exc:
        logger.error(
            "device_identity.secreto_no_borrado",
            path=str(path),
            exc=str(exc),
            msg="No se pudo borrar un secreto de la plataforma. Revisar a mano.",
        )
        return False
    if existia:
        logger.info("device_identity.secreto_borrado", path=str(path))
    return existia


# -----------------------------------------------------------------------------
# Hash del código de emparejamiento — una sola definición
# -----------------------------------------------------------------------------


def hash_claim_code(codigo: str) -> str:
    """
    SHA-256 en base64 del código, en su forma canónica.

    Vive acá y no en cada quien lo necesita porque la normalización tiene que ser
    IDÉNTICA a la del backend (`.strip().upper()`). Si dos lugares la calculan
    por su cuenta y uno cambia, el emparejamiento deja de funcionar y el síntoma
    —"código inválido"— no apunta a la causa.
    """
    return _b64(hashlib.sha256(codigo.strip().upper().encode()).digest())


def cargar_llave_privada(path: Path) -> Ed25519PrivateKey:
    """Lee la llave privada del disco. Revienta si falta o está corrupta."""
    try:
        crudo = base64.b64decode(path.read_text(encoding="ascii").strip(), validate=True)
    except (OSError, ValueError) as exc:
        raise DeviceIdentityError(
            f"No se pudo leer la llave privada en {path}: {exc}"
        ) from exc

    try:
        return Ed25519PrivateKey.from_private_bytes(crudo)
    except ValueError as exc:
        raise DeviceIdentityError(
            f"La llave privada en {path} no es una llave Ed25519 válida"
        ) from exc


# -----------------------------------------------------------------------------
# Código de emparejamiento — el que va impreso en la calcomanía
# -----------------------------------------------------------------------------

# Sin caracteres ambiguos: no hay 0/O, 1/I/L, ni U (se confunde con V escrita a
# mano). Importa porque el código también se dicta por teléfono cuando el QR no
# escanea — cámara sucia, poca luz, calcomanía rayada.
_ALFABETO = "23456789ABCDEFGHJKMNPQRSTVWXYZ"
_LARGO_CODIGO = 8


def derivar_claim_code(llave: Ed25519PrivateKey, version: int = 1) -> str:
    """
    El código de emparejamiento de esta caja. Formato: `ABCD-EFGH`.

    Se DERIVA de la llave privada en vez de guardarse:

      - No hay un secreto más que proteger en disco.
      - Se puede reimprimir una calcomanía perdida sin re-aprovisionar la caja.
      - Es HMAC, o sea de una sola vía: tener el código impreso no acerca a nadie
        a la llave privada.

    `version` entra en la derivación. Subirla da un código nuevo sin tocar el par
    de llaves — el caso "alguien fotografió la calcomanía antes de instalarla":
    se reimprime y el código viejo deja de servir.

    8 caracteres sobre un alfabeto de 30 son ~39 bits. No alcanza solo, y no
    pretende: lo que protege el emparejamiento es la suma de tres cosas — un solo
    uso, que la caja tenga que estar viva y responder un reto firmado, y el
    límite de intentos del backend.
    """
    material = hmac.new(
        _privada_a_bytes(llave),
        f"uniquex-claim-code-v{version}".encode(),
        hashlib.sha256,
    ).digest()

    # Rechazo por módulo: 256 % 30 != 0, así que tomar `byte % 30` favorecería
    # levemente a los primeros caracteres del alfabeto. Se descartan los bytes
    # del sobrante en vez de sesgar el código.
    limite = 256 - (256 % len(_ALFABETO))
    chars: list[str] = []
    for byte in material:
        if len(chars) == _LARGO_CODIGO:
            break
        if byte < limite:
            chars.append(_ALFABETO[byte % len(_ALFABETO)])

    codigo = "".join(chars)
    return f"{codigo[:4]}-{codigo[4:]}"


def construir_url_qr(backend_base_url: str, device_id: str, claim_code: str) -> str:
    """
    La URL que codifica el QR de la calcomanía.

    El código va en el FRAGMENTO (`#`), no en el query string, a propósito: el
    fragmento no viaja en la petición HTTP. Así el código de emparejamiento no
    queda en los logs de acceso del servidor, ni en un proxy, ni en el Referer
    hacia terceros. Solo lo ve el JavaScript de la página de emparejamiento.
    """
    base = backend_base_url.rstrip("/")
    return f"{base}/emparejar#d={device_id}&c={claim_code}"


def firmar(llave: Ed25519PrivateKey, mensaje: bytes) -> str:
    """
    Firma un mensaje con la llave de la caja. Devuelve la firma en base64.

    Todavía no lo usa nadie: es la operación para la que existe todo lo demás.
    El emparejamiento del paso siguiente firma con esto para probar que quien
    dice ser esta caja tiene la llave privada, no solo la pública.
    """
    return _b64(llave.sign(mensaje))


# -----------------------------------------------------------------------------
# El punto de entrada: identidad garantizada al arrancar
# -----------------------------------------------------------------------------


async def ensure_identity(db: object, key_path: Path) -> DeviceIdentity:
    """
    Devuelve la identidad de esta caja, generándola si es el primer arranque.

    Es idempotente: en el segundo arranque y en todos los siguientes lee lo que
    ya existe y no toca nada.

    Los tres estados posibles y qué hace con cada uno:

      1. Sin fila y sin archivo → primer arranque. Genera el par, escribe el
         archivo PRIMERO y la fila después. Si se corta la luz en el medio, el
         próximo arranque no ve fila, vuelve a generar y pisa el archivo huérfano.
         Ese orden es el único que no deja una fila apuntando a una llave que no
         existe.

      2. Con fila y con archivo → arranque normal. Carga y verifica que la llave
         privada del disco corresponda a la pública guardada.

      3. Con fila pero SIN archivo → estado corrupto. **Revienta a propósito.**
         Regenerar en silencio le cambiaría la identidad a una caja que quizá ya
         está emparejada, y el backend dejaría de reconocerla sin que nadie
         entienda por qué. Es mejor un arranque fallido y ruidoso que una caja
         que miente sobre quién es.
    """
    fila = await db.get_device_identity()  # type: ignore[attr-defined]

    # --- Caso 1: primer arranque ---
    if fila is None:
        device_id = uuid.uuid4().hex
        privada = Ed25519PrivateKey.generate()
        publica_b64 = _b64(_publica_a_bytes(privada.public_key()))
        hardware_id = leer_hardware_id()

        # El archivo va PRIMERO. Ver docstring: el orden inverso puede dejar una
        # fila sin llave, que es el caso 3 (irrecuperable).
        guardar_llave_privada(key_path, privada)

        creada = await db.create_device_identity(  # type: ignore[attr-defined]
            device_id=device_id,
            public_key=publica_b64,
            hardware_id=hardware_id,
            key_algorithm=KEY_ALGORITHM,
        )
        logger.info(
            "device_identity.generated",
            device_id=device_id,
            hardware_id=hardware_id,
            algoritmo=KEY_ALGORITHM,
            msg="Primer arranque: identidad generada por la caja.",
        )
        return DeviceIdentity(**creada)

    # --- Caso 3: fila huérfana ---
    if not key_path.exists():
        logger.error(
            "device_identity.private_key_missing",
            device_id=fila["device_id"],
            path=str(key_path),
            msg=(
                "La caja tiene identidad registrada pero perdió su llave privada. "
                "NO se regenera sola: si ya estaba emparejada, regenerar la "
                "desconectaría del backend en silencio."
            ),
        )
        raise DeviceIdentityError(
            f"Identidad {fila['device_id']} sin llave privada en {key_path}. "
            "Requiere intervención: restaurar el archivo desde respaldo, o "
            "borrar la fila device_identity para re-aprovisionar la caja."
        )

    # --- Caso 2: arranque normal ---
    privada = cargar_llave_privada(key_path)
    publica_calculada = _b64(_publica_a_bytes(privada.public_key()))

    if publica_calculada != fila["public_key"]:
        # La llave del disco no es la de esta identidad. Pasa si alguien copió un
        # /data de otra caja: exactamente el escenario de clonación que hay que
        # detectar, no arreglar en silencio.
        logger.error(
            "device_identity.key_mismatch",
            device_id=fila["device_id"],
            msg="La llave privada del disco no corresponde a la identidad registrada.",
        )
        raise DeviceIdentityError(
            f"La llave privada en {key_path} no corresponde a la identidad "
            f"{fila['device_id']}. ¿Se copió /data desde otra caja?"
        )

    logger.info("device_identity.loaded", device_id=fila["device_id"])
    return DeviceIdentity(**fila)
