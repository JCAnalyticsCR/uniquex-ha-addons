"""
Protocolo de registro de una caja contra la plataforma.

POR QUE EXISTE (0.24.0)
-----------------------
Hoy poner una casa en linea exige que Jeyrell este disponible: crear el tunel de
Cloudflare a mano, los hostnames, el servicio de Railway y once variables de
entorno. El tecnico no puede terminar sin el. Este modulo es el primer paso para
sacarlo del camino: la caja se presenta sola al enchufarse.

QUIEN LLAMA A QUIEN
-------------------
La caja marca HACIA AFUERA. La plataforma nunca entra a la casa.

Es la inversion que elimina el tunel, el DNS y los hostnames por casa: una
conexion saliente funciona en cualquier router domestico sin configurar nada,
igual que un celular hablando con WhatsApp. Es como lo hacen los hubs
comerciales.

COMO PRUEBA LA CAJA QUIEN ES
----------------------------
Con firma Ed25519, no con un secreto compartido (ver `identity.py`). La privada
no sale de la caja jamas; a la plataforma solo viaja la publica. Si la base de
datos de la plataforma se filtrara, el atacante no podria firmar nada en nombre
de ninguna caja.

El problema del huevo y la gallina —la plataforma todavia no conoce la llave
publica en el primer contacto— se resuelve con un **token de alta** de un solo
uso que el taller graba en la caja. Sin ese token, cualquiera que vea el numero
impreso en una etiqueta podria adelantarse y registrar una caja ajena: el
`box_id` es publico por diseno (va en la etiqueta y en el QR), asi que **no
puede ser tambien la credencial**.

    Alta (una vez)   : token de alta  → la plataforma guarda la llave publica
    Despues (siempre): firma Ed25519  → el token ya no sirve

CONTRA REENVIOS
---------------
Cada peticion lleva un `timestamp` que entra DENTRO de lo firmado, y la
plataforma rechaza lo que caiga fuera de una ventana corta. Sin eso, alguien que
capture una peticion valida podria repetirla para siempre.

Ojo con el reloj: una caja recien enchufada sin internet todavia puede tener la
hora mal hasta que NTP la corrija. Por eso el rechazo por ventana se distingue
del rechazo por firma (`ResultadoVerificacion`): es un fallo temporal que
conviene reintentar, no una credencial invalida.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final

import structlog
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from ha_mirror.identity import Identidad

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# Version del protocolo. Viaja en el cuerpo para que la plataforma pueda
# atender cajas viejas y nuevas a la vez — el add-on y la nube se despliegan por
# separado, y esa leccion ya costo caro una vez (el frontend nuevo contra un
# Mirror viejo dejo la app sin datos en vivo).
VERSION_PROTOCOLO: Final[int] = 1

# Cuanto puede desviarse el reloj de la caja del de la plataforma. 5 minutos es
# holgado para NTP y sigue siendo una ventana chica para reenviar algo robado.
TOLERANCIA_RELOJ_SEGUNDOS: Final[int] = 300

# Nombres de los encabezados. Constantes para que los dos lados no se
# desincronicen por un typo.
CAB_CAJA_ID: Final[str] = "X-Caja-Id"
CAB_TIMESTAMP: Final[str] = "X-Caja-Timestamp"
CAB_FIRMA: Final[str] = "X-Caja-Firma"
CAB_TOKEN_ALTA: Final[str] = "X-Caja-Token-Alta"


class ResultadoVerificacion(Enum):
    """
    Por que se acepto o rechazo una peticion.

    Se distinguen los motivos a proposito: "firma invalida" es un ataque o un
    error de programacion y no se reintenta; "fuera de ventana" suele ser el
    reloj de una caja recien enchufada y SI conviene reintentar. Devolver un
    booleano perderia esa diferencia y haria imposible operar el parque.
    """

    OK = "ok"
    FIRMA_INVALIDA = "firma_invalida"
    FUERA_DE_VENTANA = "fuera_de_ventana"
    CLAVE_INVALIDA = "clave_invalida"
    MALFORMADA = "malformada"


@dataclass(frozen=True)
class PeticionFirmada:
    """Lo que la caja manda: encabezados + cuerpo EXACTO que se firmo."""

    headers: dict[str, str]
    cuerpo: bytes

    def cuerpo_json(self) -> Any:
        return json.loads(self.cuerpo.decode("utf-8"))


def cuerpo_canonico(datos: dict[str, Any]) -> bytes:
    """
    Serializa de forma determinista: mismas claves → mismos bytes.

    `sort_keys` y separadores sin espacios no son cosmetica: la firma se calcula
    sobre estos bytes, asi que si los dos lados serializaran distinto (otro orden
    de claves, un espacio de mas) la verificacion fallaria siempre y el sintoma
    seria "credencial invalida", que manda a buscar el problema al lugar
    equivocado.

    `ensure_ascii=True` a proposito: un nombre de casa con tilde tiene que
    producir los mismos bytes en la caja y en la plataforma pase lo que pase con
    la codificacion del transporte.
    """
    return json.dumps(
        datos,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def material_a_firmar(box_id: str, timestamp: int, cuerpo: bytes) -> bytes:
    """
    Lo que realmente se firma.

    El `box_id` y el `timestamp` entran en la firma ademas del cuerpo: si solo se
    firmara el cuerpo, alguien podria tomar una peticion valida y reenviarla con
    otro encabezado de caja o de hora, y la firma seguiria cerrando.

    El separador `\\n` mas los largos explicitos evitan ambiguedad: sin eso,
    ("ab", 1) y ("a", 91) podrian producir el mismo material.
    """
    partes = [box_id.encode("utf-8"), str(timestamp).encode("ascii"), cuerpo]
    salida = bytearray(b"uniquex-registro-v1")
    for parte in partes:
        salida += b"\n" + str(len(parte)).encode("ascii") + b":"
        salida += parte
    return bytes(salida)


def firmar(clave_privada_hex: str, box_id: str, timestamp: int, cuerpo: bytes) -> str:
    """Firma Ed25519 en hex. La privada no se registra en ningun log."""
    privada = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(clave_privada_hex))
    return privada.sign(material_a_firmar(box_id, timestamp, cuerpo)).hex()


def verificar_firma(
    clave_publica_hex: str,
    firma_hex: str,
    box_id: str,
    timestamp: int,
    cuerpo: bytes,
    *,
    ahora: int,
    tolerancia: int = TOLERANCIA_RELOJ_SEGUNDOS,
) -> ResultadoVerificacion:
    """
    Valida firma y ventana de tiempo. La usa la PLATAFORMA.

    Vive aca, en el mismo modulo que `firmar`, para que los dos lados no puedan
    desincronizarse: cualquier cambio en como se arma el material a firmar rompe
    los tests de ida y vuelta en el acto.

    La ventana se revisa ANTES que la firma: verificar cripto sobre datos que ya
    sabemos viejos es trabajo regalado si alguien inunda el endpoint.
    """
    if abs(ahora - timestamp) > tolerancia:
        return ResultadoVerificacion.FUERA_DE_VENTANA

    try:
        crudo_pub = bytes.fromhex(clave_publica_hex)
        publica = Ed25519PublicKey.from_public_bytes(crudo_pub)
    except (ValueError, TypeError):
        return ResultadoVerificacion.CLAVE_INVALIDA

    try:
        firma = bytes.fromhex(firma_hex)
    except (ValueError, TypeError):
        return ResultadoVerificacion.MALFORMADA

    try:
        publica.verify(firma, material_a_firmar(box_id, timestamp, cuerpo))
    except InvalidSignature:
        return ResultadoVerificacion.FIRMA_INVALIDA

    return ResultadoVerificacion.OK


def construir_alta(
    identidad: Identidad,
    *,
    token_alta: str,
    version_addon: str,
    ahora: int,
    arquitectura: str | None = None,
) -> PeticionFirmada:
    """
    Primer contacto: la caja se presenta y entrega su llave PUBLICA.

    Va firmada ademas del token de alta. El token solo prueba "esta caja salio de
    nuestro taller"; la firma prueba "y ademas tengo la privada que corresponde a
    la publica que estoy declarando". Sin la firma, alguien con un token robado
    podria dar de alta una llave publica propia.
    """
    cuerpo = cuerpo_canonico({
        "version_protocolo": VERSION_PROTOCOLO,
        "box_id": identidad.box_id,
        "clave_publica": identidad.clave_publica(),
        # El hash del hardware, no las MAC: alcanza para que la plataforma note
        # que a una caja le cambio la red, sin quedarse con datos de la casa.
        "huella": identidad.huella,
        "version_addon": version_addon,
        "arquitectura": arquitectura or "",
        "timestamp": ahora,
    })
    firma = firmar(identidad.clave_privada, identidad.box_id, ahora, cuerpo)
    return PeticionFirmada(
        headers={
            CAB_CAJA_ID: identidad.box_id,
            CAB_TIMESTAMP: str(ahora),
            CAB_FIRMA: firma,
            CAB_TOKEN_ALTA: token_alta,
            "Content-Type": "application/json",
        },
        cuerpo=cuerpo,
    )


def construir_peticion(
    identidad: Identidad,
    datos: dict[str, Any],
    *,
    ahora: int,
) -> PeticionFirmada:
    """
    Cualquier peticion posterior al alta. Ya no lleva token: la firma alcanza.

    Que el token NO viaje mas es intencional. Un token de un solo uso que se
    siguiera mandando en cada llamada estaria expuesto todo el tiempo sin
    aportar nada, porque la firma ya prueba la identidad.
    """
    cuerpo = cuerpo_canonico({
        **datos,
        "version_protocolo": VERSION_PROTOCOLO,
        "box_id": identidad.box_id,
        "timestamp": ahora,
    })
    firma = firmar(identidad.clave_privada, identidad.box_id, ahora, cuerpo)
    return PeticionFirmada(
        headers={
            CAB_CAJA_ID: identidad.box_id,
            CAB_TIMESTAMP: str(ahora),
            CAB_FIRMA: firma,
            "Content-Type": "application/json",
        },
        cuerpo=cuerpo,
    )


def leer_peticion(
    headers: dict[str, str],
    cuerpo: bytes,
    clave_publica_hex: str,
    *,
    ahora: int,
    tolerancia: int = TOLERANCIA_RELOJ_SEGUNDOS,
) -> tuple[ResultadoVerificacion, dict[str, Any] | None]:
    """
    Lado PLATAFORMA: valida una peticion entrante y devuelve su cuerpo.

    Los encabezados se leen sin distinguir mayusculas porque los proxies los
    normalizan a su gusto y no hay que confiar en como llegan.
    """
    normalizados = {k.lower(): v for k, v in headers.items()}
    box_id = normalizados.get(CAB_CAJA_ID.lower())
    firma = normalizados.get(CAB_FIRMA.lower())
    ts_crudo = normalizados.get(CAB_TIMESTAMP.lower())

    if not box_id or not firma or not ts_crudo:
        return ResultadoVerificacion.MALFORMADA, None

    try:
        timestamp = int(ts_crudo)
    except ValueError:
        return ResultadoVerificacion.MALFORMADA, None

    resultado = verificar_firma(
        clave_publica_hex,
        firma,
        box_id,
        timestamp,
        cuerpo,
        ahora=ahora,
        tolerancia=tolerancia,
    )
    if resultado is not ResultadoVerificacion.OK:
        return resultado, None

    try:
        datos = json.loads(cuerpo.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return ResultadoVerificacion.MALFORMADA, None

    if not isinstance(datos, dict):
        return ResultadoVerificacion.MALFORMADA, None

    # El box_id del encabezado es el que se firmo; si el cuerpo dice otro, algo
    # se armo mal o alguien esta probando suerte.
    if datos.get("box_id") != box_id:
        return ResultadoVerificacion.MALFORMADA, None

    return ResultadoVerificacion.OK, datos
