"""
Identidad por caja: cada equipo genera lo suyo y nunca comparte llaves.

POR QUE EXISTE (0.24.0)
-----------------------
El plan de producto es fabricar cajas en el taller a partir de UNA imagen
maestra: se prepara un equipo perfecto y las demas se clonan de ahi (o se
restaura un respaldo sobre HAOS de fabrica). Eso ahorra muchisimo trabajo, pero
tiene una trampa: **clonar copia tambien los secretos**.

Hoy `run.sh` ya persiste dos secretos internos en `/data`:

    SESSION_SECRET="$(persist_secret session_secret)"
    IFRAME_TOKEN_SECRET="$(persist_secret iframe_token_secret)"

Y `/data` ENTRA en los respaldos de Home Assistant. O sea que el metodo de
clonado recomendado —restaurar un respaldo sobre una caja nueva— hoy le pondria
a todas las cajas del parque las mismas llaves. Una caja comprometida las
comprometeria todas, y no habria forma de saber cual filtro que.

La regla del negocio es explicita: **jamas dos cajas con la misma llave, cada
quien con sus datos.** Este modulo es el que la hace cumplir.

DISENO
------
La identidad se ata al HARDWARE, no al archivo. Un archivo viaja en un clon; el
hardware no.

  - `huella`: se deriva de las MAC de las interfaces de red, que las da el
    Supervisor por `/network/info`. Sirve igual en Raspberry Pi (aarch64) y en
    mini PC Intel (amd64), que son las dos arquitecturas que declara el add-on.
    No se usa el serial del CPU: existe en la Pi pero no en x86, y dentro del
    contenedor no siempre esta montado.

  - `box_id`: se DERIVA de la huella, asi que es estable. La misma caja siempre
    se llama igual aunque se reinstale, y eso importa: el ID es lo que se
    imprime en la etiqueta y lo que escanea el QR del cliente. No puede cambiar
    porque alguien restauro un respaldo.

  - `secreto`: aleatorio de verdad (`secrets.token_urlsafe`), y **se regenera si
    la huella no coincide**. Ese es el candado anti-clon: si este archivo llego
    aca dentro de un respaldo de OTRA caja, la huella no va a coincidir y el
    secreto se tira. La caja nueva nace con lo suyo sin que nadie haga nada.

  - `clave_privada`: par Ed25519 con el que la caja le prueba a la plataforma
    que es quien dice ser (ver `registro.py`). Se regenera con el mismo criterio
    anti-clon, y por el mismo motivo: si un clon heredara el par, podria hacerse
    pasar por la caja original ante la nube. **La privada no sale de la caja
    jamas** — a la plataforma solo viaja la publica.

  - Las credenciales NUNCA se escriben en el log. `run.sh` ya se niega a imprimir
    la `mirror_api_key` por la misma razon: los logs del add-on los ve cualquier
    admin de HA y quedan guardados. Por eso `Identidad.__repr__` esta acotado a
    mano — un `dataclass` normal imprimiria el secreto entero en cualquier
    traceback.

QUE NO HACE ESTE MODULO
-----------------------
No habla con la plataforma: solo resuelve "quien soy y con que firmo". El
protocolo de registro vive en `registro.py` y se apoya en esto.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Iterable, Literal

import structlog
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

if TYPE_CHECKING:  # solo para anotar; en runtime se importa dentro de la funcion
    import aiohttp

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# Ruta por defecto. `/data` es el volumen persistente del add-on: sobrevive
# reinicios y actualizaciones, igual que `mirror.sqlite3`.
RUTA_IDENTIDAD_POR_DEFECTO: Final[Path] = Path("/data/uniquex_identity.json")

# Version del formato del archivo. Si algun dia cambia la forma de derivar la
# huella, esto permite migrar en vez de regenerar identidades a ciegas (que
# borraria el secreto de cajas sanas del parque).
FORMATO_ACTUAL: Final[int] = 1

# Prefijo visible del ID. Va impreso en la etiqueta de la caja.
PREFIJO_BOX_ID: Final[str] = "UX"

# Cuantos caracteres hex del hash entran en el box_id. 10 hex = 40 bits: con
# miles de cajas la probabilidad de choque es despreciable, y sigue siendo corto
# para leerlo en voz alta por telefono al dar soporte.
LARGO_BOX_ID: Final[int] = 10

# Bytes de entropia del secreto.
BYTES_SECRETO: Final[int] = 32

# Una MAC "de verdad" para nuestro proposito. Se exige el formato completo
# aa:bb:cc:dd:ee:ff para no tragar basura del Supervisor.
_RE_MAC: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{2}(:[0-9a-f]{2}){5}$")

# MACs que NO identifican al equipo y contaminarian la huella:
#   - 00:00:00:00:00:00 → interfaz sin hardware real.
#   - Docker/veth arrancan en 02:42: cambian en cada arranque del contenedor.
# Si entraran, la huella cambiaria sola y la caja se creeria clonada en cada
# reinicio: regeneraria el secreto para siempre y nunca podria registrarse.
_MAC_NULA: Final[str] = "00:00:00:00:00:00"
_PREFIJOS_VIRTUALES: Final[tuple[str, ...]] = ("02:42:",)

MotivoIdentidad = Literal["primera_vez", "clon_detectado", "formato_viejo"]


class IdentidadIndeterminable(Exception):
    """
    No se pudo derivar una huella de hardware confiable.

    Es fail-closed a proposito: sin huella, la unica alternativa seria generar
    una identidad al azar y persistirla — que es exactamente el comportamiento
    que clona llaves cuando alguien restaura el respaldo en otra caja. Preferimos
    que el add-on avise y no arranque el registro, antes que fabricar cajas
    gemelas en silencio.
    """


@dataclass(frozen=True)
class Identidad:
    """
    Quien es esta caja. Ni `secreto` ni `clave_privada` se imprimen nunca.

    Hay DOS credenciales porque resuelven cosas distintas:

      - `secreto`: simetrico, para usos locales de la propia caja.
      - `clave_privada`: Ed25519, para probarle a la plataforma que esta caja es
        quien dice ser. **La privada no sale de la caja jamas**; a la plataforma
        solo viaja la publica.

    Por que asimetrico y no un secreto compartido con la nube: si la base de
    datos de la plataforma se filtrara, con secretos compartidos el atacante
    podria hacerse pasar por CUALQUIER caja del parque. Con llaves publicas no
    puede firmar nada. Es la misma regla del negocio ("cada quien con sus
    datos") aplicada al lado del servidor.
    """

    box_id: str
    secreto: str
    clave_privada: str  # hex de los 32 bytes crudos de la Ed25519
    huella: str
    formato: int
    motivo: MotivoIdentidad

    def clave_publica(self) -> str:
        """La mitad publica, en hex. Es lo unico que viaja a la plataforma."""
        privada = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(self.clave_privada))
        return privada.public_key().public_bytes(
            encoding=Encoding.Raw,
            format=PublicFormat.Raw,
        ).hex()

    def __repr__(self) -> str:
        # Sin esto, cualquier traceback o log estructurado que arrastre el objeto
        # publicaria las credenciales en los logs del add-on, que ve cualquier
        # admin de HA y quedan guardados.
        return (
            f"Identidad(box_id={self.box_id!r}, huella={self.huella[:8]!r}…, "
            f"formato={self.formato}, motivo={self.motivo!r}, "
            f"secreto=<oculto>, clave_privada=<oculto>)"
        )

    def publico(self) -> dict[str, Any]:
        """Lo que se puede mostrar en una API o en un log. Sin credenciales."""
        return {
            "box_id": self.box_id,
            "huella": self.huella,
            "formato": self.formato,
            "motivo": self.motivo,
        }


def normalizar_macs(macs: Iterable[str | None]) -> list[str]:
    """
    Deja solo las MAC que identifican al equipo, en minuscula y ordenadas.

    Se ordenan porque el Supervisor no promete un orden estable entre arranques:
    si el orden cambiara, la huella cambiaria y la caja se creeria clonada.
    """
    limpias: set[str] = set()
    for mac in macs:
        if not mac:
            continue
        m = mac.strip().lower()
        if not _RE_MAC.match(m):
            continue
        if m == _MAC_NULA:
            continue
        if m.startswith(_PREFIJOS_VIRTUALES):
            continue
        limpias.add(m)
    return sorted(limpias)


def calcular_huella(macs: Iterable[str | None]) -> str:
    """
    Huella estable del hardware a partir de sus MAC.

    Se hashea en vez de guardar las MAC crudas: la huella viaja a la plataforma
    al registrarse, y una MAC es un dato de red del cliente que no necesitamos
    tener. El hash alcanza para comparar "¿sigo siendo la misma caja?".
    """
    normalizadas = normalizar_macs(macs)
    if not normalizadas:
        raise IdentidadIndeterminable(
            "Ninguna interfaz de red util para identificar la caja. "
            "Sin huella de hardware no se genera identidad: hacerlo al azar "
            "clonaria llaves al restaurar un respaldo en otra caja."
        )
    material = "|".join(normalizadas).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def derivar_box_id(huella: str) -> str:
    """
    ID visible de la caja, derivado de la huella (o sea: estable).

    Se usa un hash aparte y no un prefijo de `huella`: asi el ID que se imprime
    en la etiqueta y viaja en el QR no revela parte del valor con el que se
    compara el hardware.
    """
    corto = hashlib.sha256(f"box-id-v1|{huella}".encode("utf-8")).hexdigest()
    return f"{PREFIJO_BOX_ID}-{corto[:LARGO_BOX_ID].upper()}"


def generar_clave_privada() -> str:
    """Ed25519 nueva, en hex. Nunca sale de la caja."""
    privada = Ed25519PrivateKey.generate()
    return privada.private_bytes(
        encoding=Encoding.Raw,
        format=PrivateFormat.Raw,
        encryption_algorithm=NoEncryption(),
    ).hex()


def generar_identidad(huella: str, motivo: MotivoIdentidad) -> Identidad:
    """Identidad nueva: ID derivado del hardware, credenciales recien sorteadas."""
    return Identidad(
        box_id=derivar_box_id(huella),
        secreto=secrets.token_urlsafe(BYTES_SECRETO),
        clave_privada=generar_clave_privada(),
        huella=huella,
        formato=FORMATO_ACTUAL,
        motivo=motivo,
    )


def _es_clave_privada_valida(hexa: str) -> bool:
    """32 bytes en hex y que `cryptography` la acepte. Nada mas."""
    try:
        crudo = bytes.fromhex(hexa)
    except ValueError:
        return False
    if len(crudo) != 32:
        return False
    try:
        Ed25519PrivateKey.from_private_bytes(crudo)
    except Exception:
        return False
    return True


def leer_identidad(ruta: Path) -> Identidad | None:
    """
    Lee el archivo de identidad. Devuelve None si no hay una utilizable.

    Cualquier problema (no existe, JSON roto, campo faltante o vacio) se trata
    igual: no hay identidad. Es seguro porque el que llama va a generar una
    nueva; lo inseguro seria arrancar con un secreto a medio leer.
    """
    try:
        crudo = json.loads(ruta.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("identidad.archivo_ilegible", ruta=str(ruta), error=str(exc))
        return None

    if not isinstance(crudo, dict):
        logger.warning("identidad.archivo_no_es_objeto", ruta=str(ruta))
        return None

    box_id = crudo.get("box_id")
    secreto = crudo.get("secreto")
    huella = crudo.get("huella")
    if not (isinstance(box_id, str) and box_id) or \
       not (isinstance(secreto, str) and secreto) or \
       not (isinstance(huella, str) and huella):
        logger.warning("identidad.archivo_incompleto", ruta=str(ruta))
        return None

    # Archivos anteriores a las llaves asimetricas no la traen. No es motivo
    # para descartar la identidad: la caja es la misma y su `secreto` puede
    # estar en uso. `resolver_identidad` le genera el par y sube el formato.
    clave_privada = crudo.get("clave_privada")
    if not isinstance(clave_privada, str):
        clave_privada = ""
    if clave_privada and not _es_clave_privada_valida(clave_privada):
        logger.warning("identidad.clave_privada_invalida", ruta=str(ruta))
        clave_privada = ""

    formato = crudo.get("formato")
    if not isinstance(formato, int):
        formato = 0

    motivo = crudo.get("motivo")
    if motivo not in ("primera_vez", "clon_detectado", "formato_viejo"):
        motivo = "primera_vez"

    return Identidad(
        box_id=box_id,
        secreto=secreto,
        clave_privada=clave_privada,
        huella=huella,
        formato=formato,
        motivo=motivo,  # type: ignore[arg-type]
    )


def guardar_identidad(ruta: Path, identidad: Identidad) -> None:
    """
    Escribe la identidad de forma atomica y solo legible por el dueno.

    Atomica (archivo temporal + `os.replace`) porque un corte de luz a mitad de
    la escritura dejaria un JSON truncado; `leer_identidad` lo descartaria y la
    caja regeneraria el secreto, perdiendo el registro que ya tenia en la nube.
    """
    ruta.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "box_id": identidad.box_id,
        "secreto": identidad.secreto,
        "clave_privada": identidad.clave_privada,
        "huella": identidad.huella,
        "formato": identidad.formato,
        "motivo": identidad.motivo,
    }

    fd, tmp = tempfile.mkstemp(dir=str(ruta.parent), prefix=".identidad-", suffix=".tmp")
    try:
        # 0600 ANTES de escribir: si se hiciera despues, el secreto existiria en
        # disco con permisos por defecto durante una ventana.
        #
        # `os.fchmod` no existe en Windows. El add-on siempre corre en Linux, que
        # es donde esta ventana importa; en Windows (solo el suite de tests) los
        # permisos POSIX no aplican y se sigue de largo. `mkstemp` ya crea el
        # archivo con 0600 por su cuenta, asi que esto es cinturon y tirantes.
        if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, ruta)
    except BaseException:
        # Nunca dejar el temporal con un secreto adentro si algo fallo.
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise

    try:
        os.chmod(ruta, 0o600)
    except OSError as exc:  # sistemas de archivos sin permisos POSIX
        logger.warning("identidad.sin_permisos_posix", ruta=str(ruta), error=str(exc))


def extraer_macs(payload: Any) -> list[str]:
    """
    Saca las MAC de la respuesta de `/network/info` del Supervisor.

    Forma esperada: `{"result": "ok", "data": {"interfaces": [{"mac": ...}]}}`.
    Se parsea a la defensiva porque el Supervisor cambia de version por su cuenta
    (hoy 2026.07.5 en la casa del cliente) y un campo nuevo o faltante no puede
    tumbar el arranque del add-on: lo que no se entienda se ignora, y si no queda
    nada `calcular_huella` es el que decide fallar.
    """
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    interfaces = data.get("interfaces")
    if not isinstance(interfaces, list):
        return []

    macs: list[str] = []
    for iface in interfaces:
        if not isinstance(iface, dict):
            continue
        mac = iface.get("mac")
        if isinstance(mac, str):
            macs.append(mac)
    return macs


async def obtener_macs_del_supervisor(
    session: "aiohttp.ClientSession",
    token: str,
    *,
    base_url: str = "http://supervisor",
) -> list[str]:
    """
    Pide las interfaces de red al Supervisor.

    REQUIERE `hassio_api: true` en config.yaml. Hoy el add-on lo tiene en
    `false` — solo pide `homeassistant_api`, que da el Core API pero NO el del
    Supervisor. Habilitarlo es parte de este cambio y sube el privilegio del
    add-on, asi que se documenta en el release: el Mirror pasa a poder consultar
    la configuracion de red de la caja.

    Se eligio esta fuente porque es la unica que sirve en las DOS arquitecturas
    que declara el add-on. Dentro del contenedor no hay alternativa util:
    `host_network` esta en `false`, asi que `/sys/class/net` solo muestra las
    interfaces virtuales de Docker (02:42:…), que cambian en cada arranque.
    """
    import aiohttp  # local: solo se necesita en el camino de I/O

    timeout = aiohttp.ClientTimeout(total=10, connect=5)
    async with session.get(
        f"{base_url.rstrip('/')}/network/info",
        headers={"Authorization": f"Bearer {token}"},
        timeout=timeout,
    ) as resp:
        resp.raise_for_status()
        return extraer_macs(await resp.json())


def resolver_identidad(ruta: Path, huella: str) -> Identidad:
    """
    Devuelve la identidad de ESTA caja, regenerandola si el archivo vino de otra.

    Es el corazon del candado anti-clon. Tres caminos:

      1. No hay archivo → caja de fabrica: se genera y se guarda.
      2. Hay archivo y la huella coincide → es esta caja: se reusa tal cual.
      3. Hay archivo y la huella NO coincide → el archivo llego en un clon o en
         un respaldo ajeno: se genera identidad nueva y se pisa la vieja.

    El caso 3 es el que cumple la regla del negocio. Deja un WARN a proposito:
    si aparece en una caja que no se acaba de clonar, significa que le cambio el
    hardware de red y hay que mirarlo — su registro en la plataforma quedo
    hablando de otro secreto.
    """
    actual = leer_identidad(ruta)

    if actual is None:
        nueva = generar_identidad(huella, "primera_vez")
        guardar_identidad(ruta, nueva)
        logger.info("identidad.generada", **nueva.publico())
        return nueva

    if actual.huella != huella:
        nueva = generar_identidad(huella, "clon_detectado")
        guardar_identidad(ruta, nueva)
        logger.warning(
            "identidad.clon_detectado",
            box_id_anterior=actual.box_id,
            box_id_nuevo=nueva.box_id,
            detalle=(
                "El archivo de identidad venia de otra caja (respaldo restaurado "
                "o imagen clonada). Se genero un secreto nuevo: esta caja NO "
                "comparte llaves con la de origen."
            ),
        )
        return nueva

    if actual.formato != FORMATO_ACTUAL or not actual.clave_privada:
        # Misma caja, archivo viejo. Se conserva el secreto —esta caja ya podria
        # estar registrada en la plataforma con el— y solo se sube el formato.
        migrada = Identidad(
            box_id=derivar_box_id(huella),
            secreto=actual.secreto,
            # Si ya tenia par de llaves se respeta: la plataforma pudo haber
            # registrado la publica y regenerarla la desconectaria.
            clave_privada=actual.clave_privada or generar_clave_privada(),
            huella=huella,
            formato=FORMATO_ACTUAL,
            motivo="formato_viejo",
        )
        guardar_identidad(ruta, migrada)
        logger.info("identidad.migrada", desde=actual.formato, **migrada.publico())
        return migrada

    return actual
