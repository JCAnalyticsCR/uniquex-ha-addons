"""
En qué paso está la casa para conectar sus apagadores eWeLink (Sonoff).

── PARA QUÉ ─────────────────────────────────────────────────────────────────
Una casa nueva con apagadores Sonoff necesita, UNA vez, que alguien con acceso a
su Home Assistant instale la integración SonoffLAN y entre con la cuenta de
eWeLink. La app guía a la persona paso a paso; esto le dice a la app en qué paso
va, leyendo la casa de verdad, para que la guía marque lo hecho sola.

Solo HECHOS. Los textos y los enlaces de cada paso viven en la app: cambiar una
frase de la guía no puede obligar a publicar una versión del complemento.

── POR QUÉ SonoffLAN Y NO EL COMPLEMENTO DE eWeLink ───────────────────────────
Decisión de Jeyrell, 2026-10-20. El complemento "eWeLink Smart Home" de CoolKit
—el que usa Fortunatta— está ARCHIVADO desde febrero de 2026: no recibe más
arreglos. Su reemplazo oficial pide permiso de ADMINISTRADOR sobre toda la caja.
SonoffLAN es una integración (no un complemento), está mantenida, habla con los
apagadores por la red de la casa y usa la nube solo de respaldo.

── 🔪 DOS PUENTES PARA LOS MISMOS APAGADORES, NUNCA ───────────────────────────
Si la casa ya los tiene por el complemento viejo (`ewelink_smart_home`, como
Fortunatta), `otro_puente` lo dice y la app NO ofrece instalar SonoffLAN: dos
integraciones mandando sobre el mismo apagador se pisan los estados.

── LA CONTRASEÑA DE eWeLINK NO PASA POR ACÁ ───────────────────────────────────
Esto solo lee. La cuenta se conecta en el formulario de Home Assistant, al que
la app manda con un enlace. Y el título de la integración —que HA llena con el
CORREO de la cuenta— no se devuelve: solo cuántas hay y si funcionan.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

#: Desde esta versión HACS viene adentro de Home Assistant como "Marketplace" y
#: ya no se instala aparte (hacs.xyz, 2026-10). Cambia el primer paso de la guía.
VERSION_MARKETPLACE = (2026, 11)

DOMINIO_SONOFF = "sonoff"
DOMINIO_HACS = "hacs"
#: El puente del complemento de CoolKit. Si está, ya hay quien mande.
DOMINIOS_OTRO_PUENTE = frozenset({"ewelink_smart_home"})

_RE_VERSION = re.compile(r"^(\d{4})\.(\d{1,2})")


def version_ha(texto: Any) -> tuple[int, int] | None:
    """`"2026.10.3"` → `(2026, 10)`. Tolera betas (`2026.11.0b1`) y basura (→ None)."""
    m = _RE_VERSION.match(str(texto or ""))
    return (int(m.group(1)), int(m.group(2))) if m else None


@dataclass(frozen=True)
class EstadoEwelink:
    version_ha: str | None
    marketplace: bool
    otro_puente: bool
    hacs: bool
    sonoff_descargado: bool
    cuentas: int
    cuentas_con_problema: int
    aparatos: int
    aparatos_sin_responder: int


def calcular(
    *,
    version: Any,
    entradas: list[dict[str, Any]],
    registro: list[dict[str, Any]],
    sonoff_en_disco: bool,
    estados: dict[str, str],
) -> EstadoEwelink:
    """
    Pura: recibe lo que contestó la casa y devuelve el estado de la guía.

    `estados` es `entity_id → state` del store, para contar los que no responden.
    """
    v = version_ha(version)
    marketplace = v is not None and v >= VERSION_MARKETPLACE

    por_dominio: dict[str, list[dict[str, Any]]] = {}
    for e in entradas:
        por_dominio.setdefault(str(e.get("domain") or ""), []).append(e)

    sonoff = por_dominio.get(DOMINIO_SONOFF, [])
    plataformas = {str(r.get("platform") or "") for r in registro}
    otro_puente = any(d in por_dominio for d in DOMINIOS_OTRO_PUENTE) or bool(
        plataformas & DOMINIOS_OTRO_PUENTE
    )

    mios = [
        r["entity_id"]
        for r in registro
        if r.get("platform") == DOMINIO_SONOFF and not r.get("disabled_by") and r.get("entity_id")
    ]
    sin_responder = sum(1 for eid in mios if estados.get(eid) in ("unavailable", None))

    return EstadoEwelink(
        version_ha=str(version) if version else None,
        marketplace=marketplace,
        otro_puente=otro_puente,
        # Con Marketplace no hay nada que instalar: cuenta como hecho.
        hacs=marketplace or bool(por_dominio.get(DOMINIO_HACS)),
        # Una cuenta ya conectada prueba que está descargada, aunque la consulta
        # al disco haya fallado.
        sonoff_descargado=sonoff_en_disco or bool(sonoff),
        cuentas=len(sonoff),
        cuentas_con_problema=sum(1 for e in sonoff if e.get("state") not in ("loaded", None)),
        aparatos=len(mios),
        aparatos_sin_responder=sin_responder,
    )
