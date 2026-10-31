"""
Qué ve cada quien por /ws/state (0.53.0).

POR QUÉ EXISTE
--------------
Una casa de la plataforma tiene roles ("Seguridad", "Jardinero", "Niños"…) que
ven solo ciertas cámaras. La app lo aplica en cada ruta HTTP, pero /ws/state va
DIRECTO del navegador a esta caja: hasta 0.52.x le mandaba a cualquier sesión
las 418 entidades de la casa, incluidas todas las `camera.*` de Home Assistant
con su `access_token`. Con ese token, alguien en el wifi de la casa le pide la
imagen a Home Assistant sin credenciales (`/api/camera_proxy/<id>?token=`).

DISEÑO
------
- El filtro viaja FIRMADO dentro del ticket (`ws_ticket.py`): lo arma el
  frontend del lado del servidor con el rol de la persona, y el navegador no lo
  puede cambiar sin romper la firma. Sin estado en memoria, igual que el ticket.
- Lista BLANCA de dominios + lista blanca de cámaras. Lo que no está, no sale.
- Para un cliente filtrado, un tipo de mensaje que este módulo no conoce NO
  sale: el día que alguien agregue uno nuevo al fan-out, falla cerrado.
- `access_token` y la `entity_picture` con token se quitan para TODOS, también
  para el dueño: la app no los usa (las imágenes van por go2rtc y por las rutas
  del Mirror), y un secreto que nadie lee no tiene por qué viajar.

Nunca muta el mensaje que recibe: el fan-out comparte el MISMO dict entre las
colas de todos los clientes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

_RE_DOMINIO = re.compile(r"^[a-z_]{1,40}$")
_RE_CAMARA = re.compile(r"^camera\.[a-z0-9_]{1,80}$")

#: Los mismos topes que la plataforma (`routers/equipo.py`).
MAX_CAMARAS = 300
MAX_DOMINIOS = 40

#: Lo que un cliente filtrado puede recibir. Cualquier otro tipo se descarta.
_TIPOS_PERMITIDOS = frozenset(
    {"snapshot", "state_changed", "connection_status", "service_complete", "service_timeout"}
)


@dataclass(frozen=True)
class FiltroWs:
    """Qué entidades ve un cliente de /ws/state."""

    dominios: frozenset[str]
    camaras: frozenset[str]

    def ve(self, entity_id: str) -> bool:
        dominio, _, resto = entity_id.partition(".")
        if not resto or dominio not in self.dominios:
            return False
        if dominio == "camera":
            return entity_id in self.camaras
        return True

    def a_payload(self) -> dict[str, list[str]]:
        return {"d": sorted(self.dominios), "c": sorted(self.camaras)}

    @classmethod
    def validar(cls, dominios: list[str], camaras: list[str]) -> FiltroWs:
        """Construye un filtro desde lo que manda el frontend. ValueError si algo no cuadra."""
        if len(dominios) > MAX_DOMINIOS or len(camaras) > MAX_CAMARAS:
            raise ValueError("filtro demasiado grande")
        if not all(isinstance(d, str) and _RE_DOMINIO.match(d) for d in dominios):
            raise ValueError("dominio invalido en el filtro")
        if not all(isinstance(c, str) and _RE_CAMARA.match(c) for c in camaras):
            raise ValueError("camara invalida en el filtro")
        return cls(dominios=frozenset(dominios), camaras=frozenset(camaras))

    @classmethod
    def desde_payload(cls, raw: object) -> FiltroWs | None:
        """Lee el filtro de un ticket ya verificado. `None` si está mal formado."""
        if not isinstance(raw, dict):
            return None
        d, c = raw.get("d"), raw.get("c")
        if not isinstance(d, list) or not isinstance(c, list):
            return None
        try:
            return cls.validar(d, c)
        except ValueError:
            return None


def _imagen_con_token(valor: object) -> bool:
    return isinstance(valor, str) and "token=" in valor


def limpiar_estado(estado: Any) -> Any:
    """Copia del estado sin `access_token` ni `entity_picture` con token. Igual si no hay nada que quitar."""
    if not isinstance(estado, dict):
        return estado
    attrs = estado.get("attributes")
    if not isinstance(attrs, dict):
        return estado
    if "access_token" not in attrs and not _imagen_con_token(attrs.get("entity_picture")):
        return estado
    limpios = {
        k: v
        for k, v in attrs.items()
        if k != "access_token" and not (k == "entity_picture" and _imagen_con_token(v))
    }
    return {**estado, "attributes": limpios}


def preparar_mensaje(msg: dict[str, Any], filtro: FiltroWs | None) -> dict[str, Any] | None:
    """
    El mensaje tal como lo puede ver este cliente, o `None` si no le corresponde.

    `filtro=None` = el dueño (o un frontend anterior a 0.53.0): ve todo, sin
    tokens.
    """
    tipo = msg.get("type")
    if filtro is not None and tipo not in _TIPOS_PERMITIDOS:
        return None

    if tipo == "snapshot":
        estados = msg.get("states")
        estados = estados if isinstance(estados, dict) else {}
        visibles = {
            eid: limpiar_estado(s) for eid, s in estados.items() if filtro is None or filtro.ve(eid)
        }
        salida = {**msg, "states": visibles}
        if filtro is not None:
            areas = msg.get("areas")
            salida["areas"] = [
                {**a, "entity_ids": [e for e in a.get("entity_ids", []) if isinstance(e, str) and filtro.ve(e)]}
                for a in (areas if isinstance(areas, list) else [])
                if isinstance(a, dict)
            ]
        return salida

    if tipo == "state_changed":
        eid = msg.get("entity_id")
        if filtro is not None and not (isinstance(eid, str) and filtro.ve(eid)):
            return None
        nuevo = msg.get("new_state")
        limpio = limpiar_estado(nuevo)
        return msg if limpio is nuevo else {**msg, "new_state": limpio}

    if tipo == "service_complete" and filtro is not None:
        eid = msg.get("entity_id")
        if isinstance(eid, str) and not filtro.ve(eid):
            return None

    return msg
