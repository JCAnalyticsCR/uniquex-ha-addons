"""
Catálogo de familias de integraciones y lista de dominios no recargables.

Usado por OnboardingService para:
- Clasificar las config entries de HA en grupos inteligibles para el cliente.
- Decidir qué integraciones son elegibles para el rescan (reload) desde la app.

El catálogo es AMPLIABLE sin tocar el resto del código: agregar una entrada en
FAMILIAS alcanza para que la nueva integración aparezca con su label en español.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Deny-list: dominios cuyo reload NUNCA se expone desde la app.
# Cubre integraciones de sistema, helpers de HA, TTS, automatizaciones, etc.
# Cualquier dominio que afecte el SO del gateway o que no tenga sentido
# recargar desde la app del cliente va aquí.
# ---------------------------------------------------------------------------
DENY_LIST: frozenset[str] = frozenset(
    {
        "hassio",
        "supervisor",
        "homeassistant",
        "mobile_app",
        "cloud",
        "backup",
        "hacs",
        "default_config",
        "onboarding",
        "analytics",
        "sun",
        "zone",
        "met",
        "radio_browser",
        "shopping_list",
        "todo",
        "google_translate",
        "tts",
        "conversation",
        "assist_pipeline",
        "matter",
        "thread",
        "otbr",
        "zeroconf",
        "dhcp",
        "ssdp",
        "bluetooth",
        "usb",
        "energy",
        "history",
        "logbook",
        "recorder",
        "person",
        "input_boolean",
        "input_number",
        "input_select",
        "input_text",
        "input_datetime",
        "input_button",
        "schedule",
        "counter",
        "timer",
        "group",
        "template",
        "automation",
        "script",
        "scene",
        "webhook",
        "api",
        "frontend",
        "http",
        "stream",
        "ffmpeg",
        "image_upload",
        "file_upload",
        "hardware",
        "network",
        "application_credentials",
        "my",
        "map",
        "config",
        "system_health",
        "diagnostics",
        "repairs",
        "tag",
        "blueprint",
        "device_automation",
        "logger",
        "system_log",
        "search",
        "trace",
        "update",
        "wake_on_lan",
    }
)


def es_recargable(domain: str) -> bool:
    """True si el dominio puede recargarse desde la app (no está en la deny-list)."""
    return domain.lower().strip() not in DENY_LIST


# ---------------------------------------------------------------------------
# Catálogo de familias: domain de HA → (family_id, label en español)
# Semilla mínima, ampliable sin tocar el resto del código.
# ---------------------------------------------------------------------------
FAMILIAS: dict[str, tuple[str, str]] = {
    "coolmaster": ("clima", "Aires acondicionados"),
    "overkiz": ("persianas", "Persianas Somfy"),
    "espsomfy_rts": ("persianas", "Persianas Somfy (local)"),
    "sonoff": ("interruptores", "Interruptores eWeLink"),
    "ewelink": ("interruptores", "Interruptores eWeLink"),
    "ewelink_smart_home": ("interruptores", "Interruptores eWeLink"),
    "dahua": ("camaras", "Cámaras"),
    "amcrest": ("camaras", "Cámaras"),
    "reolink": ("camaras", "Cámaras"),
    "generic": ("camaras", "Cámaras"),
    "crestron_home": ("crestron", "Crestron Home"),
    "sonos": ("audio", "Sonos"),
    "samsungtv": ("tv", "Televisores"),
    "apple_tv": ("tv", "Televisores"),
    "androidtv_remote": ("tv", "Televisores"),
    "dlna_dmr": ("tv", "Televisores"),
    "cast": ("tv", "Televisores"),
    "webostv": ("tv", "Televisores"),
    "zha": ("zigbee", "Zigbee"),
    "mqtt": ("zigbee", "Zigbee"),
    "zwave_js": ("zwave", "Z-Wave"),
    "shelly": ("wifi", "Dispositivos Wi-Fi locales"),
    "esphome": ("wifi", "Dispositivos Wi-Fi locales"),
    "tasmota": ("wifi", "Dispositivos Wi-Fi locales"),
    "tuya": ("nube", "Dispositivos en la nube"),
    "govee": ("nube", "Dispositivos en la nube"),
    "smartthings": ("nube", "Dispositivos en la nube"),
    "hue": ("luces", "Iluminación"),
    "lifx": ("luces", "Iluminación"),
    "wled": ("luces", "Iluminación"),
}

_FAMILIA_DEFAULT = "otros"


# ---------------------------------------------------------------------------
# Enmascarado de títulos — privacidad del INSTALADOR, no del cliente.
#
# Home Assistant titula muchas config entries con la CUENTA con la que se
# configuró la integración, y esa cuenta suele ser la del instalador, no la del
# dueño de casa: una entrada de Overkiz/Somfy se llama literalmente
# "sc3698889@gmail.com", y lo mismo pasa con SmartThings, Nest o cualquier
# integración de nube. Ese título viaja en /capabilities y la app lo pinta en
# el selector de "Buscar dispositivos nuevos".
#
# Es EXACTAMENTE el mismo error que ya se cometió una vez en este proyecto: el
# teléfono del instalador (`person.*` + sus sensores de ubicación) llegó a la
# pantalla del cliente porque nadie se preguntó de quién era ese dato. Se
# enmascara acá, en el origen, y no en el frontend: así ninguna pantalla futura
# puede volver a filtrarlo por olvido.
# ---------------------------------------------------------------------------
_RE_CORREO = re.compile(r"[^@\s]+@[^@\s]+\.[A-Za-z]{2,}")


def enmascarar_titulo(title: str) -> str:
    """Reemplaza cualquier correo dentro del título por una etiqueta neutra."""
    return _RE_CORREO.sub("cuenta privada", title)


def familia_de(domain: str, title: str) -> tuple[str, str]:
    """
    Clasifica una config entry en una familia.

    Devuelve (family, label). Si el dominio no está en el catálogo, el label
    cae al título del entry — YA ENMASCARADO. Si el título era solo un correo,
    el enmascarado lo deja sin información útil, así que se usa el dominio
    (p. ej. "smartthings"), que nombra al sistema sin exponer a nadie.
    """
    entry = FAMILIAS.get(domain.lower().strip())
    if entry is not None:
        return entry
    limpio = enmascarar_titulo(title).strip()
    if not limpio or limpio == "cuenta privada":
        return (_FAMILIA_DEFAULT, domain or "Integración")
    return (_FAMILIA_DEFAULT, limpio)
