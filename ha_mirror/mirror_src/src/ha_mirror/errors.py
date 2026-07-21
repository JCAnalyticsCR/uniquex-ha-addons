"""
Excepciones de dominio del mirror.

Distingue entre fallos recuperables (retry con backoff) y no recuperables
(intervención humana requerida). No mapea directamente a códigos JS de
home-assistant-js-websocket — son excepciones Python propias.
"""

from __future__ import annotations


class HaAuthError(Exception):
    """
    Token LLAT inválido o revocado.

    El servidor HA respondió 'auth_invalid'. NO se debe reintentar
    automáticamente — el LLAT exige rotación manual o intervención del operador.
    """


class HaConnectError(Exception):
    """
    Fallo de conexión TCP/TLS/WS upgrade.

    Cubre: ClientConnectorError, WSServerHandshakeError, ServerDisconnectedError.
    Elegible para backoff exponencial con jitter y reintento automático.
    """


class HaProtocolError(Exception):
    """
    Frame inesperado o malformado en el protocolo HA WebSocket.

    El servidor envió algo fuera del handshake esperado. Se trata igual
    que HaConnectError (reconectar), pero se logguea con nivel más alto
    para detectar cambios de protocolo en actualizaciones de HA.
    """


class MirrorConfigError(Exception):
    """Error de configuración detectado al arrancar (LLAT no descifrable, etc.)."""


class UpstreamNotReadyError(Exception):
    """El upstream no está en estado READY — no se puede ejecutar el comando."""
