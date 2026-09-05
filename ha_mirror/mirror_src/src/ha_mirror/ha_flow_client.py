"""
Cliente del asistente de alta de Home Assistant.

Es la ÚNICA superficie REST admin del Mirror. Todo lo demás habla WebSocket.

POR QUÉ REST Y NO WEBSOCKET
---------------------------
Se midió contra una casa real el 2026-09-03, y las dos mitades no están donde
uno esperaría:

  · **Listar** lo que HA descubrió → WebSocket `config_entries/flow/progress`.
    Por REST no se puede: `GET /api/config/config_entries/flow` devuelve **405**.
  · **Crear y avanzar** un formulario → REST. No hay comando WS que lo haga.

Así que el módulo de onboarding usa las dos vías a propósito, no por descuido.

POR QUÉ ESTO ES LO QUE PERMITE EL ESPEJO
-----------------------------------------
HA no solo acepta el formulario: lo **describe**. Cada paso vuelve con un
`data_schema` en JSON que dice qué campos hay, de qué tipo y cuáles son
obligatorios. Eso significa que la app puede dibujar el formulario de CUALQUIER
marca sin que nadie programe una pantalla por marca — y que una integración
nueva funciona sin tocar código.

CREDENCIALES
------------
Lo que la persona escribe (una contraseña de la nube del fabricante, por
ejemplo) pasa de largo hacia HA: **no se loguea, no se guarda, no se cachea.**
Por eso acá no hay un solo `logger` que reciba `data`.
"""

from __future__ import annotations

from typing import Any

import aiohttp
import structlog

from ha_mirror.errors import HaProtocolError, UpstreamNotReadyError

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# Un flow que tarda más que esto no va a terminar bien igual: casi siempre es
# una IP mal escrita y HA esperando un timeout de red que ya no le importa a
# nadie del otro lado de la pantalla.
TIMEOUT_S = 30.0


class FlowNoEncontrado(Exception):
    """El flow_id no existe (o ya terminó) — 404."""


class HaFlowClient:
    """
    Habla con `/api/config/config_entries/flow` de Home Assistant.

    No decide NADA sobre qué se puede dar de alta: eso es del servicio de
    onboarding, que tiene la lista blanca. Acá solo se hacen las llamadas.
    """

    def __init__(self, base_url: str, token: str) -> None:
        self._base = base_url.rstrip("/")
        self._token = token

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    async def _pedir(
        self, metodo: str, ruta: str, cuerpo: dict[str, Any] | None = None
    ) -> Any:
        url = f"{self._base}{ruta}"
        try:
            timeout = aiohttp.ClientTimeout(total=TIMEOUT_S)
            async with aiohttp.ClientSession(timeout=timeout) as sesion:
                async with sesion.request(
                    metodo, url, headers=self._headers, json=cuerpo
                ) as resp:
                    if resp.status == 404:
                        raise FlowNoEncontrado(ruta)
                    if resp.status in (401, 403):
                        # El token del add-on no es admin. Se trata igual que en
                        # el resto del módulo: la función se apaga sola.
                        raise HaProtocolError("unauthorized")
                    if resp.status >= 400:
                        # El cuerpo del error de HA puede traer lo que la persona
                        # escribió. Solo se conserva el código.
                        raise HaProtocolError(f"HA respondio {resp.status}")
                    if resp.status == 204 or not resp.content_length:
                        texto = await resp.text()
                        return None if not texto else await resp.json()
                    return await resp.json()
        except aiohttp.ClientError as exc:
            # Sin conexión a HA. Mismo error que usa el resto del Mirror para
            # que la capa de arriba no tenga que distinguir de dónde vino.
            raise UpstreamNotReadyError(f"No se pudo hablar con HA: {type(exc).__name__}") from exc

    async def marcas_disponibles(self) -> list[str]:
        """Dominios que HA sabe dar de alta. En la casa de referencia: 728."""
        res = await self._pedir("GET", "/api/config/config_entries/flow_handlers")
        return [str(x) for x in (res or [])]

    async def iniciar(self, handler: str) -> dict[str, Any]:
        """
        Arranca un formulario de alta.

        `show_advanced_options` va en False a propósito: son los campos que HA
        le esconde hasta a sus propios usuarios salvo que activen el modo
        avanzado. Un cliente no tiene por qué verlos.
        """
        res = await self._pedir(
            "POST",
            "/api/config/config_entries/flow",
            {"handler": handler, "show_advanced_options": False},
        )
        return dict(res or {})

    async def avanzar(self, flow_id: str, datos: dict[str, Any]) -> dict[str, Any]:
        """Contesta un paso. `datos` es lo que escribió la persona: pasa de largo."""
        res = await self._pedir(
            "POST", f"/api/config/config_entries/flow/{flow_id}", datos
        )
        return dict(res or {})

    async def cancelar(self, flow_id: str) -> None:
        """Abandona un formulario a medio llenar."""
        await self._pedir("DELETE", f"/api/config/config_entries/flow/{flow_id}")
