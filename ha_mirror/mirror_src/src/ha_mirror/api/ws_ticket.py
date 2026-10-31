"""
POST /api/ws-ticket — emite un ticket de vida corta para abrir un WebSocket.

Lo llama el FRONTEND desde el servidor (Railway), con X-API-Key. El navegador
nunca ve la key: recibe solo el ticket, que caduca en segundos y sirve para un
unico scope/entidad. Ver `ha_mirror/ws_ticket.py` para el porque.
"""

from __future__ import annotations

import re
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from ha_mirror.auth import require_api_key
from ha_mirror.filtro_ws import MAX_CAMARAS, MAX_DOMINIOS, FiltroWs
from ha_mirror.ws_ticket import issue_ticket

router = APIRouter()

# Mismo patron que usa el frontend y el allowlist de camaras (anti-SSRF).
_CAMERA_PATTERN = re.compile(r"^camera\.[a-z0-9_]+$")


class FiltroRequest(BaseModel):
    """Qué ve quien abre el WS de estado (0.53.0). Lo arma el frontend con el rol."""

    dominios: list[str] = Field(max_length=MAX_DOMINIOS)
    camaras: list[str] = Field(default_factory=list, max_length=MAX_CAMARAS)


class WsTicketRequest(BaseModel):
    scope: Literal["state", "camera"]
    entity_id: str | None = None
    filtro: FiltroRequest | None = None


class WsTicketResponse(BaseModel):
    ticket: str
    expires_in: int
    #: `True` solo si el ticket lleva el filtro pedido. El frontend lo exige
    #: para un miembro: un Mirror anterior a 0.53.0 ignoraría `filtro` en
    #: silencio y emitiría un ticket que ve todo.
    filtrado: bool = False


@router.post(
    "/api/ws-ticket",
    response_model=WsTicketResponse,
    summary="Emite un ticket de corta duracion para un WebSocket",
)
async def post_ws_ticket(
    request: Request,
    body: WsTicketRequest,
    _: None = Depends(require_api_key),
) -> WsTicketResponse:
    """
    Emite el ticket. El scope y la entidad quedan FIRMADOS dentro:
    un ticket de estado no abre una camara, y uno de una camara no abre otra.
    """
    settings = request.app.state.settings

    if body.scope == "camera":
        if body.entity_id is None or not _CAMERA_PATTERN.match(body.entity_id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="entity_id invalido para scope=camera",
            )
    elif body.entity_id is not None:
        # Un ticket de estado no lleva entidad. Rechazar en vez de ignorar:
        # si el frontend la manda, el que se emita no coincidiria al validar.
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="scope=state no acepta entity_id",
        )

    filtro: FiltroWs | None = None
    if body.filtro is not None:
        if body.scope != "state":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="solo scope=state acepta filtro",
            )
        try:
            filtro = FiltroWs.validar(body.filtro.dominios, body.filtro.camaras)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    ticket, ttl = issue_ticket(settings, body.scope, body.entity_id, filtro)
    return WsTicketResponse(ticket=ticket, expires_in=ttl, filtrado=filtro is not None)
