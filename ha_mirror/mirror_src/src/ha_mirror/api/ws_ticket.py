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
from pydantic import BaseModel

from ha_mirror.auth import require_api_key
from ha_mirror.ws_ticket import issue_ticket

router = APIRouter()

# Mismo patron que usa el frontend y el allowlist de camaras (anti-SSRF).
_CAMERA_PATTERN = re.compile(r"^camera\.[a-z0-9_]+$")


class WsTicketRequest(BaseModel):
    scope: Literal["state", "camera"]
    entity_id: str | None = None


class WsTicketResponse(BaseModel):
    ticket: str
    expires_in: int


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

    ticket, ttl = issue_ticket(settings, body.scope, body.entity_id)
    return WsTicketResponse(ticket=ticket, expires_in=ttl)
