"""
GET /api/device/identity — quién es esta caja.

Devuelve el `device_id` y la llave PÚBLICA que la caja generó sola en su primer
arranque. La llave privada no aparece acá ni en ningún otro endpoint: es el
único dato del Mirror que no tiene forma de salir por HTTP.

Protegido con X-API-Key igual que el resto. Cuando llegue el emparejamiento
habrá que revisar esta decisión — quien reclama la caja todavía no tiene la key —
pero para el paso actual (verificar en la mini PC que la identidad se genera y
sobrevive reinicios) el default seguro es el correcto.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from ha_mirror.auth import require_api_key
from ha_mirror.device_identity import (
    DeviceIdentityError,
    cargar_llave_privada,
    construir_url_qr,
    derivar_claim_code,
    hash_claim_code,
)

router = APIRouter()


class DeviceIdentityResponse(BaseModel):
    """
    La identidad publicable de la caja.

    Cada campo de acá puede terminar en un log, un ticket de soporte o un QR sin
    que eso comprometa nada. Si algún día alguien agrega un campo a este modelo,
    ese es el criterio para decidir si corresponde.
    """

    device_id: str
    public_key: str
    key_algorithm: str
    hardware_id: str | None
    created_at: str
    paired: bool
    paired_at: str | None
    paired_house_id: str | None


@router.get(
    "/api/device/identity",
    response_model=DeviceIdentityResponse,
    summary="Identidad criptográfica de esta caja",
)
async def get_device_identity(
    request: Request,
    _: None = Depends(require_api_key),
) -> DeviceIdentityResponse:
    """
    Lee la identidad que el lifespan ya resolvió al arrancar.

    No la genera acá: si la caja arrancó, la identidad existe. Un 503 en este
    endpoint significa que el arranque falló de un modo que hay que mirar en los
    logs, no algo que un reintento del cliente vaya a resolver.
    """
    identidad = getattr(request.app.state, "device_identity", None)
    if identidad is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="La identidad de la caja no está disponible — revisar los logs del arranque",
        )

    return DeviceIdentityResponse(
        device_id=identidad.device_id,
        public_key=identidad.public_key,
        key_algorithm=identidad.key_algorithm,
        hardware_id=identidad.hardware_id,
        created_at=identidad.created_at,
        paired=identidad.paired,
        paired_at=identidad.paired_at,
        paired_house_id=identidad.paired_house_id,
    )


class ClaimPayloadResponse(BaseModel):
    """Lo que hace falta para imprimir la calcomanía de una caja."""

    device_id: str
    claim_code: str
    claim_code_version: int
    # SHA-256 del código, en base64. Es lo que la caja le va a mandar al backend
    # cuando se anuncie: así la plataforma puede verificar el código que escanea
    # el cliente sin llegar a guardarlo nunca en claro.
    claim_code_hash: str
    qr_url: str


@router.get(
    "/api/device/claim-payload",
    response_model=ClaimPayloadResponse,
    summary="Datos para imprimir la calcomanía del QR (taller)",
)
async def get_claim_payload(
    request: Request,
    _: None = Depends(require_api_key),
) -> ClaimPayloadResponse:
    """
    Lo que se imprime en la calcomanía, para usar en el taller antes de instalar.

    Devuelve 409 si la caja YA está emparejada. Una calcomanía nueva para una
    caja con dueño no sirve para nada — el emparejamiento es de un solo uso — y
    exponer el código de una caja en producción es regalar información sin
    ninguna razón. Si de verdad hay que re-emparejarla, se sube
    `claim_code_version`, que emite un código nuevo y jubila el impreso.
    """
    identidad = getattr(request.app.state, "device_identity", None)
    if identidad is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="La identidad de la caja no está disponible — revisar los logs del arranque",
        )

    if identidad.paired:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Esta caja ya está emparejada (casa {identidad.paired_house_id}). "
                "Para re-emparejarla hay que subir claim_code_version."
            ),
        )

    settings = request.app.state.settings
    try:
        privada = cargar_llave_privada(settings.device_key_path)
    except DeviceIdentityError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc

    codigo = derivar_claim_code(privada, identidad.claim_code_version)
    # Una sola definición del hash, compartida con el cliente de anuncio y
    # equivalente a la del backend. Antes acá se calculaba por separado y SIN
    # `.strip().upper()`: coincidía de casualidad porque el alfabeto del código ya
    # es mayúsculas, pero era una trampa esperando a que alguien cambiara una de
    # las dos.
    codigo_hash = hash_claim_code(codigo)

    return ClaimPayloadResponse(
        device_id=identidad.device_id,
        claim_code=codigo,
        claim_code_version=identidad.claim_code_version,
        claim_code_hash=codigo_hash,
        qr_url=construir_url_qr(
            settings.platform_base_url, identidad.device_id, codigo
        ),
    )
