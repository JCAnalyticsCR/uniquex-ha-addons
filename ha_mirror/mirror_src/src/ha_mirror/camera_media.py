"""Clientes internos para snapshots de Home Assistant y WebRTC de go2rtc.

Las URLs y credenciales permanecen dentro del add-on Mirror. Los endpoints
publicos nunca aceptan una URL arbitraria ni un nombre de stream enviado por
el navegador; ambos se resuelven desde configuracion validada para evitar SSRF.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator
from urllib.parse import quote

import aiohttp


class CameraMediaError(Exception):
    """Error controlado al obtener medios de camara."""

    def __init__(self, code: str, status_code: int = 502) -> None:
        super().__init__(code)
        self.code = code
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class SnapshotPayload:
    """Imagen validada devuelta por Home Assistant."""

    body: bytes
    content_type: str


class CameraMediaClient:
    """Acceso saliente y limitado a HA Core y a go2rtc."""

    _ALLOWED_IMAGE_TYPES = frozenset({"image/jpeg", "image/png", "image/webp"})
    _MAX_SNAPSHOT_BYTES = 12 * 1024 * 1024
    _MAX_SDP_BYTES = 256 * 1024

    def __init__(
        self,
        *,
        ha_base_url: str,
        ha_token: str,
        go2rtc_base_url: str | None,
        go2rtc_username: str | None,
        go2rtc_password: str | None,
        camera_streams: dict[str, str],
    ) -> None:
        self._ha_base_url = ha_base_url.rstrip("/")
        self._ha_token = ha_token
        self._go2rtc_base_url = go2rtc_base_url.rstrip("/") if go2rtc_base_url else None
        self._go2rtc_auth = (
            aiohttp.BasicAuth(go2rtc_username, go2rtc_password)
            if go2rtc_username and go2rtc_password
            else None
        )
        self._camera_streams = dict(camera_streams)
        self._session: aiohttp.ClientSession | None = None
        self._snapshot_slots = asyncio.Semaphore(24)
        self._webrtc_slots = asyncio.Semaphore(4)
        # La cuadrícula de Fortunata muestra 20 substreams simultáneos.
        self._stream_slots = asyncio.Semaphore(24)

    async def start(self) -> None:
        """Crea una sesion HTTP reutilizable; llamar durante lifespan."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15, connect=5, sock_read=12)
            self._session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._ha_token = ""

    @property
    def webrtc_enabled(self) -> bool:
        return self._go2rtc_base_url is not None and bool(self._camera_streams)

    def has_stream(self, entity_id: str) -> bool:
        return self.webrtc_enabled and entity_id in self._camera_streams

    def list_stream_entities(self) -> list[str]:
        """entity_ids con stream go2rtc mapeado (allowlist), en orden estable."""
        if not self.webrtc_enabled:
            return []
        return sorted(self._camera_streams)

    def _require_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            raise CameraMediaError("media_client_not_started", 503)
        return self._session

    async def get_snapshot(self, entity_id: str) -> SnapshotPayload:
        """Obtiene una imagen fija desde go2rtc o el proxy de camara de HA."""
        if self.has_stream(entity_id):
            return await self._get_go2rtc_snapshot(entity_id)

        session = self._require_session()
        url = f"{self._ha_base_url}/api/camera_proxy/{entity_id}"
        headers = {
            "Authorization": f"Bearer {self._ha_token}",
            "Accept": "image/jpeg,image/png,image/webp",
        }

        try:
            async with self._snapshot_slots, session.get(
                url, headers=headers, allow_redirects=False
            ) as response:
                if response.status == 404:
                    raise CameraMediaError("snapshot_not_found", 404)
                if response.status in {401, 403}:
                    raise CameraMediaError("ha_snapshot_auth_failed", 502)
                if response.status >= 400:
                    raise CameraMediaError("ha_snapshot_unavailable", 502)

                content_type = response.headers.get("Content-Type", "").split(";", 1)[0].lower()
                if content_type not in self._ALLOWED_IMAGE_TYPES:
                    raise CameraMediaError("invalid_snapshot_content_type", 502)

                declared_size = response.content_length
                if declared_size is not None and declared_size > self._MAX_SNAPSHOT_BYTES:
                    raise CameraMediaError("snapshot_too_large", 502)

                body = await response.content.read(self._MAX_SNAPSHOT_BYTES + 1)
                if not body:
                    raise CameraMediaError("empty_snapshot", 502)
                if len(body) > self._MAX_SNAPSHOT_BYTES:
                    raise CameraMediaError("snapshot_too_large", 502)
                return SnapshotPayload(body=body, content_type=content_type)
        except CameraMediaError:
            raise
        except TimeoutError as exc:
            raise CameraMediaError("ha_snapshot_timeout", 504) from exc
        except aiohttp.ClientError as exc:
            raise CameraMediaError("ha_snapshot_connection_failed", 502) from exc

    async def _get_go2rtc_snapshot(self, entity_id: str) -> SnapshotPayload:
        """Extrae un JPEG del stream go2rtc allowlisted para camaras virtuales."""
        session = self._require_session()
        if not self._go2rtc_base_url:
            raise CameraMediaError("go2rtc_not_configured", 503)
        stream_name = self._camera_streams.get(entity_id)
        if stream_name is None:
            raise CameraMediaError("camera_stream_not_configured", 404)

        encoded = quote(stream_name, safe="")
        url = f"{self._go2rtc_base_url}/api/frame.jpeg?src={encoded}"
        try:
            async with self._snapshot_slots, session.get(
                url,
                headers={"Accept": "image/jpeg"},
                auth=self._go2rtc_auth,
                allow_redirects=False,
            ) as response:
                if response.status == 404:
                    raise CameraMediaError("go2rtc_stream_not_found", 404)
                if response.status in {401, 403}:
                    raise CameraMediaError("go2rtc_auth_failed", 502)
                if response.status >= 400:
                    raise CameraMediaError("go2rtc_snapshot_failed", 502)

                content_type = (
                    response.headers.get("Content-Type", "").split(";", 1)[0].lower()
                )
                if content_type != "image/jpeg":
                    raise CameraMediaError("invalid_snapshot_content_type", 502)
                body = await response.content.read(self._MAX_SNAPSHOT_BYTES + 1)
                if not body:
                    raise CameraMediaError("empty_snapshot", 502)
                if len(body) > self._MAX_SNAPSHOT_BYTES:
                    raise CameraMediaError("snapshot_too_large", 502)
                return SnapshotPayload(body=body, content_type=content_type)
        except CameraMediaError:
            raise
        except TimeoutError as exc:
            raise CameraMediaError("go2rtc_snapshot_timeout", 504) from exc
        except aiohttp.ClientError as exc:
            raise CameraMediaError("go2rtc_connection_failed", 502) from exc

    async def exchange_webrtc_offer(
        self,
        entity_id: str,
        offer_sdp: str,
        user_agent: str | None = None,
    ) -> str:
        """Intercambia SDP con la API WHEP interna de go2rtc."""
        session = self._require_session()
        if not self._go2rtc_base_url:
            raise CameraMediaError("go2rtc_not_configured", 503)
        stream_name = self._camera_streams.get(entity_id)
        if stream_name is None:
            raise CameraMediaError("camera_stream_not_configured", 404)
        encoded = quote(stream_name, safe="")
        url = f"{self._go2rtc_base_url}/api/webrtc?src={encoded}"
        headers = {
            "Content-Type": "application/sdp",
            "Accept": "application/sdp",
        }
        if user_agent:
            headers["User-Agent"] = user_agent[:256]

        try:
            async with self._webrtc_slots, session.post(
                url,
                data=offer_sdp.encode("utf-8"),
                headers=headers,
                auth=self._go2rtc_auth,
                allow_redirects=False,
            ) as response:
                if response.status == 404:
                    raise CameraMediaError("go2rtc_stream_not_found", 404)
                if response.status in {401, 403}:
                    raise CameraMediaError("go2rtc_auth_failed", 502)
                if response.status >= 400:
                    raise CameraMediaError("go2rtc_offer_failed", 502)

                content_type = response.headers.get("Content-Type", "").split(";", 1)[0].lower()
                if content_type != "application/sdp":
                    raise CameraMediaError("invalid_webrtc_answer_content_type", 502)
                answer = await response.content.read(self._MAX_SDP_BYTES + 1)
                if not answer or len(answer) > self._MAX_SDP_BYTES:
                    raise CameraMediaError("invalid_webrtc_answer_size", 502)
                return answer.decode("utf-8")
        except CameraMediaError:
            raise
        except (TimeoutError, UnicodeDecodeError) as exc:
            raise CameraMediaError("go2rtc_offer_timeout", 504) from exc
        except aiohttp.ClientError as exc:
            raise CameraMediaError("go2rtc_connection_failed", 502) from exc

    @asynccontextmanager
    async def open_mp4_stream(
        self,
        entity_id: str,
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        """Abre un fMP4 continuo desde un stream go2rtc mapeado."""
        session = self._require_session()
        if not self._go2rtc_base_url:
            raise CameraMediaError("go2rtc_not_configured", 503)
        stream_name = self._camera_streams.get(entity_id)
        if stream_name is None:
            raise CameraMediaError("camera_stream_not_configured", 404)

        encoded = quote(stream_name, safe="")
        url = f"{self._go2rtc_base_url}/api/stream.mp4?src={encoded}"
        timeout = aiohttp.ClientTimeout(total=None, connect=5, sock_read=15)

        async with self._stream_slots:
            try:
                async with session.get(
                    url,
                    headers={"Accept": "video/mp4"},
                    auth=self._go2rtc_auth,
                    allow_redirects=False,
                    timeout=timeout,
                ) as response:
                    if response.status == 404:
                        raise CameraMediaError("go2rtc_stream_not_found", 404)
                    if response.status in {401, 403}:
                        raise CameraMediaError("go2rtc_auth_failed", 502)
                    if response.status >= 400:
                        raise CameraMediaError("go2rtc_stream_failed", 502)
                    content_type = (
                        response.headers.get("Content-Type", "").split(";", 1)[0].lower()
                    )
                    if content_type != "video/mp4":
                        raise CameraMediaError("invalid_stream_content_type", 502)
                    yield response
            except CameraMediaError:
                raise
            except TimeoutError as exc:
                raise CameraMediaError("go2rtc_stream_timeout", 504) from exc
            except aiohttp.ClientError as exc:
                raise CameraMediaError("go2rtc_connection_failed", 502) from exc
