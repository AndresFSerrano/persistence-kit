from __future__ import annotations

import json as _json
import xml.etree.ElementTree as _ET
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, runtime_checkable


@dataclass(slots=True)
class RestRequest:
    method: str
    url: str
    params: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    json_body: Any | None = None
    content: bytes | None = None


@dataclass(slots=True)
class RestResponse:
    status_code: int
    headers: Mapping[str, str]
    content: bytes
    url: str

    @property
    def content_type(self) -> str:
        for key, value in self.headers.items():
            if key.lower() == "content-type":
                return value.split(";", 1)[0].strip().lower()
        return ""

    def _charset(self) -> str:
        for key, value in self.headers.items():
            if key.lower() == "content-type" and "charset=" in value.lower():
                return value.lower().split("charset=", 1)[1].split(";")[0].strip() or "utf-8"
        return "utf-8"

    @property
    def text(self) -> str:
        return self.content.decode(self._charset(), errors="replace")

    def json(self) -> Any:
        return _json.loads(self.content or b"null")

    def xml(self) -> _ET.Element:
        return _ET.fromstring(self.content)

    def body(self) -> Any:
        """Decode the payload based on Content-Type: JSON to objects, XML/SOAP
        to an Element, anything else to text. Falls back to sniffing the first
        byte when no Content-Type is present."""
        content_type = self.content_type
        if "json" in content_type:
            return self.json()
        if "xml" in content_type or "soap" in content_type:
            return self.xml()
        if not content_type:
            head = self.content.lstrip()[:1]
            if head in (b"{", b"["):
                return self.json()
            if head == b"<":
                return self.xml()
        return self.text

    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300


@runtime_checkable
class Authenticator(Protocol):
    async def apply(self, request: RestRequest) -> RestRequest: ...

    async def on_unauthorized(self, response: RestResponse) -> bool: ...


@runtime_checkable
class EndpointResolver(Protocol):
    async def resolve(self, service: str) -> str: ...


@runtime_checkable
class RestClient(Protocol):
    async def request(
        self,
        method: str,
        service: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
        xml: Any | None = None,
        content: bytes | None = None,
        content_type: str | None = None,
        soap_action: str | None = None,
        headers: Mapping[str, str] | None = None,
        cache_ttl: float | None = None,
    ) -> RestResponse: ...

    async def aclose(self) -> None: ...
