from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Mapping

from persistence_kit.restclient.contracts import RestRequest

XML_CONTENT_TYPE = "application/xml; charset=utf-8"
SOAP_CONTENT_TYPE = "text/xml; charset=utf-8"


def serialize_xml(value: Any) -> bytes:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, ET.Element):
        return ET.tostring(value, encoding="utf-8")
    raise TypeError("xml debe ser str, bytes o xml.etree.ElementTree.Element.")


def prepare_request(
    method: str,
    url: str,
    *,
    default_headers: Mapping[str, str] | None = None,
    params: Mapping[str, Any] | None = None,
    json: Any | None = None,
    xml: Any | None = None,
    content: bytes | None = None,
    content_type: str | None = None,
    soap_action: str | None = None,
    headers: Mapping[str, str] | None = None,
) -> RestRequest:
    """Merge default and per-request headers and normalize the body.

    Bodies are mutually exclusive by intent: ``json`` for JSON, ``xml`` for
    XML/SOAP, or ``content`` for raw bytes. ``soap_action`` sets the SOAPAction
    header and defaults the content type to text/xml.
    """
    final_headers: dict[str, str] = {}
    if default_headers:
        final_headers.update(default_headers)
    if headers:
        final_headers.update(headers)

    if json is not None and xml is not None:
        raise ValueError("No se puede enviar 'json' y 'xml' en la misma solicitud.")

    body: bytes | None = content
    if xml is not None:
        body = serialize_xml(xml)
        default_ct = SOAP_CONTENT_TYPE if soap_action is not None else XML_CONTENT_TYPE
        final_headers.setdefault("Content-Type", content_type or default_ct)
    elif content is not None and content_type:
        final_headers.setdefault("Content-Type", content_type)

    if soap_action is not None:
        final_headers["SOAPAction"] = f'"{soap_action}"'

    return RestRequest(
        method=method.upper(),
        url=url,
        params=dict(params or {}),
        headers=final_headers,
        json_body=json if body is None else None,
        content=body,
    )
