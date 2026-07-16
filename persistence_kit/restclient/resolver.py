from __future__ import annotations

import time
from typing import Awaitable, Callable

from persistence_kit.restclient.contracts import RestResponse
from persistence_kit.restclient.errors import RestConfigError

DirectoryFetch = Callable[[str], Awaitable[RestResponse]]
DirectoryParse = Callable[[RestResponse], str]

_DEFAULT_URL_KEYS = ("url", "endpoint", "href", "location", "wsdl")


def _dig(data: object, path: str) -> object:
    current = data
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            current = getattr(current, part, None)
    return current


class StaticEndpointResolver:
    """Resolves a service name against a fixed base URL.

    An absolute URL is returned untouched, so the same client can mix relative
    paths and full URLs.
    """

    def __init__(self, base_url: str = "") -> None:
        self._base_url = base_url.rstrip("/")

    @property
    def base_url(self) -> str:
        return self._base_url

    async def resolve(self, service: str) -> str:
        if service.startswith(("http://", "https://")):
            return service
        if not self._base_url:
            raise RestConfigError(
                "StaticEndpointResolver requiere base_url para resolver rutas relativas."
            )
        return f"{self._base_url}/{service.lstrip('/')}"


class DirectoryEndpointResolver:
    """Resolves a service name to its URL by querying a directory endpoint.

    Generic: any directory that maps a service id to a URL fits. ``url_template``
    is formatted with ``{service}``.

    Extraction of the URL from the directory response, easiest first:
    - Nothing: a plain-text URL body, or a common JSON field (``url``,
      ``endpoint``, ``href``, ``location``, ``wsdl``), is picked automatically.
    - ``url_field``: the JSON field holding the URL (dotted path, e.g. ``data.url``).
    - ``parse``: a callable for anything else (XML directories, odd shapes).

    Resolved URLs are cached per service for ``cache_ttl_seconds``. ``fetch`` is
    injectable so the resolver stays decoupled from any transport and testable
    without network.
    """

    def __init__(
        self,
        url_template: str,
        *,
        url_field: str | None = None,
        parse: DirectoryParse | None = None,
        cache_ttl_seconds: float = 300.0,
        fetch: DirectoryFetch | None = None,
        verify_tls: bool = True,
        timeout_seconds: float = 10.0,
    ) -> None:
        if "{service}" not in url_template:
            raise RestConfigError(
                "url_template debe incluir el marcador '{service}'."
            )
        if url_field is not None and parse is not None:
            raise RestConfigError("Usa 'url_field' o 'parse', no ambos.")
        self._url_template = url_template
        self._url_field = url_field
        self._parse = parse
        self._ttl = cache_ttl_seconds
        self._fetch = fetch
        self._verify_tls = verify_tls
        self._timeout = timeout_seconds
        self._cache: dict[str, tuple[str, float]] = {}

    async def resolve(self, service: str) -> str:
        cached = self._cache.get(service)
        if cached is not None and time.monotonic() < cached[1]:
            return cached[0]

        directory_url = self._url_template.format(service=service)
        response = await self._do_fetch(directory_url)
        if not response.is_success:
            raise RestConfigError(
                f"El directorio respondio {response.status_code} al resolver '{service}'."
            )

        extracted = self._extract(response)
        resolved = "" if extracted is None else str(extracted).strip()
        if not resolved:
            raise RestConfigError(
                f"El directorio no devolvio una URL para el servicio '{service}'."
            )

        self._cache[service] = (resolved, time.monotonic() + self._ttl)
        return resolved

    def _extract(self, response: RestResponse) -> object:
        if self._parse is not None:
            return self._parse(response)
        if self._url_field is not None:
            return _dig(response.body(), self._url_field)

        data = response.body()
        if isinstance(data, str):
            return data
        if isinstance(data, dict):
            for key in _DEFAULT_URL_KEYS:
                value = data.get(key)
                if isinstance(value, str) and value:
                    return value
        return response.text

    def invalidate(self, service: str | None = None) -> None:
        if service is None:
            self._cache.clear()
        else:
            self._cache.pop(service, None)

    async def _do_fetch(self, url: str) -> RestResponse:
        if self._fetch is not None:
            return await self._fetch(url)

        import httpx

        async with httpx.AsyncClient(
            verify=self._verify_tls, timeout=self._timeout
        ) as client:
            raw = await client.get(url)
        return RestResponse(
            status_code=raw.status_code,
            headers=dict(raw.headers),
            content=raw.content,
            url=str(raw.request.url),
        )
