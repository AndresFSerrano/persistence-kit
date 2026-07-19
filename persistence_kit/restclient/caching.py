from __future__ import annotations

import asyncio
import base64
import hashlib
import json as _json
import logging
import time
from typing import Any, Awaitable, Callable, Mapping

from persistence_kit.cache.contracts import Cache
from persistence_kit.restclient.contracts import RestClient, RestResponse

logger = logging.getLogger(__name__)

OnChange = Callable[[str], None]


def _serialize(response: RestResponse) -> dict[str, Any]:
    return {
        "status_code": response.status_code,
        "headers": dict(response.headers),
        "content_b64": base64.b64encode(response.content).decode("ascii"),
        "url": response.url,
    }


def _deserialize(data: Mapping[str, Any]) -> RestResponse:
    return RestResponse(
        status_code=data["status_code"],
        headers=dict(data["headers"]),
        content=base64.b64decode(data["content_b64"]),
        url=data["url"],
    )


class CachingRestClient:
    """Decora un RestClient con cache de respuestas idempotentes (GET) y
    deteccion de cambios por hash de contenido.

    La cacheabilidad se controla por la config del servicio
    (``cache_ttl_seconds``) o por-llamada con el parametro ``cache_ttl`` de
    ``request``: ``None`` usa el default del servicio, ``0`` no cachea esa
    llamada (bypass), y un valor ``> 0`` cachea con ese TTL.

    Cada entry guarda ``{response, hash, stored_at}``. Dentro del TTL fresco se
    sirve del cache; dentro de la ventana ``stale-while-revalidate`` se sirve el
    valor viejo Y se dispara una revalidacion en background (sin bloquear, sin
    jobs): re-consulta, compara el hash y, si cambio, actualiza y llama
    ``on_change(key)``.
    """

    def __init__(
        self,
        inner: RestClient,
        cache: Cache,
        *,
        default_ttl_seconds: float | None = None,
        default_swr_seconds: float = 0.0,
        name: str = "",
        on_change: OnChange | None = None,
        cacheable_methods: tuple[str, ...] = ("GET",),
    ) -> None:
        self._inner = inner
        self._cache = cache
        self._default_ttl = default_ttl_seconds
        self._default_swr = default_swr_seconds
        self._name = name
        self._on_change = on_change
        self._methods = frozenset(method.upper() for method in cacheable_methods)
        self._inflight: set[str] = set()

    def _key(self, method: str, service: str, params: Mapping[str, Any] | None) -> str:
        params_repr = _json.dumps(params or {}, sort_keys=True, default=str)
        return f"{self._name}|{method}|{service}|{params_repr}"

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
    ) -> RestResponse:
        fresh = cache_ttl if cache_ttl is not None else self._default_ttl
        swr = self._default_swr

        cacheable = (
            method.upper() in self._methods
            and json is None
            and xml is None
            and content is None
            and fresh is not None
            and fresh > 0
        )

        async def _fetch() -> RestResponse:
            return await self._inner.request(
                method,
                service,
                params=params,
                json=json,
                xml=xml,
                content=content,
                content_type=content_type,
                soap_action=soap_action,
                headers=headers,
            )

        if not cacheable:
            return await _fetch()

        key = self._key(method.upper(), service, params)
        entry = await self._cache.get(key)
        now = time.time()

        if entry is not None:
            age = now - entry.get("stored_at", now)
            if age < fresh:
                return _deserialize(entry["response"])
            if swr and age < fresh + swr:
                self._schedule_revalidate(key, entry.get("hash"), _fetch, fresh, swr)
                return _deserialize(entry["response"])

        response = await _fetch()
        if response.is_success:
            await self._store(key, response, fresh, swr, now)
        return response

    async def _store(
        self, key: str, response: RestResponse, fresh: float, swr: float, now: float
    ) -> None:
        value = {
            "response": _serialize(response),
            "hash": hashlib.sha256(response.content).hexdigest(),
            "stored_at": now,
        }
        await self._cache.set(key, value, fresh + (swr or 0.0))

    def _schedule_revalidate(
        self,
        key: str,
        old_hash: str | None,
        fetch: Callable[[], Awaitable[RestResponse]],
        fresh: float,
        swr: float,
    ) -> None:
        if key in self._inflight:
            return
        self._inflight.add(key)

        async def _run() -> None:
            try:
                response = await fetch()
                if response.is_success:
                    new_hash = hashlib.sha256(response.content).hexdigest()
                    await self._store(key, response, fresh, swr, time.time())
                    if new_hash != old_hash:
                        logger.info("cache: cambio detectado en %s", key)
                        if self._on_change is not None:
                            self._on_change(key)
            except Exception:
                logger.exception("cache: fallo revalidando %s", key)
            finally:
                self._inflight.discard(key)

        asyncio.create_task(_run())

    async def aclose(self) -> None:
        await self._inner.aclose()
