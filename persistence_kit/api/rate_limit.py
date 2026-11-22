from __future__ import annotations

import asyncio
import time
from collections import deque
from math import ceil
from time import monotonic
from typing import Callable

from fastapi import Depends, HTTPException, Request, status

from persistence_kit.cache.contracts import Cache
from persistence_kit.cache.factory import get_cache

RATE_LIMIT_ERROR_DETAIL = {
    "code": "TOO_MANY_AUTH_ATTEMPTS",
    "message": "Demasiados intentos. Intenta nuevamente en unos minutos.",
}
RATE_LIMIT_CACHE_NAME = "rate-limit"


class InMemoryRateLimiter:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._buckets: dict[str, deque[float]] = {}

    async def hit(
        self,
        *,
        scope: str,
        client_id: str,
        limit: int,
        window_seconds: int,
    ) -> int | None:
        if limit <= 0 or window_seconds <= 0:
            return None

        bucket_key = f"{scope}:{client_id}"
        now = monotonic()
        cutoff = now - window_seconds

        async with self._lock:
            bucket = self._buckets.setdefault(bucket_key, deque())

            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            if len(bucket) >= limit:
                retry_after = max(1, ceil(window_seconds - (now - bucket[0])))
                return retry_after

            bucket.append(now)
            return None


class CacheRateLimiter:
    """Limitador de intentos guardado en una ``Cache``: con un backend compartido
    (``CACHE_BACKEND=dynamodb`` o ``mongo``) todas las replicas cuentan juntas y
    un despliegue no reinicia el conteo.

    Cuenta con ventana deslizante aproximada: un contador por ventana fija
    (``{scope}:{client_id}:{ventana}`` en la cache ``rate-limit``) y la
    estimacion ``anterior * fraccion_que_queda_de_ella + actual``. Cada contador
    vive dos ventanas, lo justo para servir de "anterior", y luego lo borra el TTL.

    Un intento rechazado tambien cuenta: quien sigue martillando sigue
    bloqueado. ``Retry-After`` dice cuando pasaria el siguiente intento si
    nadie mas golpea. Usa reloj de pared (``time.time``) porque lo comparten
    varios hosts.
    """

    def __init__(self, cache: Cache, *, clock: Callable[[], float] = time.time) -> None:
        self._cache = cache
        self._clock = clock

    async def hit(
        self,
        *,
        scope: str,
        client_id: str,
        limit: int,
        window_seconds: int,
    ) -> int | None:
        if limit <= 0 or window_seconds <= 0:
            return None

        now = self._clock()
        window = int(now // window_seconds)
        elapsed = now - window * window_seconds
        bucket = f"{scope}:{client_id}"
        current, previous = await asyncio.gather(
            self._cache.increment(f"{bucket}:{window}", ttl_seconds=2 * window_seconds),
            self._cache.get(f"{bucket}:{window - 1}"),
        )
        previous = previous or 0
        if previous * (window_seconds - elapsed) + current * window_seconds <= limit * window_seconds:
            return None
        return _sliding_retry_after(previous, current, limit, window_seconds, elapsed)


def _sliding_retry_after(
    previous: int,
    current: int,
    limit: int,
    window_seconds: int,
    elapsed: float,
) -> int:
    """Segundos hasta que un intento mas cabria en el limite.

    Si la ventana actual aun tiene cupo, basta con que el peso de la anterior
    baje lo suficiente; si no, hay que esperar a la siguiente ventana, donde la
    actual pasa a ser la anterior y su peso va bajando."""
    if current < limit:
        wait = window_seconds - elapsed - window_seconds * (limit - current - 1) / previous
    else:
        wait = 2 * window_seconds - elapsed - window_seconds * (limit - 1) / current
    return max(1, ceil(wait))


def build_rate_limiter() -> CacheRateLimiter:
    """Arma el limitador sobre ``get_cache("rate-limit")``, asi que el backend lo
    decide ``CACHE_BACKEND``. Es el que usan las dependencias de auth cuando la
    app no puso uno propio en ``app.state.auth_rate_limiter``."""
    return CacheRateLimiter(get_cache(RATE_LIMIT_CACHE_NAME))


def _resolve_client_id(request: Request, trusted_proxy_hops: int) -> str:
    forwarded_for = request.headers.get("x-forwarded-for", "")
    chain = [hop.strip() for hop in forwarded_for.split(",") if hop.strip()]
    if 0 < trusted_proxy_hops <= len(chain):
        return chain[-trusted_proxy_hops]

    client_host = request.client.host if request.client else None
    return client_host or "unknown"


def _get_rate_limiter(request: Request) -> InMemoryRateLimiter | CacheRateLimiter:
    limiter = getattr(request.app.state, "auth_rate_limiter", None)
    if limiter is None:
        limiter = build_rate_limiter()
        request.app.state.auth_rate_limiter = limiter
    return limiter


def build_auth_rate_limit_dependency(
    *,
    scope: str,
    limit_setting_name: str,
    settings_dep: Callable,
) -> Callable:
    async def dependency(
        request: Request,
        settings=Depends(settings_dep),
    ) -> None:
        if not settings.auth_rate_limit_enabled:
            return

        limit = getattr(settings, limit_setting_name)
        retry_after = await _get_rate_limiter(request).hit(
            scope=scope,
            client_id=_resolve_client_id(request, getattr(settings, "trusted_proxy_hops", 1)),
            limit=limit,
            window_seconds=settings.auth_rate_limit_window_seconds,
        )
        if retry_after is None:
            return

        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=RATE_LIMIT_ERROR_DETAIL,
            headers={"Retry-After": str(retry_after)},
        )

    return dependency
