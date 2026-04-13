from __future__ import annotations

import asyncio
from collections import deque
from math import ceil
from time import monotonic
from typing import Callable

from fastapi import Depends, HTTPException, Request, status

RATE_LIMIT_ERROR_DETAIL = {
    "code": "TOO_MANY_AUTH_ATTEMPTS",
    "message": "Demasiados intentos. Intenta nuevamente en unos minutos.",
}


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


def _resolve_client_id(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        first_ip = forwarded_for.split(",")[0].strip()
        if first_ip:
            return first_ip

    cf_connecting_ip = request.headers.get("cf-connecting-ip")
    if cf_connecting_ip:
        return cf_connecting_ip.strip()

    client_host = request.client.host if request.client else None
    return client_host or "unknown"


def _get_rate_limiter(request: Request) -> InMemoryRateLimiter:
    limiter = getattr(request.app.state, "auth_rate_limiter", None)
    if limiter is None:
        limiter = InMemoryRateLimiter()
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
            client_id=_resolve_client_id(request),
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
