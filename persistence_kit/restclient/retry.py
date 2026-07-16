from __future__ import annotations

import asyncio
import random
from typing import Iterable

from persistence_kit.restclient.errors import RestTimeoutError, RestTransportError

DEFAULT_RETRY_STATUS = (429, 500, 502, 503, 504)


class RetryPolicy:
    def __init__(
        self,
        max_retries: int = 2,
        *,
        backoff_base: float = 0.2,
        backoff_max: float = 5.0,
        retry_on: Iterable[int] = DEFAULT_RETRY_STATUS,
        retry_on_timeout: bool = True,
        retry_on_transport: bool = True,
        jitter: bool = True,
    ) -> None:
        self.max_retries = max(0, max_retries)
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.retry_on = tuple(retry_on)
        self.retry_on_timeout = retry_on_timeout
        self.retry_on_transport = retry_on_transport
        self.jitter = jitter

    def should_retry(self, status_code: int, attempt: int) -> bool:
        return attempt < self.max_retries and status_code in self.retry_on

    def should_retry_exception(self, exc: Exception, attempt: int) -> bool:
        if attempt >= self.max_retries:
            return False
        if isinstance(exc, RestTimeoutError):
            return self.retry_on_timeout
        if isinstance(exc, RestTransportError):
            return self.retry_on_transport
        return False

    async def sleep(self, attempt: int) -> None:
        delay = min(self.backoff_max, self.backoff_base * (2**attempt))
        if self.jitter:
            delay += random.uniform(0, self.backoff_base)
        await asyncio.sleep(delay)
