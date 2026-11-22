from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from math import ceil

from persistence_kit.cache.contracts import Cache
from persistence_kit.cache.factory import get_cache

LOGIN_LOCKOUT_CACHE_NAME = "login-lockout"


class LockoutScope(StrEnum):
    PAIR = "pair"
    USER = "user"
    CLIENT = "client"


@dataclass(frozen=True, slots=True)
class Lockout:
    scope: LockoutScope
    retry_after: int


class LoginLockout:
    """Bloqueo del inicio de sesion por intentos fallidos, guardado en una ``Cache``.

    Cuenta los fallos del usuario desde una direccion (``pair``), del usuario desde
    cualquier direccion (``user``) y de la direccion con cualquier usuario
    (``client``). Al llegar al tope de un alcance lo bloquea ``lock_seconds``; un
    tope en 0 apaga ese alcance. Los ingresos buenos no suman y limpian el conteo
    del par. Las claves van con hash: la cache no guarda usuarios ni direcciones.
    """

    def __init__(
        self,
        cache: Cache,
        *,
        pair_attempts: int,
        user_attempts: int,
        client_attempts: int,
        lock_seconds: int,
        enabled: bool = True,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._cache = cache
        self._limits = {
            LockoutScope.PAIR: pair_attempts,
            LockoutScope.USER: user_attempts,
            LockoutScope.CLIENT: client_attempts,
        }
        self._lock_seconds = lock_seconds
        self._enabled = enabled and lock_seconds > 0
        self._clock = clock

    async def blocked(self, *, username: str | None, client_id: str) -> Lockout | None:
        """El bloqueo vigente que mas tarda en vencer, o ``None`` si se puede intentar."""
        if not self._enabled:
            return None
        now = self._clock()
        keys = self._keys(username, client_id)
        unlocks = await asyncio.gather(*(self._cache.get(_lock(key)) for key in keys.values()))
        waits = [
            Lockout(scope, max(1, ceil(unlock_at - now)))
            for scope, unlock_at in zip(keys, unlocks)
            if isinstance(unlock_at, (int, float)) and unlock_at > now
        ]
        return _longest(waits)

    async def fail(self, *, username: str | None, client_id: str) -> Lockout | None:
        """Suma un fallo y devuelve el bloqueo que este fallo arranco, si arranco uno."""
        if not self._enabled:
            return None
        keys = self._keys(username, client_id)
        counts = await asyncio.gather(
            *(
                self._cache.increment(_count(key), ttl_seconds=self._lock_seconds)
                for key in keys.values()
            )
        )
        unlock_at = self._clock() + self._lock_seconds
        started = []
        for (scope, key), count in zip(keys.items(), counts):
            limit = self._limits[scope]
            if limit <= 0 or count < limit:
                continue
            if await self._cache.set_if_absent(
                _lock(key), unlock_at, ttl_seconds=self._lock_seconds
            ):
                started.append(Lockout(scope, self._lock_seconds))
        return _longest(started)

    async def succeed(self, *, username: str | None, client_id: str) -> None:
        """Limpia el conteo del par; el del usuario y el de la direccion siguen."""
        user = _normalized(username)
        if not self._enabled or not user:
            return
        await self._cache.delete(_count(_digest(LockoutScope.PAIR, user, client_id)))

    def _keys(self, username: str | None, client_id: str) -> dict[LockoutScope, str]:
        user = _normalized(username)
        keys: dict[LockoutScope, str] = {}
        if user:
            keys[LockoutScope.PAIR] = _digest(LockoutScope.PAIR, user, client_id)
            keys[LockoutScope.USER] = _digest(LockoutScope.USER, user)
        keys[LockoutScope.CLIENT] = _digest(LockoutScope.CLIENT, client_id)
        return keys


def _normalized(username: str | None) -> str:
    return (username or "").strip().lower()


def _digest(scope: LockoutScope, *parts: str) -> str:
    raw = "\n".join((scope.value, *parts)).encode("utf-8")
    return f"{scope.value}:{hashlib.sha256(raw).hexdigest()}"


def _count(key: str) -> str:
    return f"count:{key}"


def _lock(key: str) -> str:
    return f"lock:{key}"


def _longest(lockouts: list[Lockout]) -> Lockout | None:
    return max(lockouts, key=_retry_after, default=None)


def _retry_after(lockout: Lockout) -> int:
    return lockout.retry_after


def build_login_lockout(settings) -> LoginLockout:
    """Arma el bloqueo sobre ``get_cache("login-lockout")`` con los topes de ``settings``.

    Se enciende y se apaga con ``AUTH_RATE_LIMIT_ENABLED``, como el limite de intentos.
    """
    return LoginLockout(
        get_cache(LOGIN_LOCKOUT_CACHE_NAME),
        pair_attempts=settings.auth_lockout_pair_attempts,
        user_attempts=settings.auth_lockout_user_attempts,
        client_attempts=settings.auth_lockout_client_attempts,
        lock_seconds=settings.auth_lockout_seconds,
        enabled=bool(settings.auth_rate_limit_enabled),
    )
