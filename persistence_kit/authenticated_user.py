from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class AuthenticatedUser:
    subject: str
    username: str
    email: str | None
    roles: tuple[str, ...]

    @classmethod
    def from_values(
        cls,
        *,
        subject: str,
        username: str | None = None,
        email: str | None,
        roles: Iterable[str],
    ) -> AuthenticatedUser:
        resolved_username = (
            username.strip()
            if isinstance(username, str) and username.strip()
            else subject
        )
        return cls(
            subject=subject,
            username=resolved_username,
            email=email,
            roles=tuple(
                role.strip().lower()
                for role in roles
                if isinstance(role, str) and role.strip()
            ),
        )
