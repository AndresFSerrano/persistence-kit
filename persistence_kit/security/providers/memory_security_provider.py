from collections.abc import Iterable
from dataclasses import dataclass
import hashlib
import hmac
import os
import random
from threading import RLock
import time
from uuid import uuid4

from fastapi import HTTPException, status

from persistence_kit.security.ports import IdentityProvider
from persistence_kit.security.registration import (
    LoginResult,
    LogoutResult,
    PasswordResetCodeRequestResult,
    PasswordResetConfirmResult,
    PasswordResetResult,
    RegisteredUserResult,
    RegisteredUsersPageResult,
    RefreshTokensResult,
    RegistrationResult,
    RoleAssignmentResult,
    UserStatusUpdateResult,
)


@dataclass
class _MemoryUser:
    username: str
    email: str
    given_name: str | None
    family_name: str | None
    created_by: str | None
    password_hash: str
    password_salt: str
    enabled: bool
    roles: tuple[str, ...]


class MemorySecurityProvider(IdentityProvider):
    def __init__(
        self,
        *,
        jwt_secret: str,
        jwt_issuer: str,
        token_ttl_seconds: int = 3600,
        refresh_ttl_seconds: int = 604800,
        seed_role_users: bool = False,
        seed_user_password: str = "Temporal123!",
        seed_role_codes: Iterable[str] = (),
        seed_user_domain: str = "example.com",
        seed_created_by: str | None = None,
    ) -> None:
        self._lock = RLock()
        self._users_by_username: dict[str, _MemoryUser] = {}
        self._username_by_email: dict[str, str] = {}
        self._jwt_secret = jwt_secret
        self._jwt_issuer = jwt_issuer
        self._token_ttl_seconds = token_ttl_seconds
        self._refresh_ttl_seconds = refresh_ttl_seconds
        self._seed_user_password = seed_user_password
        self._seed_role_codes = tuple(
            dict.fromkeys(role.strip().lower() for role in seed_role_codes if role.strip())
        )
        self._seed_user_domain = seed_user_domain.strip().lower() or "example.com"
        self._seed_created_by = (
            seed_created_by.strip().lower()
            if isinstance(seed_created_by, str) and seed_created_by.strip()
            else None
        )
        self._revoked_refresh_jtis: set[str] = set()
        self._password_reset_codes: dict[str, str] = {}
        if seed_role_users:
            self._seed_default_role_users(seed_user_password)

    @staticmethod
    def _normalize_email(email: str) -> str:
        return email.strip().lower()

    @staticmethod
    def _build_username(email: str) -> str:
        normalized = email.strip().lower()
        local_part, _, _ = normalized.partition("@")
        return local_part or normalized

    @staticmethod
    def _hash_password(password: str, salt: str) -> str:
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt.encode("utf-8"),
            100_000,
        )
        return digest.hex()

    @staticmethod
    def _new_salt() -> str:
        return os.urandom(16).hex()

    def _seed_default_role_users(self, seed_password: str) -> None:
        for role_code in self._seed_role_codes:
            username = f"{role_code}.seed"
            email = f"{username}@{self._seed_user_domain}"
            normalized_email = self._normalize_email(email)
            salt = self._new_salt()
            self._users_by_username[username] = _MemoryUser(
                username=username,
                email=normalized_email,
                given_name=None,
                family_name=None,
                created_by=self._seed_created_by,
                password_hash=self._hash_password(seed_password, salt),
                password_salt=salt,
                enabled=True,
                roles=(role_code,),
            )
            self._username_by_email[normalized_email] = username

    async def sign_up_user(
        self,
        *,
        email: str,
        password: str,
        given_name: str | None = None,
        family_name: str | None = None,
        created_by: str | None = None,
    ) -> RegistrationResult:
        normalized_email = self._normalize_email(email)
        username = self._build_username(normalized_email)

        with self._lock:
            if username in self._users_by_username or normalized_email in self._username_by_email:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="El usuario ya está registrado.",
                )
            salt = self._new_salt()
            self._users_by_username[username] = _MemoryUser(
                username=username,
                email=normalized_email,
                given_name=given_name.strip() if isinstance(given_name, str) and given_name.strip() else None,
                family_name=family_name.strip() if isinstance(family_name, str) and family_name.strip() else None,
                created_by=created_by.strip().lower() if isinstance(created_by, str) and created_by.strip() else None,
                password_hash=self._hash_password(password, salt),
                password_salt=salt,
                enabled=False,
                roles=(),
            )
            self._username_by_email[normalized_email] = username

        return RegistrationResult(
            username=username,
            user_confirmed=False,
            message="Cuenta creada en sandbox. Queda deshabilitada hasta activación administrativa.",
        )

    async def assign_user_roles(
        self,
        *,
        username: str,
        roles: tuple[str, ...],
    ) -> RoleAssignmentResult:
        normalized_username = username.strip().lower()
        with self._lock:
            user = self._users_by_username.get(normalized_username)
            if user is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Usuario no encontrado.",
                )
            user.roles = tuple(dict.fromkeys(role.strip().lower() for role in roles if role.strip()))

        return RoleAssignmentResult(
            username=normalized_username,
            roles=user.roles,
            message="Roles asignados correctamente.",
        )

    async def authenticate_user(
        self,
        *,
        email: str,
        password: str,
    ) -> LoginResult:
        normalized_login = self._normalize_email(email)
        with self._lock:
            username = self._username_by_email.get(normalized_login, normalized_login)
            user = self._users_by_username.get(username)
            if user is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail={
                        "code": "INVALID_CREDENTIALS",
                        "message": "Credenciales inválidas.",
                    },
                )
            expected_hash = self._hash_password(password, user.password_salt)
            if not hmac.compare_digest(user.password_hash, expected_hash):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail={
                        "code": "INVALID_CREDENTIALS",
                        "message": "Credenciales inválidas.",
                    },
                )
            if not user.enabled:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail={
                        "code": "ACCOUNT_DISABLED",
                        "message": "La cuenta está deshabilitada.",
                    },
                )

            now = int(time.time())
            base_payload: dict[str, object] = {
                "sub": user.username,
                "email": user.email,
                "roles": list(user.roles),
                "iss": self._jwt_issuer,
                "iat": now,
                "exp": now + self._token_ttl_seconds,
            }
            access_payload = {**base_payload, "token_use": "access"}
            id_payload = {**base_payload, "token_use": "id"}
            refresh_payload = {
                "sub": user.username,
                "email": user.email,
                "iss": self._jwt_issuer,
                "iat": now,
                "exp": now + self._refresh_ttl_seconds,
                "token_use": "refresh",
                "jti": uuid4().hex,
            }
            access_payload["jti"] = uuid4().hex
            id_payload["jti"] = uuid4().hex
            access_token = self._encode_token(access_payload)
            id_token = self._encode_token(id_payload)
            refresh_token = self._encode_token(refresh_payload)

        return LoginResult(
            access_token=access_token,
            id_token=id_token,
            refresh_token=refresh_token,
            expires_in=self._token_ttl_seconds,
            token_type="Bearer",
        )

    def _encode_token(self, payload: dict[str, object]) -> str:
        import jwt

        return str(jwt.encode(payload, self._jwt_secret, algorithm="HS256"))

    def _decode_token(self, token: str) -> dict[str, object]:
        import jwt

        return jwt.decode(
            token,
            key=self._jwt_secret,
            algorithms=["HS256"],
            issuer=self._jwt_issuer,
            audience=None,
            options={"verify_aud": False},
        )

    def _resolve_username(self, login: str) -> str:
        normalized = self._normalize_email(login)
        return self._username_by_email.get(normalized, normalized)

    async def set_user_enabled(
        self,
        *,
        username: str,
        enabled: bool,
    ) -> UserStatusUpdateResult:
        normalized_username = username.strip().lower()
        with self._lock:
            user = self._users_by_username.get(normalized_username)
            if user is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Usuario no encontrado.",
                )
            user.enabled = enabled

        return UserStatusUpdateResult(
            username=normalized_username,
            enabled=enabled,
            message=(
                "Cuenta activada correctamente."
                if enabled
                else "Cuenta deshabilitada correctamente."
            ),
        )

    async def delete_user(
        self,
        *,
        username: str,
    ) -> None:
        normalized_username = username.strip().lower()
        with self._lock:
            user = self._users_by_username.get(normalized_username)
            if user is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Usuario no encontrado.",
                )
            self._users_by_username.pop(normalized_username, None)
            self._username_by_email.pop(user.email, None)

    async def reset_user_password(
        self,
        *,
        username: str,
    ) -> PasswordResetResult:
        normalized_username = username.strip().lower()
        with self._lock:
            user = self._users_by_username.get(normalized_username)
            if user is None:
                return PasswordResetResult(
                    username=normalized_username,
                    message="Si el usuario existe, se envió el flujo de restablecimiento de contraseña.",
                )
            reset_salt = self._new_salt()
            user.password_salt = reset_salt
            user.password_hash = self._hash_password("password", reset_salt)

        return PasswordResetResult(
            username=normalized_username,
            message="Contraseña restablecida. La nueva contraseña temporal es: password",
        )

    async def refresh_tokens(
        self,
        *,
        refresh_token: str,
        login: str | None = None,
    ) -> RefreshTokensResult:
        _ = login
        try:
            payload = self._decode_token(refresh_token)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token inválido o expirado.",
            ) from exc

        if payload.get("token_use") != "refresh":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token inválido.",
            )
        token_jti = str(payload.get("jti", ""))
        if token_jti and token_jti in self._revoked_refresh_jtis:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token revocado.",
            )

        username = str(payload.get("sub", "")).strip().lower()
        with self._lock:
            user = self._users_by_username.get(username)
            if user is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Usuario no encontrado.",
                )
            if not user.enabled:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail={
                        "code": "ACCOUNT_DISABLED",
                        "message": "La cuenta está deshabilitada.",
                    },
                )

            now = int(time.time())
            base_payload: dict[str, object] = {
                "sub": user.username,
                "email": user.email,
                "roles": list(user.roles),
                "iss": self._jwt_issuer,
                "iat": now,
                "exp": now + self._token_ttl_seconds,
            }
            id_token = self._encode_token({**base_payload, "token_use": "id", "jti": uuid4().hex})
            access_token = self._encode_token(
                {**base_payload, "token_use": "access", "jti": uuid4().hex}
            )
            rotated_refresh = self._encode_token(
                {
                    "sub": user.username,
                    "email": user.email,
                    "iss": self._jwt_issuer,
                    "iat": now,
                    "exp": now + self._refresh_ttl_seconds,
                    "token_use": "refresh",
                    "jti": uuid4().hex,
                }
            )

        return RefreshTokensResult(
            access_token=access_token,
            id_token=id_token,
            refresh_token=rotated_refresh,
            expires_in=self._token_ttl_seconds,
            token_type="Bearer",
        )

    async def request_password_reset_code(
        self,
        *,
        login: str,
    ) -> PasswordResetCodeRequestResult:
        with self._lock:
            username = self._resolve_username(login)
            if username not in self._users_by_username:
                return PasswordResetCodeRequestResult(
                    message="Si el usuario existe, se envió el código de recuperación.",
                )
            code = f"{random.randint(0, 999999):06d}"
            self._password_reset_codes[username] = code

        return PasswordResetCodeRequestResult(
            message=f"Código de recuperación generado (sandbox): {code}",
        )

    async def confirm_password_reset(
        self,
        *,
        username: str,
        confirmation_code: str,
        new_password: str,
    ) -> PasswordResetConfirmResult:
        with self._lock:
            normalized_username = username.strip().lower()
            user = self._users_by_username.get(normalized_username)
            if user is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Código inválido o expirado. Solicita uno nuevo.",
                )
            expected_code = self._password_reset_codes.get(normalized_username)
            if not expected_code or expected_code != confirmation_code:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Código inválido o expirado. Solicita uno nuevo.",
                )
            reset_salt = self._new_salt()
            user.password_salt = reset_salt
            user.password_hash = self._hash_password(new_password, reset_salt)
            self._password_reset_codes.pop(normalized_username, None)

        return PasswordResetConfirmResult(
            message="Contraseña actualizada correctamente.",
        )

    async def logout(
        self,
        *,
        refresh_token: str | None = None,
        access_token: str | None = None,
        login: str | None = None,
    ) -> LogoutResult:
        _ = (access_token, login)
        if not refresh_token:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="refresh_token es requerido para cerrar sesión.",
            )
        try:
            payload = self._decode_token(refresh_token)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token inválido o expirado.",
            ) from exc
        if payload.get("token_use") != "refresh":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token inválido.",
            )
        token_jti = str(payload.get("jti", ""))
        if token_jti:
            self._revoked_refresh_jtis.add(token_jti)
        return LogoutResult(message="Sesión cerrada correctamente.")

    async def resolve_email_from_access_token(
        self,
        *,
        access_token: str,
    ) -> str | None:
        try:
            payload = self._decode_token(access_token)
        except Exception:
            return None
        email = payload.get("email")
        if isinstance(email, str) and email.strip():
            return email.strip().lower()
        return None

    async def resolve_username_from_access_token(
        self,
        *,
        access_token: str,
    ) -> str | None:
        try:
            payload = self._decode_token(access_token)
        except Exception:
            return None
        username = payload.get("username") or payload.get("sub")
        if isinstance(username, str) and username.strip():
            return username.strip().lower()
        return None

    async def list_users(
        self,
        *,
        page: int,
        page_size: int,
        roles: tuple[str, ...] = (),
    ) -> RegisteredUsersPageResult:
        normalized_roles = tuple(role.strip().lower() for role in roles if role.strip())
        with self._lock:
            all_users = tuple(
                RegisteredUserResult(
                    username=user.username,
                    email=user.email,
                    given_name=user.given_name,
                    family_name=user.family_name,
                    created_by=user.created_by,
                    enabled=user.enabled,
                    roles=user.roles,
                )
                for user in sorted(self._users_by_username.values(), key=lambda item: item.username)
                if not normalized_roles or any(role in user.roles for role in normalized_roles)
            )
        total = len(all_users)
        start = (page - 1) * page_size
        end = start + page_size
        return RegisteredUsersPageResult(
            page=page,
            page_size=page_size,
            total=total,
            users=all_users[start:end],
        )

    def reset(self) -> None:
        with self._lock:
            self._users_by_username.clear()
            self._username_by_email.clear()
            self._revoked_refresh_jtis.clear()
            self._password_reset_codes.clear()

    async def ensure_roles_exist(
        self,
        *,
        role_codes: tuple[str, ...],
    ) -> None:
        _ = tuple(role_code.strip().lower() for role_code in role_codes if role_code.strip())

    async def delete_roles(
        self,
        *,
        role_codes: tuple[str, ...],
    ) -> None:
        _ = tuple(role_code.strip().lower() for role_code in role_codes if role_code.strip())
