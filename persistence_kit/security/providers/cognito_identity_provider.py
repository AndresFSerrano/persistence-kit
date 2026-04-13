from typing import Any
import base64
import hashlib
import hmac
import logging

from fastapi import HTTPException, status
from fastapi.concurrency import run_in_threadpool

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

try:
    import boto3
except ImportError:  # pragma: no cover - handled at runtime with HTTPException
    boto3 = None

logger = logging.getLogger(__name__)
PASSWORD_POLICY_MESSAGE = (
    "La contraseña no cumple la política de seguridad: mínimo 10 caracteres, "
    "al menos una mayúscula, una minúscula, un número y un símbolo."
)


class CognitoIdentityProvider(IdentityProvider):
    def __init__(
        self,
        *,
        region: str,
        user_pool_client_id: str | None = None,
        user_pool_client_secret: str | None = None,
        user_pool_id: str | None = None,
        auto_verify_email: bool = False,
    ) -> None:
        if boto3 is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta dependencia boto3 para integrar con Cognito.",
            )
        self._region = region
        self._user_pool_client_id = user_pool_client_id
        self._user_pool_client_secret = user_pool_client_secret
        self._user_pool_id = user_pool_id
        self._auto_verify_email = auto_verify_email
        self._client = boto3.client("cognito-idp", region_name=self._region)

    def _build_secret_hash(self, username: str) -> str | None:
        if not self._user_pool_client_secret or not self._user_pool_client_id:
            return None
        digest = hmac.new(
            self._user_pool_client_secret.encode("utf-8"),
            f"{username}{self._user_pool_client_id}".encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("utf-8")

    def _build_auth_parameters_with_secret_hash(self, login: str) -> tuple[str, dict[str, str]]:
        cognito_username = self._build_cognito_username(login)
        params: dict[str, str] = {"USERNAME": cognito_username}
        secret_hash = self._build_secret_hash(cognito_username)
        if secret_hash:
            params["SECRET_HASH"] = secret_hash
        return cognito_username, params

    @staticmethod
    def _extract_user_attributes(attributes: Any) -> dict[str, str]:
        values: dict[str, str] = {}
        if not isinstance(attributes, list):
            return values
        for item in attributes:
            if not isinstance(item, dict):
                continue
            name = str(item.get("Name", "")).strip().lower()
            raw_value = item.get("Value")
            if not name or not isinstance(raw_value, str):
                continue
            value = raw_value.strip()
            if value:
                values[name] = value
        return values

    def _list_user_group_names(self, *, username: str) -> set[str]:
        if not self._user_pool_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuración COGNITO_USER_POOL_ID.",
            )

        names: set[str] = set()
        next_token: str | None = None
        while True:
            params: dict[str, Any] = {
                "UserPoolId": self._user_pool_id,
                "Username": username,
                "Limit": 60,
            }
            if next_token:
                params["NextToken"] = next_token
            response = self._client.admin_list_groups_for_user(**params)
            groups = response.get("Groups", [])
            for group in groups:
                if not isinstance(group, dict):
                    continue
                name = str(group.get("GroupName", "")).strip().lower()
                if name:
                    names.add(name)
            next_token = response.get("NextToken")
            if not next_token:
                break
        return names

    @staticmethod
    def _build_cognito_username(email: str) -> str:
        normalized_email = email.strip().lower()
        local_part, _, _ = normalized_email.partition("@")
        if local_part:
            return local_part
        return normalized_email

    @staticmethod
    def _iter_exception_chain(exc: BaseException) -> list[BaseException]:
        pending: list[BaseException] = [exc]
        collected: list[BaseException] = []
        seen: set[int] = set()

        while pending:
            current = pending.pop(0)
            marker = id(current)
            if marker in seen:
                continue
            seen.add(marker)
            collected.append(current)

            cause = getattr(current, "__cause__", None)
            if isinstance(cause, BaseException):
                pending.append(cause)

            context = getattr(current, "__context__", None)
            if isinstance(context, BaseException):
                pending.append(context)

            group_errors = getattr(current, "exceptions", None)
            if isinstance(group_errors, (tuple, list)):
                pending.extend(error for error in group_errors if isinstance(error, BaseException))

        return collected

    @classmethod
    def _extract_cognito_error_data(cls, exc: Exception) -> tuple[str | None, str | None]:
        for candidate in cls._iter_exception_chain(exc):
            response = getattr(candidate, "response", None)
            if isinstance(response, dict):
                error = response.get("Error", {})
                if isinstance(error, dict):
                    code = error.get("Code")
                    message = error.get("Message")
                    if code or message:
                        return code, message
        return None, None

    @classmethod
    def _extract_cognito_error(cls, exc: Exception, default_message: str) -> str:
        code, message = cls._extract_cognito_error_data(exc)
        if code and message:
            return f"{default_message} ({code}: {message})"
        fallback = str(exc).strip()
        if fallback:
            return f"{default_message} ({fallback})"
        return default_message

    @classmethod
    def _build_error_detail(
        cls,
        *,
        code: str,
        message: str,
        cognito_code: str | None = None,
        cognito_message: str | None = None,
    ) -> dict[str, str]:
        _ = (cognito_code, cognito_message)
        return {
            "code": code,
            "message": message,
        }

    @classmethod
    def _map_cognito_sign_up_error(
        cls,
        exc: Exception,
        *,
        phase: str,
    ) -> tuple[int, dict[str, str]]:
        code, message = cls._extract_cognito_error_data(exc)
        normalized = (message or "").lower()
        base_code = f"COGNITO_SIGNUP_{phase.upper()}"

        if code == "UsernameExistsException":
            return (
                status.HTTP_409_CONFLICT,
                cls._build_error_detail(
                    code="USER_ALREADY_EXISTS",
                    message="El usuario ya está registrado.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            )
        if code == "TooManyRequestsException":
            return (
                status.HTTP_429_TOO_MANY_REQUESTS,
                cls._build_error_detail(
                    code="TOO_MANY_SIGNUP_ATTEMPTS",
                    message="Demasiados intentos de registro. Intenta nuevamente en unos minutos.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            )
        if code == "InvalidPasswordException":
            return (
                status.HTTP_400_BAD_REQUEST,
                cls._build_error_detail(
                    code="INVALID_PASSWORD_POLICY",
                    message=PASSWORD_POLICY_MESSAGE,
                    cognito_code=code,
                    cognito_message=message,
                ),
            )
        if code == "InvalidParameterException":
            return (
                status.HTTP_400_BAD_REQUEST,
                cls._build_error_detail(
                    code=f"{base_code}_INVALID_PARAMETER",
                    message="Los datos de registro no son válidos.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            )
        if code == "NotAuthorizedException" and "secret hash" in normalized:
            return (
                status.HTTP_400_BAD_REQUEST,
                cls._build_error_detail(
                    code="COGNITO_CLIENT_SECRET_REQUIRED",
                    message=(
                        "El App Client de Cognito requiere SecretHash. "
                        "Configura COGNITO_APP_CLIENT_SECRET o usa un cliente sin secreto."
                    ),
                    cognito_code=code,
                    cognito_message=message,
                ),
            )
        return (
            status.HTTP_400_BAD_REQUEST,
            cls._build_error_detail(
                code=f"{base_code}_FAILED",
                message="No fue posible registrar el usuario en Cognito.",
                cognito_code=code,
                cognito_message=message,
            ),
        )        

    @classmethod
    def _map_cognito_login_error(cls, exc: Exception) -> tuple[int, dict[str, str]]:
        code, message = cls._extract_cognito_error_data(exc)
        normalized = (message or "").lower()

        if code == "NotAuthorizedException":
            if "secret hash" in normalized:
                return (
                    status.HTTP_500_INTERNAL_SERVER_ERROR,
                    cls._build_error_detail(
                        code="COGNITO_CLIENT_SECRET_REQUIRED",
                        message=(
                            "El App Client de Cognito requiere SecretHash. "
                            "Configura COGNITO_APP_CLIENT_SECRET o usa un cliente sin secreto."
                        ),
                        cognito_code=code,
                        cognito_message=message,
                    ),
                )
            if "password attempts exceeded" in normalized:
                return (
                    status.HTTP_429_TOO_MANY_REQUESTS,
                    cls._build_error_detail(
                        code="TOO_MANY_LOGIN_ATTEMPTS",
                        message=(
                            "Demasiados intentos de inicio de sesión. "
                            "Tu usuario está temporalmente bloqueado; intenta nuevamente en unos minutos."
                        ),
                        cognito_code=code,
                        cognito_message=message,
                    ),
                )
            if "disabled" in normalized:
                return (
                    status.HTTP_403_FORBIDDEN,
                    cls._build_error_detail(
                        code="ACCOUNT_DISABLED",
                        message="La cuenta está deshabilitada.",
                        cognito_code=code,
                        cognito_message=message,
                    ),
                )
            return (
                status.HTTP_401_UNAUTHORIZED,
                cls._build_error_detail(
                    code="INVALID_CREDENTIALS",
                    message="Credenciales inválidas.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            )
        if code == "UserNotConfirmedException":
            return (
                status.HTTP_403_FORBIDDEN,
                cls._build_error_detail(
                    code="USER_NOT_CONFIRMED",
                    message="El usuario no está confirmado.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            )
        if code == "PasswordResetRequiredException":
            return (
                status.HTTP_403_FORBIDDEN,
                cls._build_error_detail(
                    code="PASSWORD_RESET_REQUIRED",
                    message="El usuario debe restablecer su contraseña.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            )
        if code == "TooManyRequestsException":
            return (
                status.HTTP_429_TOO_MANY_REQUESTS,
                cls._build_error_detail(
                    code="TOO_MANY_LOGIN_ATTEMPTS",
                    message="Demasiados intentos de inicio de sesión. Intenta nuevamente en unos minutos.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            )
        if code == "InvalidPasswordException":
            return (
                status.HTTP_400_BAD_REQUEST,
                cls._build_error_detail(
                    code="INVALID_PASSWORD_POLICY",
                    message=PASSWORD_POLICY_MESSAGE,
                    cognito_code=code,
                    cognito_message=message,
                ),
            )
        if code == "UserNotFoundException":
            return (
                status.HTTP_401_UNAUTHORIZED,
                cls._build_error_detail(
                    code="INVALID_CREDENTIALS",
                    message="Credenciales inválidas.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            )
        return (
            status.HTTP_401_UNAUTHORIZED,
            cls._build_error_detail(
                code="INVALID_CREDENTIALS",
                message="Credenciales inválidas.",
                cognito_code=code,
                cognito_message=message,
            ),
        )

    @classmethod
    def _map_cognito_password_reset_error(cls, exc: Exception) -> tuple[int, dict[str, str]]:
        code, message = cls._extract_cognito_error_data(exc)
        normalized = (message or "").lower()

        if code == "UserNotFoundException":
            return (
                status.HTTP_404_NOT_FOUND,
                cls._build_error_detail(
                    code="USER_NOT_FOUND",
                    message="Usuario no encontrado.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            )
        if code == "TooManyRequestsException":
            return (
                status.HTTP_429_TOO_MANY_REQUESTS,
                cls._build_error_detail(
                    code="TOO_MANY_PASSWORD_RESET_ATTEMPTS",
                    message="Demasiadas solicitudes de restablecimiento. Intenta nuevamente en unos minutos.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            )
        if code == "NotAuthorizedException" and "disabled" in normalized:
            return (
                status.HTTP_403_FORBIDDEN,
                cls._build_error_detail(
                    code="ACCOUNT_DISABLED",
                    message="La cuenta está deshabilitada.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            )
        return (
            status.HTTP_400_BAD_REQUEST,
            cls._build_error_detail(
                code="COGNITO_PASSWORD_RESET_FAILED",
                message="No fue posible iniciar el restablecimiento de contraseña.",
                cognito_code=code,
                cognito_message=message,
            ),
        )

    async def sign_up_user(
        self,
        *,
        email: str,
        password: str,
        given_name: str | None = None,
        family_name: str | None = None,
        created_by: str | None = None,
    ) -> RegistrationResult:
        attributes: list[dict[str, str]] = [{"Name": "email", "Value": email}]
        if given_name:
            attributes.append({"Name": "given_name", "Value": given_name})
        if family_name:
            attributes.append({"Name": "family_name", "Value": family_name})
        if created_by:
            attributes.append({"Name": "custom:created_by", "Value": created_by.strip().lower()})

        if not self._user_pool_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuración COGNITO_USER_POOL_ID.",
            )

        cognito_username = self._build_cognito_username(email)

        def _sign_up() -> None:
            self._client.admin_create_user(
                UserPoolId=self._user_pool_id,
                Username=cognito_username,
                UserAttributes=attributes,
                MessageAction="SUPPRESS",
            )

            try:
                if self._auto_verify_email:
                    self._client.admin_update_user_attributes(
                        UserPoolId=self._user_pool_id,
                        Username=cognito_username,
                        UserAttributes=[{"Name": "email_verified", "Value": "true"}],
                    )
                self._client.admin_set_user_password(
                    UserPoolId=self._user_pool_id,
                    Username=cognito_username,
                    Password=password,
                    Permanent=True,
                )
                self._client.admin_disable_user(
                    UserPoolId=self._user_pool_id,
                    Username=cognito_username,
                )
            except Exception as exc:
                # Evita usuario huérfano si falla algún paso posterior a la creación.
                try:
                    self._client.admin_delete_user(
                        UserPoolId=self._user_pool_id,
                        Username=cognito_username,
                    )
                except Exception:
                    logger.exception(
                        "No se pudo revertir usuario de Cognito tras fallo de registro username=%s pool_id=%s",
                        cognito_username,
                        self._user_pool_id,
                    )
                raise RuntimeError("signup_post_create_failed") from exc

        logger.info("Cognito sign_up iniciado para email=%s", email)
        try:
            await run_in_threadpool(_sign_up)
        except Exception as exc:
            phase = "create_user"
            if str(exc) == "signup_post_create_failed":
                phase = "post_create"
            status_code, detail = self._map_cognito_sign_up_error(exc, phase=phase)
            logger.exception(
                "Cognito sign_up fallo para email=%s en region=%s",
                email,
                self._region,
            )
            raise HTTPException(
                status_code=status_code,
                detail=detail,
            ) from exc

        logger.info(
            "Cognito sign_up administrativo exitoso para email=%s user_confirmed=%s",
            email,
            False,
        )
        return RegistrationResult(
            username=cognito_username,
            user_confirmed=False,
            message="Cuenta creada sin correo. Queda deshabilitada hasta activación administrativa.",
        )

    async def assign_user_roles(
        self,
        *,
        username: str,
        roles: tuple[str, ...],
    ) -> RoleAssignmentResult:
        if not self._user_pool_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuración COGNITO_USER_POOL_ID.",
            )

        def _assign() -> tuple[str, ...]:
            self._client.admin_get_user(
                UserPoolId=self._user_pool_id,
                Username=username,
            )
            current_group_names = self._list_user_group_names(username=username)
            desired_group_names = {role.strip().lower() for role in roles if role.strip()}
            removable_role_groups = {
                group_name
                for group_name in current_group_names
                if group_name not in desired_group_names
            }
            for group_name in sorted(removable_role_groups):
                logger.info(
                    "Removiendo rol de Cognito username=%s role=%s pool_id=%s",
                    username,
                    group_name,
                    self._user_pool_id,
                )
                self._client.admin_remove_user_from_group(
                    UserPoolId=self._user_pool_id,
                    Username=username,
                    GroupName=group_name,
                )
                current_group_names.discard(group_name)

            for role in desired_group_names:
                if role in current_group_names:
                    logger.info(
                        "Rol ya asignado en Cognito username=%s role=%s pool_id=%s",
                        username,
                        role,
                        self._user_pool_id,
                    )
                    continue
                logger.info(
                    "Asignando rol de Cognito username=%s role=%s pool_id=%s",
                    username,
                    role,
                    self._user_pool_id,
                )
                self._client.admin_add_user_to_group(
                    UserPoolId=self._user_pool_id,
                    Username=username,
                    GroupName=role,
                )
                current_group_names.add(role)

            return tuple(sorted(current_group_names))

        try:
            assigned_roles = await run_in_threadpool(_assign)
        except Exception as exc:
            code, message = self._extract_cognito_error_data(exc)
            logger.exception(
                "Fallo al asignar roles en Cognito username=%s roles=%s pool_id=%s code=%s message=%s",
                username,
                list(roles),
                self._user_pool_id,
                code,
                message,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=self._build_error_detail(
                    code="COGNITO_ASSIGN_ROLES_FAILED",
                    message="No fue posible asignar roles al usuario.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            ) from exc

        logger.info(
            "Roles asignados en Cognito username=%s roles=%s",
            username,
            list(assigned_roles),
        )
        return RoleAssignmentResult(
            username=username,
            roles=assigned_roles,
            message="Roles asignados correctamente.",
        )

    async def list_users(
        self,
        *,
        page: int,
        page_size: int,
        roles: tuple[str, ...] = (),
    ) -> RegisteredUsersPageResult:
        if not self._user_pool_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuración COGNITO_USER_POOL_ID.",
            )
        normalized_roles = tuple(role.strip().lower() for role in roles if role.strip())

        def _list_all() -> RegisteredUsersPageResult:
            out: list[RegisteredUserResult] = []
            pagination_token: str | None = None
            while True:
                params: dict[str, Any] = {
                    "UserPoolId": self._user_pool_id,
                    "Limit": 60,
                }
                if pagination_token:
                    params["PaginationToken"] = pagination_token

                response = self._client.list_users(**params)
                users = response.get("Users", [])
                for item in users:
                    if not isinstance(item, dict):
                        continue
                    username = str(item.get("Username", "")).strip()
                    if not username:
                        continue
                    attributes = self._extract_user_attributes(item.get("Attributes"))
                    group_names = self._list_user_group_names(username=username)
                    roles: list[str] = []
                    for name in sorted(group_names):
                        roles.append(name)
                    if normalized_roles and not any(role in roles for role in normalized_roles):
                        continue
                    out.append(
                        RegisteredUserResult(
                            username=username,
                            email=attributes.get("email"),
                            given_name=attributes.get("given_name"),
                            family_name=attributes.get("family_name"),
                            created_by=attributes.get("custom:created_by"),
                            enabled=bool(item.get("Enabled", False)),
                            roles=tuple(roles),
                        )
                    )

                pagination_token = response.get("PaginationToken")
                if not pagination_token:
                    break

            out.sort(key=lambda user: user.username)
            total = len(out)
            start = (page - 1) * page_size
            end = start + page_size
            return RegisteredUsersPageResult(
                page=page,
                page_size=page_size,
                total=total,
                users=tuple(out[start:end]),
            )

        try:
            return await run_in_threadpool(_list_all)
        except HTTPException:
            raise
        except Exception as exc:
            code, message = self._extract_cognito_error_data(exc)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=self._build_error_detail(
                    code="COGNITO_LIST_USERS_FAILED",
                    message="No fue posible listar los usuarios.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            ) from exc

    async def ensure_roles_exist(
        self,
        *,
        role_codes: tuple[str, ...],
    ) -> None:
        if not self._user_pool_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuración COGNITO_USER_POOL_ID.",
            )

        def _ensure() -> None:
            existing_group_names: set[str] = set()
            pagination_token: str | None = None
            while True:
                params: dict[str, Any] = {
                    "UserPoolId": self._user_pool_id,
                    "Limit": 60,
                }
                if pagination_token:
                    params["NextToken"] = pagination_token
                response = self._client.list_groups(**params)
                for group in response.get("Groups", []):
                    if not isinstance(group, dict):
                        continue
                    group_name = str(group.get("GroupName", "")).strip().lower()
                    if group_name:
                        existing_group_names.add(group_name)
                pagination_token = response.get("NextToken")
                if not pagination_token:
                    break

            for role_code in role_codes:
                normalized_role = role_code.strip().lower()
                if not normalized_role or normalized_role in existing_group_names:
                    continue
                self._client.create_group(
                    UserPoolId=self._user_pool_id,
                    GroupName=normalized_role,
                )
                existing_group_names.add(normalized_role)

        try:
            await run_in_threadpool(_ensure)
        except Exception as exc:
            code, message = self._extract_cognito_error_data(exc)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=self._build_error_detail(
                    code="COGNITO_ENSURE_ROLES_FAILED",
                    message="No fue posible sincronizar los grupos de roles en Cognito.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            ) from exc

    async def delete_roles(
        self,
        *,
        role_codes: tuple[str, ...],
    ) -> None:
        if not self._user_pool_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuración COGNITO_USER_POOL_ID.",
            )

        def _delete() -> None:
            for role_code in role_codes:
                normalized_role = role_code.strip().lower()
                if not normalized_role:
                    continue
                try:
                    self._client.delete_group(
                        UserPoolId=self._user_pool_id,
                        GroupName=normalized_role,
                    )
                except Exception as exc:
                    code, _message = self._extract_cognito_error_data(exc)
                    if code == "ResourceNotFoundException":
                        continue
                    raise

        try:
            await run_in_threadpool(_delete)
        except Exception as exc:
            code, message = self._extract_cognito_error_data(exc)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=self._build_error_detail(
                    code="COGNITO_DELETE_ROLES_FAILED",
                    message="No fue posible eliminar los grupos de roles en Cognito.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            ) from exc

    async def authenticate_user(
        self,
        *,
        email: str,
        password: str,
    ) -> LoginResult:
        if not self._user_pool_client_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuración COGNITO_APP_CLIENT_ID.",
            )

        cognito_username = self._build_cognito_username(email)

        def _authenticate() -> dict[str, Any]:
            auth_parameters: dict[str, str] = {
                "USERNAME": cognito_username,
                "PASSWORD": password,
            }
            secret_hash = self._build_secret_hash(cognito_username)
            if secret_hash:
                auth_parameters["SECRET_HASH"] = secret_hash

            return self._client.initiate_auth(
                ClientId=self._user_pool_client_id,
                AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters=auth_parameters,
            )

        logger.info("Cognito login iniciado para email=%s", email)
        try:
            response = await run_in_threadpool(_authenticate)
        except Exception as exc:
            status_code, detail = self._map_cognito_login_error(exc)
            logger.exception(
                "Cognito login fallo para email=%s en region=%s",
                email,
                self._region,
            )
            raise HTTPException(status_code=status_code, detail=detail) from exc

        auth_result = response.get("AuthenticationResult", {})
        access_token = auth_result.get("AccessToken")
        if not access_token:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cognito no devolvió AccessToken.",
            )

        logger.info("Cognito login exitoso para email=%s", email)
        return LoginResult(
            access_token=access_token,
            id_token=auth_result.get("IdToken"),
            refresh_token=auth_result.get("RefreshToken"),
            expires_in=auth_result.get("ExpiresIn"),
            token_type=auth_result.get("TokenType"),
        )

    async def set_user_enabled(
        self,
        *,
        username: str,
        enabled: bool,
    ) -> UserStatusUpdateResult:
        if not self._user_pool_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuración COGNITO_USER_POOL_ID.",
            )

        def _change_status() -> None:
            if enabled:
                self._client.admin_enable_user(
                    UserPoolId=self._user_pool_id,
                    Username=username,
                )
                return
            self._client.admin_disable_user(
                UserPoolId=self._user_pool_id,
                Username=username,
            )

        logger.info(
            "Actualizacion de estado de cuenta Cognito iniciada username=%s enabled=%s pool_id=%s",
            username,
            enabled,
            self._user_pool_id,
        )
        try:
            await run_in_threadpool(_change_status)
        except Exception as exc:
            code, message = self._extract_cognito_error_data(exc)
            logger.exception(
                "Actualizacion de estado de cuenta Cognito fallida username=%s enabled=%s pool_id=%s",
                username,
                enabled,
                self._user_pool_id,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=self._build_error_detail(
                    code="COGNITO_SET_ACCOUNT_STATUS_FAILED",
                    message="No fue posible actualizar el estado de la cuenta.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            ) from exc

        logger.info(
            "Actualizacion de estado de cuenta Cognito exitosa username=%s enabled=%s",
            username,
            enabled,
        )
        return UserStatusUpdateResult(
            username=username,
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
        if not self._user_pool_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuración COGNITO_USER_POOL_ID.",
            )

        def _delete() -> None:
            self._client.admin_delete_user(
                UserPoolId=self._user_pool_id,
                Username=username,
            )

        try:
            await run_in_threadpool(_delete)
        except Exception as exc:
            code, message = self._extract_cognito_error_data(exc)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=self._build_error_detail(
                    code="COGNITO_DELETE_USER_FAILED",
                    message="No fue posible eliminar el usuario.",
                    cognito_code=code,
                    cognito_message=message,
                ),
            ) from exc

    async def reset_user_password(
        self,
        *,
        username: str,
    ) -> PasswordResetResult:
        if not self._user_pool_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuración COGNITO_USER_POOL_ID.",
            )

        def _reset() -> None:
            self._client.admin_reset_user_password(
                UserPoolId=self._user_pool_id,
                Username=username,
            )

        logger.info(
            "Reset de contraseña Cognito iniciado username=%s pool_id=%s",
            username,
            self._user_pool_id,
        )
        try:
            await run_in_threadpool(_reset)
        except Exception as exc:
            code, message = self._extract_cognito_error_data(exc)
            normalized = (message or "").lower()
            if code == "UserNotFoundException" or (
                code == "NotAuthorizedException" and "disabled" in normalized
            ):
                logger.info(
                    "reset_user_password opaco para username=%s code=%s",
                    username,
                    code,
                )
                return PasswordResetResult(
                    username=username,
                    message="Si el usuario existe, se envió el flujo de restablecimiento de contraseña.",
                )
            status_code, detail = self._map_cognito_password_reset_error(exc)
            logger.exception(
                "Reset de contraseña Cognito fallido username=%s pool_id=%s",
                username,
                self._user_pool_id,
            )
            raise HTTPException(
                status_code=status_code,
                detail=detail,
            ) from exc

        logger.info("Reset de contraseña Cognito solicitado username=%s", username)
        return PasswordResetResult(
            username=username,
            message="Si el usuario existe, se envió el flujo de restablecimiento de contraseña.",
        )

    async def refresh_tokens(
        self,
        *,
        refresh_token: str,
        login: str | None = None,
    ) -> RefreshTokensResult:
        if not self._user_pool_client_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuración COGNITO_APP_CLIENT_ID.",
            )

        def _refresh() -> dict[str, Any]:
            auth_parameters: dict[str, str] = {
                "REFRESH_TOKEN": refresh_token,
            }
            if self._user_pool_client_secret:
                if not login:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=(
                            "Para refrescar tokens con App Client Secret "
                            "debes enviar también el campo 'login'."
                        ),
                    )
                cognito_username = self._build_cognito_username(login)
                secret_hash = self._build_secret_hash(cognito_username)
                if secret_hash:
                    auth_parameters["SECRET_HASH"] = secret_hash

            return self._client.initiate_auth(
                ClientId=self._user_pool_client_id,
                AuthFlow="REFRESH_TOKEN_AUTH",
                AuthParameters=auth_parameters,
            )

        logger.info("Cognito refresh iniciado")
        try:
            response = await run_in_threadpool(_refresh)
        except HTTPException:
            raise
        except Exception as exc:
            status_code, detail = self._map_cognito_login_error(exc)
            logger.exception("Cognito refresh fallo")
            raise HTTPException(status_code=status_code, detail=detail) from exc

        auth_result = response.get("AuthenticationResult", {})
        access_token = auth_result.get("AccessToken")
        if not access_token:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cognito no devolvió AccessToken.",
            )

        return RefreshTokensResult(
            access_token=access_token,
            id_token=auth_result.get("IdToken"),
            refresh_token=refresh_token,
            expires_in=auth_result.get("ExpiresIn"),
            token_type=auth_result.get("TokenType"),
        )

    async def logout(
        self,
        *,
        refresh_token: str | None = None,
        access_token: str | None = None,
        login: str | None = None,
    ) -> LogoutResult:
        _ = (access_token, login)
        if not self._user_pool_client_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuración COGNITO_APP_CLIENT_ID.",
            )
        if not refresh_token:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="refresh_token es requerido para cerrar sesión.",
            )

        def _logout() -> None:
            params: dict[str, str] = {
                "ClientId": self._user_pool_client_id,
                "Token": refresh_token,
            }
            if self._user_pool_client_secret:
                params["ClientSecret"] = self._user_pool_client_secret
            self._client.revoke_token(**params)

        try:
            await run_in_threadpool(_logout)
        except Exception as exc:
            status_code, detail = self._map_cognito_login_error(exc)
            raise HTTPException(status_code=status_code, detail=detail) from exc

        return LogoutResult(message="Sesión cerrada correctamente.")

    async def resolve_email_from_access_token(
        self,
        *,
        access_token: str,
    ) -> str | None:
        def _get_user() -> dict[str, Any]:
            return self._client.get_user(AccessToken=access_token)

        try:
            response = await run_in_threadpool(_get_user)
        except Exception:
            return None

        attributes = response.get("UserAttributes", [])
        if isinstance(attributes, list):
            for item in attributes:
                if not isinstance(item, dict):
                    continue
                if str(item.get("Name", "")).strip().lower() != "email":
                    continue
                value = item.get("Value")
                if isinstance(value, str) and value.strip():
                    return value.strip().lower()
        return None

    async def resolve_username_from_access_token(
        self,
        *,
        access_token: str,
    ) -> str | None:
        def _get_user() -> dict[str, Any]:
            return self._client.get_user(AccessToken=access_token)

        try:
            response = await run_in_threadpool(_get_user)
        except Exception:
            return None

        username = response.get("Username")
        if isinstance(username, str) and username.strip():
            return username.strip()
        return None

    async def request_password_reset_code(
        self,
        *,
        login: str,
    ) -> PasswordResetCodeRequestResult:
        if not self._user_pool_client_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuración COGNITO_APP_CLIENT_ID.",
            )

        _, auth_params = self._build_auth_parameters_with_secret_hash(login)

        def _forgot() -> None:
            payload: dict[str, str] = {
                "ClientId": self._user_pool_client_id,
                "Username": auth_params["USERNAME"],
            }
            if "SECRET_HASH" in auth_params:
                payload["SecretHash"] = auth_params["SECRET_HASH"]
            self._client.forgot_password(**payload)

        try:
            await run_in_threadpool(_forgot)
        except Exception as exc:
            code, _ = self._extract_cognito_error_data(exc)
            if code == "UserNotFoundException":
                logger.info(
                    "forgot_password para usuario inexistente (login=%s) — respuesta opaca",
                    login,
                )
                return PasswordResetCodeRequestResult(
                    message="Si el usuario existe, se envió el código de recuperación al correo.",
                )
            status_code, detail = self._map_cognito_login_error(exc)
            raise HTTPException(status_code=status_code, detail=detail) from exc

        return PasswordResetCodeRequestResult(
            message="Si el usuario existe, se envió el código de recuperación al correo.",
        )

    async def confirm_password_reset(
        self,
        *,
        username: str,
        confirmation_code: str,
        new_password: str,
    ) -> PasswordResetConfirmResult:
        if not self._user_pool_client_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Falta configuración COGNITO_APP_CLIENT_ID.",
            )

        _, auth_params = self._build_auth_parameters_with_secret_hash(username)

        def _confirm() -> None:
            payload: dict[str, str] = {
                "ClientId": self._user_pool_client_id,
                "Username": auth_params["USERNAME"],
                "ConfirmationCode": confirmation_code,
                "Password": new_password,
            }
            if "SECRET_HASH" in auth_params:
                payload["SecretHash"] = auth_params["SECRET_HASH"]
            self._client.confirm_forgot_password(**payload)

        try:
            await run_in_threadpool(_confirm)
        except Exception as exc:
            code, _ = self._extract_cognito_error_data(exc)
            if code in ("UserNotFoundException", "CodeMismatchException", "ExpiredCodeException"):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=self._build_error_detail(
                        code="INVALID_RESET_REQUEST",
                        message="Código inválido o expirado. Solicita uno nuevo.",
                    ),
                ) from exc
            status_code, detail = self._map_cognito_login_error(exc)
            raise HTTPException(status_code=status_code, detail=detail) from exc

        return PasswordResetConfirmResult(
            message="Contraseña actualizada correctamente.",
        )
