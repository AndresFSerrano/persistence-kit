import base64
import hashlib
import hmac
from enum import Enum

import pytest
from fastapi import HTTPException

import persistence_kit.security.providers.cognito_identity_provider as mod


class Role(str, Enum):
    ADMIN_GENERAL = "admin_general"
    PROGRAMADOR_ALMACEN = "programador_almacen"
    AUXILIAR_ALMACEN = "auxiliar_almacen"


def _expected_secret_hash(username: str, client_id: str, client_secret: str) -> str:
    digest = hmac.new(
        client_secret.encode("utf-8"),
        f"{username}{client_id}".encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(digest).decode("utf-8")


def _expected_username(email: str) -> str:
    normalized = email.strip().lower()
    return normalized.split("@", 1)[0]


class _FakeCognitoError(Exception):
    def __init__(self, code: str | None = None, message: str | None = None):
        if code is not None or message is not None:
            self.response = {"Error": {"Code": code, "Message": message}}
        super().__init__(message or "")


def test_init_fails_when_boto3_dependency_is_missing(monkeypatch):
    monkeypatch.setattr(mod, "boto3", None)

    with pytest.raises(HTTPException) as exc_info:
        mod.CognitoIdentityProvider(region="us-east-1")

    assert exc_info.value.status_code == 500
    assert "boto3" in exc_info.value.detail


def test_build_cognito_username_handles_email_without_local_part():
    assert mod.CognitoIdentityProvider._build_cognito_username("@UDEA.EDU.CO") == "@udea.edu.co"


def test_iter_exception_chain_collects_cause_context_and_group_errors():
    cause = RuntimeError("cause")
    context = RuntimeError("context")
    leaf = RuntimeError("leaf")
    grouped = ExceptionGroup("group", [leaf])

    root = RuntimeError("root")
    root.__cause__ = cause
    root.__context__ = context
    cause.__cause__ = grouped

    chain = mod.CognitoIdentityProvider._iter_exception_chain(root)
    messages = {str(item) for item in chain}

    assert "root" in messages
    assert "cause" in messages
    assert "context" in messages
    assert "leaf" in messages


def test_iter_exception_chain_skips_duplicate_exception_references():
    shared = RuntimeError("shared")
    root = RuntimeError("root")
    root.__cause__ = shared
    root.__context__ = shared

    chain = mod.CognitoIdentityProvider._iter_exception_chain(root)
    shared_count = sum(1 for item in chain if item is shared)

    assert shared_count == 1


def test_extract_cognito_error_data_returns_none_for_unknown_exception():
    code, message = mod.CognitoIdentityProvider._extract_cognito_error_data(RuntimeError("boom"))
    assert code is None
    assert message is None


def test_extract_cognito_error_returns_default_message_when_exception_is_empty():
    detail = mod.CognitoIdentityProvider._extract_cognito_error(
        RuntimeError(),
        "fallback",
    )
    assert detail == "fallback"


@pytest.mark.parametrize(
    "code,message,expected_status,expected_text",
    [
        ("UsernameExistsException", "exists", 409, "usuario ya está registrado"),
        ("TooManyRequestsException", "too many", 429, "Demasiados intentos de registro"),
        ("InvalidPasswordException", "weak", 400, "no cumple la política"),
        ("InvalidParameterException", "bad attr", 400, "datos de registro no son válidos"),
    ],
)
def test_map_cognito_sign_up_error_known_codes(code, message, expected_status, expected_text):
    status_code, detail = mod.CognitoIdentityProvider._map_cognito_sign_up_error(
        _FakeCognitoError(code, message),
        phase="create_user",
    )
    assert status_code == expected_status
    assert expected_text in detail["message"]


def test_map_cognito_sign_up_error_generic_without_response_uses_fallback_text():
    status_code, detail = mod.CognitoIdentityProvider._map_cognito_sign_up_error(
        RuntimeError("network down"),
        phase="create_user",
    )
    assert status_code == 400
    assert detail["message"] == "No fue posible registrar el usuario en Cognito."


@pytest.mark.parametrize(
    "code,message,expected_status,expected_text",
    [
        ("UserNotConfirmedException", "pending", 403, "no está confirmado"),
        ("PasswordResetRequiredException", "reset", 403, "debe restablecer"),
        ("TooManyRequestsException", "spam", 429, "Demasiados intentos"),
        ("InvalidPasswordException", "weak", 400, "mínimo 10 caracteres"),
    ],
)
def test_map_cognito_login_error_known_codes(code, message, expected_status, expected_text):
    status_code, detail = mod.CognitoIdentityProvider._map_cognito_login_error(
        _FakeCognitoError(code, message),
    )
    assert status_code == expected_status
    assert expected_text in detail["message"]


def test_map_cognito_login_error_secret_hash_returns_500():
    status_code, detail = mod.CognitoIdentityProvider._map_cognito_login_error(
        _FakeCognitoError("NotAuthorizedException", "Unable to verify secret hash"),
    )
    assert status_code == 500
    assert "COGNITO_APP_CLIENT_SECRET" in detail["message"]


def test_map_cognito_login_error_password_attempts_exceeded_returns_429():
    status_code, detail = mod.CognitoIdentityProvider._map_cognito_login_error(
        _FakeCognitoError("NotAuthorizedException", "Password attempts exceeded"),
    )
    assert status_code == 429
    assert detail["code"] == "TOO_MANY_LOGIN_ATTEMPTS"


def test_map_cognito_login_error_generic_without_response_uses_fallback_text():
    status_code, detail = mod.CognitoIdentityProvider._map_cognito_login_error(
        RuntimeError("timeout"),
    )
    assert status_code == 401
    assert detail["code"] == "INVALID_CREDENTIALS"
    assert detail["message"] == "Credenciales inválidas."


@pytest.mark.asyncio
async def test_sign_up_user_creates_admin_user_without_email_and_disables_until_confirmation(
    monkeypatch,
):
    captured = {}

    class FakeClient:
        def admin_create_user(self, **kwargs):
            captured["create"] = kwargs
            return {}

        def admin_set_user_password(self, **kwargs):
            captured["set_password"] = kwargs
            return {}

        def admin_disable_user(self, **kwargs):
            captured["disable"] = kwargs
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            assert service_name == "cognito-idp"
            assert region_name == "us-east-1"
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(
        region="us-east-1",
        user_pool_id="pool-id",
    )

    email = "docente@udea.edu.co"
    expected_username = _expected_username(email)
    result = await provider.sign_up_user(
        email=email,
        password="password123",
    )

    assert captured["create"]["UserPoolId"] == "pool-id"
    assert captured["create"]["Username"] == expected_username
    assert captured["create"]["MessageAction"] == "SUPPRESS"
    assert captured["set_password"]["UserPoolId"] == "pool-id"
    assert captured["set_password"]["Username"] == expected_username
    assert captured["set_password"]["Password"] == "password123"
    assert captured["set_password"]["Permanent"] is True
    assert captured["disable"]["UserPoolId"] == "pool-id"
    assert captured["disable"]["Username"] == expected_username
    assert result.username == expected_username
    assert result.user_confirmed is False


@pytest.mark.asyncio
async def test_sign_up_user_returns_non_email_username(monkeypatch):
    class FakeClient:
        def admin_create_user(self, **kwargs):
            return {}

        def admin_set_user_password(self, **kwargs):
            return {}

        def admin_disable_user(self, **kwargs):
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            assert service_name == "cognito-idp"
            assert region_name == "us-east-1"
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(
        region="us-east-1",
        user_pool_id="pool-id",
    )

    email = "docente@udea.edu.co"
    result = await provider.sign_up_user(
        email=email,
        password="password123",
    )

    assert result.username == _expected_username(email)


@pytest.mark.asyncio
async def test_sign_up_user_rolls_back_created_user_when_post_create_step_fails(monkeypatch):
    captured = {}

    class FakeClientError(Exception):
        def __init__(self):
            self.response = {
                "Error": {
                    "Code": "InvalidPasswordException",
                    "Message": "Password did not conform with policy",
                }
            }

    class FakeClient:
        def admin_create_user(self, **kwargs):
            captured["create"] = kwargs
            return {}

        def admin_set_user_password(self, **kwargs):
            raise FakeClientError()

        def admin_disable_user(self, **kwargs):
            raise AssertionError("should not disable when set password fails")

        def admin_delete_user(self, **kwargs):
            captured["delete"] = kwargs
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id="pool-id")

    with pytest.raises(HTTPException) as exc_info:
        await provider.sign_up_user(
            email="docente@udea.edu.co",
            password="password123",
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "INVALID_PASSWORD_POLICY"
    assert captured["delete"]["UserPoolId"] == "pool-id"


@pytest.mark.asyncio
async def test_sign_up_user_requires_user_pool_id(monkeypatch):
    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return object()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id=None)

    with pytest.raises(HTTPException) as exc_info:
        await provider.sign_up_user(email="docente@udea.edu.co", password="password123")

    assert exc_info.value.status_code == 500
    assert "COGNITO_USER_POOL_ID" in exc_info.value.detail


@pytest.mark.asyncio
async def test_sign_up_user_sends_given_and_family_name_attributes(monkeypatch):
    captured = {}

    class FakeClient:
        def admin_create_user(self, **kwargs):
            captured["attrs"] = kwargs["UserAttributes"]
            return {}

        def admin_set_user_password(self, **kwargs):
            return {}

        def admin_disable_user(self, **kwargs):
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id="pool-id")

    result = await provider.sign_up_user(
        email="docente@udea.edu.co",
        password="password123",
        given_name="Doc",
        family_name="Udea",
    )

    names = {(item["Name"], item["Value"]) for item in captured["attrs"]}
    assert ("given_name", "Doc") in names
    assert ("family_name", "Udea") in names
    assert result.user_confirmed is False


@pytest.mark.asyncio
async def test_sign_up_user_auto_verifies_email_when_enabled(monkeypatch):
    captured = {}

    class FakeClient:
        def admin_create_user(self, **kwargs):
            return {}

        def admin_update_user_attributes(self, **kwargs):
            captured["update_attrs"] = kwargs
            return {}

        def admin_set_user_password(self, **kwargs):
            return {}

        def admin_disable_user(self, **kwargs):
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(
        region="us-east-1",
        user_pool_id="pool-id",
        auto_verify_email=True,
    )

    await provider.sign_up_user(
        email="docente@udea.edu.co",
        password="password123",
    )

    assert captured["update_attrs"]["UserPoolId"] == "pool-id"
    assert captured["update_attrs"]["UserAttributes"] == [
        {"Name": "email_verified", "Value": "true"}
    ]


@pytest.mark.asyncio
async def test_authenticate_user_sends_secret_hash_when_client_secret_exists(monkeypatch):
    captured = {}

    class FakeClient:
        def initiate_auth(self, **kwargs):
            captured["initiate_auth"] = kwargs
            return {
                "AuthenticationResult": {
                    "AccessToken": "access-token",
                    "IdToken": "id-token",
                    "RefreshToken": "refresh-token",
                    "ExpiresIn": 3600,
                    "TokenType": "Bearer",
                }
            }

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            assert service_name == "cognito-idp"
            assert region_name == "us-east-1"
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(
        region="us-east-1",
        user_pool_client_id="client-id",
        user_pool_client_secret="client-secret",
    )

    email = "docente@udea.edu.co"
    expected_username = _expected_username(email)
    result = await provider.authenticate_user(
        email=email,
        password="password123",
    )

    auth_params = captured["initiate_auth"]["AuthParameters"]
    assert auth_params["USERNAME"] == expected_username
    assert auth_params["SECRET_HASH"] == _expected_secret_hash(
        expected_username,
        "client-id",
        "client-secret",
    )
    assert result.access_token == "access-token"


@pytest.mark.asyncio
async def test_authenticate_user_invalid_credentials_returns_401(monkeypatch):
    class FakeClientError(Exception):
        def __init__(self):
            self.response = {
                "Error": {
                    "Code": "NotAuthorizedException",
                    "Message": "Incorrect username or password.",
                }
            }

    class FakeClient:
        def initiate_auth(self, **kwargs):
            raise FakeClientError()

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            assert service_name == "cognito-idp"
            assert region_name == "us-east-1"
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(
        region="us-east-1",
        user_pool_client_id="client-id",
    )

    with pytest.raises(HTTPException) as exc_info:
        await provider.authenticate_user(
            email="docente@udea.edu.co",
            password="bad-password",
        )

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail["message"] == "Credenciales inválidas."


@pytest.mark.asyncio
async def test_authenticate_user_requires_client_id(monkeypatch):
    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return object()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_client_id=None)

    with pytest.raises(HTTPException) as exc_info:
        await provider.authenticate_user(
            email="docente@udea.edu.co",
            password="password123",
        )

    assert exc_info.value.status_code == 500
    assert "COGNITO_APP_CLIENT_ID" in exc_info.value.detail


@pytest.mark.asyncio
async def test_authenticate_user_fails_when_access_token_is_missing(monkeypatch):
    class FakeClient:
        def initiate_auth(self, **kwargs):
            return {"AuthenticationResult": {"IdToken": "id-token"}}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_client_id="client-id")

    with pytest.raises(HTTPException) as exc_info:
        await provider.authenticate_user(
            email="docente@udea.edu.co",
            password="password123",
        )

    assert exc_info.value.status_code == 400
    assert "AccessToken" in exc_info.value.detail


@pytest.mark.asyncio
async def test_assign_roles_reads_cognito_error_from_wrapped_exception(monkeypatch):
    class FakeClientError(Exception):
        def __init__(self):
            self.response = {
                "Error": {
                    "Code": "ResourceNotFoundException",
                    "Message": "Group admin_general does not exist.",
                }
            }

    class WrappedError(Exception):
        pass

    class FakeClient:
        def admin_get_user(self, **kwargs):
            return {"Username": kwargs["Username"]}

        def admin_list_groups_for_user(self, **kwargs):
            return {"Groups": []}

        def admin_add_user_to_group(self, **kwargs):
            raise WrappedError("worker wrapper") from FakeClientError()

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            assert service_name == "cognito-idp"
            assert region_name == "us-east-1"
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(
        region="us-east-1",
        user_pool_id="pool-1",
    )

    with pytest.raises(HTTPException) as exc_info:
            await provider.assign_user_roles(
                username="andres.serrano",
                roles=(Role.ADMIN_GENERAL,),
            )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "COGNITO_ASSIGN_ROLES_FAILED"
    assert exc_info.value.detail["message"] == "No fue posible asignar roles al usuario."


@pytest.mark.asyncio
async def test_assign_roles_uses_exception_text_when_no_cognito_response(monkeypatch):
    class FakeClient:
        def admin_get_user(self, **kwargs):
            return {"Username": kwargs["Username"]}

        def admin_list_groups_for_user(self, **kwargs):
            return {"Groups": []}

        def admin_add_user_to_group(self, **kwargs):
            raise RuntimeError("network timeout")

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            assert service_name == "cognito-idp"
            assert region_name == "us-east-1"
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(
        region="us-east-1",
        user_pool_id="pool-1",
    )

    with pytest.raises(HTTPException) as exc_info:
            await provider.assign_user_roles(
                username="andres.serrano",
                roles=(Role.ADMIN_GENERAL,),
            )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["message"] == "No fue posible asignar roles al usuario."


@pytest.mark.asyncio
async def test_assign_roles_fails_with_clear_message_when_user_not_found(monkeypatch):
    class FakeClientError(Exception):
        def __init__(self):
            self.response = {
                "Error": {
                    "Code": "UserNotFoundException",
                    "Message": "User does not exist.",
                }
            }

    class FakeClient:
        def admin_get_user(self, **kwargs):
            raise FakeClientError()

        def admin_list_groups_for_user(self, **kwargs):
            raise AssertionError("should not list groups when user is missing")

        def admin_add_user_to_group(self, **kwargs):
            raise AssertionError("should not be called when user is missing")

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            assert service_name == "cognito-idp"
            assert region_name == "us-east-1"
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(
        region="us-east-1",
        user_pool_id="pool-1",
    )

    with pytest.raises(HTTPException) as exc_info:
            await provider.assign_user_roles(
                username="andres.serrano",
                roles=(Role.ADMIN_GENERAL,),
            )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "COGNITO_ASSIGN_ROLES_FAILED"


@pytest.mark.asyncio
async def test_assign_roles_requires_user_pool_id(monkeypatch):
    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return object()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id=None)

    with pytest.raises(HTTPException) as exc_info:
            await provider.assign_user_roles(
                username="andres.serrano",
                roles=(Role.ADMIN_GENERAL,),
            )

    assert exc_info.value.status_code == 500
    assert "COGNITO_USER_POOL_ID" in exc_info.value.detail


@pytest.mark.asyncio
async def test_assign_roles_skips_existing_groups_and_returns_final_roles(monkeypatch):
    captured = {"added": []}

    class FakeClient:
        def admin_get_user(self, **kwargs):
            return {"Username": kwargs["Username"]}

        def admin_list_groups_for_user(self, **kwargs):
            return {"Groups": [{"GroupName": "auxiliar_almacen"}]}

        def admin_add_user_to_group(self, **kwargs):
            captured["added"].append(kwargs["GroupName"])
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            assert service_name == "cognito-idp"
            assert region_name == "us-east-1"
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(
        region="us-east-1",
        user_pool_id="pool-1",
    )

    result = await provider.assign_user_roles(
        username="andres.serrano",
        roles=(Role.AUXILIAR_ALMACEN, Role.ADMIN_GENERAL),
    )

    assert captured["added"] == ["admin_general"]
    assert list(result.roles) == ["admin_general", "auxiliar_almacen"]


@pytest.mark.asyncio
async def test_assign_roles_removes_stale_known_groups(monkeypatch):
    captured = {"added": [], "removed": []}

    class FakeClient:
        def admin_get_user(self, **kwargs):
            return {"Username": kwargs["Username"]}

        def admin_list_groups_for_user(self, **kwargs):
            return {"Groups": [{"GroupName": "admin_general"}, {"GroupName": "custom-role"}]}

        def admin_remove_user_from_group(self, **kwargs):
            captured["removed"].append(kwargs["GroupName"])
            return {}

        def admin_add_user_to_group(self, **kwargs):
            captured["added"].append(kwargs["GroupName"])
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id="pool-1")

    result = await provider.assign_user_roles(
        username="andres.serrano",
        roles=(Role.AUXILIAR_ALMACEN,),
    )

    assert captured["removed"] == ["admin_general", "custom-role"]
    assert captured["added"] == ["auxiliar_almacen"]
    assert list(result.roles) == ["auxiliar_almacen"]


@pytest.mark.asyncio
async def test_assign_roles_supports_group_pagination_and_removes_stale_groups(monkeypatch):
    captured = {"list_calls": [], "added": [], "removed": []}

    class FakeClient:
        def admin_get_user(self, **kwargs):
            return {"Username": kwargs["Username"]}

        def admin_list_groups_for_user(self, **kwargs):
            captured["list_calls"].append(kwargs)
            if len(captured["list_calls"]) == 1:
                return {
                    "Groups": [{"GroupName": "programador_almacen"}, "skip-me"],
                    "NextToken": "next-token",
                }
            return {
                "Groups": [{"GroupName": "custom-role"}],
            }

        def admin_remove_user_from_group(self, **kwargs):
            captured["removed"].append(kwargs["GroupName"])
            return {}

        def admin_add_user_to_group(self, **kwargs):
            captured["added"].append(kwargs["GroupName"])
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id="pool-1")

    result = await provider.assign_user_roles(
        username="andres.serrano",
        roles=(Role.PROGRAMADOR_ALMACEN, Role.AUXILIAR_ALMACEN),
    )

    assert "NextToken" not in captured["list_calls"][0]
    assert captured["list_calls"][1]["NextToken"] == "next-token"
    assert captured["removed"] == ["custom-role"]
    assert captured["added"] == ["auxiliar_almacen"]
    assert set(result.roles) == {"programador_almacen", "auxiliar_almacen"}


@pytest.mark.asyncio
async def test_set_user_enabled_requires_user_pool_id(monkeypatch):
    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return object()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id=None)

    with pytest.raises(HTTPException) as exc_info:
        await provider.set_user_enabled(username="andres.serrano", enabled=True)

    assert exc_info.value.status_code == 500
    assert "COGNITO_USER_POOL_ID" in exc_info.value.detail


@pytest.mark.asyncio
async def test_set_user_enabled_activates_user(monkeypatch):
    captured = {}

    class FakeClient:
        def admin_enable_user(self, **kwargs):
            captured["enable"] = kwargs
            return {}

        def admin_disable_user(self, **kwargs):
            raise AssertionError("should not disable when enabled=True")
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id="pool-1")

    result = await provider.set_user_enabled(username="andres.serrano", enabled=True)

    assert captured["enable"]["UserPoolId"] == "pool-1"
    assert result.username == "andres.serrano"
    assert result.enabled is True
    assert result.message == "Cuenta activada correctamente."


@pytest.mark.asyncio
async def test_set_user_enabled_deactivates_user(monkeypatch):
    captured = {}

    class FakeClient:
        def admin_enable_user(self, **kwargs):
            raise AssertionError("should not enable when enabled=False")

        def admin_disable_user(self, **kwargs):
            captured["disable"] = kwargs
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id="pool-1")

    result = await provider.set_user_enabled(username="andres.serrano", enabled=False)

    assert captured["disable"]["UserPoolId"] == "pool-1"
    assert result.enabled is False
    assert result.message == "Cuenta deshabilitada correctamente."


@pytest.mark.asyncio
async def test_set_user_enabled_maps_cognito_errors(monkeypatch):
    class FakeClientError(Exception):
        def __init__(self):
            self.response = {
                "Error": {
                    "Code": "UserNotFoundException",
                    "Message": "User does not exist.",
                }
            }

    class FakeClient:
        def admin_enable_user(self, **kwargs):
            raise FakeClientError()

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id="pool-1")

    with pytest.raises(HTTPException) as exc_info:
        await provider.set_user_enabled(username="andres.serrano", enabled=True)

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "COGNITO_SET_ACCOUNT_STATUS_FAILED"


@pytest.mark.asyncio
async def test_reset_user_password_requires_user_pool_id(monkeypatch):
    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return object()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id=None)

    with pytest.raises(HTTPException) as exc_info:
        await provider.reset_user_password(username="andres.serrano")

    assert exc_info.value.status_code == 500
    assert "COGNITO_USER_POOL_ID" in exc_info.value.detail


@pytest.mark.asyncio
async def test_reset_user_password_calls_admin_reset_and_returns_message(monkeypatch):
    captured = {}

    class FakeClient:
        def admin_reset_user_password(self, **kwargs):
            captured["reset"] = kwargs
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            assert service_name == "cognito-idp"
            assert region_name == "us-east-1"
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id="pool-1")

    result = await provider.reset_user_password(username="andres.serrano")

    assert captured["reset"]["UserPoolId"] == "pool-1"
    assert captured["reset"]["Username"] == "andres.serrano"
    assert "restablecimiento" in result.message.lower()


@pytest.mark.asyncio
async def test_reset_user_password_returns_opaque_for_user_not_found(monkeypatch):
    class FakeClientError(Exception):
        def __init__(self):
            self.response = {
                "Error": {
                    "Code": "UserNotFoundException",
                    "Message": "User does not exist.",
                }
            }

    class FakeClient:
        def admin_reset_user_password(self, **kwargs):
            raise FakeClientError()

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id="pool-1")

    result = await provider.reset_user_password(username="missing-user")
    assert "Si el usuario existe" in result.message


@pytest.mark.asyncio
async def test_reset_user_password_maps_too_many_requests_to_429(monkeypatch):
    class FakeClientError(Exception):
        def __init__(self):
            self.response = {
                "Error": {
                    "Code": "TooManyRequestsException",
                    "Message": "Rate exceeded",
                }
            }

    class FakeClient:
        def admin_reset_user_password(self, **kwargs):
            raise FakeClientError()

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id="pool-1")

    with pytest.raises(HTTPException) as exc_info:
        await provider.reset_user_password(username="any-user")

    assert exc_info.value.status_code == 429
    assert exc_info.value.detail["code"] == "TOO_MANY_PASSWORD_RESET_ATTEMPTS"


@pytest.mark.asyncio
async def test_refresh_tokens_calls_refresh_token_auth_flow(monkeypatch):
    captured = {}

    class FakeClient:
        def initiate_auth(self, **kwargs):
            captured["initiate_auth"] = kwargs
            return {
                "AuthenticationResult": {
                    "AccessToken": "new-access",
                    "IdToken": "new-id",
                    "ExpiresIn": 3600,
                    "TokenType": "Bearer",
                }
            }

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(
        region="us-east-1",
        user_pool_client_id="client-id",
    )

    result = await provider.refresh_tokens(refresh_token="refresh-token")

    assert captured["initiate_auth"]["AuthFlow"] == "REFRESH_TOKEN_AUTH"
    assert captured["initiate_auth"]["AuthParameters"]["REFRESH_TOKEN"] == "refresh-token"
    assert result.access_token == "new-access"
    assert result.refresh_token == "refresh-token"


@pytest.mark.asyncio
async def test_refresh_tokens_requires_login_when_client_secret_is_configured(monkeypatch):
    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return object()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(
        region="us-east-1",
        user_pool_client_id="client-id",
        user_pool_client_secret="secret-value",
    )

    with pytest.raises(HTTPException) as exc_info:
        await provider.refresh_tokens(refresh_token="refresh-token")

    assert exc_info.value.status_code == 400
    assert "login" in exc_info.value.detail


@pytest.mark.asyncio
async def test_logout_revokes_refresh_token_in_cognito(monkeypatch):
    captured = {}

    class FakeClient:
        def revoke_token(self, **kwargs):
            captured["revoke"] = kwargs
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(
        region="us-east-1",
        user_pool_client_id="client-id",
        user_pool_client_secret="secret-1",
    )

    result = await provider.logout(refresh_token="refresh-token")

    assert captured["revoke"]["ClientId"] == "client-id"
    assert captured["revoke"]["Token"] == "refresh-token"
    assert captured["revoke"]["ClientSecret"] == "secret-1"
    assert "cerrada" in result.message.lower()


@pytest.mark.asyncio
async def test_request_password_reset_code_calls_cognito(monkeypatch):
    captured = {}

    class FakeClient:
        def forgot_password(self, **kwargs):
            captured["forgot"] = kwargs
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_client_id="client-id")

    result = await provider.request_password_reset_code(login="docente@udea.edu.co")

    assert captured["forgot"]["ClientId"] == "client-id"
    assert captured["forgot"]["Username"] == "docente"
    assert "código" in result.message.lower()


@pytest.mark.asyncio
async def test_confirm_password_reset_calls_cognito(monkeypatch):
    captured = {}

    class FakeClient:
        def confirm_forgot_password(self, **kwargs):
            captured["confirm"] = kwargs
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_client_id="client-id")

    result = await provider.confirm_password_reset(
        username="docente",
        confirmation_code="123456",
        new_password="NuevaPass123!",
    )

    assert captured["confirm"]["ClientId"] == "client-id"
    assert captured["confirm"]["Username"] == "docente"
    assert captured["confirm"]["ConfirmationCode"] == "123456"
    assert captured["confirm"]["Password"] == "NuevaPass123!"
    assert "correctamente" in result.message.lower()


@pytest.mark.asyncio
async def test_list_users_requires_user_pool_id(monkeypatch):
    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return object()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id=None)

    with pytest.raises(HTTPException) as exc_info:
        await provider.list_users(page=1, page_size=20)

    assert exc_info.value.status_code == 500
    assert "COGNITO_USER_POOL_ID" in exc_info.value.detail


@pytest.mark.asyncio
async def test_list_users_returns_profile_status_and_roles_with_pagination(monkeypatch):
    captured = {"list_users_calls": [], "group_calls": []}

    class FakeClient:
        def list_users(self, **kwargs):
            captured["list_users_calls"].append(kwargs)
            if len(captured["list_users_calls"]) == 1:
                return {
                    "Users": [
                        {
                            "Username": "docente",
                            "Enabled": True,
                            "Attributes": [
                                {"Name": "email", "Value": "docente@udea.edu.co"},
                                {"Name": "given_name", "Value": "Doc"},
                                {"Name": "family_name", "Value": "Udea"},
                            ],
                        }
                    ],
                    "PaginationToken": "next-page",
                }
            return {
                "Users": [
                    {
                        "Username": "auxiliar",
                        "Enabled": False,
                        "Attributes": [
                            {"Name": "email", "Value": "auxiliar@udea.edu.co"},
                        ],
                    }
                ]
            }

        def admin_list_groups_for_user(self, **kwargs):
            captured["group_calls"].append(kwargs)
            username = kwargs.get("Username")
            if username == "docente":
                return {"Groups": [{"GroupName": "admin_general"}, {"GroupName": "custom_group"}]}
            return {"Groups": [{"GroupName": "auxiliar_ucara"}]}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id="pool-1")

    page = await provider.list_users(page=2, page_size=1)

    assert "PaginationToken" not in captured["list_users_calls"][0]
    assert captured["list_users_calls"][1]["PaginationToken"] == "next-page"
    assert len(captured["group_calls"]) == 2
    assert page.total == 2
    assert page.page == 2
    assert page.page_size == 1
    assert len(page.users) == 1
    assert page.users[0].username == "docente"
    assert page.users[0].email == "docente@udea.edu.co"
    assert page.users[0].given_name == "Doc"
    assert page.users[0].family_name == "Udea"
    assert page.users[0].roles == ("admin_general", "custom_group")


@pytest.mark.asyncio
async def test_resolve_access_token_identity_fields_from_cognito(monkeypatch):
    class FakeClient:
        def get_user(self, **kwargs):
            assert kwargs["AccessToken"] == "access-token"
            return {
                "Username": "andres.serrano",
                "UserAttributes": [
                    {"Name": "email", "Value": " ANDRES.SERRANO@UDEA.EDU.CO "},
                ],
            }

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            assert service_name == "cognito-idp"
            assert region_name == "us-east-1"
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1")

    assert await provider.resolve_email_from_access_token(access_token="access-token") == (
        "andres.serrano@udea.edu.co"
    )
    assert await provider.resolve_username_from_access_token(access_token="access-token") == "andres.serrano"


@pytest.mark.asyncio
async def test_resolve_access_token_identity_fields_return_none_when_cognito_response_lacks_data(
    monkeypatch,
):
    class FakeClient:
        def get_user(self, **kwargs):
            return {
                "Username": "   ",
                "UserAttributes": [
                    {"Name": "custom:department", "Value": "Sistemas"},
                ],
            }

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1")

    assert await provider.resolve_email_from_access_token(access_token="access-token") is None
    assert await provider.resolve_username_from_access_token(access_token="access-token") is None


@pytest.mark.asyncio
async def test_resolve_access_token_identity_fields_return_none_when_cognito_get_user_fails(
    monkeypatch,
):
    class FakeClient:
        def get_user(self, **kwargs):
            raise RuntimeError("expired token")

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1")

    assert await provider.resolve_email_from_access_token(access_token="bad-token") is None
    assert await provider.resolve_username_from_access_token(access_token="bad-token") is None


@pytest.mark.asyncio
async def test_ensure_roles_exist_creates_only_missing_groups(monkeypatch):
    captured = {"created": [], "list_calls": []}

    class FakeClient:
        def list_groups(self, **kwargs):
            captured["list_calls"].append(kwargs)
            return {"Groups": [{"GroupName": "admin_general"}]}

        def create_group(self, **kwargs):
            captured["created"].append(kwargs["GroupName"])
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id="pool-1")

    await provider.ensure_roles_exist(role_codes=("admin_general", "coord_bienestar"))

    assert captured["created"] == ["coord_bienestar"]


@pytest.mark.asyncio
async def test_delete_roles_ignores_missing_group_and_deletes_existing_one(monkeypatch):
    captured = {"deleted": []}

    class MissingGroupError(Exception):
        def __init__(self):
            self.response = {
                "Error": {
                    "Code": "ResourceNotFoundException",
                    "Message": "missing",
                }
            }

    class FakeClient:
        def delete_group(self, **kwargs):
            group_name = kwargs["GroupName"]
            if group_name == "missing_role":
                raise MissingGroupError()
            captured["deleted"].append(group_name)
            return {}

    class FakeBoto3:
        @staticmethod
        def client(service_name, region_name):
            return FakeClient()

    monkeypatch.setattr(mod, "boto3", FakeBoto3)
    provider = mod.CognitoIdentityProvider(region="us-east-1", user_pool_id="pool-1")

    await provider.delete_roles(role_codes=("coord_bienestar", "missing_role"))

    assert captured["deleted"] == ["coord_bienestar"]
