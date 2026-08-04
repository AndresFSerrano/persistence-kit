from enum import Enum

import pytest
from fastapi import HTTPException

from persistence_kit.security.providers.memory_security_provider import MemorySecurityProvider
from persistence_kit.security.token_verifiers.memory_jwt_verifier import MemoryJwtVerifier


class Role(str, Enum):
    ADMIN_GENERAL = "admin_general"
    PROGRAMADOR_ALMACEN = "programador_almacen"
    AUXILIAR_UCARA = "auxiliar_ucara"
    AUXILIAR_ALMACEN = "auxiliar_almacen"


def get_default_role_codes() -> tuple[str, ...]:
    return tuple(role.value for role in Role)


def _build_provider() -> MemorySecurityProvider:
    return MemorySecurityProvider(
        jwt_secret="test-secret-with-32-characters-minimum",
        jwt_issuer="memory-sandbox",
        token_ttl_seconds=3600,
    )


@pytest.mark.asyncio
async def test_sign_up_user_creates_disabled_user_without_default_roles():
    provider = _build_provider()

    result = await provider.sign_up_user(
        email="Docente@Udea.edu.co",
        password="password123",
        created_by="admin_general.seed",
    )

    assert result.username == "docente"
    assert result.user_confirmed is False
    page = await provider.list_users(page=1, page_size=10)
    assert page.users[0].roles == ()


@pytest.mark.asyncio
async def test_sign_up_user_rejects_duplicate_email_or_username():
    provider = _build_provider()
    await provider.sign_up_user(email="docente@udea.edu.co", password="password123")

    with pytest.raises(HTTPException) as err:
        await provider.sign_up_user(email="docente@udea.edu.co", password="password123")

    assert err.value.status_code == 409


@pytest.mark.asyncio
async def test_authenticate_requires_enabled_user_and_generates_tokens():
    provider = _build_provider()
    verifier = MemoryJwtVerifier(
        secret="test-secret-with-32-characters-minimum",
        issuer="memory-sandbox",
    )
    await provider.sign_up_user(email="docente@udea.edu.co", password="password123")

    with pytest.raises(HTTPException) as disabled_err:
        await provider.authenticate_user(email="docente@udea.edu.co", password="password123")
    assert disabled_err.value.status_code == 403

    await provider.set_user_enabled(username="docente", enabled=True)
    login = await provider.authenticate_user(email="docente@udea.edu.co", password="password123")

    payload = verifier.verify(login.access_token)
    assert payload["sub"] == "docente"
    assert payload["roles"] == []


@pytest.mark.asyncio
async def test_assign_roles_replaces_existing_roles():
    provider = _build_provider()
    await provider.sign_up_user(email="docente@udea.edu.co", password="password123")

    result = await provider.assign_user_roles(
        username="docente",
        roles=(Role.ADMIN_GENERAL, Role.ADMIN_GENERAL, Role.AUXILIAR_UCARA),
    )

    assert result.roles == (
        Role.ADMIN_GENERAL,
        Role.AUXILIAR_UCARA,
    )


def test_verify_rejects_unknown_token():
    verifier = MemoryJwtVerifier(
        secret="test-secret-with-32-characters-minimum",
        issuer="memory-sandbox",
    )

    with pytest.raises(HTTPException) as err:
        verifier.verify("missing")

    assert err.value.status_code == 401


@pytest.mark.asyncio
async def test_reset_user_password_sets_default_password_in_memory():
    provider = _build_provider()
    await provider.sign_up_user(email="docente@udea.edu.co", password="initial123")
    await provider.set_user_enabled(username="docente", enabled=True)

    reset = await provider.reset_user_password(username="docente")
    assert "password" in reset.message

    with pytest.raises(HTTPException) as wrong_password:
        await provider.authenticate_user(email="docente@udea.edu.co", password="initial123")
    assert wrong_password.value.status_code == 401

    login = await provider.authenticate_user(email="docente@udea.edu.co", password="password")
    assert login.access_token


@pytest.mark.asyncio
async def test_memory_tokens_expire_when_ttl_is_reached():
    provider = MemorySecurityProvider(
        jwt_secret="test-secret-with-32-characters-minimum",
        jwt_issuer="memory-sandbox",
        token_ttl_seconds=0,
    )
    verifier = MemoryJwtVerifier(
        secret="test-secret-with-32-characters-minimum",
        issuer="memory-sandbox",
    )

    await provider.sign_up_user(email="docente@udea.edu.co", password="password123")
    await provider.set_user_enabled(username="docente", enabled=True)
    login = await provider.authenticate_user(email="docente@udea.edu.co", password="password123")

    with pytest.raises(HTTPException) as err:
        verifier.verify(login.access_token)

    assert err.value.status_code == 401


@pytest.mark.asyncio
async def test_refresh_tokens_rotates_tokens_in_memory():
    provider = _build_provider()
    verifier = MemoryJwtVerifier(
        secret="test-secret-with-32-characters-minimum",
        issuer="memory-sandbox",
    )
    await provider.sign_up_user(email="docente@udea.edu.co", password="password123")
    await provider.set_user_enabled(username="docente", enabled=True)
    login = await provider.authenticate_user(email="docente@udea.edu.co", password="password123")

    refreshed = await provider.refresh_tokens(refresh_token=login.refresh_token or "")

    assert refreshed.access_token != login.access_token
    payload = verifier.verify(refreshed.access_token)
    assert payload["sub"] == "docente"


@pytest.mark.asyncio
async def test_refresh_tokens_rejects_invalid_refresh_token():
    provider = _build_provider()

    with pytest.raises(HTTPException) as err:
        await provider.refresh_tokens(refresh_token="invalid")

    assert err.value.status_code == 401


@pytest.mark.asyncio
async def test_logout_revokes_refresh_token_in_memory():
    provider = _build_provider()
    await provider.sign_up_user(email="docente@udea.edu.co", password="password123")
    await provider.set_user_enabled(username="docente", enabled=True)
    login = await provider.authenticate_user(email="docente@udea.edu.co", password="password123")

    out = await provider.logout(refresh_token=login.refresh_token)
    assert "cerrada" in out.message.lower()

    with pytest.raises(HTTPException) as err:
        await provider.refresh_tokens(refresh_token=login.refresh_token or "")
    assert err.value.status_code == 401


@pytest.mark.asyncio
async def test_memory_forgot_and_confirm_password_flow():
    provider = _build_provider()
    await provider.sign_up_user(email="docente@udea.edu.co", password="password123")
    await provider.set_user_enabled(username="docente", enabled=True)

    forgot = await provider.request_password_reset_code(login="docente@udea.edu.co")
    assert "sandbox" in forgot.message.lower()
    code = forgot.message.split(":")[-1].strip()

    confirm = await provider.confirm_password_reset(
        username="docente",
        confirmation_code=code,
        new_password="NuevaPass123!",
    )
    assert "correctamente" in confirm.message.lower()

    with pytest.raises(HTTPException):
        await provider.authenticate_user(email="docente@udea.edu.co", password="password123")
    login = await provider.authenticate_user(email="docente@udea.edu.co", password="NuevaPass123!")
    assert login.access_token


@pytest.mark.asyncio
async def test_list_users_returns_profile_status_and_roles():
    provider = _build_provider()
    await provider.sign_up_user(
        email="docente@udea.edu.co",
        password="password123",
        given_name="Doc",
        family_name="Udea",
        created_by="admin_general.seed",
    )
    await provider.assign_user_roles(
        username="docente",
        roles=(Role.ADMIN_GENERAL,),
    )
    await provider.set_user_enabled(username="docente", enabled=True)

    page = await provider.list_users(page=1, page_size=10)

    assert page.total == 1
    assert page.page == 1
    assert page.page_size == 10
    assert len(page.users) == 1
    user = page.users[0]
    assert user.username == "docente"
    assert user.email == "docente@udea.edu.co"
    assert user.given_name == "Doc"
    assert user.family_name == "Udea"
    assert user.created_by == "admin_general.seed"
    assert user.enabled is True
    assert user.roles == (Role.ADMIN_GENERAL,)


@pytest.mark.asyncio
async def test_list_users_filters_by_roles():
    provider = _build_provider()
    await provider.sign_up_user(email="programador.almacen@udea.edu.co", password="password123")
    await provider.sign_up_user(email="auxiliar.ucara@udea.edu.co", password="password123")
    await provider.assign_user_roles(
        username="programador.almacen",
        roles=(Role.PROGRAMADOR_ALMACEN,),
    )
    await provider.assign_user_roles(username="auxiliar.ucara", roles=(Role.AUXILIAR_UCARA,))

    page = await provider.list_users(page=1, page_size=20, roles=(Role.AUXILIAR_UCARA,))

    assert page.total == 1
    assert [user.username for user in page.users] == ["auxiliar.ucara"]


@pytest.mark.asyncio
async def test_seed_role_users_creates_one_user_per_role():
    provider = MemorySecurityProvider(
        jwt_secret="test-secret-with-32-characters-minimum",
        jwt_issuer="memory-sandbox",
        token_ttl_seconds=3600,
        seed_role_users=True,
        seed_user_password="Temporal123!",
        seed_role_codes=get_default_role_codes(),
        seed_user_domain="udea.edu.co",
        seed_created_by="admin_general.seed",
    )

    page = await provider.list_users(page=1, page_size=50)

    assert page.total == len(get_default_role_codes())
    usernames = {u.username for u in page.users}
    for role in Role:
        assert f"{role.value}.seed" in usernames
    created_by_by_username = {u.username: u.created_by for u in page.users}
    assert created_by_by_username[f"{Role.ADMIN_GENERAL.value}.seed"] == "admin_general.seed"

    login = await provider.authenticate_user(
        email=f"{Role.ADMIN_GENERAL.value}.seed@udea.edu.co",
        password="Temporal123!",
    )
    assert login.access_token


@pytest.mark.asyncio
async def test_resolve_access_token_identity_fields_from_memory_provider():
    provider = _build_provider()
    await provider.sign_up_user(email="Docente@Udea.edu.co", password="password123")
    await provider.set_user_enabled(username="docente", enabled=True)
    login = await provider.authenticate_user(email="docente@udea.edu.co", password="password123")

    assert await provider.resolve_email_from_access_token(access_token=login.access_token) == "docente@udea.edu.co"
    assert await provider.resolve_username_from_access_token(access_token=login.access_token) == "docente"


@pytest.mark.asyncio
async def test_resolve_access_token_identity_fields_return_none_for_invalid_or_missing_claims():
    provider = _build_provider()

    assert await provider.resolve_email_from_access_token(access_token="invalid-token") is None
    assert await provider.resolve_username_from_access_token(access_token="invalid-token") is None

    token_without_identity = provider._encode_token(
        {
            "iss": "memory-sandbox",
            "token_use": "access",
            "iat": 1,
            "exp": 9999999999,
            "jti": "missing-identity",
        }
    )

    assert await provider.resolve_email_from_access_token(access_token=token_without_identity) is None
    assert await provider.resolve_username_from_access_token(access_token=token_without_identity) is None


@pytest.mark.asyncio
async def test_assign_roles_raises_not_found_for_missing_user():
    provider = _build_provider()
    with pytest.raises(HTTPException) as err:
        await provider.assign_user_roles(username="ghost", roles=("admin_general",))
    assert err.value.status_code == 404


@pytest.mark.asyncio
async def test_authenticate_user_raises_for_nonexistent_user():
    provider = _build_provider()
    with pytest.raises(HTTPException) as err:
        await provider.authenticate_user(email="ghost@udea.edu.co", password="password123")
    assert err.value.status_code == 401


@pytest.mark.asyncio
async def test_authenticate_user_raises_for_wrong_password():
    provider = _build_provider()
    await provider.sign_up_user(email="docente@udea.edu.co", password="password123")
    await provider.set_user_enabled(username="docente", enabled=True)
    with pytest.raises(HTTPException) as err:
        await provider.authenticate_user(email="docente@udea.edu.co", password="wrong")
    assert err.value.status_code == 401


@pytest.mark.asyncio
async def test_set_user_enabled_raises_not_found():
    provider = _build_provider()
    with pytest.raises(HTTPException) as err:
        await provider.set_user_enabled(username="ghost", enabled=True)
    assert err.value.status_code == 404


@pytest.mark.asyncio
async def test_delete_user_raises_not_found():
    provider = _build_provider()
    with pytest.raises(HTTPException) as err:
        await provider.delete_user(username="ghost")
    assert err.value.status_code == 404


@pytest.mark.asyncio
async def test_reset_user_password_returns_opaque_for_missing_user():
    provider = _build_provider()
    result = await provider.reset_user_password(username="ghost")
    assert "Si el usuario existe" in result.message


@pytest.mark.asyncio
async def test_refresh_tokens_rejects_non_refresh_token():
    provider = _build_provider()
    token = provider._encode_token({
        "sub": "docente",
        "iss": "memory-sandbox",
        "token_use": "access",
        "iat": 1,
        "exp": 9999999999,
        "jti": "test",
    })
    with pytest.raises(HTTPException) as err:
        await provider.refresh_tokens(refresh_token=token)
    assert err.value.status_code == 401


@pytest.mark.asyncio
async def test_refresh_tokens_rejects_disabled_user():
    provider = _build_provider()
    await provider.sign_up_user(email="docente@udea.edu.co", password="password123")
    await provider.set_user_enabled(username="docente", enabled=True)
    login = await provider.authenticate_user(email="docente@udea.edu.co", password="password123")
    await provider.set_user_enabled(username="docente", enabled=False)
    with pytest.raises(HTTPException) as err:
        await provider.refresh_tokens(refresh_token=login.refresh_token)
    assert err.value.status_code == 403


@pytest.mark.asyncio
async def test_refresh_tokens_rejects_deleted_user():
    provider = _build_provider()
    await provider.sign_up_user(email="docente@udea.edu.co", password="password123")
    await provider.set_user_enabled(username="docente", enabled=True)
    login = await provider.authenticate_user(email="docente@udea.edu.co", password="password123")
    await provider.delete_user(username="docente")
    with pytest.raises(HTTPException) as err:
        await provider.refresh_tokens(refresh_token=login.refresh_token)
    assert err.value.status_code == 404


@pytest.mark.asyncio
async def test_request_password_reset_code_returns_opaque_for_missing_user():
    provider = _build_provider()
    result = await provider.request_password_reset_code(login="ghost")
    assert "Si el usuario existe" in result.message


@pytest.mark.asyncio
async def test_confirm_password_reset_returns_generic_error_for_missing_user():
    provider = _build_provider()
    with pytest.raises(HTTPException) as err:
        await provider.confirm_password_reset(username="ghost", confirmation_code="000000", new_password="x")
    assert err.value.status_code == 400
    assert "inválido o expirado" in err.value.detail.lower()


@pytest.mark.asyncio
async def test_confirm_password_reset_rejects_invalid_code():
    provider = _build_provider()
    await provider.sign_up_user(email="docente@udea.edu.co", password="password123")
    await provider.request_password_reset_code(login="docente")
    with pytest.raises(HTTPException) as err:
        await provider.confirm_password_reset(username="docente", confirmation_code="WRONG", new_password="x")
    assert err.value.status_code == 400


@pytest.mark.asyncio
async def test_logout_raises_without_refresh_token():
    provider = _build_provider()
    with pytest.raises(HTTPException) as err:
        await provider.logout(refresh_token=None)
    assert err.value.status_code == 400


@pytest.mark.asyncio
async def test_logout_rejects_non_refresh_token():
    provider = _build_provider()
    token = provider._encode_token({
        "sub": "docente",
        "iss": "memory-sandbox",
        "token_use": "access",
        "iat": 1,
        "exp": 9999999999,
        "jti": "test",
    })
    with pytest.raises(HTTPException) as err:
        await provider.logout(refresh_token=token)
    assert err.value.status_code == 401


@pytest.mark.asyncio
async def test_logout_rejects_invalid_token():
    provider = _build_provider()
    with pytest.raises(HTTPException) as err:
        await provider.logout(refresh_token="invalid-token")
    assert err.value.status_code == 401


def test_reset_clears_all_state():
    provider = MemorySecurityProvider(
        jwt_secret="test-secret-with-32-characters-minimum",
        jwt_issuer="memory-sandbox",
        seed_role_users=True,
    )
    provider.reset()
    import asyncio
    page = asyncio.run(provider.list_users(page=1, page_size=50))
    assert page.total == 0


@pytest.mark.asyncio
async def test_ensure_roles_exist_does_not_create_seed_user_for_new_role():
    provider = _build_provider()

    await provider.ensure_roles_exist(role_codes=("coord_bienestar",))

    page = await provider.list_users(page=1, page_size=20)
    usernames = {user.username for user in page.users}
    assert "coord_bienestar.seed" not in usernames


@pytest.mark.asyncio
async def test_delete_roles_does_not_remove_existing_users():
    provider = _build_provider()
    await provider.sign_up_user(email="auxiliar@udea.edu.co", password="password123")

    await provider.delete_roles(role_codes=("coord_bienestar",))

    page = await provider.list_users(page=1, page_size=20)
    usernames = {user.username for user in page.users}
    assert "auxiliar" in usernames


@pytest.mark.asyncio
async def test_ensure_roles_exist_keeps_default_seed_users():
    provider = _build_provider()
    initial_page = await provider.list_users(page=1, page_size=20)
    initial_usernames = {user.username for user in initial_page.users}

    await provider.ensure_roles_exist(role_codes=("coord_bienestar",))

    page = await provider.list_users(page=1, page_size=20)
    usernames = {user.username for user in page.users}
    assert usernames == initial_usernames


@pytest.mark.asyncio
async def test_delete_user_removes_disabled_user():
    provider = _build_provider()
    await provider.sign_up_user(email="auxiliar@udea.edu.co", password="password123")

    await provider.delete_user(username="auxiliar")

    page = await provider.list_users(page=1, page_size=20)
    assert all(user.username != "auxiliar" for user in page.users)
