import pytest

from persistence_kit.settings import (
    AuthProvider,
    DeploymentStage,
    ExportStorageProvider,
    PersistenceKitSettings,
)


def test_persistence_kit_settings_exposes_common_defaults():
    settings = PersistenceKitSettings()

    assert settings.stage == DeploymentStage.LOCAL
    assert settings.auth_provider == AuthProvider.MEMORY
    assert settings.export_storage_provider == ExportStorageProvider.LOCAL
    assert settings.docs_enabled is True
    assert settings.auth_rate_limit_enabled is False
    assert settings.memory_seed_role_codes == ()


def test_persistence_kit_settings_enables_rate_limit_outside_local_by_default():
    settings = PersistenceKitSettings(stage=DeploymentStage.DEV)

    assert settings.auth_rate_limit_enabled is True


@pytest.fixture
def no_cache_backend(monkeypatch):
    monkeypatch.delenv("CACHE_BACKEND", raising=False)
    monkeypatch.delenv("AUTH_RATE_LIMIT_ENABLED", raising=False)


@pytest.mark.parametrize("backend", ["dynamodb", "mongo"])
def test_production_starts_with_a_shared_cache(monkeypatch, no_cache_backend, backend):
    monkeypatch.setenv("CACHE_BACKEND", backend)

    settings = PersistenceKitSettings(stage=DeploymentStage.PRODUCTION)

    assert settings.auth_rate_limit_enabled is True


def test_production_refuses_a_memory_cache(monkeypatch, no_cache_backend):
    monkeypatch.setenv("CACHE_BACKEND", "memory")

    with pytest.raises(ValueError, match="CACHE_BACKEND=memory is not allowed"):
        PersistenceKitSettings(stage=DeploymentStage.PRODUCTION)


def test_production_refuses_the_default_cache_backend(no_cache_backend):
    with pytest.raises(ValueError, match="CACHE_BACKEND=memory is not allowed"):
        PersistenceKitSettings(stage=DeploymentStage.PRODUCTION)


def test_production_refuses_a_disabled_rate_limit(monkeypatch, no_cache_backend):
    monkeypatch.setenv("CACHE_BACKEND", "dynamodb")

    with pytest.raises(ValueError, match="AUTH_RATE_LIMIT_ENABLED cannot be false"):
        PersistenceKitSettings(stage=DeploymentStage.PRODUCTION, auth_rate_limit_enabled=False)


def test_production_refuses_a_rate_limit_disabled_from_the_environment(monkeypatch, no_cache_backend):
    monkeypatch.setenv("CACHE_BACKEND", "dynamodb")
    monkeypatch.setenv("AUTH_RATE_LIMIT_ENABLED", "false")

    with pytest.raises(ValueError, match="AUTH_RATE_LIMIT_ENABLED cannot be false"):
        PersistenceKitSettings(stage=DeploymentStage.PRODUCTION)


@pytest.mark.parametrize("stage", [DeploymentStage.DEV, DeploymentStage.LOCAL])
def test_other_stages_still_start_with_a_memory_cache_and_no_rate_limit(
    monkeypatch, no_cache_backend, stage
):
    monkeypatch.setenv("CACHE_BACKEND", "memory")

    settings = PersistenceKitSettings(stage=stage, auth_rate_limit_enabled=False)

    assert settings.stage == stage
    assert settings.auth_rate_limit_enabled is False


@pytest.mark.parametrize(
    ("stage", "enabled"), [(DeploymentStage.DEV, True), (DeploymentStage.LOCAL, False)]
)
def test_other_stages_keep_their_rate_limit_default(no_cache_backend, stage, enabled):
    assert PersistenceKitSettings(stage=stage).auth_rate_limit_enabled is enabled
