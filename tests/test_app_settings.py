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
