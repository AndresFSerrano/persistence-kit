__all__ = [
    "CognitoIdentityProvider",
    "KmsKeyProvider",
    "LocalKeyProvider",
    "MemorySecurityProvider",
]

_PROVIDERS = {
    "CognitoIdentityProvider": (
        "persistence_kit.security.providers.cognito_identity_provider",
        "CognitoIdentityProvider",
    ),
    "MemorySecurityProvider": (
        "persistence_kit.security.providers.memory_security_provider",
        "MemorySecurityProvider",
    ),
    "LocalKeyProvider": (
        "persistence_kit.security.providers.encryption_provider",
        "LocalKeyProvider",
    ),
    "KmsKeyProvider": (
        "persistence_kit.security.providers.encryption_provider",
        "KmsKeyProvider",
    ),
}


def __getattr__(name: str):
    if name in _PROVIDERS:
        from importlib import import_module

        module_name, attr = _PROVIDERS[name]
        value = getattr(import_module(module_name), attr)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
