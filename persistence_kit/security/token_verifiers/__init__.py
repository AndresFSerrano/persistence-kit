__all__ = ["CognitoJwtVerifier", "MemoryJwtVerifier"]

_VERIFIERS = {
    "CognitoJwtVerifier": (
        "persistence_kit.security.token_verifiers.cognito_jwt_verifier",
        "CognitoJwtVerifier",
    ),
    "MemoryJwtVerifier": (
        "persistence_kit.security.token_verifiers.memory_jwt_verifier",
        "MemoryJwtVerifier",
    ),
}


def __getattr__(name: str):
    if name in _VERIFIERS:
        from importlib import import_module

        module_name, attr = _VERIFIERS[name]
        value = getattr(import_module(module_name), attr)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
