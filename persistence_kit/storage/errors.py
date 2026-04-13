class StorageError(Exception):
    """Base exception for object storage adapters."""


class StorageConfigError(StorageError):
    """Raised when a storage adapter is not configured correctly."""


class StoragePathError(StorageError, ValueError):
    """Raised when a storage key resolves outside the configured storage root."""


class StorageUploadError(StorageError):
    """Raised when an upload operation fails."""


class StoragePresignError(StorageError):
    """Raised when a presigned URL cannot be generated."""
