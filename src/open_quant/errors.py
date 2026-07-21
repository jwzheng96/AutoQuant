class OpenQuantError(Exception):
    """Base class for stable application failures."""


class MissingCapabilityError(OpenQuantError):
    """A required external capability is not configured."""


class VendorAuthenticationError(OpenQuantError):
    """A vendor rejected authentication without exposing credentials."""


class VendorResponseError(OpenQuantError):
    """A vendor response violated the declared contract."""


class PersistenceUnavailableError(OpenQuantError):
    """A required durable store is unavailable."""
