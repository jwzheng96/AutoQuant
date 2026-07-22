class AutoQuantError(Exception):
    """Base class for stable application failures."""


class MissingCapabilityError(AutoQuantError):
    """A required external capability is not configured."""


class VendorAuthenticationError(AutoQuantError):
    """A vendor rejected authentication without exposing credentials."""


class VendorResponseError(AutoQuantError):
    """A vendor response violated the declared contract."""


class PersistenceUnavailableError(AutoQuantError):
    """A required durable store is unavailable."""
