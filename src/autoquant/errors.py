class AutoQuantError(Exception):
    """Base class for stable application failures."""


class MissingCapabilityError(AutoQuantError):
    """A required external capability is not configured."""


class VendorAuthenticationError(AutoQuantError):
    """A vendor rejected authentication without exposing credentials."""


class VendorResponseError(AutoQuantError):
    """A vendor response violated the declared contract."""


class VendorPermissionError(AutoQuantError):
    """A vendor denied access to a requested capability."""


class VendorRateLimitError(AutoQuantError):
    """A vendor rate limit remained exhausted after bounded retries."""


class PersistenceUnavailableError(AutoQuantError):
    """A required durable store is unavailable."""
