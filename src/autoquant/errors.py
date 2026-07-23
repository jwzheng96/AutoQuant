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


class LiveTradingLockedError(AutoQuantError):
    """A live-broker mutation was attempted while the release lock is active."""


class BrokerStateUnknownError(AutoQuantError):
    """A broker response could not prove a known account or order state."""


class QmtSessionConflictError(AutoQuantError):
    """A QMT session identifier is leased by another active adapter."""


class QmtSessionLeaseLostError(AutoQuantError):
    """A QMT adapter no longer owns its durable session lease."""


class PaperSchedulerLeaseConflictError(AutoQuantError):
    """A paper account already has another active scheduler owner."""


class PaperSchedulerLeaseLostError(AutoQuantError):
    """A paper scheduler no longer owns its durable process lease."""


class MarketCalendarUnavailableError(AutoQuantError):
    """A point-in-time exchange calendar cannot prove the current market phase."""


class QuoteStreamUnavailableError(AutoQuantError):
    """A continuous quote stream is disconnected, stale, incomplete, or inconsistent."""
