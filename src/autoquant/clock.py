from datetime import UTC, datetime
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")


def require_aware(value: datetime, *, name: str = "datetime") -> datetime:
    """Return an aware datetime or reject an ambiguous wall-clock value."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value


def to_utc(value: datetime, *, name: str = "datetime") -> datetime:
    """Normalize an aware datetime to UTC."""
    return require_aware(value, name=name).astimezone(UTC)


def to_shanghai(value: datetime, *, name: str = "datetime") -> datetime:
    """Convert an aware instant to the Asia/Shanghai civil timezone."""
    return require_aware(value, name=name).astimezone(SHANGHAI)
