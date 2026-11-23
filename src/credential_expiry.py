"""Token expiry handling shared by OAuth and credential selection."""
from datetime import datetime, timezone


def parse_expiry(value):
    if not isinstance(value, str) or not value:
        return None
    try:
        expiry = datetime.fromisoformat(value)
    except ValueError:
        return None
    return expiry if expiry.tzinfo is not None else expiry.replace(tzinfo=timezone.utc)


def expiry_allows_dispatch(expiry, now):
    expires_at = parse_expiry(expiry)
    return bool(expires_at and (expires_at - now).total_seconds() > 300)
