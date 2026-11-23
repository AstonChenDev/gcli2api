"""Keep the Lite distinction supplied by account entitlements."""
from typing import Optional


DEFAULT_PROJECT_ID = "aicode-consumers"


def generation_project_id(credential):
    return (credential.get("project_id") or "").strip() or DEFAULT_PROJECT_ID


def normalize_credential_tier(raw_tier: Optional[str]) -> Optional[str]:
    if not raw_tier:
        return None
    tier = raw_tier.lower()
    if "ultra" in tier:
        return "ultra_lite" if "lite" in tier else "ultra"
    if "pro" in tier:
        return "pro"
    return "free"


def parse_tier(data):
    # currentTier is often free-tier even for paying Ultra customers.
    if not isinstance(data, dict):
        raise ValueError("Malformed entitlement response")
    paid = data.get("paidTier")
    if paid is not None and not isinstance(paid, dict):
        raise ValueError("Malformed paid tier")
    tier = paid if paid is not None else data.get("currentTier")
    raw = tier.get("id") if isinstance(tier, dict) else None
    if not isinstance(raw, str):
        raise ValueError("Unrecognized entitlement tier")
    raw = raw.strip().lower()
    # Disabling an account requires an explicit known Free identifier. Do not
    # guess from missing IDs, display names, or future IDs containing "free".
    if raw in ("free", "free-tier"):
        return "free"
    if not any(k in raw for k in ("pro", "ultra")):
        raise ValueError("Unrecognized entitlement tier")
    return normalize_credential_tier(raw)


def parse_project_id(data):
    value = data.get("cloudaicompanionProject") if isinstance(data, dict) else None
    if isinstance(value, dict):
        value = value.get("id")
    return (value.strip() or None) if isinstance(value, str) else None
