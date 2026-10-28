"""Keep the Lite distinction supplied by account entitlements."""
from typing import Optional


def normalize_credential_tier(raw_tier: Optional[str]) -> Optional[str]:
    if not raw_tier:
        return None
    tier = raw_tier.lower()
    if "ultra" in tier:
        return "ultra_lite" if "lite" in tier else "ultra"
    if "pro" in tier:
        return "pro"
    return "free"
