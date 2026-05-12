"""Custom DRF permissions for pricemon's non-public endpoints.

The public surface under /api/v1/{prices,current,candles,health} uses
``AllowAny``. The /api/v1/internal/ tree is gated by a bearer token shared
with one or more peers (currently only the primary host pulling backfill
data from a fallback).
"""

from django.conf import settings
from rest_framework.permissions import BasePermission


class BackfillTokenPermission(BasePermission):
    """Bearer-token auth backed by ``BACKFILL_API_TOKEN`` in settings.

    Returns 503 (via PermissionDenied with a specific code) when the token
    is unset — the endpoint is disabled on hosts that haven't opted in to
    serving backfill data, rather than silently accepting any request.
    """

    message = "Backfill endpoint requires a valid bearer token."

    def has_permission(self, request, view) -> bool:
        configured = getattr(settings, "BACKFILL_API_TOKEN", "") or ""
        if not configured:
            return False
        auth = request.META.get("HTTP_AUTHORIZATION", "")
        if not auth.startswith("Bearer "):
            return False
        provided = auth[len("Bearer "):].strip()
        # Constant-time-ish compare. Tokens are 64+ random bytes so length
        # is uniform across legitimate callers.
        if len(provided) != len(configured):
            return False
        mismatch = 0
        for a, b in zip(provided, configured):
            mismatch |= ord(a) ^ ord(b)
        return mismatch == 0
