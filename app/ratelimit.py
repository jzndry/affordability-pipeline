from slowapi import Limiter
from slowapi.util import get_remote_address
from starlette.requests import Request


def client_ip(request: Request) -> str:
    """Best-effort real client IP for rate-limit keying.

    NOTE: this trusts the ``CF-Connecting-IP`` / ``X-Forwarded-For`` request
    headers. That is only sound because the app is never directly exposed to the
    internet: Render + Cloudflare are the sole ingress and set these headers, so
    a caller cannot spoof its way past a per-IP limit. If the app is ever served
    without that proxy in front, this must revert to ``get_remote_address``.
    """
    cf = request.headers.get("cf-connecting-ip")
    if cf:
        return cf
    forwarded = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
    if forwarded:
        return forwarded
    return get_remote_address(request)


limiter = Limiter(key_func=client_ip)
