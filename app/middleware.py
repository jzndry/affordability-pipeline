import json

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.config import settings


class _BodyTooLargeError(Exception):
    """Internal signal raised from the wrapped receive when streamed bytes overflow."""


class MaxBodySizeMiddleware:
    """Reject request bodies larger than settings.MAX_REQUEST_BYTES with a 413."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        limit = settings.MAX_REQUEST_BYTES
        headers = dict(scope.get("headers") or [])
        declared = headers.get(b"content-length")
        if declared is not None:
            try:
                if int(declared) > limit:
                    await self._reject(send)
                    return
            except ValueError:
                pass

        received = 0

        async def counting_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > limit:
                    raise _BodyTooLargeError
            return message

        try:
            await self.app(scope, counting_receive, send)
        except _BodyTooLargeError:
            await self._reject(send)

    async def _reject(self, send: Send) -> None:
        body = json.dumps({"detail": "Request body too large."}).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
