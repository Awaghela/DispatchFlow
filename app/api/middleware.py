import time

from .. import metrics


class MetricsMiddleware:
    """Pure-ASGI request metrics (cheaper than BaseHTTPMiddleware, and leaves WebSockets alone)."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        start = time.perf_counter()
        status = {"code": 500}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            route = scope.get("route")
            path = getattr(route, "path", None) or "unmatched"
            if path != "/metrics":
                metrics.HTTP_REQUESTS.labels(scope["method"], path, str(status["code"])).inc()
                metrics.HTTP_LATENCY.labels(scope["method"], path).observe(time.perf_counter() - start)
