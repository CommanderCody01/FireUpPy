import time
from typing import Awaitable, Callable

from common_metrics.metrics import metrics
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from cif.persistence import MetricLabels, MetricName


class RequestMetricsMiddleware(BaseHTTPMiddleware):
    # override the dispatch function in BaseHTTPMiddleware
    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        """Used for tracking the latencies and response status code for all incoming requests."""
        timer_start = time.perf_counter()
        response = None
        try:
            metrics.increment_counter(
                MetricName.REQUEST_COUNT, 1, labels=MetricLabels(method=request.method, path=request.url.path).model_dump()
            )
            response = await call_next(request)
        finally:
            time_taken_ms = (time.perf_counter() - timer_start) * 1000
            labels = MetricLabels(
                method=request.method, status_code=response.status_code if response else 500, path=request.url.path
            ).model_dump()
            metrics.increment_counter(MetricName.RESPONSE_COUNT, 1, labels=labels)
            # tracks latencies as distributions, useful for percentiles
            metrics.record_histogram(MetricName.REQUEST_LATENCY, time_taken_ms, labels=labels)
            # tracks the current latencies, does not support percentiles, useful for checking latency at that instant
            metrics.set_gauge_value(MetricName.REQUEST_LATENCY_CURRENT, time_taken_ms, labels=labels)

        return response
