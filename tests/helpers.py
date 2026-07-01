"""Non-fixture test helpers shared across all test modules."""

import asyncio
from collections.abc import AsyncGenerator

import httpx

from monitor.config import Config


async def make_stream(*lines: str) -> AsyncGenerator[str]:
    """Async generator yielding *lines* — stand-in for stream_logs in controller tests."""
    for line in lines:
        yield line


async def blocking_stream() -> AsyncGenerator[str]:
    """Async generator that blocks forever — triggers silence timeout in controller tests."""
    _ = await asyncio.Event().wait()
    yield ""  # unreachable


def make_config(
    *,
    pod_name: str = "test-pod",
    pod_namespace: str = "test-ns",
    silence_window: int = 1800,
    max_failures: int = 3,
    startup_grace_seconds: int = 5,
    stream_read_timeout: int = 120,
    k8s_api: str = "https://k8s.test",
    error_patterns: list[str] | None = None,
) -> Config:
    """Build a Config with safe test defaults, overridable per test."""
    return Config(
        pod_name=pod_name,
        pod_namespace=pod_namespace,
        silence_window=silence_window,
        max_failures=max_failures,
        startup_grace_seconds=startup_grace_seconds,
        stream_read_timeout=stream_read_timeout,
        k8s_api=k8s_api,
        error_patterns=error_patterns or ["docket.strikelist - Error monitoring strikes"],
    )


class FakeLogger:
    """Typed stand-in for loguru.logger, capturing warning and error calls for assertions."""

    def __init__(self) -> None:
        self.warnings: list[tuple[object, ...]] = []
        self.errors: list[tuple[object, ...]] = []

    def warning(self, msg: str, *args: object) -> None:
        """Capture a warning message."""
        self.warnings.append((msg, *args))

    def error(self, msg: str, *args: object) -> None:
        """Capture an error message."""
        self.errors.append((msg, *args))


def make_mock_client(batches: list[list[str]]) -> httpx.AsyncClient:
    """Return an AsyncClient backed by MockTransport serving *batches* sequentially.

    Each call to client.stream() consumes the next batch. After all batches are
    exhausted every subsequent call raises httpx.ConnectError.
    """
    it = iter(batches)

    def handler(_request: httpx.Request) -> httpx.Response:
        try:
            lines = next(it)
            return httpx.Response(200, text="\n".join(lines))
        except StopIteration:
            raise httpx.ConnectError("no more batches") from None

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))
