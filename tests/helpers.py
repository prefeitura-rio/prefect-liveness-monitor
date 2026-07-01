"""Non-fixture test helpers shared across all test modules."""

from queue import Queue

import httpx

from monitor.config import Config


def queue_to_list(out: Queue[str | None]) -> list[str | None]:
    """Return all items currently in *out* without consuming them via the sentinel."""
    return [out.get_nowait() for _ in range(out.qsize())]


def drain_queue(out: Queue[str | None]) -> list[str | None]:
    """Consume *out* into a list, stopping after the None sentinel."""
    results: list[str | None] = []
    while True:
        item = out.get_nowait()
        results.append(item)
        if item is None:
            break
    return results


def make_config(
    *,
    pod_name: str = "test-pod",
    pod_namespace: str = "test-ns",
    silence_window: int = 1800,
    max_failures: int = 3,
    startup_grace_seconds: int = 5,
    stream_read_timeout: int = 120,
    k8s_api: str = "https://k8s.test",
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
    )


class FakeLogger:
    """Typed stand-in for loguru.logger, capturing warning calls for assertions."""

    def __init__(self) -> None:
        self.warnings: list[tuple[object, ...]] = []

    def warning(self, msg: str, *args: object) -> None:
        self.warnings.append((msg, *args))

    def error(self, _msg: str, *_args: object) -> None:
        pass


def make_mock_client(batches: list[list[str]]) -> httpx.Client:
    """Return a real httpx.Client backed by MockTransport serving *batches* sequentially.

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

    return httpx.Client(transport=httpx.MockTransport(handler))
