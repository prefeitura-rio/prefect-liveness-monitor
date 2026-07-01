import httpx
import pytest

from monitor.producer import stream_logs
from tests.helpers import FakeLogger, make_config, make_mock_client


@pytest.fixture(autouse=True)
def patch_read_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub out the filesystem token read so producer tests need no real serviceaccount."""
    monkeypatch.setattr("monitor.producer.read_token", lambda: "tok")


async def collect(client: httpx.AsyncClient) -> list[str]:
    """Drain stream_logs into a list for assertion."""
    return [line async for line in stream_logs(client, make_config())]


class TestStreamLogs:
    @pytest.mark.parametrize(
        ("batches", "max_attempts", "expected"),
        [
            ([["line-A", "line-B", "line-C"]], 1, ["line-A", "line-B", "line-C"]),
            ([["line-A", "", "  ", "line-B"]], 1, ["line-A", "line-B"]),
            (
                [["line-A", "line-B", "line-C"], ["line-C", "line-D"]],
                2,
                ["line-A", "line-B", "line-C", "line-D"],
            ),
            ([["line-A"], ["line-B"]], 2, ["line-A", "line-B"]),
            ([], 3, []),
        ],
    )
    async def test_stream_output(
        self,
        batches: list[list[str]],
        max_attempts: int,
        expected: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """stream_logs forwards lines, drops blanks, deduplicates across reconnects."""
        monkeypatch.setattr("monitor.producer.MAX_RECONNECT_ATTEMPTS", max_attempts)
        assert await collect(make_mock_client(batches)) == expected

    async def test_logs_warning_on_reconnect(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A warning is emitted for each ConnectError that is not the final attempt."""
        monkeypatch.setattr("monitor.producer.MAX_RECONNECT_ATTEMPTS", 2)
        fake_logger = FakeLogger()
        monkeypatch.setattr("monitor.producer.logger", fake_logger)

        # Empty mock → ConnectError on every call; attempt 0 warns, attempt 1 errors.
        _ = await collect(make_mock_client([]))

        assert len(fake_logger.warnings) == 1
        assert len(fake_logger.errors) == 1

    async def test_logs_error_on_exhaustion(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An error is logged and no warning when the only attempt fails."""
        monkeypatch.setattr("monitor.producer.MAX_RECONNECT_ATTEMPTS", 1)
        fake_logger = FakeLogger()
        monkeypatch.setattr("monitor.producer.logger", fake_logger)

        _ = await collect(make_mock_client([]))

        assert len(fake_logger.errors) == 1
        assert len(fake_logger.warnings) == 0

    async def test_intra_batch_dedup(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Duplicate lines within a single batch are forwarded only once."""
        monkeypatch.setattr("monitor.producer.MAX_RECONNECT_ATTEMPTS", 1)
        result = await collect(make_mock_client([["x", "x", "x"]]))
        assert result == ["x"]

    async def test_cross_batch_dedup(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Lines seen in a previous batch are not forwarded again on reconnect."""
        monkeypatch.setattr("monitor.producer.MAX_RECONNECT_ATTEMPTS", 2)
        result = await collect(make_mock_client([["line-A"], ["line-A", "line-B"]]))
        assert result == ["line-A", "line-B"]

    async def test_blank_lines_dropped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty and whitespace-only lines are never forwarded."""
        monkeypatch.setattr("monitor.producer.MAX_RECONNECT_ATTEMPTS", 1)
        result = await collect(make_mock_client([["", "  ", "\t", "real"]]))
        assert result == ["real"]
