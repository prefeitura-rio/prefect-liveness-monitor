from datetime import UTC, datetime
from queue import Queue

import httpx
import pytest
from stamina.instrumentation import RetryDetails

from monitor.producer import connect_and_stream, log_retry, stream_producer
from tests.helpers import FakeLogger, drain_queue, make_config, make_mock_client


@pytest.fixture(autouse=True)
def patch_read_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub out the filesystem token read so producer tests need no real serviceaccount."""
    monkeypatch.setattr("monitor.producer.read_token", lambda: "tok")


class TestLogRetry:
    def test_logs_warning_with_attempt_and_wait(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """log_retry emits a warning with attempt number, max attempts, and wait duration."""
        fake_logger = FakeLogger()
        monkeypatch.setattr("monitor.producer.logger", fake_logger)
        details = RetryDetails(
            name="test",
            args=(),
            kwargs={},
            retry_num=2,
            wait_for=1.5,
            waited_so_far=1.5,
            caused_by=ValueError("oops"),
        )
        log_retry(details)
        assert len(fake_logger.warnings) == 1


class TestConnectAndStream:
    def test_returns_original_last_seen_when_no_lines(self) -> None:
        """When the response stream is empty, last_seen is returned unchanged."""
        last_seen = datetime.now(UTC)
        result = connect_and_stream(
            make_mock_client([[]]), make_config(), set(), Queue(), last_seen
        )
        assert result is last_seen

    def test_returns_updated_timestamp_when_lines_drained(self) -> None:
        """When lines are drained, a newer timestamp is returned and lines appear in the queue."""
        last_seen = datetime(2000, 1, 1, tzinfo=UTC)
        out: Queue[str | None] = Queue()
        result = connect_and_stream(
            make_mock_client([["line-A"]]), make_config(), set(), out, last_seen
        )
        assert result > last_seen
        assert out.get_nowait() == "line-A"

    def test_propagates_connection_errors(self) -> None:
        """ConnectError from the underlying HTTP client propagates out of connect_and_stream."""
        with pytest.raises(httpx.ConnectError):
            _ = connect_and_stream(
                make_mock_client([]), make_config(), set(), Queue(), datetime.now(UTC)
            )


class TestStreamProducer:
    @pytest.mark.parametrize(
        ("batches", "max_attempts", "expected"),
        [
            ([["line-A", "line-B", "line-C"]], 1, ["line-A", "line-B", "line-C", None]),
            ([["line-A", "", "  ", "line-B"]], 1, ["line-A", "line-B", None]),
            (
                [["line-A", "line-B", "line-C"], ["line-C", "line-D"]],
                2,
                ["line-A", "line-B", "line-C", "line-D", None],
            ),
            ([["line-A"], ["line-B"]], 2, ["line-A", "line-B", None]),
            ([], 3, [None]),
        ],
    )
    def test_stream_output(
        self,
        batches: list[list[str]],
        max_attempts: int,
        expected: list[str | None],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """stream_producer forwards lines, drops blanks, deduplicates, and emits None sentinel."""
        monkeypatch.setattr("monitor.producer.MAX_RECONNECT_ATTEMPTS", max_attempts)
        out: Queue[str | None] = Queue()
        stream_producer(make_mock_client(batches), make_config(), out)
        assert drain_queue(out) == expected

    def test_logs_last_error_on_exhaustion(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """After all reconnect attempts fail, the last exception is logged as a warning."""
        monkeypatch.setattr("monitor.producer.MAX_RECONNECT_ATTEMPTS", 1)
        fake_logger = FakeLogger()
        monkeypatch.setattr("monitor.producer.logger", fake_logger)

        stream_producer(make_mock_client([]), make_config(), Queue())

        assert len(fake_logger.warnings) == 1
        assert isinstance(fake_logger.warnings[0][1], httpx.ConnectError)
