from datetime import UTC, datetime, timedelta
from queue import Queue

import pytest

from monitor.config import Config
from monitor.controller import MonitorFatalError, controller_loop, startup_grace
from monitor.detector import ERROR_PATTERNS
from tests.helpers import make_config

ERROR_LINE = f"12:00:00 | ERROR | {ERROR_PATTERNS[0]}"
HEALTHY_LINE = "12:00:00 | INFO | prefect.server.scheduler - scheduled 0 runs"


def make_queue(*lines: str | None) -> Queue[str | None]:
    """Return a Queue pre-loaded with *lines*."""
    q: Queue[str | None] = Queue()
    for line in lines:
        q.put(line)
    return q


class TestStartupGrace:
    def test_uses_config_grace_seconds_when_no_deadline_given(self) -> None:
        """Without a deadline argument, startup_grace uses config.startup_grace_seconds instead."""
        startup_grace(Queue(), make_config(startup_grace_seconds=0))

    def test_returns_normally_with_past_deadline(self) -> None:
        """startup_grace returns immediately when the grace deadline has already elapsed."""
        startup_grace(Queue(), make_config(), datetime.now(UTC) - timedelta(seconds=1))

    def test_ignores_silence_within_deadline(self) -> None:
        """An empty queue does not raise while the grace deadline is still in the future."""
        startup_grace(Queue(), make_config(), datetime.now(UTC) + timedelta(milliseconds=50))

    def test_cumulative_fail_count_not_reset_by_healthy_line(self) -> None:
        """Healthy lines do not reset the error counter during grace; counting is cumulative."""
        far = datetime.now(UTC) + timedelta(hours=1)
        with pytest.raises(MonitorFatalError, match="max failures"):
            startup_grace(
                make_queue(ERROR_LINE, HEALTHY_LINE, ERROR_LINE, None),
                make_config(max_failures=2),
                far,
            )

    @pytest.mark.parametrize(
        ("queue_lines", "cfg", "match"),
        [
            ([None], make_config(), "stream ended"),
            ([ERROR_LINE, ERROR_LINE], make_config(max_failures=2), "max failures"),
        ],
    )
    def test_raises_on_fatal_input(
        self,
        queue_lines: list[str | None],
        cfg: Config,
        match: str,
    ) -> None:
        """startup_grace raises MonitorFatalError on stream-end sentinel or repeated errors."""
        far = datetime.now(UTC) + timedelta(hours=1)
        with pytest.raises(MonitorFatalError, match=match):
            startup_grace(make_queue(*queue_lines), cfg, far)


class TestControllerLoop:
    @pytest.mark.parametrize(
        ("queue_lines", "cfg", "match"),
        [
            ([], make_config(silence_window=0), "silence timeout"),
            ([None], make_config(), "stream ended"),
            ([ERROR_LINE] * 3, make_config(max_failures=3), "max failures"),
        ],
    )
    def test_raises_on_fatal_condition(
        self,
        queue_lines: list[str | None],
        cfg: Config,
        match: str,
    ) -> None:
        """Raises MonitorFatalError on silence timeout, sentinel, or error-count overflow."""
        with pytest.raises(MonitorFatalError, match=match):
            controller_loop(make_queue(*queue_lines), cfg)

    def test_resets_fail_count_on_healthy_line(self) -> None:
        """A healthy line resets the counter; sub-limit error bursts never trigger max_failures."""
        q = make_queue(ERROR_LINE, ERROR_LINE, HEALTHY_LINE, ERROR_LINE, ERROR_LINE, None)
        with pytest.raises(MonitorFatalError, match="stream ended"):
            controller_loop(q, make_config(max_failures=3))

    def test_healthy_lines_do_not_raise(self) -> None:
        """Healthy lines never increment the failure counter; only the sentinel ends the loop."""
        q = make_queue(*([HEALTHY_LINE] * 10), None)
        with pytest.raises(MonitorFatalError, match="stream ended"):
            controller_loop(q, make_config(max_failures=3))
