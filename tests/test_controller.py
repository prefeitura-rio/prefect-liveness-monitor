import pytest

from monitor.config import Config
from monitor.controller import Controller, MonitorFatalError
from monitor.detector import ERROR_PATTERNS
from tests.helpers import blocking_stream, make_config, make_stream

ERROR_LINE = f"12:00:00 | ERROR | {ERROR_PATTERNS[0]}"
HEALTHY_LINE = "12:00:00 | INFO | prefect.server.scheduler - scheduled 0 runs"


class TestStartupGrace:
    async def test_returns_normally_when_grace_expires(self) -> None:
        """startup_grace returns without error when the timeout fires with no lines."""
        await Controller(blocking_stream(), make_config(startup_grace_seconds=0)).startup_grace()

    async def test_ignores_silence_within_deadline(self) -> None:
        """An empty stream does not raise — silence is tolerated during grace."""
        await Controller(make_stream(), make_config(startup_grace_seconds=60)).startup_grace()

    async def test_cumulative_fail_count_not_reset_by_healthy_line(self) -> None:
        """Healthy lines do not reset the error counter during grace; counting is cumulative."""
        with pytest.raises(MonitorFatalError, match="max failures"):
            await Controller(
                make_stream(ERROR_LINE, HEALTHY_LINE, ERROR_LINE),
                make_config(max_failures=2, startup_grace_seconds=60),
            ).startup_grace()

    @pytest.mark.parametrize(
        ("lines", "cfg", "match"),
        [
            (
                [ERROR_LINE, ERROR_LINE],
                make_config(max_failures=2, startup_grace_seconds=60),
                "max failures",
            ),
        ],
    )
    async def test_raises_on_max_failures(
        self,
        lines: list[str],
        cfg: Config,
        match: str,
    ) -> None:
        """startup_grace raises MonitorFatalError when cumulative errors reach max_failures."""
        with pytest.raises(MonitorFatalError, match=match):
            await Controller(make_stream(*lines), cfg).startup_grace()  # type: ignore[arg-type]


class TestController:
    async def test_raises_on_silence_timeout(self) -> None:
        """run() raises when no line arrives within silence_window seconds."""
        with pytest.raises(MonitorFatalError, match="silence timeout"):
            await Controller(blocking_stream(), make_config(silence_window=0)).run()

    async def test_raises_when_stream_ends(self) -> None:
        """run() raises when the async generator is exhausted."""
        with pytest.raises(MonitorFatalError, match="stream ended"):
            await Controller(make_stream(), make_config()).run()

    async def test_raises_on_max_failures(self) -> None:
        """run() raises after max_failures consecutive error lines."""
        with pytest.raises(MonitorFatalError, match="max failures"):
            await Controller(make_stream(*([ERROR_LINE] * 3)), make_config(max_failures=3)).run()

    async def test_resets_fail_count_on_healthy_line(self) -> None:
        """A healthy line resets the counter; sub-limit bursts never trigger max_failures."""
        with pytest.raises(MonitorFatalError, match="stream ended"):
            await Controller(
                make_stream(ERROR_LINE, ERROR_LINE, HEALTHY_LINE, ERROR_LINE, ERROR_LINE),
                make_config(max_failures=3),
            ).run()

    async def test_healthy_lines_do_not_raise(self) -> None:
        """Healthy lines never increment the failure counter."""
        with pytest.raises(MonitorFatalError, match="stream ended"):
            await Controller(make_stream(*([HEALTHY_LINE] * 10)), make_config(max_failures=3)).run()
