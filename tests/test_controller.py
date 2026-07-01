import pytest

from monitor.config import Config
from monitor.controller import (
    Controller,
    ControlStrategy,
    MaxFailuresError,
    SilenceTimeoutError,
    StartupStrategy,
    StreamEndedError,
)
from tests.helpers import blocking_stream, make_config, make_stream

ERROR_LINE = "12:00:00 | ERROR | docket.strikelist - Error monitoring strikes"
HEALTHY_LINE = "12:00:00 | INFO | prefect.server.scheduler - scheduled 0 runs"


class TestStartupStrategy:
    async def test_returns_normally_when_grace_expires(self) -> None:
        """Returns without error when the timeout fires with no lines."""
        await Controller(blocking_stream(), make_config(startup_grace_seconds=0)).run(
            StartupStrategy(make_config(startup_grace_seconds=0))
        )

    async def test_ignores_silence_within_deadline(self) -> None:
        """An empty stream does not raise — silence is tolerated during grace."""
        await Controller(make_stream(), make_config(startup_grace_seconds=60)).run(
            StartupStrategy(make_config(startup_grace_seconds=60))
        )

    async def test_cumulative_fail_count_not_reset_by_healthy_line(self) -> None:
        """Healthy lines do not reset the error counter; counting is cumulative."""
        with pytest.raises(MaxFailuresError):
            await Controller(
                make_stream(ERROR_LINE, HEALTHY_LINE, ERROR_LINE),
                make_config(max_failures=2, startup_grace_seconds=60),
            ).run(StartupStrategy(make_config(max_failures=2, startup_grace_seconds=60)))

    @pytest.mark.parametrize(
        ("lines", "cfg"),
        [([ERROR_LINE, ERROR_LINE], make_config(max_failures=2, startup_grace_seconds=60))],
    )
    async def test_raises_on_max_failures(self, lines: list[str], cfg: Config) -> None:
        """Raises MaxFailuresError when cumulative errors reach max_failures."""
        with pytest.raises(MaxFailuresError):
            await Controller(make_stream(*lines), cfg).run(StartupStrategy(cfg))


class TestControlStrategy:
    async def test_raises_on_silence_timeout(self) -> None:
        """Raises SilenceTimeoutError when no line arrives within silence_window seconds."""
        with pytest.raises(SilenceTimeoutError):
            await Controller(blocking_stream(), make_config(silence_window=0)).run(
                ControlStrategy(make_config(silence_window=0))
            )

    async def test_raises_when_stream_ends(self) -> None:
        """Raises StreamEndedError when the async generator is exhausted."""
        with pytest.raises(StreamEndedError):
            await Controller(make_stream(), make_config()).run(ControlStrategy(make_config()))

    async def test_raises_on_max_failures(self) -> None:
        """Raises MaxFailuresError after max_failures consecutive error lines."""
        cfg = make_config(max_failures=3)
        with pytest.raises(MaxFailuresError):
            await Controller(make_stream(*([ERROR_LINE] * 3)), cfg).run(ControlStrategy(cfg))

    async def test_resets_fail_count_on_healthy_line(self) -> None:
        """A healthy line resets the counter; sub-limit bursts never trigger MaxFailuresError."""
        cfg = make_config(max_failures=3)
        with pytest.raises(StreamEndedError):
            await Controller(
                make_stream(ERROR_LINE, ERROR_LINE, HEALTHY_LINE, ERROR_LINE, ERROR_LINE),
                cfg,
            ).run(ControlStrategy(cfg))

    async def test_healthy_lines_do_not_raise(self) -> None:
        """Healthy lines never increment the failure counter."""
        cfg = make_config(max_failures=3)
        with pytest.raises(StreamEndedError):
            await Controller(make_stream(*([HEALTHY_LINE] * 10)), cfg).run(ControlStrategy(cfg))
