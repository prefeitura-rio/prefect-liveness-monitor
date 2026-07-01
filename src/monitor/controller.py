import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import NoReturn

from loguru import logger

from .config import Config
from .detector import is_error_line


class MonitorFatalError(Exception):
    """Raised when the monitor detects an unrecoverable condition.

    Caught at the top-level main() which logs it and exits with code 1,
    triggering a Kubernetes pod restart.
    """


async def startup_grace(
    stream: AsyncGenerator[str],
    config: Config,
) -> None:
    """Drain the stream for startup_grace_seconds, enforcing errors but not silence.

    The container may not emit logs immediately after boot. This grace period
    tolerates silence while still catching fatal error patterns.

    Unlike controller_loop, error counting here is cumulative: healthy lines
    do not reset the failure counter. Any max_failures errors during the grace
    period — even non-consecutive — trigger a fatal restart.
    """
    fail_count: int = 0
    logger.info("startup grace started ({} seconds)", config.startup_grace_seconds)

    try:
        async with asyncio.timeout(config.startup_grace_seconds):
            async for line in stream:
                if is_error_line(line):
                    fail_count += 1
                    logger.warning("error during grace ({}/{})", fail_count, config.max_failures)
                    if fail_count >= config.max_failures:
                        raise MonitorFatalError("max failures reached during startup grace")
    except TimeoutError:
        pass  # grace period expired normally — silence during startup is expected

    logger.info("startup grace complete")


@dataclass
class Controller:
    """React to each arriving log line; raise on silence or too many consecutive errors."""

    stream: AsyncGenerator[str]
    config: Config
    fail_count: int = field(default=0, init=False)

    async def run(self) -> NoReturn:
        """Start the monitoring loop."""
        logger.info("controller started")

        while True:
            try:
                line = await asyncio.wait_for(
                    anext(self.stream), timeout=self.config.silence_window
                )
            except StopAsyncIteration:
                raise MonitorFatalError("stream ended unexpectedly") from None
            except TimeoutError:
                raise MonitorFatalError(
                    f"silence timeout exceeded ({self.config.silence_window}s)"
                ) from None

            if is_error_line(line):
                self.fail_count += 1
                logger.warning(
                    "error detected ({}/{}): {}",
                    self.fail_count,
                    self.config.max_failures,
                    line,
                )
                if self.fail_count >= self.config.max_failures:
                    raise MonitorFatalError("max failures reached")
            else:
                self.fail_count = 0
