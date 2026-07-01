import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import NoReturn

from loguru import logger

from .config import Config
from .detector import is_error_line


class MonitorFatalError(Exception):
    """Raised when the monitor detects an unrecoverable condition.

    Caught at the top-level main() which logs it and exits with code 1,
    triggering a Kubernetes pod restart.
    """


@dataclass
class Controller:
    """Monitor a log stream through two sequential phases: grace then control."""

    stream: AsyncGenerator[str]
    config: Config

    async def startup_grace(self) -> None:
        """Cumulative error counting with silence tolerated; returns when the deadline expires."""
        fail_count = 0
        logger.info("startup grace started ({} seconds)", self.config.startup_grace_seconds)

        try:
            async with asyncio.timeout(self.config.startup_grace_seconds):
                async for line in self.stream:
                    if is_error_line(line):
                        fail_count += 1
                        logger.warning(
                            "error during grace ({}/{})", fail_count, self.config.max_failures
                        )
                        if fail_count >= self.config.max_failures:
                            raise MonitorFatalError("max failures reached during startup grace")
        except TimeoutError:
            pass

        logger.info("startup grace complete")

    async def run(self) -> NoReturn:
        """Consecutive error counting with silence fatal; never returns normally."""
        fail_count = 0
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
                fail_count += 1
                logger.warning(
                    "error detected ({}/{}): {}", fail_count, self.config.max_failures, line
                )
                if fail_count >= self.config.max_failures:
                    raise MonitorFatalError("max failures reached")
            else:
                fail_count = 0
