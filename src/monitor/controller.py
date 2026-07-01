import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Protocol

from loguru import logger

from .config import Config
from .detector import is_error_line


class MonitorFatalError(Exception):
    """Raised when the monitor detects an unrecoverable condition.

    Caught at the top-level main() which logs it and exits with code 1,
    triggering a Kubernetes pod restart.
    """


class Strategy(Protocol):
    """Contract for per-iteration behaviour inside the monitoring loop."""

    timeout: float

    async def on_line(self, line: str) -> None: ...
    async def on_timeout(self) -> None: ...
    async def on_stream_end(self) -> None: ...


@dataclass
class StartupStrategy:
    """Cumulative error counting; silence and stream exhaustion are tolerated."""

    config: Config
    timeout: float = field(init=False)
    fail_count: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self.timeout = float(self.config.startup_grace_seconds)

    async def on_line(self, line: str) -> None:
        if not is_error_line(line):
            return
        self.fail_count += 1
        logger.warning("error during grace ({}/{})", self.fail_count, self.config.max_failures)
        if self.fail_count >= self.config.max_failures:
            raise MonitorFatalError("max failures reached during startup grace")

    async def on_timeout(self) -> None:
        pass

    async def on_stream_end(self) -> None:
        pass


@dataclass
class ControlStrategy:
    """Consecutive error counting; silence and stream exhaustion are fatal."""

    config: Config
    timeout: float = field(init=False)
    fail_count: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self.timeout = float(self.config.silence_window)

    async def on_line(self, line: str) -> None:
        if is_error_line(line):
            self.fail_count += 1
            logger.warning(
                "error detected ({}/{}): {}", self.fail_count, self.config.max_failures, line
            )
            if self.fail_count >= self.config.max_failures:
                raise MonitorFatalError("max failures reached")
        else:
            self.fail_count = 0

    async def on_timeout(self) -> None:
        raise MonitorFatalError(f"silence timeout exceeded ({self.config.silence_window}s)")

    async def on_stream_end(self) -> None:
        raise MonitorFatalError("stream ended unexpectedly")


@dataclass
class Controller:
    """Drive a log stream through sequential monitoring phases."""

    stream: AsyncGenerator[str]
    config: Config

    async def run(self, strategy: Strategy) -> None:
        """Apply strategy to each incoming line; delegate timeout and stream-end handling."""
        while True:
            try:
                line = await asyncio.wait_for(anext(self.stream), timeout=strategy.timeout)
            except StopAsyncIteration:
                await strategy.on_stream_end()
                return
            except TimeoutError:
                await strategy.on_timeout()
                return
            await strategy.on_line(line)
