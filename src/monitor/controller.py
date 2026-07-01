import asyncio
from collections.abc import AsyncGenerator
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


async def controller_loop(
    stream: AsyncGenerator[str],
    config: Config,
) -> NoReturn:
    """React to each arriving log line; raise on silence or too many consecutive errors.

    Reschedules the silence deadline on every line received. A TimeoutError
    means no line arrived for silence_window seconds — the container is hung.
    Error lines increment a counter that resets on any healthy line.
    """
    fail_count: int = 0
    logger.info("controller started")

    try:
        async with asyncio.timeout(config.silence_window) as cm:
            async for line in stream:
                cm.reschedule(asyncio.get_running_loop().time() + config.silence_window)
                if is_error_line(line):
                    fail_count += 1
                    logger.warning(
                        "error detected ({}/{}): {}", fail_count, config.max_failures, line
                    )
                    if fail_count >= config.max_failures:
                        raise MonitorFatalError("max failures reached")
                else:
                    fail_count = 0
    except TimeoutError:
        raise MonitorFatalError(f"silence timeout exceeded ({config.silence_window}s)") from None

    raise MonitorFatalError("stream ended unexpectedly")
