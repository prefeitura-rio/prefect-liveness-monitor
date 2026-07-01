from datetime import UTC, datetime, timedelta
from queue import Empty, Queue
from typing import NoReturn

from loguru import logger

from .config import Config
from .detector import is_error_line


class MonitorFatalError(Exception):
    """Raised when the monitor detects an unrecoverable condition.

    Caught at the top-level main() which logs it and exits with code 1,
    triggering a Kubernetes pod restart.
    """


def startup_grace(
    queue: Queue[str | None],
    config: Config,
    deadline: datetime | None = None,
) -> None:
    """Drain the queue for startup_grace_seconds, enforcing errors but not silence.

    The container may not emit logs immediately after boot. This grace period
    tolerates silence while still catching fatal error patterns.

    Unlike controller_loop, error counting here is cumulative: healthy lines
    do not reset the failure counter. Any max_failures errors during the grace
    period — even non-consecutive — trigger a fatal restart.
    """

    if deadline is None:
        deadline = datetime.now(UTC) + timedelta(seconds=config.startup_grace_seconds)

    fail_count: int = 0

    logger.info("startup grace started ({} seconds)", config.startup_grace_seconds)

    while (now := datetime.now(UTC)) < deadline:
        remaining = (deadline - now).total_seconds()
        try:
            line = queue.get(timeout=max(remaining, 0.01))
        except Empty:
            continue

        if line is None:
            raise MonitorFatalError("stream ended during startup grace")

        if is_error_line(line):
            fail_count += 1
            logger.warning("error during grace ({}/{})", fail_count, config.max_failures)
            if fail_count >= config.max_failures:
                raise MonitorFatalError("max failures reached during startup grace")

    logger.info("startup grace complete")


def controller_loop(
    log_queue: Queue[str | None],
    config: Config,
) -> NoReturn:
    """React to each arriving log line; raise on silence or too many errors.

    Blocks on the queue with a timeout of silence_window seconds. A queue
    timeout means no line has arrived for that long — the container is hung.
    Error lines increment a counter that resets on any healthy line.
    """
    fail_count: int = 0
    logger.info("controller started")

    while True:
        try:
            line = log_queue.get(timeout=float(config.silence_window))
        except Empty:
            raise MonitorFatalError(
                f"silence timeout exceeded ({config.silence_window}s)"
            ) from None

        if line is None:
            raise MonitorFatalError("stream ended unexpectedly")

        if is_error_line(line):
            fail_count += 1
            logger.warning("error detected ({}/{}): {}", fail_count, config.max_failures, line)
            if fail_count >= config.max_failures:
                raise MonitorFatalError("max failures reached")
        else:
            fail_count = 0
