from queue import Queue
from sys import exit
from threading import Thread

from loguru import logger
from pydantic import ValidationError

from monitor.config import Config
from monitor.controller import MonitorFatalError, controller_loop, startup_grace
from monitor.http import make_session
from monitor.producer import stream_producer


def main() -> None:
    try:
        config = Config()  # pyright: ignore[reportCallIssue]  # fields injected from env vars
    except ValidationError as exc:
        logger.critical("invalid configuration:\n{}", exc)
        exit(1)

    session = make_session(config)
    log_queue: Queue[str | None] = Queue()

    Thread(
        target=stream_producer,
        args=(session, config, log_queue),
        daemon=True,
        name="log-stream-producer",
    ).start()

    logger.info("liveness monitor started")

    try:
        startup_grace(log_queue, config)
        controller_loop(log_queue, config)
    except MonitorFatalError as exc:
        logger.critical("{}", exc)
        exit(1)


if __name__ == "__main__":
    main()
