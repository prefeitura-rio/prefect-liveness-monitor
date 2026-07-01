import asyncio
from sys import exit

from loguru import logger

from monitor.config import Config
from monitor.controller import Controller, ControlStrategy, MonitorFatalError, StartupStrategy
from monitor.http import make_session
from monitor.producer import stream_logs


async def main() -> None:
    """Entry point: stream K8s logs and restart the pod on fatal conditions."""
    config = Config()

    async with make_session(config) as session:
        stream = stream_logs(session, config)
        logger.info("liveness monitor started")

        try:
            controller = Controller(stream, config)
            await controller.run(StartupStrategy(config))
            await controller.run(ControlStrategy(config))
        except MonitorFatalError as exc:
            logger.critical("{}", exc)
            exit(1)


if __name__ == "__main__":
    asyncio.run(main())
