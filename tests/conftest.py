"""Pytest fixtures shared across all test modules."""

import os
from collections.abc import Generator
from unittest.mock import AsyncMock

import pytest
from hypothesis import HealthCheck, settings
from loguru import logger

settings.register_profile("ci", max_examples=500, suppress_health_check=[HealthCheck.too_slow])
settings.register_profile("default", max_examples=100)
settings.load_profile("ci" if os.getenv("CI") else "default")


@pytest.fixture(autouse=True)
def suppress_loguru() -> Generator[None]:
    """Silence monitor loguru output during tests."""
    logger.disable("monitor")
    yield
    logger.enable("monitor")


@pytest.fixture(autouse=True)
def instant_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch asyncio.sleep in producer so reconnect backoffs complete instantly."""
    monkeypatch.setattr("monitor.producer.asyncio.sleep", AsyncMock())
