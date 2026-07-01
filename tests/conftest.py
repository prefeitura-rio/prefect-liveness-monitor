"""Pytest fixtures shared across all test modules."""

import os
from collections.abc import Generator
from functools import partial

import pytest
from hypothesis import HealthCheck, settings
from loguru import logger
from stamina import retry_context

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
def instant_stamina(monkeypatch: pytest.MonkeyPatch) -> None:
    """Zero out all stamina wait times so retry tests run instantly."""
    monkeypatch.setattr(
        "monitor.producer.retry_context",
        partial(retry_context, wait_initial=0.0, wait_max=0.0, wait_jitter=0.0),
    )
