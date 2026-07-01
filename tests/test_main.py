from typing import cast
from unittest.mock import MagicMock

import pytest
from httpx import Client

from main import main
from monitor.config import Config
from monitor.controller import MonitorFatalError


class TestMain:
    def test_exits_on_missing_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() logs and exits with code 1 when a required env var is absent."""
        monkeypatch.delenv("POD_NAME", raising=False)
        monkeypatch.delenv("POD_NAMESPACE", raising=False)

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 1

    def test_exits_on_monitor_fatal_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() catches MonitorFatalError from the controller and exits with code 1."""

        def raise_fatal(*_: object) -> None:
            raise MonitorFatalError("test fatal error")

        def fake_make_session(_config: Config) -> Client:
            return cast(Client, MagicMock())

        def fake_startup_grace(*_: object) -> None:
            pass

        monkeypatch.setattr("main.Config", MagicMock)
        monkeypatch.setattr("main.make_session", fake_make_session)
        monkeypatch.setattr("main.Thread", MagicMock())
        monkeypatch.setattr("main.startup_grace", fake_startup_grace)
        monkeypatch.setattr("main.controller_loop", raise_fatal)

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 1
