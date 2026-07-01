from unittest.mock import AsyncMock, MagicMock

import pytest

from main import main
from monitor.controller import MaxFailuresError
from tests.helpers import make_config, make_stream


class TestMain:
    async def test_exits_on_monitor_fatal_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() catches MonitorFatalError from the controller and exits with code 1."""

        async def raise_fatal(*_: object) -> None:
            raise MaxFailuresError

        monkeypatch.setattr("main.Config", MagicMock(return_value=make_config()))
        monkeypatch.setattr("main.make_session", MagicMock(return_value=AsyncMock()))
        monkeypatch.setattr("main.stream_logs", MagicMock(return_value=make_stream()))
        monkeypatch.setattr("monitor.controller.Controller.run", raise_fatal)

        with pytest.raises(SystemExit) as exc_info:
            await main()

        assert exc_info.value.code == 1
