from pathlib import Path

import pytest
from httpx import Timeout

from monitor.http import make_session, read_token
from tests.helpers import make_config


class TestMakeSession:
    def test_returns_async_client(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """make_session returns an AsyncClient instance."""
        captured: list[Timeout] = []

        class FakeAsyncClient:
            def __init__(self, verify: str, timeout: Timeout) -> None:
                captured.append(timeout)

        monkeypatch.setattr("monitor.http.AsyncClient", FakeAsyncClient)
        _ = make_session(make_config(stream_read_timeout=60))

        assert captured[0].read == 60.0


class TestReadToken:
    def test_reads_token_from_serviceaccount(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """read_token reads the bearer token from the SERVICEACCOUNT/token file."""
        _ = (tmp_path / "token").write_text("my-bearer-token")
        monkeypatch.setattr("monitor.http.SERVICEACCOUNT", tmp_path)
        assert read_token() == "my-bearer-token"
