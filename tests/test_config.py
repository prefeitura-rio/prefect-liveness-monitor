import pytest

from monitor.config import Config
from tests.helpers import make_config


class TestConfig:
    def test_log_url_built_correctly(self) -> None:
        """log_url assembles k8s_api, pod_namespace, and pod_name into the K8s pod log endpoint."""
        assert make_config().log_url == (
            "https://k8s.test/api/v1/namespaces/test-ns/pods/test-pod/log"
        )

    @pytest.mark.parametrize(
        ("cfg", "field", "expected"),
        [
            (make_config(silence_window=42), "silence_window", 42),
            (make_config(max_failures=7), "max_failures", 7),
            (make_config(startup_grace_seconds=10), "startup_grace_seconds", 10),
            (make_config(stream_read_timeout=30), "stream_read_timeout", 30),
            (make_config(k8s_api="https://custom.api"), "k8s_api", "https://custom.api"),
        ],
    )
    def test_field_accepts_override(self, cfg: Config, field: str, expected: object) -> None:
        """Each Config field stores the exact value it was constructed with."""
        assert getattr(cfg, field) == expected
