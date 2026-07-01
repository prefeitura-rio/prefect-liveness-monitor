from pydantic import computed_field
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    """Runtime configuration sourced entirely from environment variables."""

    pod_name: str = ""
    pod_namespace: str = "prefect-server"
    silence_window: int = 1800
    max_failures: int = 3
    startup_grace_seconds: int = 90
    stream_read_timeout: int = 120
    k8s_api: str = "https://kubernetes.default.svc"
    error_patterns: list[str] = [
        "docket.strikelist - Error monitoring strikes",
        "docket.strikelist - Connection error",
        "docket.worker - Error sending worker heartbeat",
    ]

    @computed_field
    @property
    def log_url(self) -> str:
        return f"{self.k8s_api}/api/v1/namespaces/{self.pod_namespace}/pods/{self.pod_name}/log"
