from pathlib import Path

from httpx import Client, Timeout

from .config import Config

SERVICEACCOUNT = Path("/var/run/secrets/kubernetes.io/serviceaccount")


def make_session(config: Config) -> Client:
    """Build an httpx client pinned to the pod CA cert with a streaming timeout."""
    return Client(
        verify=str(SERVICEACCOUNT / "ca.crt"),
        timeout=Timeout(
            connect=10.0,
            read=float(config.stream_read_timeout),
            write=None,
            pool=None,
        ),
    )


def read_token() -> str:
    """Read the current service-account bearer token (rotated automatically by K8s)."""
    return (SERVICEACCOUNT / "token").read_text()
