ERROR_PATTERNS: tuple[str, ...] = (
    "docket.strikelist - Error monitoring strikes",
    "docket.strikelist - Connection error",
    "docket.worker - Error sending worker heartbeat",
)


def is_error_line(line: str) -> bool:
    """Return True if *line* matches any known docket error pattern."""
    return any(pattern in line for pattern in ERROR_PATTERNS)
