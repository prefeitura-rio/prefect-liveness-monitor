def is_error_line(line: str, patterns: list[str]) -> bool:
    """Return True if *line* matches any of the configured error patterns."""
    return any(pattern in line for pattern in patterns)
