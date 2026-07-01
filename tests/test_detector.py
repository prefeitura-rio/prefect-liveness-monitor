import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from monitor.detector import is_error_line

PATTERNS = [
    "docket.strikelist - Error monitoring strikes",
    "docket.strikelist - Connection error",
    "docket.worker - Error sending worker heartbeat",
]


class TestIsErrorLine:
    @pytest.mark.parametrize("pattern", PATTERNS)
    def test_matches_known_pattern(self, pattern: str) -> None:
        """Each pattern is detected when embedded in a realistic log line."""
        line = f"12:00:00.000 | ERROR | {pattern} | extra context"
        assert is_error_line(line, PATTERNS) is True

    @given(st.text())
    def test_no_false_positives(self, line: str) -> None:
        """is_error_line returns False for any text containing none of the patterns."""
        _ = assume(not any(pattern in line for pattern in PATTERNS))
        assert is_error_line(line, PATTERNS) is False

    @given(st.text(), st.sampled_from(PATTERNS), st.text())
    def test_detects_pattern_at_any_position(self, prefix: str, pattern: str, suffix: str) -> None:
        """is_error_line is True when a pattern appears anywhere in the line."""
        assert is_error_line(f"{prefix}{pattern}{suffix}", PATTERNS) is True
