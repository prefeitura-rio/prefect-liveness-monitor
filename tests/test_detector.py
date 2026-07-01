import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from monitor.detector import ERROR_PATTERNS, is_error_line


class TestIsErrorLine:
    @pytest.mark.parametrize("pattern", ERROR_PATTERNS)
    def test_matches_known_pattern(self, pattern: str) -> None:
        """Each string in ERROR_PATTERNS is detected when embedded in a realistic log line."""
        line = f"12:00:00.000 | ERROR | {pattern} | extra context"
        assert is_error_line(line) is True

    @given(st.text())
    def test_no_false_positives(self, line: str) -> None:
        """is_error_line returns False for any text containing none of the known error patterns."""
        _ = assume(not any(pattern in line for pattern in ERROR_PATTERNS))
        assert is_error_line(line) is False

    @given(st.text(), st.sampled_from(ERROR_PATTERNS), st.text())
    def test_detects_pattern_at_any_position(self, prefix: str, pattern: str, suffix: str) -> None:
        """is_error_line is True when a known pattern appears anywhere in the line."""
        assert is_error_line(f"{prefix}{pattern}{suffix}") is True
