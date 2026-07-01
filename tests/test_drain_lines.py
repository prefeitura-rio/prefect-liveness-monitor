from queue import Queue

from hypothesis import given
from hypothesis import strategies as st

from monitor.producer import drain_lines
from tests.helpers import queue_to_list


class TestDrainLines:
    @given(st.lists(st.text()))
    def test_emitted_lines_are_stripped(self, lines: list[str]) -> None:
        """Every line forwarded to the output queue has no leading or trailing whitespace."""
        out: Queue[str | None] = Queue()
        _ = drain_lines(lines, set(), out)
        emitted = [line for line in queue_to_list(out) if line is not None]
        assert all(line == line.strip() for line in emitted)

    @given(st.lists(st.text()))
    def test_no_intra_batch_duplicates(self, lines: list[str]) -> None:
        """drain_lines never forwards the same line twice within a single call."""
        out: Queue[str | None] = Queue()
        _ = drain_lines(lines, set(), out)
        emitted = [line for line in queue_to_list(out) if line is not None]
        assert len(emitted) == len(set(emitted))

    @given(st.lists(st.text()), st.frozensets(st.integers()))
    def test_pre_seen_lines_excluded(
        self, lines: list[str], pre_seen_frozen: frozenset[int]
    ) -> None:
        """Lines already present in seen are never forwarded, enabling cross-call deduplication."""
        pre_seen = set(pre_seen_frozen)
        snapshot = set(pre_seen)
        out: Queue[str | None] = Queue()
        _ = drain_lines(lines, pre_seen, out)
        emitted = [line for line in queue_to_list(out) if line is not None]
        assert all(hash(line) not in snapshot for line in emitted)

    @given(st.lists(st.text()))
    def test_emitted_hashes_recorded_in_seen(self, lines: list[str]) -> None:
        """Every forwarded line's hash is recorded in seen so future calls can filter it out."""
        seen: set[int] = set()
        out: Queue[str | None] = Queue()
        _ = drain_lines(lines, seen, out)
        emitted = [line for line in queue_to_list(out) if line is not None]
        assert all(hash(line) in seen for line in emitted)

    @given(st.lists(st.text()))
    def test_returns_none_iff_all_blank(self, lines: list[str]) -> None:
        """Returns None iff every input line is blank; otherwise returns a datetime."""
        out: Queue[str | None] = Queue()
        result = drain_lines(lines, set(), out)
        has_non_blank = any(line.strip() for line in lines)
        assert (result is None) == (not has_non_blank)
