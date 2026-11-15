# -*- coding: utf-8 -*-
"""Regression tests for the concurrent read defect.

Scenario under test: the data clerk supplements anchors (one transaction that
inserts the anchors and registers a new publication) while a colleague
refreshes the batch detail page.  The page render must never mix the two
committed states — e.g. the old version's 24 pending sequence numbers next to
the new version's "pending 0" day-file links.  Either the complete old
version or the complete new version is acceptable.

The deterministic tests use ``Service.state_read_hook`` to pause a page read
*after* the correction inputs have been read and *before* the publication
rows are read, and commit the supplement exactly in that window — the
interleaving that produced the mixed page.  With per-query connections (the
old code) these tests fail; with the snapshot read they pass.
"""

from __future__ import annotations

import threading
import time

import pytest

from app import core, demo, publish
from app.service import NoPublication, Service

SUPPLEMENT = demo.SUPPLEMENT_ANCHORS_TEXT  # dates seqs 13..36 -> pending 0
INITIAL_PENDING = 24  # seqs 13..36 pending in revision 1


def _supplement_anchors() -> list[core.AnchorInput]:
    return core.parse_anchors(SUPPLEMENT.encode("utf-8"))


def _assert_complete_old_version(state) -> None:
    """Everything revision 1: 2 anchors, 24 pending, only rev-1 files."""
    assert state.latest is not None
    assert state.latest.revision == 1
    assert len(state.anchors) == 2
    assert state.latest.anchor_count == 2
    assert len(state.result.pending) == INITIAL_PENDING
    assert state.latest.pending_count == INITIAL_PENDING
    assert len(state.result.rows) == state.latest.dated_count == 12
    assert state.publications[0].id == state.latest.id
    assert state.files, "revision 1 must have a file directory"
    assert all(f["publication_id"] == state.latest.id for f in state.files)
    assert {f["day"] for f in state.files if f["kind"] == "day"} == {"2026-03-01"}


def _assert_complete_new_version(state, revision: int = 2) -> None:
    """Everything revision 2: 4 anchors, 0 pending, both day files."""
    assert state.latest is not None
    assert state.latest.revision == revision
    assert len(state.anchors) == 4
    assert state.latest.anchor_count == 4
    assert len(state.result.pending) == 0
    assert state.latest.pending_count == 0
    assert len(state.result.rows) == state.latest.dated_count == 36
    assert state.publications[0].id == state.latest.id
    assert all(f["publication_id"] == state.latest.id for f in state.files)
    assert {f["day"] for f in state.files if f["kind"] == "day"} == {
        "2026-03-01",
        "2026-03-02",
    }


def test_refresh_interrupted_by_supplement_sees_complete_old_version(
    service: Service, seeded
) -> None:
    """A supplement commits in the middle of a page read.

    The read started before the commit, so it must render the complete old
    version — never the old anchors together with the new publication row.
    """
    ds = seeded
    entered = threading.Event()
    release = threading.Event()

    def hook() -> None:
        service.state_read_hook = None  # fire once, for this read only
        entered.set()
        assert release.wait(10), "test orchestration broken: never released"

    service.state_read_hook = hook

    outcome: dict = {}

    def refresh() -> None:
        outcome["state"] = service.state(ds.id)

    reader = threading.Thread(target=refresh)
    reader.start()
    try:
        # The page read is now paused between reading the anchors and reading
        # the publication rows.  The clerk's supplement commits right here.
        assert entered.wait(10), "page read never reached the hook"
        publication, added = service.add_anchors_and_publish(
            ds.id, _supplement_anchors()
        )
        assert publication.revision == 2
        assert len(added) == 2
    finally:
        release.set()
        reader.join(10)
    assert service.state_read_hook is None

    # The interrupted read belongs entirely to the pre-supplement commit.
    _assert_complete_old_version(outcome["state"])


def test_refresh_after_supplement_sees_complete_new_version(
    service: Service, seeded
) -> None:
    """A read that starts after the commit renders the complete new version."""
    ds = seeded
    service.add_anchors_and_publish(ds.id, _supplement_anchors())
    _assert_complete_new_version(service.state(ds.id), revision=2)


def test_concurrent_supplements_and_refreshes_never_mix_versions(
    service: Service, seeded
) -> None:
    """Hammer the read path while supplements land; every read is consistent.

    Each ``state()`` result must satisfy the cross-entity invariants that the
    mixed page violated: the recomputed correction (pending/dated counts and
    the anchor list) must agree with the publication row and its file
    directory returned in the same response.
    """
    ds = seeded

    # Six conflict-free supplements, each producing one new revision.
    batches = [
        "13,2026-03-01T22:00:00Z\n36,2026-03-02T01:55:00Z",
        "24,2026-03-02T00:05:00Z",
        "18,2026-03-01T22:50:00Z",
        "30,2026-03-02T00:50:00Z",
        "20,2026-03-01T23:10:00Z",
        "33,2026-03-02T01:20:00Z",
    ]

    stop = threading.Event()
    failures: list[str] = []
    seen_revisions: set[int] = set()
    seen_lock = threading.Lock()

    def check(state) -> None:
        latest = state.latest
        assert latest is not None, "no publication while supplements run"
        problems = []
        if len(state.anchors) != latest.anchor_count:
            problems.append(
                f"anchors {len(state.anchors)} != publication.anchor_count "
                f"{latest.anchor_count}"
            )
        if len(state.result.pending) != latest.pending_count:
            problems.append(
                f"pending {len(state.result.pending)} != "
                f"publication.pending_count {latest.pending_count}"
            )
        if len(state.result.rows) != latest.dated_count:
            problems.append(
                f"dated {len(state.result.rows)} != "
                f"publication.dated_count {latest.dated_count}"
            )
        if not state.publications or state.publications[0].id != latest.id:
            problems.append("publication history head != latest publication")
        if latest.revision != len(state.publications):
            problems.append(
                f"revision {latest.revision} but {len(state.publications)} "
                "publications in history"
            )
        if not state.files:
            problems.append("latest publication has no file directory")
        if any(f["publication_id"] != latest.id for f in state.files):
            problems.append("file directory does not belong to latest")
        if problems:
            raise AssertionError(
                f"mixed state at revision {latest.revision}: "
                + "; ".join(problems)
            )

    def refresh_loop() -> None:
        try:
            while not stop.is_set():
                state = service.state(ds.id)
                check(state)
                with seen_lock:
                    seen_revisions.add(state.latest.revision)
        except Exception as exc:  # keep the first failure per reader
            failures.append(f"{type(exc).__name__}: {exc}")
            stop.set()

    readers = [threading.Thread(target=refresh_loop) for _ in range(4)]
    for thread in readers:
        thread.start()
    # make sure the readers are actually producing states before the writes
    deadline = time.time() + 10
    while not seen_revisions and time.time() < deadline:
        time.sleep(0.001)
    assert seen_revisions, "readers never produced a state"

    for batch in batches:
        anchors = core.parse_anchors(("序号,UTC\n" + batch + "\n").encode("utf-8"))
        service.add_anchors_and_publish(ds.id, anchors)
    stop.set()
    for thread in readers:
        thread.join(10)

    assert not failures, f"inconsistent page reads observed: {failures[:3]}"
    # the readers genuinely raced the supplements: they saw more than one
    # committed revision while running
    assert len(seen_revisions) >= 2, f"readers never raced a commit: {seen_revisions}"
    # and the final state is the complete last version
    final = service.state(ds.id)
    assert final.latest.revision == 1 + len(batches)
    assert len(final.anchors) == 2 + sum(len(b.splitlines()) for b in batches)
    assert len(final.result.pending) == 0


def test_rejected_supplement_keeps_old_version(service: Service, seeded) -> None:
    """A conflicting anchor aborts the whole submission: old version stays."""
    ds = seeded
    before = service.state(ds.id)
    conflict = core.parse_anchors("序号,UTC\n12,2026-03-01T23:59:00Z\n".encode())
    with pytest.raises(core.AnchorConflict):
        service.add_anchors_and_publish(ds.id, conflict)
    after = service.state(ds.id)
    assert after.latest.revision == before.latest.revision == 1
    assert [a.seq for a in after.anchors] == [a.seq for a in before.anchors]
    assert after.result.pending_seqs == before.result.pending_seqs


def test_failed_publish_rolls_back_anchors_and_keeps_old_version(
    service: Service, seeded, monkeypatch
) -> None:
    """A failure while staging the new tree rolls anchors back with it."""
    ds = seeded
    before = service.state(ds.id)

    def boom(*args, **kwargs):
        raise publish.PublishError("磁盘已满（模拟）")

    monkeypatch.setattr(publish, "build_staging", boom)
    with pytest.raises(publish.PublishError):
        service.add_anchors_and_publish(ds.id, _supplement_anchors())

    after = service.state(ds.id)
    assert after.latest.revision == before.latest.revision == 1
    assert [a.seq for a in after.anchors] == [a.seq for a in before.anchors]
    assert after.result.pending_seqs == before.result.pending_seqs
    assert len(after.publications) == 1


def test_old_publication_links_keep_original_bytes(service: Service, seeded) -> None:
    """After a supplement, revision-1 downloads still return revision-1 bytes."""
    ds = seeded
    pub1 = service.store.latest_publication(ds.id)
    _, row_before, path_before = service.file_path(pub1.id, "pending", None)
    bytes_before = path_before.read_bytes()
    assert b"\r\n13," in bytes_before  # seq 13 is pending in revision 1

    pub2, _ = service.add_anchors_and_publish(ds.id, _supplement_anchors())
    assert pub2.revision == 2

    # the old link resolves to the same registered row and the same bytes
    _, row_after, path_after = service.file_path(pub1.id, "pending", None)
    assert row_after["sha256"] == row_before["sha256"]
    assert path_after.read_bytes() == bytes_before

    # revision 1 still has no 2026-03-02 day file; revision 2 does
    with pytest.raises(NoPublication):
        service.file_path(pub1.id, "day", "2026-03-02")
    _, _, new_day = service.file_path(pub2.id, "day", "2026-03-02")
    assert new_day.is_file()

    # the new pending.csv carries zero pending rows (header only)
    _, _, new_pending = service.file_path(pub2.id, "pending", None)
    lines = [ln for ln in new_pending.read_text().splitlines() if ln.strip()]
    assert lines == [",".join(core.PENDING_HEADER)]
