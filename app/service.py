# -*- coding: utf-8 -*-
"""Thin service layer tying the correction engine, store and publisher.

Every publication follows the same protocol::

    1. stage a complete new tree        (all files written and fsynced)
    2. atomically rename it into place  (never partially visible)
    3. register it in SQLite, committing the same transaction that holds any
       newly inserted anchors

If step 1 or 2 fails nothing is registered and the previous publication stays
the visible one.  If the commit in step 3 fails, the freshly renamed directory
is unregisterable debris: it is moved out of the namespace immediately and is
swept on the next startup.  The raw readings are never touched by any of this.

A submission that adds anchors is a *single* transaction.  In particular a
rejected anchor (same sequence number, different UTC) rolls back the whole
submission, so the page never shows a mixture of old and new anchors.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from . import core, publish
from .store import Dataset, Publication, Store, now_iso


class ServiceError(Exception):
    pass


class DatasetNotFound(ServiceError):
    pass


class NoPublication(ServiceError):
    pass


@dataclass
class DatasetState:
    """Everything one page render needs, all from one committed snapshot."""

    dataset: Dataset
    result: core.CorrectionResult
    anchors: list[core.AnchorInput]
    latest: Publication | None
    publications: list[Publication]
    # files of ``latest`` (empty when nothing is published yet), read in the
    # same snapshot as everything else
    files: list[sqlite3.Row] = field(default_factory=list)

    @property
    def pending_seqs(self) -> list[int]:
        return self.result.pending_seqs


class Service:
    def __init__(self, db_path: str | Path, export_root: str | Path) -> None:
        self.store = Store(db_path)
        self.export_root = Path(export_root)
        self.export_root.mkdir(parents=True, exist_ok=True)
        (self.export_root / "raw").mkdir(parents=True, exist_ok=True)
        # One publish at a time inside this process.  The deployment runs a
        # single uvicorn worker (see docker-compose.yml) because revision
        # allocation and the rename step rely on this lock.
        self._publish_lock = threading.Lock()
        # Test instrumentation, never set in production: when assigned,
        # ``state()`` invokes this callable after the correction inputs have
        # been read and before the publication rows are read.  Regression
        # tests use it to commit a supplement in the middle of a page read
        # and prove the response still comes from one committed snapshot.
        self.state_read_hook: Callable[[], None] | None = None
        self.recovered = self._recover()

    # ------------------------------------------------------------------
    # startup recovery
    # ------------------------------------------------------------------

    def _recover(self) -> int:
        """Expose only committed publications after a restart.

        Any ``.staging`` directory and any publication directory that no
        committed publication references is debris from an interrupted
        publish.  It was never registered, so it was never served; it is
        deleted before the first request is handled rather than being exposed
        as a half-finished revision.  Registered publications are left exactly
        as they were, so links already handed out keep returning their bytes.
        """
        keep = [
            publish.dir_name(p.dataset_id, p.revision)
            for p in self.store.all_publications()
        ]
        return publish.prune_staging(self.export_root, keep=keep)

    # ------------------------------------------------------------------
    # read side
    # ------------------------------------------------------------------

    def dataset(self, dataset_id: int) -> Dataset:
        ds = self.store.get_dataset(dataset_id)
        if ds is None:
            raise DatasetNotFound(f"批次 {dataset_id} 不存在")
        return ds

    def datasets(self) -> list[Dataset]:
        return self.store.list_datasets()

    def state(self, dataset_id: int) -> DatasetState:
        """Assemble one page render from a single committed snapshot.

        Every query runs inside one SQLite read transaction, so the segment
        correction, the pending list, the anchors, the current publication and
        its file directory all belong to the same committed publication.  A
        supplement that commits while the page is being assembled can only
        shift the whole snapshot to the newer commit — it can never mix the
        old anchors (24 pending readings) with the new publication row
        (pending 0, new day files), which is what separate connections did.
        """
        with self.store.snapshot() as conn:
            ds = self.store.get_dataset(dataset_id, conn)
            if ds is None:
                raise DatasetNotFound(f"批次 {dataset_id} 不存在")
            readings = self.store.readings(dataset_id, conn)
            anchors = self.store.anchors(dataset_id, conn)
            hook = self.state_read_hook
            if hook is not None:
                hook()
            latest = self.store.latest_publication(dataset_id, conn)
            publications = self.store.publications(dataset_id, conn)
            files = (
                self.store.files(latest.id, conn) if latest is not None else []
            )
        result = core.correct(readings, anchors)
        return DatasetState(
            dataset=ds,
            result=result,
            anchors=anchors,
            latest=latest,
            publications=publications,
            files=files,
        )

    # ------------------------------------------------------------------
    # ingest
    # ------------------------------------------------------------------

    def ingest(
        self,
        raw_bytes: bytes,
        readings: list[core.Reading],
        anchors: list[core.AnchorInput],
        name: str,
    ) -> Dataset:
        """Store a new probe file, its anchors and its first publication.

        The original bytes are written to ``raw/`` first (write + fsync, then
        rename), then everything else happens in one transaction.  A failure
        rolls the dataset back rather than leaving a batch that can never be
        downloaded.
        """
        raw_dir = self.export_root / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(raw_bytes).hexdigest()
        tmp = raw_dir / f".upload-{uuid.uuid4().hex}"
        publish.write_fsync(tmp, raw_bytes)
        os.replace(tmp, raw_dir / f"{digest}.csv")

        created = now_iso()
        with self._publish_lock:
            with self.store.transaction() as conn:
                ds = self.store.create_dataset(
                    conn,
                    name,
                    readings,
                    f"raw/{digest}.csv",
                    digest,
                    created_at=created,
                )
                self.store.insert_anchors(conn, ds.id, anchors, created_at=created)
                self._publish_in(conn, ds.id, created_at=created)
        return ds

    def add_anchors_and_publish(
        self, dataset_id: int, anchors: list[core.AnchorInput]
    ) -> tuple[Publication, list[core.AnchorInput]]:
        """Add anchors and publish a new snapshot, atomically.

        A :class:`core.AnchorConflict` (same sequence number, different UTC)
        aborts the whole submission: no anchor is written, no directory is
        published, and the previous revision remains the visible one.
        """
        self.dataset(dataset_id)
        with self._publish_lock:
            with self.store.transaction() as conn:
                added = self.store.insert_anchors(conn, dataset_id, anchors)
                publication = self._publish_in(conn, dataset_id)
        return publication, added

    def republish(self, dataset_id: int) -> Publication:
        """Re-derive and publish again without changing any anchor."""
        self.dataset(dataset_id)
        with self._publish_lock:
            with self.store.transaction() as conn:
                return self._publish_in(conn, dataset_id)

    # ------------------------------------------------------------------
    # publish internals
    # ------------------------------------------------------------------

    def _publish_in(
        self, conn, dataset_id: int, created_at: str | None = None
    ) -> Publication:
        """Stage, rename and register one publication inside ``conn``.

        The caller owns the transaction.  Any exception leaves the tree
        unregistered and the directory either staged (still under
        ``.staging``) or quarantined, never servable.
        """
        readings = self.store.readings(dataset_id, conn)
        anchors = self.store.anchors(dataset_id, conn)
        result = core.correct(readings, anchors)
        result.generated_at = datetime.now(timezone.utc)
        latest = self.store.latest_publication(dataset_id, conn)
        revision = (latest.revision + 1) if latest else 1
        staged = publish.build_staging(
            self.export_root, dataset_id, revision, result, anchors
        )
        try:
            publish.commit_staging(staged)
        except Exception:
            publish.discard_staging(staged)
            raise
        try:
            return self.store.insert_publication(
                conn,
                dataset_id,
                revision,
                [(f.kind, f.day, f.relpath, f.sha256, f.size) for f in staged.files],
                staged.counts,
                created_at=created_at,
            )
        except Exception:
            # never leave an unregistered directory that looks publishable
            self._quarantine(staged.final_dir)
            raise

    def _quarantine(self, final_dir: Path) -> None:
        try:
            target = self.export_root / ".staging" / f"orphan-{uuid.uuid4().hex}"
            target.parent.mkdir(parents=True, exist_ok=True)
            final_dir.rename(target)
        except OSError:
            pass

    # ------------------------------------------------------------------
    # downloads
    # ------------------------------------------------------------------

    def publication_files(self, publication_id: int):
        with self.store.snapshot() as conn:
            publication = self.store.get_publication(publication_id, conn)
            if publication is None:
                raise NoPublication(f"发布版本 {publication_id} 不存在")
            return publication, self.store.files(publication_id, conn)

    def publication_days(self, publication_id: int) -> list[str]:
        return [
            row["day"]
            for row in self.store.files(publication_id)
            if row["kind"] == "day" and row["day"]
        ]

    def file_path(self, publication_id: int, kind: str, day: str | None):
        """Resolve a registered file to an absolute path.

        Resolution goes through the database, so only files registered for a
        committed publication are reachable.  The recorded SHA-256 must still
        match the bytes on disk, which turns a truncated copy into a clear
        error rather than a silently corrupt download.

        Publication rows are immutable once committed, but the two lookups
        still share one read snapshot so a resolved file can never come from
        a different commit than its publication.
        """
        with self.store.snapshot() as conn:
            publication = self.store.get_publication(publication_id, conn)
            if publication is None:
                raise NoPublication(f"发布版本 {publication_id} 不存在")
            row = self.store.get_file(publication_id, kind, day, conn)
            if row is None:
                raise NoPublication("该版本中没有对应的文件")
        path = publish.resolve(
            self.export_root,
            publication.dataset_id,
            publication.revision,
            row["relpath"],
        )
        if not path.is_file():
            raise NoPublication("发布文件缺失")
        if not publish.verify(path, row["sha256"]):
            raise NoPublication("发布文件校验失败")
        return publication, row, path
