# -*- coding: utf-8 -*-
"""SQLite persistence.

What is stored
--------------
``dataset``
    one row per uploaded probe file, with the original CSV kept byte-for-byte
    in ``raw/`` and its SHA-256 recorded.  The raw file is written once and
    never rewritten.
``reading``
    the *raw* readings exactly as parsed from the probe CSV (device time and
    dissolved oxygen).  Nothing in this table is ever updated by the
    correction logic; corrected UTC values exist only in the published export.
``anchor``
    the 核时锚点 rows keyed by ``(dataset_id, seq)``.  A sequence number can
    only ever have one UTC value per dataset.
``publication`` / ``published_file``
    an immutable snapshot of the derived CSV tree.

Atomicity
---------
A submission (new anchors + new publication) is written in **one** SQLite
transaction: the anchors are inserted, the derived tree is staged and renamed
into place, the publication rows are inserted, and only then is the
transaction committed.  A failure or a crash at any point rolls the anchors and
the publication back together, so the database never claims an anchor that has
no published snapshot behind it.

Consistent reads
----------------
The write side is atomic, so the read side must be too: one logical read (one
page render) that needs several queries runs them inside **one** read
transaction via :meth:`Store.snapshot`.  In WAL mode a read transaction sees a
stable snapshot of the last commit as of its first statement, no matter what
commits while it is open.  A refresh that races a supplement therefore renders
either the complete old publication or the complete new one — never old
anchors next to the new publication row.
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from . import core

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS dataset (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    source_rows INTEGER NOT NULL,
    raw_path    TEXT,
    raw_sha256  TEXT
);

CREATE TABLE IF NOT EXISTS reading (
    dataset_id   INTEGER NOT NULL REFERENCES dataset(id) ON DELETE CASCADE,
    seq          INTEGER NOT NULL,
    device_time  TEXT NOT NULL,      -- canonical ISO-8601 UTC text
    dissolved_oxygen REAL NOT NULL,
    file_order   INTEGER NOT NULL,
    PRIMARY KEY (dataset_id, seq)
);

CREATE TABLE IF NOT EXISTS anchor (
    dataset_id  INTEGER NOT NULL REFERENCES dataset(id) ON DELETE CASCADE,
    seq         INTEGER NOT NULL,
    utc         TEXT NOT NULL,       -- canonical ISO-8601 UTC text
    created_at  TEXT NOT NULL,
    PRIMARY KEY (dataset_id, seq)
);

CREATE TABLE IF NOT EXISTS publication (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id  INTEGER NOT NULL REFERENCES dataset(id) ON DELETE CASCADE,
    revision    INTEGER NOT NULL,
    created_at  TEXT NOT NULL,
    anchor_count INTEGER NOT NULL,
    dated_count INTEGER NOT NULL,
    pending_count INTEGER NOT NULL,
    UNIQUE (dataset_id, revision)
);

CREATE TABLE IF NOT EXISTS published_file (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    publication_id INTEGER NOT NULL REFERENCES publication(id) ON DELETE CASCADE,
    day            TEXT,             -- NULL for pending / manifest
    kind           TEXT NOT NULL,    -- 'day' | 'pending' | 'manifest'
    relpath        TEXT NOT NULL,    -- path relative to the publication dir
    sha256         TEXT NOT NULL,
    size           INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pub_dataset ON publication(dataset_id, revision);
CREATE INDEX IF NOT EXISTS idx_file_pub ON published_file(publication_id);
"""


class StoreError(Exception):
    """Raised when persistence fails."""


@dataclass(frozen=True)
class Dataset:
    id: int
    name: str
    created_at: str
    source_rows: int
    raw_path: str | None = None
    raw_sha256: str | None = None


@dataclass(frozen=True)
class Publication:
    id: int
    dataset_id: int
    revision: int
    created_at: str
    anchor_count: int
    dated_count: int
    pending_count: int


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _iso(value: str) -> datetime:
    return core.parse_utc(value)


_DATASET_COLS = "id, name, created_at, source_rows, raw_path, raw_sha256"
_PUB_COLS = (
    "id, dataset_id, revision, created_at, anchor_count, dated_count, pending_count"
)
_FILE_COLS = "id, publication_id, day, kind, relpath, sha256, size"


class Store:
    """A small SQLite store: one connection per operation, WAL mode."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self._lock = threading.RLock()
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    # ------------------------------------------------------------------
    # connections
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """An IMMEDIATE transaction; rolled back on any exception."""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            finally:
                conn.close()

    @contextmanager
    def snapshot(self) -> Iterator[sqlite3.Connection]:
        """One consistent read snapshot for any number of queries.

        In WAL mode every statement inside a single read transaction sees the
        database as of the same commit, even if a writer commits while the
        snapshot is open.  Use this whenever one logical read (for example one
        page render) issues several queries that must agree with each other;
        with separate connections each query could observe a different commit
        and the response would mix two publications.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # datasets and raw readings
    # ------------------------------------------------------------------

    def create_dataset(
        self,
        conn: sqlite3.Connection,
        name: str,
        readings: list[core.Reading],
        raw_path: str | None,
        raw_sha256: str | None,
        created_at: str | None = None,
    ) -> Dataset:
        created = created_at or now_iso()
        cur = conn.execute(
            "INSERT INTO dataset(name, created_at, source_rows, raw_path, raw_sha256)"
            " VALUES (?,?,?,?,?)",
            (name, created, len(readings), raw_path, raw_sha256),
        )
        dataset_id = int(cur.lastrowid)
        conn.executemany(
            "INSERT INTO reading(dataset_id, seq, device_time,"
            " dissolved_oxygen, file_order) VALUES (?,?,?,?,?)",
            [
                (dataset_id, r.seq, core.iso(r.device_time), r.dissolved_oxygen, i)
                for i, r in enumerate(readings)
            ],
        )
        return Dataset(dataset_id, name, created, len(readings), raw_path, raw_sha256)

    def list_datasets(self) -> list[Dataset]:
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT {_DATASET_COLS} FROM dataset ORDER BY id"
            ).fetchall()
        return [Dataset(**dict(r)) for r in rows]

    def get_dataset(
        self, dataset_id: int, conn: sqlite3.Connection | None = None
    ) -> Dataset | None:
        sql = f"SELECT {_DATASET_COLS} FROM dataset WHERE id=?"
        if conn is None:
            with self.connect() as own:
                row = own.execute(sql, (dataset_id,)).fetchone()
        else:
            row = conn.execute(sql, (dataset_id,)).fetchone()
        return Dataset(**dict(row)) if row else None

    def readings(
        self, dataset_id: int, conn: sqlite3.Connection | None = None
    ) -> list[core.Reading]:
        sql = (
            "SELECT seq, device_time, dissolved_oxygen FROM reading"
            " WHERE dataset_id=? ORDER BY seq"
        )
        if conn is None:
            with self.connect() as own:
                rows = own.execute(sql, (dataset_id,)).fetchall()
        else:
            rows = conn.execute(sql, (dataset_id,)).fetchall()
        return [
            core.Reading(r["seq"], _iso(r["device_time"]), r["dissolved_oxygen"])
            for r in rows
        ]

    # ------------------------------------------------------------------
    # anchors
    # ------------------------------------------------------------------

    def anchors(
        self, dataset_id: int, conn: sqlite3.Connection | None = None
    ) -> list[core.AnchorInput]:
        sql = "SELECT seq, utc FROM anchor WHERE dataset_id=? ORDER BY seq"
        if conn is None:
            with self.connect() as own:
                rows = own.execute(sql, (dataset_id,)).fetchall()
        else:
            rows = conn.execute(sql, (dataset_id,)).fetchall()
        return [core.AnchorInput(r["seq"], _iso(r["utc"])) for r in rows]

    def insert_anchors(
        self,
        conn: sqlite3.Connection,
        dataset_id: int,
        incoming: list[core.AnchorInput],
        *,
        known: dict[int, core.AnchorInput] | None = None,
        created_at: str | None = None,
    ) -> list[core.AnchorInput]:
        """Validate and insert anchors inside an existing transaction.

        A conflict on *any* row raises :class:`core.AnchorConflict` before
        anything is written, so the whole submission is rejected as a unit.
        Identical values are idempotent and reported as not added.
        """
        if known is None:
            known = {a.seq: a for a in self.anchors(dataset_id, conn)}
        for anchor in incoming:
            existing = known.get(anchor.seq)
            if existing is not None and existing.utc != anchor.utc:
                raise core.AnchorConflict(anchor.seq, existing.utc, anchor.utc)
        added: list[core.AnchorInput] = []
        stamp = created_at or now_iso()
        for anchor in incoming:
            if anchor.seq in known:
                continue
            conn.execute(
                "INSERT INTO anchor(dataset_id, seq, utc, created_at)"
                " VALUES (?,?,?,?)",
                (dataset_id, anchor.seq, core.iso(anchor.utc), stamp),
            )
            added.append(anchor)
        return added

    # ------------------------------------------------------------------
    # publications
    # ------------------------------------------------------------------

    def publications(
        self, dataset_id: int, conn: sqlite3.Connection | None = None
    ) -> list[Publication]:
        sql = (
            f"SELECT {_PUB_COLS} FROM publication"
            " WHERE dataset_id=? ORDER BY revision DESC"
        )
        if conn is None:
            with self.connect() as own:
                rows = own.execute(sql, (dataset_id,)).fetchall()
        else:
            rows = conn.execute(sql, (dataset_id,)).fetchall()
        return [Publication(**dict(r)) for r in rows]

    def all_publications(self) -> list[Publication]:
        with self.connect() as conn:
            rows = conn.execute(f"SELECT {_PUB_COLS} FROM publication").fetchall()
        return [Publication(**dict(r)) for r in rows]

    def latest_publication(
        self, dataset_id: int, conn: sqlite3.Connection | None = None
    ) -> Publication | None:
        sql = (
            f"SELECT {_PUB_COLS} FROM publication WHERE dataset_id=?"
            " ORDER BY revision DESC LIMIT 1"
        )
        if conn is None:
            with self.connect() as own:
                row = own.execute(sql, (dataset_id,)).fetchone()
        else:
            row = conn.execute(sql, (dataset_id,)).fetchone()
        return Publication(**dict(row)) if row else None

    def get_publication(
        self, publication_id: int, conn: sqlite3.Connection | None = None
    ) -> Publication | None:
        sql = f"SELECT {_PUB_COLS} FROM publication WHERE id=?"
        if conn is None:
            with self.connect() as own:
                row = own.execute(sql, (publication_id,)).fetchone()
        else:
            row = conn.execute(sql, (publication_id,)).fetchone()
        return Publication(**dict(row)) if row else None

    def files(
        self, publication_id: int, conn: sqlite3.Connection | None = None
    ) -> list[sqlite3.Row]:
        sql = (
            f"SELECT {_FILE_COLS} FROM published_file"
            " WHERE publication_id=? ORDER BY id"
        )
        if conn is None:
            with self.connect() as own:
                return own.execute(sql, (publication_id,)).fetchall()
        return conn.execute(sql, (publication_id,)).fetchall()

    def get_file(
        self,
        publication_id: int,
        kind: str,
        day: str | None,
        conn: sqlite3.Connection | None = None,
    ) -> sqlite3.Row | None:
        sql = (
            f"SELECT {_FILE_COLS} FROM published_file"
            " WHERE publication_id=? AND kind=?"
        )
        params: list[object] = [publication_id, kind]
        if day is not None:
            sql += " AND day=?"
            params.append(day)
        if conn is None:
            with self.connect() as own:
                return own.execute(sql, params).fetchone()
        return conn.execute(sql, params).fetchone()

    def insert_publication(
        self,
        conn: sqlite3.Connection,
        dataset_id: int,
        revision: int,
        files: list[tuple[str, str | None, str, str, int]],
        counts: tuple[int, int, int],
        created_at: str | None = None,
    ) -> Publication:
        stamp = created_at or now_iso()
        anchor_count, dated_count, pending_count = counts
        cur = conn.execute(
            "INSERT INTO publication(dataset_id, revision, created_at,"
            " anchor_count, dated_count, pending_count) VALUES (?,?,?,?,?,?)",
            (dataset_id, revision, stamp, anchor_count, dated_count, pending_count),
        )
        publication_id = int(cur.lastrowid)
        conn.executemany(
            "INSERT INTO published_file(publication_id, day, kind, relpath,"
            " sha256, size) VALUES (?,?,?,?,?,?)",
            [
                (publication_id, day, kind, relpath, sha, size)
                for (kind, day, relpath, sha, size) in files
            ],
        )
        return Publication(
            publication_id,
            dataset_id,
            revision,
            stamp,
            anchor_count,
            dated_count,
            pending_count,
        )
