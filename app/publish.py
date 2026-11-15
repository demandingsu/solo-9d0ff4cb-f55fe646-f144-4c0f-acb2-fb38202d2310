# -*- coding: utf-8 -*-
"""Atomic publication of the derived UTC-day CSV tree.

Layout on disk::

    <root>/
        pub-0001/
            manifest.csv
            pending.csv
            day/2026-03-01.csv
            day/2026-03-02.csv
        pub-0002/ ...
        .staging/            <- never served, reclaimed on startup

Guarantees
----------
* Every file is written into a fresh staging directory, fsynced, and only
  then is the directory ``os.rename``d to its final ``pub-NNNN`` name.  Rename
  inside one filesystem is atomic, so a reader can only ever open a complete
  directory.
* The SQLite registration happens **after** the rename.  If the process dies
  between rename and registration, the directory exists but is not referenced
  by any publication row, so the UI and download route (which resolve paths
  through the database) do not serve it.  On startup the orphan directory is
  moved out of the way.
* Files are never modified after publishing.  Downloading an old link therefore
  always returns the bytes of that revision, even after newer anchors change
  the day assignment of the readings.  ``sha256`` is stored alongside to
  detect tampering / partial copies.
"""

from __future__ import annotations

import csv
import hashlib
import io
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

from . import core


class PublishError(Exception):
    """Raised when the derived tree could not be staged / committed."""


@dataclass(frozen=True)
class StagedFile:
    kind: str          # 'day' | 'pending' | 'manifest'
    day: str | None
    relpath: str
    sha256: str
    size: int


@dataclass
class StagedPublication:
    revision: int
    staging_dir: Path
    final_dir: Path
    files: list[StagedFile]
    counts: tuple[int, int, int]


def _csv_bytes(header: list[str], rows: list[list[str]]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\r\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def write_fsync(path: str | Path, data: bytes) -> None:
    """Write ``data`` to ``path`` and flush it all the way to the disk."""
    with open(path, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())


def _fsync_write(path: Path, data: bytes) -> None:
    write_fsync(path, data)


def _write_and_hash(path: Path, data: bytes) -> tuple[str, int]:
    _fsync_write(path, data)
    return hashlib.sha256(data).hexdigest(), len(data)


def build_staging(
    root: str | Path,
    dataset_id: int,
    revision: int,
    result: core.CorrectionResult,
    anchors: list[core.AnchorInput],
) -> StagedPublication:
    """Write the whole derived tree into a private staging directory."""
    root = Path(root)
    staging_root = root / ".staging"
    try:
        staging_root.mkdir(parents=True, exist_ok=True)
        staging_dir = staging_root / (
            f"ds-{dataset_id:04d}-rev-{revision:04d}-{uuid.uuid4().hex}"
        )
        staging_dir.mkdir()
        day_dir = staging_dir / "day"
        day_dir.mkdir()

        files: list[StagedFile] = []

        def emit(kind: str, day: str | None, relpath: str, data: bytes) -> None:
            path = staging_dir / relpath
            sha, size = _write_and_hash(path, data)
            files.append(StagedFile(kind, day, relpath, sha, size))

        dates = result.dates()
        for date in dates:
            rows = [core.render_row(row) for row in result.rows_for_date(date)]
            # a day file must never be empty: its name states that these
            # readings belong to this UTC day
            if not rows:
                continue
            emit("day", date, f"day/{date}.csv", _csv_bytes(core.CORRECTED_HEADER, rows))

        emit(
            "pending",
            None,
            "pending.csv",
            _csv_bytes(
                core.PENDING_HEADER,
                [
                    [
                        str(r.seq),
                        core.iso(r.device_time),
                        core.format_do(r.dissolved_oxygen),
                        str(result.segment_of(r.seq) + 1),
                    ]
                    for r in result.pending
                ],
            ),
        )

        manifest_rows: list[list[str]] = [
            ["修订", str(revision)],
            ["生成时间", core.iso(result.generated_at) if result.generated_at else ""],
            ["锚点数", str(len(anchors))],
            ["已校时读数", str(len(result.rows))],
            ["待核读数", str(len(result.pending))],
            [],
            ["文件", "日期", "字节", "SHA-256"],
        ]
        for staged in files:
            manifest_rows.append(
                [staged.relpath, staged.day or "", str(staged.size), staged.sha256]
            )
        emit("manifest", None, "manifest.csv", _csv_bytes(["项目", "值"], manifest_rows))

        # fsync the directory entries as well
        for directory in (day_dir, staging_dir):
            fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

        final_dir = root / dir_name(dataset_id, revision)
        if final_dir.exists():
            raise PublishError(f"发布目录已存在: {final_dir}")
        return StagedPublication(revision, staging_dir, final_dir, files,
                                 (len(anchors), len(result.rows), len(result.pending)))
    except OSError as exc:
        raise PublishError(f"写文件失败: {exc}") from exc


def commit_staging(staged: StagedPublication) -> None:
    """Atomically move the staged tree into its final, servable name."""
    try:
        os.rename(staged.staging_dir, staged.final_dir)
    except OSError as exc:
        raise PublishError(f"发布目录替换失败: {exc}") from exc


def discard_staging(staged: StagedPublication) -> None:
    """Remove a staging directory that will never be registered."""
    shutil.rmtree(staged.staging_dir, ignore_errors=True)


def prune_staging(root: str | Path, keep: list[str] | None = None) -> int:
    """Remove stale staging directories and unreferenced publication dirs.

    Called once when the process starts, *before* any request is served, so a
    death mid-publish can never leave half-written files visible.  Directories
    referenced by committed publications must be listed in ``keep`` (their
    basename, e.g. ``ds-0001-rev-0003``); everything else is residue from a
    crash between the atomic rename and the database commit and is removed.
    """
    root = Path(root)
    removed = 0
    staging = root / ".staging"
    if staging.is_dir():
        for child in staging.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)
            removed += 1
    keep_names = set(keep or [])
    for child in root.glob("ds-*-rev-*"):
        if child.is_dir() and child.name not in keep_names:
            shutil.rmtree(child, ignore_errors=True)
            removed += 1
    return removed


def dir_name(dataset_id: int, revision: int) -> str:
    """Directory name of one publication.

    The dataset id is part of the name because revisions are numbered per
    dataset: two probes may both legitimately publish a "revision 1".
    """
    return f"ds-{dataset_id:04d}-rev-{revision:04d}"


def publication_dir(root: str | Path, dataset_id: int, revision: int) -> Path:
    return Path(root) / dir_name(dataset_id, revision)


def resolve(root: str | Path, dataset_id: int, revision: int, relpath: str) -> Path:
    """Resolve a registered relative path, refusing path traversal."""
    base = publication_dir(root, dataset_id, revision).resolve()
    target = (base / relpath).resolve()
    if base not in target.parents and target != base:
        raise PublishError(f"非法发布路径: {relpath}")
    return target


def verify(path: Path, sha256: str) -> bool:
    digest = hashlib.sha256()
    try:
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 16), b""):
                digest.update(chunk)
    except OSError:
        return False
    return digest.hexdigest() == sha256
