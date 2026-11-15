# -*- coding: utf-8 -*-
"""Domain model, CSV parsing and the UTC correction engine.

The module is deliberately free of web / database dependencies so that the
correction rules can be unit tested in isolation.

Vocabulary
----------
segment
    A maximal run of readings whose *device* timestamps are strictly
    increasing.  A non-increasing device timestamp starts a new segment.
anchor
    A row from a 核时锚点 CSV: ``(序号, UTC)``.  An anchor pins the reading
    with that sequence number to the given UTC instant.
pending (待核)
    A reading that cannot be dated: it lies in a segment that has no anchor,
    or it lies outside the range covered by the anchors of its segment.
"""

from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Sequence

UTC = timezone.utc

# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------


class InputError(ValueError):
    """Raised when an uploaded CSV cannot be parsed."""

    def __init__(self, message: str, line: int | None = None) -> None:
        self.line = line
        super().__init__(message)

    def as_text(self) -> str:
        if self.line is None:
            return str(self)
        return f"第 {self.line} 行: {self}"


class AnchorConflict(Exception):
    """An anchor already exists for the sequence number with another UTC."""

    def __init__(self, seq: int, existing: datetime, incoming: datetime) -> None:
        self.seq = seq
        self.existing = existing
        self.incoming = incoming
        super().__init__(
            f"序号 {seq} 已有锚点 {iso(existing)}，不能改为 {iso(incoming)}"
        )


# ---------------------------------------------------------------------------
# time helpers
# ---------------------------------------------------------------------------

_DT_RE = re.compile(
    r"^\s*(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})"
    r"(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?\s*$"
)


def parse_utc(text: str) -> datetime:
    """Parse a UTC timestamp.

    Accepted forms::

        2026-03-01 08:00:00
        2026-03-01T08:00:00Z
        2026-03-01T08:00:00+00:00
        2026-03-01 08:00

    A value carrying a non-zero offset is converted to UTC.  Naive values are
    taken to be UTC already.
    """
    raw = (text or "").strip()
    if not raw:
        raise InputError("时间为空")
    if raw.endswith(("Z", "z")):
        raw = raw[:-1] + "+00:00"
    try:
        value = datetime.fromisoformat(raw)
    except ValueError:
        m = _DT_RE.match(raw)
        if not m:
            raise InputError(f"无法识别的时间格式: {text!r}") from None
        parts = [int(p) if p else 0 for p in m.groups()]
        try:
            value = datetime(*parts)
        except ValueError:
            raise InputError(f"无效的时间: {text!r}") from None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def iso(value: datetime) -> str:
    """Canonical UTC representation used in storage and downloads."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_date(value: datetime) -> str:
    """The UTC calendar day a reading belongs to (``YYYY-MM-DD``)."""
    return value.astimezone(UTC).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------

_READING_HEADERS = ("序号", "设备时间", "溶氧")
_ANCHOR_HEADERS = ("序号", "UTC")


def _clean_cell(cell: str | None) -> str:
    return (cell or "").strip().lstrip("﻿")


def _decode(data: bytes) -> str:
    for enc in ("utf-8-sig", "gb18030", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    raise InputError("文件编码无法识别，请使用 UTF-8")


def _rows(text: str) -> list[tuple[int, list[str]]]:
    reader = csv.reader(io.StringIO(text))
    out: list[tuple[int, list[str]]] = []
    for line_no, row in enumerate(reader, start=1):
        if not row or all(not _clean_cell(c) for c in row):
            continue
        out.append((line_no, row))
    return out


def _cells(row: Sequence[str]) -> list[str]:
    return [_clean_cell(c) for c in row]


def _check_header(got: list[str], expected: Sequence[str], kind: str) -> None:
    got_trimmed = [h for h in got if h]
    if got_trimmed[: len(expected)] != list(expected):
        raise InputError(
            f"{kind}表头应为 {'、'.join(expected)}，"
            f"实际为 {'、'.join(got_trimmed) or '空'}",
            line=1,
        )


def _parse_seq(text: str, line: int) -> int:
    raw = _clean_cell(text)
    if not raw:
        raise InputError("序号为空", line=line)
    try:
        value = int(raw)
    except ValueError:
        raise InputError(f"序号不是整数: {raw!r}", line=line) from None
    if value <= 0:
        raise InputError(f"序号必须为正整数: {value}", line=line)
    return value


@dataclass(frozen=True)
class Reading:
    seq: int
    device_time: datetime
    dissolved_oxygen: float


def parse_readings(data: bytes) -> list[Reading]:
    """Parse the probe CSV (序号, 设备时间, 溶氧).

    File order is preserved; the sequence number is only the identity of a
    reading, it does not need to be sorted.
    """
    text = _decode(data)
    rows = _rows(text)
    if not rows:
        raise InputError("读数文件为空")
    _check_header(_cells(rows[0][1]), _READING_HEADERS, "读数")
    readings: list[Reading] = []
    seen: dict[int, int] = {}
    for line_no, row in rows[1:]:
        cells = _cells(row)
        if len(cells) < 3:
            raise InputError(
                f"读数行缺少列（需要 3 列，实际 {len(cells)} 列）", line=line_no
            )
        seq = _parse_seq(cells[0], line_no)
        if seq in seen:
            raise InputError(
                f"序号 {seq} 重复（首次出现在第 {seen[seq]} 行）", line=line_no
            )
        seen[seq] = line_no
        try:
            device_time = parse_utc(cells[1])
        except InputError as exc:
            raise InputError(str(exc), line=line_no) from None
        raw_do = cells[2]
        if not raw_do:
            raise InputError("溶氧为空", line=line_no)
        try:
            dissolved = float(raw_do)
        except ValueError:
            raise InputError(f"溶氧不是数字: {raw_do!r}", line=line_no) from None
        if not (dissolved == dissolved) or dissolved in (
            float("inf"),
            float("-inf"),
        ):
            raise InputError(f"溶氧不是有限数字: {raw_do!r}", line=line_no)
        readings.append(Reading(seq, device_time, dissolved))
    if not readings:
        raise InputError("读数文件没有数据行")
    return readings


@dataclass(frozen=True)
class AnchorInput:
    seq: int
    utc: datetime


def parse_anchors(data: bytes) -> list[AnchorInput]:
    """Parse an anchor CSV (序号, UTC).

    A duplicate inside one file with the *same* UTC is harmless and collapsed;
    the same sequence number with a different UTC is rejected.
    """
    text = _decode(data)
    rows = _rows(text)
    if not rows:
        raise InputError("锚点文件为空")
    _check_header(_cells(rows[0][1]), _ANCHOR_HEADERS, "锚点")
    anchors: list[AnchorInput] = []
    seen: dict[int, datetime] = {}
    for line_no, row in rows[1:]:
        cells = _cells(row)
        if len(cells) < 2:
            raise InputError(
                f"锚点行缺少列（需要 2 列，实际 {len(cells)} 列）", line=line_no
            )
        seq = _parse_seq(cells[0], line_no)
        try:
            utc_value = parse_utc(cells[1])
        except InputError as exc:
            raise InputError(str(exc), line=line_no) from None
        if seq in seen:
            if seen[seq] != utc_value:
                raise AnchorConflict(seq, seen[seq], utc_value)
            continue
        seen[seq] = utc_value
        anchors.append(AnchorInput(seq, utc_value))
    if not anchors:
        raise InputError("锚点文件没有数据行")
    return anchors


# ---------------------------------------------------------------------------
# segments and correction
# ---------------------------------------------------------------------------


@dataclass
class Segment:
    index: int
    readings: list[Reading]
    anchors: list[AnchorInput] = field(default_factory=list)

    @property
    def first_seq(self) -> int:
        return self.readings[0].seq

    @property
    def last_seq(self) -> int:
        return self.readings[-1].seq


def split_segments(readings: Sequence[Reading]) -> list[Segment]:
    """Split readings at every non-increasing device timestamp.

    Segments are formed in file order.  A break happens when a reading's device
    time is less than *or equal to* the previous reading's: equal timestamps
    cannot be ordered and therefore cannot take part in a linear interpolation.
    Inside a segment the device clock is strictly increasing, which is exactly
    the precondition for correction.
    """
    segments: list[Segment] = []
    current: list[Reading] = []
    for reading in readings:
        if current and reading.device_time <= current[-1].device_time:
            segments.append(Segment(len(segments), current))
            current = []
        current.append(reading)
    if current:
        segments.append(Segment(len(segments), current))
    return segments


@dataclass(frozen=True)
class CorrectedRow:
    seq: int
    device_time: datetime
    dissolved_oxygen: float
    utc: datetime
    segment: int
    anchored: bool  # True when the reading itself is an anchor

    @property
    def date(self) -> str:
        return utc_date(self.utc)


@dataclass
class CorrectionResult:
    readings: list[Reading]
    segments: list[Segment]
    rows: list[CorrectedRow]
    pending: list[Reading]
    # anchors whose sequence number is absent from the readings
    orphan_anchors: list[AnchorInput] = field(default_factory=list)
    # anchors present in the file but not usable (duplicates with another UTC)
    conflicts: list[str] = field(default_factory=list)
    # wall clock of the correction run, used in the manifest
    generated_at: datetime | None = None

    @property
    def pending_seqs(self) -> list[int]:
        return [r.seq for r in self.pending]

    def dates(self) -> list[str]:
        return sorted({row.date for row in self.rows})

    def rows_for_date(self, date: str) -> list[CorrectedRow]:
        return [row for row in self.rows if row.date == date]

    def segment_of(self, seq: int) -> int:
        for seg in self.segments:
            for reading in seg.readings:
                if reading.seq == seq:
                    return seg.index
        return -1


def correct(
    readings: Sequence[Reading], anchors: Sequence[AnchorInput]
) -> CorrectionResult:
    """Apply the clock correction.

    Rules
    -----
    * An anchor fixes the reading with the same sequence number to the given
      UTC instant.
    * A segment with two or more anchors is corrected between its first and
      its last anchor by linear interpolation over device time.  Readings
      before the first / after the last anchor of such a segment have no
      reference and stay pending.
    * A segment with fewer than two anchors is entirely pending, except that
      the single anchored reading itself is dated by its own anchor.
    * An anchor naming a sequence number absent from the readings is reported
      and ignored.
    """
    readings = sorted(readings, key=lambda r: r.seq)
    segments = split_segments(readings)
    by_seq: dict[int, Reading] = {r.seq: r for r in readings}
    seg_of_seq: dict[int, Segment] = {
        r.seq: seg for seg in segments for r in seg.readings
    }

    result = CorrectionResult(readings=readings, segments=segments, rows=[], pending=[])

    anchor_by_seq: dict[int, AnchorInput] = {}
    for anchor in anchors:
        existing = anchor_by_seq.get(anchor.seq)
        if existing is not None:
            if existing.utc != anchor.utc:
                result.conflicts.append(
                    f"序号 {anchor.seq}: {iso(existing.utc)} / {iso(anchor.utc)}"
                )
            continue
        anchor_by_seq[anchor.seq] = anchor
        if anchor.seq not in by_seq:
            result.orphan_anchors.append(anchor)

    dated: dict[int, CorrectedRow] = {}
    for seg in segments:
        seg.anchors = sorted(
            (a for a in anchor_by_seq.values() if seg_of_seq.get(a.seq) is seg),
            key=lambda a: by_seq[a.seq].device_time,
        )
        if len(seg.anchors) == 1:
            only = seg.anchors[0]
            reading = by_seq[only.seq]
            dated[reading.seq] = CorrectedRow(
                reading.seq, reading.device_time, reading.dissolved_oxygen,
                only.utc, seg.index, True,
            )
            continue
        for left, right in zip(seg.anchors, seg.anchors[1:]):
            r_left = by_seq[left.seq]
            r_right = by_seq[right.seq]
            span = (r_right.device_time - r_left.device_time).total_seconds()
            utc_span = (right.utc - left.utc).total_seconds()
            for reading in seg.readings:
                if not (
                    r_left.device_time <= reading.device_time <= r_right.device_time
                ):
                    continue
                if reading.seq == left.seq:
                    utc_value = left.utc
                elif reading.seq == right.seq:
                    utc_value = right.utc
                else:
                    frac = (
                        reading.device_time - r_left.device_time
                    ).total_seconds() / span
                    utc_value = left.utc + timedelta(seconds=utc_span * frac)
                anchored = reading.seq in (left.seq, right.seq)
                previous = dated.get(reading.seq)
                # a shared end anchor is written twice; prefer the anchored
                # (exact) value from whichever pair produced it
                if previous is None or anchored or not previous.anchored:
                    dated[reading.seq] = CorrectedRow(
                        reading.seq,
                        reading.device_time,
                        reading.dissolved_oxygen,
                        utc_value,
                        seg.index,
                        anchored,
                    )

    result.rows = sorted(dated.values(), key=lambda r: (r.utc, r.seq))
    # pending keeps the original file order
    result.pending = [r for r in readings if r.seq not in dated]
    return result


def render_row(row: CorrectedRow) -> list[str]:
    """CSV cells for one corrected reading.

    The dissolved-oxygen value is the original reading, untouched; the UTC
    column carries the corrected timestamp, and the device time is kept for
    auditing.
    """
    return [
        str(row.seq),
        row.device_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        format_do(row.dissolved_oxygen),
        row.utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        str(row.segment + 1),
        "锚点" if row.anchored else "校时",
    ]


def format_do(value: float) -> str:
    text = repr(float(value))
    if text.endswith(".0"):
        text = text[:-2]
    return text


CORRECTED_HEADER = ["序号", "设备时间", "溶氧", "UTC", "分段", "来源"]
PENDING_HEADER = ["序号", "设备时间", "溶氧", "分段"]
