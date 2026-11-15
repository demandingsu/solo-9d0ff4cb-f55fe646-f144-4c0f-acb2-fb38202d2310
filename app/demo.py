# -*- coding: utf-8 -*-
"""Built-in demo data.

One scenario, generated here so the app has no external data-file dependency.

A single probe logs 10-minute readings starting 2026-03-01 20:00 UTC.  After
sequence 12 its clock falls back four hours, so the device timestamps stop
increasing and the sequence splits into two segments.  The upload carries two
anchors (1 and 12), which date the first segment; the whole second segment
(13..36) is pending.

Supplementing two more anchors (13 at 22:00Z and 36 at 2026-03-02 01:55Z) dates
the second segment.  The straight-line correction puts sequence 24 at
2026-03-01 23:52Z and sequence 25 just after midnight, so a second day file
appears.

Supplementing one more anchor (24 at 2026-03-02 00:05Z) shows the drift is not
linear: sequence 24 migrates from the 2026-03-01 day file into the 2026-03-02
day file.  Both day files and the manifest are republished as one new
revision; the previous revision's links keep returning the previous bytes.

The text of each supplement is exposed on the dataset page so the browser demo
can be driven purely by copy-paste.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from . import core

UTC = timezone.utc

READING_COUNT = 36
ROLLBACK_AFTER = 12
ROLLBACK_HOURS = 4
START = datetime(2026, 3, 1, 20, 0, 0, tzinfo=UTC)


def _do_value(seq: int) -> float:
    # smooth, slightly noisy dissolved-oxygen trace
    return round(6.0 + 0.04 * seq + 0.06 * ((seq % 5) - 2), 2)


def _device_rows() -> list[tuple[int, str, float]]:
    rows: list[tuple[int, str, float]] = []
    for seq in range(1, READING_COUNT + 1):
        device = START + timedelta(minutes=10 * (seq - 1))
        if seq > ROLLBACK_AFTER:
            device -= timedelta(hours=ROLLBACK_HOURS)  # the clock jumped backwards
        rows.append((seq, device.strftime("%Y-%m-%d %H:%M:%S"), _do_value(seq)))
    return rows


def readings_csv() -> str:
    lines = ["序号,设备时间,溶氧"]
    for seq, device, value in _device_rows():
        lines.append(f"{seq},{device},{value:.2f}")
    return "\n".join(lines) + "\n"


# Anchors that arrive with the probe file: they date segment 1 only.
INITIAL_ANCHORS_CSV = (
    "序号,UTC\n"
    "1,2026-03-01T20:00:00Z\n"
    "12,2026-03-01T21:50:00Z\n"
)

# Supplement 1: dates the whole second segment.  The clock lost four hours and
# then ran slightly fast, so this straight line is only a first approximation.
SUPPLEMENT_ANCHORS_TEXT = (
    "序号,UTC\n"
    "13,2026-03-01T22:00:00Z\n"
    "36,2026-03-02T01:55:00Z\n"
)

# Supplement 2: a third, authoritative time check inside the span.  It moves
# sequence 24 across UTC midnight (23:52Z -> 00:05Z).
MIGRATION_ANCHORS_TEXT = "序号,UTC\n24,2026-03-02T00:05:00Z\n"

# Same sequence number, different UTC: rejected as a whole; old version stays.
CONFLICT_ANCHORS_TEXT = "序号,UTC\n24,2026-03-02T00:20:00Z\n"


def samples_meta() -> list[dict]:
    return [
        {
            "key": "rollback",
            "title": "样例：探头时钟回拨 + 跨日补锚点",
            "readings": readings_csv(),
            "anchors": INITIAL_ANCHORS_CSV,
            "supplement": SUPPLEMENT_ANCHORS_TEXT,
            "migration": MIGRATION_ANCHORS_TEXT,
            "conflict": CONFLICT_ANCHORS_TEXT,
        }
    ]


def seed(service):
    """Create the demo dataset fresh, with the initial anchors, and publish it."""
    raw = readings_csv().encode("utf-8")
    readings = core.parse_readings(raw)
    anchors = core.parse_anchors(INITIAL_ANCHORS_CSV.encode("utf-8"))
    return service.ingest(
        raw, readings, anchors, "样例：时钟回拨探头（2026-03-01 起）"
    )
