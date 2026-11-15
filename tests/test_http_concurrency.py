# -*- coding: utf-8 -*-
"""End-to-end regression tests: concurrent supplement POST and page GET.

A real uvicorn server (single worker, like the deployment) serves the app on
a temporary database.  One test pauses a ``GET /datasets/{id}`` in the middle
of its read snapshot — via ``Service.state_read_hook`` — and commits the
supplement POST exactly in that window, then asserts the interrupted response
is the complete old version.  Another hammers concurrent GETs while POSTs
land and checks the page-internal consistency invariants on every response:

* the "当前发布版本" stat and the "当日发布目录" heading show one revision;
* the "待核" stat equals the "待核 N 条" caption of that directory;
* the pending sequence tags equal the pending count;
* every download link in the current directory points at one publication id;
* the anchor tags equal the directory's anchor count.
"""

from __future__ import annotations

import re
import socket
import threading
import time

import httpx
import pytest
import uvicorn

from app import demo, main
from app.service import Service

INITIAL_PENDING = 24


# ---------------------------------------------------------------------------
# live server fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def live_server(service: Service, monkeypatch):
    """Run the FastAPI app against the test service on an ephemeral port."""
    monkeypatch.setattr(main, "_service", service)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    config = uvicorn.Config(
        main.app,
        host="127.0.0.1",
        port=port,
        log_level="error",
        access_log=False,
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 15
    while not server.started:
        if time.time() > deadline:
            raise RuntimeError("uvicorn did not start")
        time.sleep(0.05)
    base_url = f"http://127.0.0.1:{port}"
    try:
        yield base_url, service
    finally:
        server.should_exit = True
        thread.join(15)


# ---------------------------------------------------------------------------
# page consistency assertions
# ---------------------------------------------------------------------------

_REV_STAT = re.compile(r"当前发布版本</span><span class=\"v\">第 (\d+) 版</span>")
_REV_HEADING = re.compile(r"当日发布目录（第 (\d+) 版）")
_PENDING_STAT = re.compile(r"<span class=\"k\">待核</span><span class=\"v\">(\d+)</span>")
_CAPTION = re.compile(r"锚点 (\d+) 个，已校时 (\d+) 条，待核 (\d+) 条")
_PUB_LINK = re.compile(r'href="/p/(\d+)/')
_SEQ_TAG = re.compile(r'class="tag seq">(\d+)<')
_ANCHOR_TAG = re.compile(r'class="tag">(\d+) →')


def _directory_section(html: str) -> str:
    """The 当日发布目录 panel: links there must point at one publication."""
    start = html.index("当日发布目录")
    end = html.index("</section>", start)
    return html[start:end]


def assert_page_consistent(html: str) -> dict:
    """All versioned facts on one page must belong to one publication."""
    rev_stat = _REV_STAT.search(html)
    rev_heading = _REV_HEADING.search(html)
    assert rev_stat, "page has no 当前发布版本 stat"
    assert rev_heading, "page has no 当日发布目录 heading"
    assert rev_stat.group(1) == rev_heading.group(1), (
        f"stat says revision {rev_stat.group(1)} but the file directory "
        f"heading says {rev_heading.group(1)}"
    )

    pending_stat = _PENDING_STAT.search(html)
    caption = _CAPTION.search(html)
    assert pending_stat and caption, "page has no pending figures"
    assert pending_stat.group(1) == caption.group(3), (
        f"待核 stat {pending_stat.group(1)} != directory caption "
        f"{caption.group(3)} (mixed correction result and publication)"
    )

    seq_tags = _SEQ_TAG.findall(html)
    assert len(seq_tags) == int(pending_stat.group(1)), (
        f"{len(seq_tags)} pending sequence tags but 待核 stat is "
        f"{pending_stat.group(1)}"
    )

    anchor_tags = _ANCHOR_TAG.findall(html)
    assert len(anchor_tags) == int(caption.group(1)), (
        f"{len(anchor_tags)} anchor tags but directory caption says "
        f"{caption.group(1)} anchors"
    )

    directory = _directory_section(html)
    pub_ids = set(_PUB_LINK.findall(directory))
    assert len(pub_ids) == 1, (
        f"current directory links point at several publications: {pub_ids}"
    )
    return {
        "revision": int(rev_stat.group(1)),
        "pending": int(pending_stat.group(1)),
        "publication_id": int(pub_ids.pop()),
    }


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


def test_refresh_interrupted_by_supplement_post_renders_one_version(live_server):
    """The exact reported race, driven through real HTTP requests."""
    base_url, service = live_server
    ds = demo.seed(service)
    pub1 = service.store.latest_publication(ds.id)

    entered = threading.Event()
    release = threading.Event()

    def hook() -> None:
        service.state_read_hook = None  # fire once, for the GET below only
        entered.set()
        assert release.wait(10), "test orchestration broken: never released"

    service.state_read_hook = hook

    outcome: dict = {}

    def refresh() -> None:
        outcome["response"] = httpx.get(f"{base_url}/datasets/{ds.id}", timeout=15)

    reader = threading.Thread(target=refresh)
    reader.start()
    try:
        # The colleague's refresh is now paused mid-read.  The clerk's
        # supplement POST commits while the GET is in flight.
        assert entered.wait(10), "GET never reached the read hook"
        post = httpx.post(
            f"{base_url}/datasets/{ds.id}/anchors",
            data={"anchors_text": demo.SUPPLEMENT_ANCHORS_TEXT},
            timeout=15,
        )
        assert post.status_code == 200
        pub2 = service.store.latest_publication(ds.id)
        assert pub2.revision == pub1.revision + 1
    finally:
        release.set()
        reader.join(15)
    assert service.state_read_hook is None

    # The interrupted GET started before the commit: it must be the complete
    # old version — 24 pending, revision 1, only revision-1 links.
    page = outcome["response"]
    assert page.status_code == 200
    facts = assert_page_consistent(page.text)
    assert facts == {
        "revision": 1,
        "pending": INITIAL_PENDING,
        "publication_id": pub1.id,
    }

    # A later refresh renders the complete new version.
    page2 = httpx.get(f"{base_url}/datasets/{ds.id}", timeout=15)
    facts2 = assert_page_consistent(page2.text)
    assert facts2 == {"revision": 2, "pending": 0, "publication_id": pub2.id}

    # The old version's download links still return the original files.
    old_pending = httpx.get(f"{base_url}/p/{pub1.id}/pending.csv", timeout=15)
    assert old_pending.status_code == 200
    rows = [ln for ln in old_pending.text.splitlines() if ln.strip()]
    assert len(rows) == 1 + INITIAL_PENDING  # header + 24 pending readings
    assert old_pending.headers["x-publication-revision"] == "1"

    new_pending = httpx.get(f"{base_url}/p/{pub2.id}/pending.csv", timeout=15)
    new_rows = [ln for ln in new_pending.text.splitlines() if ln.strip()]
    assert len(new_rows) == 1  # header only: 待核 0 条
    # the new day file exists only in the new version
    assert httpx.get(
        f"{base_url}/p/{pub2.id}/day/2026-03-02.csv", timeout=15
    ).status_code == 200
    assert httpx.get(
        f"{base_url}/p/{pub1.id}/day/2026-03-02.csv", timeout=15
    ).status_code == 404


def test_concurrent_posts_and_gets_never_render_a_mixed_page(live_server):
    """Stress: repeated supplements with continuous refreshes."""
    base_url, service = live_server
    ds = demo.seed(service)

    batches = [
        demo.SUPPLEMENT_ANCHORS_TEXT,
        demo.MIGRATION_ANCHORS_TEXT,
        "序号,UTC\n18,2026-03-01T22:50:00Z\n",
        "序号,UTC\n30,2026-03-02T00:50:00Z\n",
        "序号,UTC\n20,2026-03-01T23:10:00Z\n",
    ]

    stop = threading.Event()
    failures: list[str] = []
    seen: set[int] = set()
    seen_lock = threading.Lock()

    def refresh_loop() -> None:
        try:
            with httpx.Client(base_url=base_url, timeout=15) as client:
                while not stop.is_set():
                    resp = client.get(f"/datasets/{ds.id}")
                    assert resp.status_code == 200
                    facts = assert_page_consistent(resp.text)
                    with seen_lock:
                        seen.add(facts["revision"])
        except Exception as exc:
            failures.append(f"{type(exc).__name__}: {exc}")
            stop.set()

    readers = [threading.Thread(target=refresh_loop) for _ in range(3)]
    for thread in readers:
        thread.start()
    deadline = time.time() + 10
    while not seen and time.time() < deadline:
        time.sleep(0.001)
    assert seen, "readers never rendered a page"

    for batch in batches:
        resp = httpx.post(
            f"{base_url}/datasets/{ds.id}/anchors",
            data={"anchors_text": batch},
            timeout=15,
        )
        assert resp.status_code == 200
    stop.set()
    for thread in readers:
        thread.join(15)

    assert not failures, f"mixed pages observed: {failures[:3]}"
    assert len(seen) >= 2, f"readers never raced a supplement: {seen}"
    final = assert_page_consistent(
        httpx.get(f"{base_url}/datasets/{ds.id}", timeout=15).text
    )
    assert final["revision"] == 1 + len(batches)
    assert final["pending"] == 0
