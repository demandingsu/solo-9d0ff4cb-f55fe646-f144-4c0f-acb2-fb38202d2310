# -*- coding: utf-8 -*-
"""FastAPI application: upload, inspect, supplement anchors, download.

The app is intentionally a single-process, single-worker deployment: the
publication protocol (allocate revision -> atomic directory rename -> commit)
relies on a process-wide lock, and SQLite is the only shared state.  See
``docker-compose.yml``.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import core, demo
from .service import DatasetNotFound, NoPublication, Service

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("WQ_DATA_DIR", BASE_DIR.parent / "var"))
DB_PATH = Path(os.environ.get("WQ_DB_PATH", DATA_DIR / "water_quality.sqlite3"))
EXPORT_ROOT = Path(os.environ.get("WQ_EXPORT_ROOT", DATA_DIR / "exports"))

MAX_UPLOAD = 8 * 1024 * 1024

app = FastAPI(title="野外水质探头 时钟回拨校时")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

_service: Service | None = None


def get_service() -> Service:
    global _service
    if _service is None:
        _service = Service(DB_PATH, EXPORT_ROOT)
    return _service


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------


def _page(
    request: Request, name: str, context: dict, status_code: int = 200
) -> HTMLResponse:
    context.setdefault("request", request)
    return templates.TemplateResponse(request, name, context, status_code=status_code)


def _empty_index(error: str, status_code: int) -> HTMLResponse:
    request = Request(scope={"type": "http", "method": "GET", "headers": [], "path": "/"})
    return _page(
        request,
        "index.html",
        {
            "datasets": get_service().datasets(),
            "samples": demo.samples_meta(),
            "error": error,
        },
        status_code=status_code,
    )


def _read_upload(upload: UploadFile | None, label: str) -> bytes:
    if upload is None or not upload.filename:
        raise core.InputError(f"请选择{label}文件")
    data = upload.file.read(MAX_UPLOAD + 1)
    if len(data) > MAX_UPLOAD:
        raise core.InputError(f"{label}文件超过 {MAX_UPLOAD // 1024 // 1024} MB")
    if not data.strip():
        raise core.InputError(f"{label}文件为空")
    return data


def _state_context(
    dataset_id: int,
    error: str | None = None,
    notice: str | None = None,
) -> dict:
    state = get_service().state(dataset_id)
    latest = state.latest
    files = (
        [
            {
                "kind": row["kind"],
                "day": row["day"],
                "relpath": row["relpath"],
                "size": row["size"],
                "sha256": row["sha256"],
            }
            for row in get_service().store.files(latest.id)
        ]
        if latest is not None
        else []
    )
    return {
        "dataset": state.dataset,
        "result": state.result,
        "latest": latest,
        "publications": state.publications,
        "files": files,
        "error": error,
        "notice": notice,
        "sample_supplement": demo.SUPPLEMENT_ANCHORS_TEXT,
        "sample_migration": demo.MIGRATION_ANCHORS_TEXT,
        "sample_conflict": demo.CONFLICT_ANCHORS_TEXT,
    }


def _render_dataset(
    request: Request,
    dataset_id: int,
    *,
    error: str | None = None,
    notice: str | None = None,
    status_code: int = 200,
) -> HTMLResponse:
    try:
        context = _state_context(dataset_id, error=error, notice=notice)
    except DatasetNotFound as exc:
        return _page(request, "message.html", {"message": str(exc)}, status_code=404)
    return _page(request, "dataset.html", context, status_code=status_code)


def _check_day(text: str) -> bool:
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return False
    return True


# ---------------------------------------------------------------------------
# pages
# ---------------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return _page(
        request,
        "index.html",
        {
            "datasets": get_service().datasets(),
            "samples": demo.samples_meta(),
            "error": None,
        },
    )


@app.post("/datasets")
async def create_dataset(
    request: Request,
    readings: UploadFile = File(None),
    anchors: UploadFile = File(None),
    name: str = Form(""),
):
    try:
        reading_bytes = _read_upload(readings, "读数 CSV")
        parsed = core.parse_readings(reading_bytes)
        anchor_inputs: list[core.AnchorInput] = []
        if anchors is not None and anchors.filename:
            anchor_inputs = core.parse_anchors(_read_upload(anchors, "锚点 CSV"))
    except core.InputError as exc:
        return _page(
            request,
            "index.html",
            {
                "datasets": get_service().datasets(),
                "samples": demo.samples_meta(),
                "error": exc.as_text(),
            },
            status_code=400,
        )
    except core.AnchorConflict as exc:
        return _page(
            request,
            "index.html",
            {
                "datasets": get_service().datasets(),
                "samples": demo.samples_meta(),
                "error": f"锚点文件内部冲突：{exc}",
            },
            status_code=409,
        )

    title = (name or "").strip() or (
        f"探头读数 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}"
    )
    try:
        ds = get_service().ingest(reading_bytes, parsed, anchor_inputs, title)
    except core.AnchorConflict as exc:
        return _page(
            request,
            "index.html",
            {
                "datasets": get_service().datasets(),
                "samples": demo.samples_meta(),
                "error": f"锚点冲突，本次上传整体回滚：{exc}",
            },
            status_code=409,
        )
    except Exception as exc:
        return _page(
            request,
            "index.html",
            {
                "datasets": get_service().datasets(),
                "samples": demo.samples_meta(),
                "error": f"保存或初次发布失败，本次上传整体回滚：{exc}",
            },
            status_code=500,
        )
    return RedirectResponse(f"/datasets/{ds.id}", status_code=303)


@app.get("/datasets/{dataset_id}", response_class=HTMLResponse)
def dataset_page(request: Request, dataset_id: int):
    return _render_dataset(request, dataset_id)


@app.post("/datasets/{dataset_id}/anchors")
async def add_anchors(
    request: Request,
    dataset_id: int,
    anchors_file: UploadFile = File(None),
    anchors_text: str = Form(""),
):
    try:
        get_service().dataset(dataset_id)
    except DatasetNotFound as exc:
        return _page(request, "message.html", {"message": str(exc)}, status_code=404)

    try:
        data: bytes | None = None
        if anchors_file is not None and anchors_file.filename:
            data = _read_upload(anchors_file, "锚点 CSV")
        if data is None:
            text = (anchors_text or "").strip()
            data = text.encode("utf-8") if text else None
        if data is None:
            raise core.InputError("请上传锚点文件或在文本框中粘贴锚点")
        parsed = core.parse_anchors(data)
    except core.InputError as exc:
        return _render_dataset(request, dataset_id, error=exc.as_text(), status_code=400)
    except core.AnchorConflict as exc:
        return _render_dataset(
            request,
            dataset_id,
            error=f"锚点文件内部冲突，本次未作任何修改，仍使用旧版：{exc}",
            status_code=409,
        )

    try:
        publication, added = get_service().add_anchors_and_publish(
            dataset_id, parsed
        )
    except core.AnchorConflict as exc:
        return _render_dataset(
            request,
            dataset_id,
            error=(
                f"序号 {exc.seq} 已有锚点 {core.iso(exc.existing)}，"
                f"本次提交为 {core.iso(exc.incoming)}。"
                "同序号不同 UTC 的锚点不被接受，整次提交未生效，"
                "数据目录仍为上一版。"
            ),
            status_code=409,
        )
    except DatasetNotFound as exc:
        return _page(request, "message.html", {"message": str(exc)}, status_code=404)
    except Exception as exc:
        return _render_dataset(
            request,
            dataset_id,
            error=f"补交失败，未发布新版本，仍使用旧版：{exc}",
            status_code=500,
        )

    note = f"已补交 {len(added)} 个锚点，发布第 {publication.revision} 版。"
    if len(added) < len(parsed):
        note += " 其中同值锚点已存在，未重复写入。"
    return _render_dataset(request, dataset_id, notice=note)


@app.post("/datasets/{dataset_id}/republish")
def republish(request: Request, dataset_id: int):
    try:
        get_service().republish(dataset_id)
    except DatasetNotFound as exc:
        return _page(request, "message.html", {"message": str(exc)}, status_code=404)
    except Exception as exc:
        return _render_dataset(
            request,
            dataset_id,
            error=f"重新发布失败，仍使用旧版：{exc}",
            status_code=500,
        )
    return RedirectResponse(f"/datasets/{dataset_id}", status_code=303)


@app.post("/demo")
def load_demo():
    ds = demo.seed(get_service())
    return RedirectResponse(f"/datasets/{ds.id}", status_code=303)


# ---------------------------------------------------------------------------
# downloads
# ---------------------------------------------------------------------------


def _download(
    request: Request,
    publication_id: int,
    kind: str,
    day: str | None,
    filename: str,
) -> Response:
    try:
        publication, row, path = get_service().file_path(publication_id, kind, day)
    except NoPublication as exc:
        return _page(request, "message.html", {"message": str(exc)}, status_code=404)
    return FileResponse(
        path,
        media_type="text/csv; charset=utf-8",
        filename=filename,
        headers={
            "Cache-Control": "no-store",
            "X-Publication-Id": str(publication.id),
            "X-Publication-Revision": str(publication.revision),
            "X-Checksum-SHA256": row["sha256"],
        },
    )


@app.get("/p/{publication_id}/day/{date}.csv")
def download_day(request: Request, publication_id: int, date: str):
    if not _check_day(date):
        return _page(request, "message.html", {"message": "日期格式无效"}, status_code=404)
    return _download(request, publication_id, "day", date, f"{date}.csv")


@app.get("/p/{publication_id}/pending.csv")
def download_pending(request: Request, publication_id: int):
    return _download(request, publication_id, "pending", None, "待核读数.csv")


@app.get("/p/{publication_id}/manifest.csv")
def download_manifest(request: Request, publication_id: int):
    return _download(request, publication_id, "manifest", None, "发布清单.csv")


@app.get("/healthz")
def healthz():
    try:
        get_service().store.list_datasets()
    except Exception:
        return Response("unhealthy", status_code=503)
    return Response("ok", media_type="text/plain")
