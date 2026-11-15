# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    # database, raw uploads and published trees live here (mount a volume)
    WQ_DATA_DIR=/data

WORKDIR /srv/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN useradd --system --uid 10001 wq \
    && mkdir -p /data \
    && chown -R wq:wq /data
USER wq

EXPOSE 8000
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=4)" || exit 1

# Single worker on purpose: revision allocation and the atomic
# stage -> rename -> register publish protocol rely on one process-wide
# lock, and SQLite (WAL) is the only shared state.  Do not raise --workers;
# scale by running one container per probe team instead.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
