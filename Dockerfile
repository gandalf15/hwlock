# hwlock server: Python standard library only, runs as a non-root user, state in /data (a volume).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    HWLOCK_HOST=0.0.0.0 HWLOCK_PORT=8770 HWLOCK_DB=/data/hwlock.db

RUN useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin hwlock \
 && mkdir -p /data /app && chown hwlock:hwlock /data
WORKDIR /app
COPY hwlock_server /app/hwlock_server
COPY client/hwlock /usr/local/bin/hwlock
VOLUME ["/data"]
USER hwlock
EXPOSE 8770

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD ["python", "-c", "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/health' % os.environ.get('HWLOCK_PORT','8770'), timeout=2)"]

STOPSIGNAL SIGTERM
CMD ["python", "-m", "hwlock_server"]
