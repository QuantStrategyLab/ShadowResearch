# Secret-free Cloud Run Jobs (us-east1) container image for SHADOW-N SH-2
FROM python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    TZ=UTC \
    SHADOW_DATA_DIR=/var/lib/shadow/data \
    SHADOW_CONFIG_PATH=/app/config/production-scheduler.json

RUN groupadd --gid 10001 shadow \
    && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin shadow \
    && mkdir -p /app /var/lib/shadow/data \
    && chown -R shadow:shadow /var/lib/shadow

WORKDIR /app

COPY --chown=root:root src/ /app/src/
COPY --chown=root:root vendor/ /app/vendor/
COPY --chown=root:root specs/ /app/specs/
COPY --chown=root:root config/ /app/config/

# Enforce read-only vendored engine permissions (0444 files / 0555 directories)
RUN find /app/vendor/fuse_engine -type f -exec chmod 0444 {} + \
    && find /app/vendor/fuse_engine -type d -exec chmod 0555 {} +

USER shadow

ENTRYPOINT ["python3", "-B", "-m", "shadow.cloudrun_job"]
