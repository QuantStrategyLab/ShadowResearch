# Secret-free Cloud Run Jobs (us-east1) container image for SHADOW-N SH-2
FROM python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    TZ=UTC \
    SHADOW_CONFIG_PATH=/app/config/production-scheduler.json

# Debian base already reserves group 'shadow' (GID 42); use 'shadowapp' with UID/GID 10001 to match GCS FUSE mountOptions
RUN groupadd --gid 10001 shadowapp \
    && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin shadowapp \
    && mkdir -p /app

WORKDIR /app

COPY --chown=root:root src/ /app/src/
COPY --chown=root:root vendor/ /app/vendor/
COPY --chown=root:root specs/ /app/specs/
COPY --chown=root:root config/ /app/config/

# Enforce read-only vendored engine permissions (0444 files / 0555 directories)
RUN find /app/vendor/fuse_engine -type f -exec chmod 0444 {} + \
    && find /app/vendor/fuse_engine -type d -exec chmod 0555 {} +

USER shadowapp

ENTRYPOINT ["python3", "-B", "-m", "shadow.cloudrun_job"]
