"""Shared bounded-read helpers for upstream HTTP response and error streams."""

from __future__ import annotations

import urllib.error
from typing import Any

MAX_ERROR_BODY_BYTES = 65_536


def read_bounded_http_body(
    stream: Any,
    *,
    max_bytes: int,
    error_cls: type[Exception],
    error_prefix: str,
    content_length_header: str | None = None,
    chunk_size: int = 65_536,
) -> bytes:
    """Stream an HTTP response body up to max_bytes, failing closed before large allocations."""
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
        raise ValueError("max_bytes must be a positive integer")
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size < 1:
        raise ValueError("chunk_size must be a positive integer")

    if isinstance(content_length_header, str):
        raw_cl = content_length_header.strip()
        if raw_cl:
            try:
                declared = int(raw_cl)
            except ValueError:
                declared = None
            if declared is not None and declared >= 0 and declared > max_bytes:
                raise error_cls(f"{error_prefix}:content_length_exceeds_{max_bytes}")

    chunks: list[bytes] = []
    total = 0
    while True:
        to_read = min(chunk_size, (max_bytes - total) + 1)
        chunk = stream.read(to_read)
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
        if total > max_bytes:
            raise error_cls(f"{error_prefix}:response_exceeds_{max_bytes}_bytes")
    return b"".join(chunks)


def read_bounded_error_body(
    exc: urllib.error.HTTPError,
    *,
    max_bytes: int = MAX_ERROR_BODY_BYTES,
) -> bytes:
    """Read at most min(max_bytes, MAX_ERROR_BODY_BYTES) from an HTTPError body stream."""
    try:
        if exc.fp is None:
            return b""
        capped = max(1, min(int(max_bytes), MAX_ERROR_BODY_BYTES))
        return bytes(exc.read(capped))
    finally:
        exc.close()
