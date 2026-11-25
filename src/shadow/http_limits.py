"""Shared bounded-read and deadline helpers for upstream HTTP response and error streams."""

from __future__ import annotations

import http.client
import io
import socket
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

MAX_ERROR_BODY_BYTES = 65_536
DEFAULT_HTTP_TOTAL_TIMEOUT_SECONDS = 15.0


def _remaining_timeout(
    deadline_monotonic: float,
    *,
    clock: Callable[[], float] = time.monotonic,
) -> float:
    return deadline_monotonic - clock()


def _set_stream_socket_timeout(stream: Any, remaining: float) -> None:
    clamped = max(0.001, float(remaining))
    seen: set[int] = set()
    stack: list[Any] = [stream]
    while stack:
        obj = stack.pop()
        if obj is None or id(obj) in seen:
            continue
        seen.add(id(obj))
        setter = getattr(obj, "settimeout", None)
        if callable(setter):
            try:
                setter(clamped)
            except OSError:
                pass
        for attr in ("_fp", "fp", "raw", "_sock"):
            try:
                child = (
                    object.__getattribute__(obj, attr)
                    if attr == "_fp"
                    else getattr(obj, attr, None)
                )
            except Exception:
                child = None
            if child is not None and id(child) not in seen:
                stack.append(child)


class _DeadlineSocketProxy:
    """Wrap a socket/SSLSocket so every low-level recv/send/makefile enforces a monotonic deadline."""

    def __init__(
        self,
        sock: Any,
        *,
        deadline_monotonic: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._sock = sock
        self._deadline_monotonic = deadline_monotonic
        self._clock = clock

    def _enforce_before(self) -> None:
        rem = _remaining_timeout(self._deadline_monotonic, clock=self._clock)
        if rem <= 0:
            raise TimeoutError("http_response_deadline_exceeded")
        _set_stream_socket_timeout(self._sock, rem)

    def _call_with_deadline(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        self._enforce_before()
        out = getattr(self._sock, method_name)(*args, **kwargs)
        if self._clock() > self._deadline_monotonic:
            raise TimeoutError("http_response_deadline_exceeded")
        return out

    def recv(self, *args: Any, **kwargs: Any) -> bytes:
        return self._call_with_deadline("recv", *args, **kwargs)

    def recv_into(self, *args: Any, **kwargs: Any) -> int:
        return self._call_with_deadline("recv_into", *args, **kwargs)

    def send(self, *args: Any, **kwargs: Any) -> int:
        return self._call_with_deadline("send", *args, **kwargs)

    def sendall(self, *args: Any, **kwargs: Any) -> None:
        self._call_with_deadline("sendall", *args, **kwargs)

    def makefile(
        self,
        mode: str = "r",
        buffering: int | None = None,
        *,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> Any:
        if not set(mode) <= {"r", "w", "b"}:
            raise ValueError(f"invalid mode: {mode!r}")
        writing = "w" in mode
        reading = "r" in mode or not writing
        binary = "b" in mode
        rawmode = ("r" if reading else "") + ("w" if writing else "")
        raw = socket.SocketIO(self, rawmode)
        if hasattr(self._sock, "_io_refs"):
            self._sock._io_refs += 1
        if buffering is None:
            buffering = -1
        if buffering < 0:
            buffering = io.DEFAULT_BUFFER_SIZE
        if buffering == 0:
            if not binary:
                raise ValueError("unbuffered streams must be binary")
            return raw
        if reading and writing:
            buf: Any = io.BufferedRWPair(raw, raw, buffering)
        elif reading:
            buf = io.BufferedReader(raw, buffering)
        else:
            buf = io.BufferedWriter(raw, buffering)
        if binary:
            return buf
        return io.TextIOWrapper(buf, encoding=encoding, errors=errors, newline=newline)

    def _decref_socketios(self) -> None:
        decref = getattr(self._sock, "_decref_socketios", None)
        if callable(decref):
            decref()
        else:
            self._sock.close()

    def settimeout(self, timeout: float | None) -> None:
        rem = _remaining_timeout(self._deadline_monotonic, clock=self._clock)
        if rem <= 0:
            raise TimeoutError("http_response_deadline_exceeded")
        eff = rem if timeout is None else min(float(timeout), rem)
        _set_stream_socket_timeout(self._sock, eff)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._sock, name)


class _DeadlineHTTPResponse(http.client.HTTPResponse):
    """HTTPResponse whose underlying socket enforces an elapsed monotonic deadline per socket op."""

    _deadline_monotonic: float = 0.0
    _deadline_clock: Callable[[], float] = staticmethod(time.monotonic)

    def __init__(
        self,
        sock: Any,
        debuglevel: int = 0,
        method: str | None = None,
        url: str | None = None,
    ) -> None:
        wrapped_sock = _DeadlineSocketProxy(
            sock,
            deadline_monotonic=type(self)._deadline_monotonic,
            clock=type(self)._deadline_clock,
        )
        super().__init__(wrapped_sock, debuglevel=debuglevel, method=method, url=url)


class _DeadlineConnectionMixin:
    """Mixin binding connect, TLS, tunnel, send, and response phases to a total monotonic deadline."""

    _deadline_monotonic: float
    _deadline_clock: Callable[[], float]

    def _remaining_or_raise(self, label: str) -> float:
        rem = _remaining_timeout(type(self)._deadline_monotonic, clock=type(self)._deadline_clock)
        if rem <= 0:
            raise TimeoutError(label)
        return rem

    def _tighten_timeout(self, label: str) -> float:
        rem = self._remaining_or_raise(label)
        self.timeout = max(0.001, rem)
        sock = getattr(self, "sock", None)
        if sock is not None:
            _set_stream_socket_timeout(sock, rem)
        return rem

    def _tunnel(self) -> None:
        self._tighten_timeout("http_tunnel_deadline_exceeded")
        super()._tunnel()  # type: ignore[misc]
        self._remaining_or_raise("http_tunnel_deadline_exceeded")

    def send(self, data: Any) -> None:
        self._tighten_timeout("http_send_deadline_exceeded")
        super().send(data)  # type: ignore[misc]
        self._remaining_or_raise("http_send_deadline_exceeded")

    def getresponse(self) -> http.client.HTTPResponse:
        self._tighten_timeout("http_response_deadline_exceeded")
        resp = super().getresponse()  # type: ignore[misc]
        self._remaining_or_raise("http_response_deadline_exceeded")
        return resp


class _DeadlineHTTPConnection(_DeadlineConnectionMixin, http.client.HTTPConnection):
    def connect(self) -> None:
        self._tighten_timeout("http_connect_deadline_exceeded")
        super().connect()
        self._remaining_or_raise("http_connect_deadline_exceeded")


class _DeadlineHTTPSConnection(_DeadlineConnectionMixin, http.client.HTTPSConnection):
    def connect(self) -> None:
        self._tighten_timeout("http_connect_deadline_exceeded")
        http.client.HTTPConnection.connect(self)
        rem = self._remaining_or_raise("http_tls_deadline_exceeded")
        _set_stream_socket_timeout(self.sock, rem)
        server_hostname = self._tunnel_host if self._tunnel_host else self.host
        self.sock = self._context.wrap_socket(self.sock, server_hostname=server_hostname)
        self._remaining_or_raise("http_tls_deadline_exceeded")


def make_deadline_http_handlers(
    deadline_monotonic: float,
    *,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[urllib.request.HTTPHandler, urllib.request.HTTPSHandler]:
    """Build urllib HTTP/HTTPS handlers that share one monotonic deadline across connect/headers/body."""

    class _BoundResponse(_DeadlineHTTPResponse):
        _deadline_monotonic = deadline_monotonic
        _deadline_clock = staticmethod(clock)

    class _BoundHTTPConnection(_DeadlineHTTPConnection):
        response_class = _BoundResponse
        _deadline_monotonic = deadline_monotonic
        _deadline_clock = staticmethod(clock)

    class _BoundHTTPSConnection(_DeadlineHTTPSConnection):
        response_class = _BoundResponse
        _deadline_monotonic = deadline_monotonic
        _deadline_clock = staticmethod(clock)

    def _clamp_req(req: urllib.request.Request) -> None:
        rem = _remaining_timeout(deadline_monotonic, clock=clock)
        if rem <= 0:
            raise urllib.error.URLError(TimeoutError("http_total_deadline_exceeded"))
        clamped = max(0.001, rem)
        req_timeout = getattr(req, "timeout", None)
        req.timeout = min(float(req_timeout), clamped) if req_timeout is not None else clamped

    class _DeadlineHTTPHandler(urllib.request.HTTPHandler):
        def http_open(self, req: urllib.request.Request) -> Any:
            _clamp_req(req)
            return self.do_open(_BoundHTTPConnection, req)

    class _DeadlineHTTPSHandler(urllib.request.HTTPSHandler):
        def https_open(self, req: urllib.request.Request) -> Any:
            _clamp_req(req)
            return self.do_open(_BoundHTTPSConnection, req, context=self._context)

    return _DeadlineHTTPHandler(), _DeadlineHTTPSHandler()


def read_bounded_http_body(
    stream: Any,
    *,
    max_bytes: int,
    error_cls: type[Exception],
    error_prefix: str,
    content_length_header: str | None = None,
    chunk_size: int = 65_536,
    deadline_monotonic: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> bytes:
    """Stream an HTTP response body up to max_bytes and deadline_monotonic, failing closed cleanly."""
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
        raise ValueError("max_bytes must be a positive integer")
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size < 1:
        raise ValueError("chunk_size must be a positive integer")

    declared: int | None = None
    is_chunked = bool(getattr(stream, "chunked", False))
    if isinstance(content_length_header, str) and not is_chunked:
        raw_cl = content_length_header.strip()
        if raw_cl:
            parts = [p.strip() for p in raw_cl.split(",")]
            parsed_lengths: set[int] = set()
            for part in parts:
                if not part or not part.isascii() or not part.isdigit():
                    raise error_cls(f"{error_prefix}:invalid_content_length")
                parsed_lengths.add(int(part))
            if len(parsed_lengths) != 1:
                raise error_cls(f"{error_prefix}:invalid_content_length")
            declared = next(iter(parsed_lengths))
            if declared > max_bytes:
                raise error_cls(f"{error_prefix}:content_length_exceeds_{max_bytes}")

    read_fn = getattr(stream, "read1", None)
    if not callable(read_fn):
        read_fn = stream.read

    chunks: list[bytes] = []
    total = 0
    while True:
        if deadline_monotonic is not None:
            rem = _remaining_timeout(deadline_monotonic, clock=clock)
            if rem <= 0:
                raise error_cls(f"{error_prefix}:deadline_exceeded")
            _set_stream_socket_timeout(stream, rem)
        to_read = min(chunk_size, (max_bytes - total) + 1)
        try:
            chunk = read_fn(to_read)
        except TimeoutError as exc:
            raise error_cls(f"{error_prefix}:deadline_exceeded") from exc
        except http.client.IncompleteRead as exc:
            raise error_cls(f"{error_prefix}:incomplete_body") from exc
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
        if total > max_bytes:
            raise error_cls(f"{error_prefix}:response_exceeds_{max_bytes}_bytes")
        if deadline_monotonic is not None and clock() > deadline_monotonic:
            raise error_cls(f"{error_prefix}:deadline_exceeded")
    if not is_chunked:
        if declared is not None and total < declared:
            raise error_cls(f"{error_prefix}:incomplete_body:{total}_of_{declared}")
        remaining = getattr(stream, "length", None)
        if isinstance(remaining, int) and not isinstance(remaining, bool) and remaining > 0:
            raise error_cls(f"{error_prefix}:incomplete_body:remaining_{remaining}")
    return b"".join(chunks)


def read_bounded_error_body(
    exc: urllib.error.HTTPError,
    *,
    max_bytes: int = MAX_ERROR_BODY_BYTES,
    deadline_monotonic: float | None = None,
    clock: Callable[[], float] = time.monotonic,
    chunk_size: int = 8_192,
) -> bytes:
    """Read at most min(max_bytes, MAX_ERROR_BODY_BYTES) within deadline from an HTTPError body stream."""
    try:
        fp = getattr(exc, "fp", None)
        if fp is None:
            return b""
        capped = max(1, min(int(max_bytes), MAX_ERROR_BODY_BYTES))
        stream = fp if callable(getattr(fp, "read1", None)) else exc
        read_fn = getattr(stream, "read1", None)
        if not callable(read_fn):
            read_fn = getattr(stream, "read", None)
        if not callable(read_fn):
            return b""
        chunks: list[bytes] = []
        total = 0
        while total < capped:
            if deadline_monotonic is not None:
                rem = _remaining_timeout(deadline_monotonic, clock=clock)
                if rem <= 0:
                    break
                _set_stream_socket_timeout(stream, rem)
            to_read = min(chunk_size, capped - total)
            try:
                chunk = read_fn(to_read)
            except (TimeoutError, OSError):
                break
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if deadline_monotonic is not None and clock() > deadline_monotonic:
                break
        return b"".join(chunks)[:capped]
    finally:
        exc.close()
