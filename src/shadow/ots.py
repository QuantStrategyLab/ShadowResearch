"""OpenTimestamps (.ots) detached binary receipt interface and offline verifier."""

from __future__ import annotations

import hashlib
import http.client
import json
import struct
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from shadow.http_limits import (
    make_deadline_http_handlers,
    read_bounded_error_body,
    read_bounded_http_body,
)

OTS_HEADER_MAGIC = b"\x00OpenTimestamps\x00\x00Proof\x00\xbf\x89\xe2\xe8\x84\xe8\x92\x94"
OTS_VERSION = 1

OP_SHA1 = 0x02
OP_RIPEMD160 = 0x03
OP_SHA256 = 0x08
OP_APPEND = 0xF0
OP_PREPEND = 0xF1
OP_REVERSE = 0xF2
OP_HEXLIFY = 0xF3

TAG_ATTESTATION = 0x00
TAG_FORK = 0xFF

PENDING_ATTESTATION_TAG = bytes.fromhex("83dfe30d2ef90c8e")
BITCOIN_ATTESTATION_TAG = bytes.fromhex("0588960d73d71901")

DEFAULT_OTS_CALENDARS: tuple[str, ...] = (
    "https://a.pool.opentimestamps.org",
    "https://b.pool.opentimestamps.org",
    "https://alice.btc.calendar.opentimestamps.org",
    "https://bob.btc.calendar.opentimestamps.org",
)

OtsTransport = Callable[[str, str, bytes | None, Mapping[str, str], float], tuple[int, bytes]]


class OtsFormatError(ValueError):
    """Raised when an .ots receipt payload violates the OpenTimestamps binary format."""


class OtsSubmitError(RuntimeError):
    """Raised when submitting or upgrading an OpenTimestamps digest fails across calendars."""


@dataclass(frozen=True)
class OtsAttestation:
    """Single parsed terminal attestation inside an .ots commitment tree."""

    kind: str  # "pending" | "bitcoin" | "unknown"
    commitment_bytes: bytes
    calendar_uri: str | None = None
    bitcoin_height: int | None = None
    raw_tag_hex: str = ""


@dataclass(frozen=True)
class OtsProof:
    """Parsed .ots binary proof anchored to a SHA-256 digest."""

    version: int
    digest_bytes: bytes
    digest_hex: str
    attestations: tuple[OtsAttestation, ...]

    @property
    def pending_calendars(self) -> tuple[str, ...]:
        return tuple(
            att.calendar_uri
            for att in self.attestations
            if att.kind == "pending" and att.calendar_uri is not None
        )

    @property
    def bitcoin_attestations(self) -> tuple[OtsAttestation, ...]:
        return tuple(att for att in self.attestations if att.kind == "bitcoin")

    @property
    def upgrade_state(self) -> str:
        # Format parsing alone can never certify Bitcoin confirmation without trusted headers.
        return "bitcoin_unverified" if self.bitcoin_attestations else "pending_calendar"


def _encode_varuint(value: int) -> bytes:
    if value < 0:
        raise ValueError(f"varuint must be non-negative, got {value}")
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _encode_varbytes(data: bytes) -> bytes:
    return _encode_varuint(len(data)) + data


class _ByteCursor:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0

    @property
    def remaining(self) -> int:
        return len(self.data) - self.pos

    def read_byte(self) -> int:
        if self.pos >= len(self.data):
            raise OtsFormatError("unexpected_eof_in_ots")
        val = self.data[self.pos]
        self.pos += 1
        return val

    def read_exact(self, n: int) -> bytes:
        if n < 0 or self.pos + n > len(self.data):
            raise OtsFormatError("unexpected_eof_reading_bytes")
        chunk = self.data[self.pos : self.pos + n]
        self.pos += n
        return chunk

    def read_varuint(self) -> int:
        shift = 0
        result = 0
        while True:
            b = self.read_byte()
            result |= (b & 0x7F) << shift
            if not (b & 0x80):
                return result
            shift += 7
            if shift > 63:
                raise OtsFormatError("varuint_overflow")

    def read_varbytes(self, max_len: int = 1048576) -> bytes:
        length = self.read_varuint()
        if length > max_len:
            raise OtsFormatError(f"varbytes_length_exceeds_limit:{length}")
        return self.read_exact(length)


def _parse_attestation(cur: _ByteCursor, msg: bytes) -> OtsAttestation:
    tag = cur.read_exact(8)
    payload = cur.read_varbytes(max_len=4096)
    inner = _ByteCursor(payload)

    if tag == PENDING_ATTESTATION_TAG:
        uri_bytes = inner.read_varbytes(max_len=1000)
        if inner.remaining != 0:
            raise OtsFormatError("trailing_bytes_in_pending_attestation")
        try:
            uri = uri_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise OtsFormatError("invalid_utf8_calendar_uri") from exc
        if not uri.startswith(("http://", "https://")):
            raise OtsFormatError(f"invalid_calendar_uri:{uri!r}")
        return OtsAttestation(
            kind="pending",
            commitment_bytes=msg,
            calendar_uri=uri,
            raw_tag_hex=tag.hex(),
        )

    if tag == BITCOIN_ATTESTATION_TAG:
        height = inner.read_varuint()
        if inner.remaining != 0:
            raise OtsFormatError("trailing_bytes_in_bitcoin_attestation")
        return OtsAttestation(
            kind="bitcoin",
            commitment_bytes=msg,
            bitcoin_height=height,
            raw_tag_hex=tag.hex(),
        )

    return OtsAttestation(
        kind="unknown",
        commitment_bytes=msg,
        raw_tag_hex=tag.hex(),
    )


MAX_OP_BYTES = 4096
MAX_TREE_DEPTH = 256
MAX_TREE_OPS = 4096


def _apply_op(op_tag: int, cur: _ByteCursor, msg: bytes) -> bytes:
    if len(msg) > MAX_OP_BYTES:
        raise OtsFormatError("ots_op_input_exceeds_4096_bytes")
    if op_tag == OP_APPEND:
        arg = cur.read_varbytes(max_len=MAX_OP_BYTES)
        res = msg + arg
    elif op_tag == OP_PREPEND:
        arg = cur.read_varbytes(max_len=MAX_OP_BYTES)
        res = arg + msg
    elif op_tag == OP_REVERSE:
        res = msg[::-1]
    elif op_tag == OP_HEXLIFY:
        res = msg.hex().encode("ascii")
    elif op_tag == OP_SHA256:
        res = hashlib.sha256(msg).digest()
    elif op_tag == OP_SHA1:
        res = hashlib.sha1(msg).digest()  # noqa: S324 - OTS protocol opcode
    elif op_tag == OP_RIPEMD160:
        h = hashlib.new("ripemd160")
        h.update(msg)
        res = h.digest()
    else:
        raise OtsFormatError(f"unsupported_ots_opcode:0x{op_tag:02x}")
    if len(res) > MAX_OP_BYTES:
        raise OtsFormatError("ots_op_result_exceeds_4096_bytes")
    return res


def _parse_timestamp_tree(
    cur: _ByteCursor,
    msg: bytes,
    out: list[tuple[OtsAttestation, int, int]],
    depth: int = 0,
    op_counter: list[int] | None = None,
) -> None:
    if depth > MAX_TREE_DEPTH:
        raise OtsFormatError("ots_recursion_depth_exceeded")
    counter = op_counter if op_counter is not None else [0]
    while True:
        start_pos = cur.pos
        tag = cur.read_byte()
        if tag == TAG_FORK:
            _parse_timestamp_tree(cur, msg, out, depth + 1, counter)
            continue
        if tag == TAG_ATTESTATION:
            att = _parse_attestation(cur, msg)
            out.append((att, start_pos, cur.pos))
            return
        counter[0] += 1
        if counter[0] > MAX_TREE_OPS:
            raise OtsFormatError("ots_op_count_exceeded")
        next_msg = _apply_op(tag, cur, msg)
        _parse_timestamp_tree(cur, next_msg, out, depth + 1, counter)
        return


def _parse_proof_with_spans(ots_bytes: bytes) -> tuple[OtsProof, list[tuple[OtsAttestation, int, int]]]:
    if not isinstance(ots_bytes, (bytes, bytearray)):
        raise OtsFormatError("ots_bytes_must_be_bytes")
    raw = bytes(ots_bytes)
    if not raw.startswith(OTS_HEADER_MAGIC):
        raise OtsFormatError("invalid_ots_magic_header")

    cur = _ByteCursor(raw[len(OTS_HEADER_MAGIC) :])
    version = cur.read_varuint()
    if version != OTS_VERSION:
        raise OtsFormatError(f"unsupported_ots_version:{version}")

    hash_op = cur.read_byte()
    if hash_op != OP_SHA256:
        raise OtsFormatError(f"unsupported_ots_file_hash_op:0x{hash_op:02x}")

    digest_bytes = cur.read_exact(32)
    spans: list[tuple[OtsAttestation, int, int]] = []
    _parse_timestamp_tree(cur, digest_bytes, spans)
    if cur.remaining != 0:
        raise OtsFormatError(f"trailing_bytes_after_ots_tree:{cur.remaining}")
    if not spans:
        raise OtsFormatError("ots_contains_no_attestations")

    offset = len(OTS_HEADER_MAGIC)
    abs_spans = [(att, offset + s, offset + e) for att, s, e in spans]
    proof = OtsProof(
        version=version,
        digest_bytes=digest_bytes,
        digest_hex=digest_bytes.hex(),
        attestations=tuple(att for att, _, _ in abs_spans),
    )
    return proof, abs_spans


def parse_ots_proof(ots_bytes: bytes) -> OtsProof:
    """Parse and structurally verify an OpenTimestamps .ots detached binary receipt."""
    proof, _ = _parse_proof_with_spans(ots_bytes)
    return proof


def _parse_block_timestamp_utc(ts: object) -> str:
    if isinstance(ts, bool):
        raise ValueError("Bitcoin block timestamp cannot be a bool")
    if isinstance(ts, int):
        if ts < 0 or ts > 0xFFFFFFFF:
            raise ValueError(f"Bitcoin block epoch out of uint32 range: {ts}")
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    if isinstance(ts, str):
        s = ts.strip()
        if s.isdigit():
            return _parse_block_timestamp_utc(int(s))
        norm = s[:-1] + "+00:00" if s.endswith("Z") else s
        dt = datetime.fromisoformat(norm)
        if dt.tzinfo is None:
            raise ValueError(f"Bitcoin block timestamp must be timezone-aware: {ts!r}")
        utc = dt.astimezone(timezone.utc)
        return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"
    raise ValueError(f"Unsupported Bitcoin block timestamp type: {type(ts).__name__}")


def _normalize_trusted_header_entry(height: int, val: object) -> tuple[str, str | None]:
    """Normalize trusted block entry into (internal_merkle_root_hex, block_time_utc_ms_or_None)."""
    if isinstance(val, Mapping):
        raw_hdr = val.get("raw_header") or val.get("header")
        explicit_root = val.get("merkle_root")
        explicit_ts = (
            val.get("block_time_utc")
            if val.get("block_time_utc") is not None
            else (val.get("timestamp") if val.get("timestamp") is not None else val.get("time"))
        )
        if raw_hdr is not None:
            hdr_hex = str(raw_hdr).strip().lower()
            if len(hdr_hex) != 160:
                raise ValueError(f"Invalid 80-byte raw_header hex length at height {height}")
            header_bytes = bytes.fromhex(hdr_hex)
            root_hex = header_bytes[36:68].hex()
            epoch_sec = struct.unpack("<I", header_bytes[68:72])[0]
            hdr_ts = _parse_block_timestamp_utc(epoch_sec)
            if explicit_root is not None and str(explicit_root).strip().lower() != root_hex:
                raise ValueError(f"Header merkle_root mismatch at height {height}")
            if explicit_ts is not None and _parse_block_timestamp_utc(explicit_ts) != hdr_ts:
                raise ValueError(f"Header timestamp mismatch at height {height}")
            return root_hex, hdr_ts
        if explicit_root is not None:
            root_hex = str(explicit_root).strip().lower()
            if len(root_hex) != 64:
                raise ValueError(f"Invalid Merkle root hex length at height {height}")
            bytes.fromhex(root_hex)
            ts_utc = _parse_block_timestamp_utc(explicit_ts) if explicit_ts is not None else None
            return root_hex, ts_utc
        raise ValueError(f"Missing merkle_root/raw_header at height {height}")

    hex_str = str(val).strip().lower()
    if len(hex_str) == 160:
        # 80-byte raw Bitcoin header: [36:68] Merkle root (internal OTS byte order), [68:72] uint32 LE nTime
        header_bytes = bytes.fromhex(hex_str)
        epoch_sec = struct.unpack("<I", header_bytes[68:72])[0]
        return header_bytes[36:68].hex(), _parse_block_timestamp_utc(epoch_sec)
    if len(hex_str) == 64:
        bytes.fromhex(hex_str)
        return hex_str, None
    raise ValueError(f"Invalid Merkle root or 80-byte header hex at height {height}")


def load_trusted_bitcoin_headers(headers_path: str | Path) -> dict[int, dict[str, object]]:
    """Load trusted Bitcoin height -> {merkle_root, block_time_utc} from JSON (80B headers or root/timestamp)."""
    raw = json.loads(Path(headers_path).read_text(encoding="utf-8"))
    source = raw.get("merkle_roots") or raw.get("headers") or raw
    if not isinstance(source, dict):
        raise ValueError("trusted_bitcoin_headers JSON must be an object mapping height to hex or entry dict")
    out: dict[int, dict[str, object]] = {}
    for k, v in source.items():
        height = int(k)
        root_hex, ts_utc = _normalize_trusted_header_entry(height, v)
        entry: dict[str, object] = {"merkle_root": root_hex}
        if ts_utc is not None:
            entry["block_time_utc"] = ts_utc
        out[height] = entry
    return out


def verify_ots_bytes(
    ots_bytes: bytes,
    expected_record_sha256: str,
    *,
    bitcoin_merkle_roots: Mapping[int, object] | None = None,
    require_bitcoin_verification: bool = False,
    require_bitcoin_timestamp: bool = False,
    max_block_time_utc: str | None = None,
) -> tuple[bool, dict | str]:
    """Verify .ots bytes offline; Bitcoin confirmation requires trusted block headers/roots."""
    try:
        proof = parse_ots_proof(ots_bytes)
    except OtsFormatError as exc:
        return False, f"ots_parse_error:{exc}"

    if proof.digest_hex != expected_record_sha256.lower():
        return (
            False,
            f"ots_digest_mismatch:expected={expected_record_sha256.lower()},got={proof.digest_hex}",
        )

    valid_known = [att for att in proof.attestations if att.kind in ("pending", "bitcoin")]
    if not valid_known:
        return False, "ots_no_supported_attestations"

    btc_atts = proof.bitcoin_attestations
    bitcoin_verified = False
    verified_heights: list[int] = []
    verified_block_times: list[str] = []
    upgrade_state = proof.upgrade_state  # "pending_calendar" or "bitcoin_unverified"

    if bitcoin_merkle_roots is not None:
        if not btc_atts:
            if require_bitcoin_verification:
                return False, "ots_missing_bitcoin_attestation_for_header_check"
        else:
            matched_roots: list[tuple[int, str | None]] = []
            for att in btc_atts:
                assert att.bitcoin_height is not None
                raw_entry = bitcoin_merkle_roots.get(att.bitcoin_height)
                if raw_entry is None:
                    continue
                try:
                    expected_root, block_time_utc = _normalize_trusted_header_entry(
                        att.bitcoin_height, raw_entry
                    )
                except ValueError as exc:
                    return False, f"ots_invalid_trusted_header:{exc}"
                if att.commitment_bytes.hex() == expected_root:
                    matched_roots.append((att.bitcoin_height, block_time_utc))

            if not matched_roots:
                return False, "ots_bitcoin_merkle_root_mismatch"

            if require_bitcoin_timestamp:
                with_ts = [(h, ts) for h, ts in matched_roots if ts is not None]
                if not with_ts:
                    return False, "ots_missing_trusted_bitcoin_block_timestamp"
                if max_block_time_utc is not None:
                    timely = [(h, ts) for h, ts in with_ts if ts <= max_block_time_utc]
                    if not timely:
                        return False, "ots_bitcoin_block_time_after_commitment_deadline"
                    matched_roots = timely
                else:
                    matched_roots = with_ts
            elif max_block_time_utc is not None:
                with_ts = [(h, ts) for h, ts in matched_roots if ts is not None]
                if with_ts:
                    timely = [(h, ts) for h, ts in with_ts if ts <= max_block_time_utc]
                    if not timely:
                        return False, "ots_bitcoin_block_time_after_commitment_deadline"
                    matched_roots = [(h, ts) for h, ts in matched_roots if ts is None or ts <= max_block_time_utc]

            bitcoin_verified = True
            verified_heights = [h for h, _ in matched_roots]
            verified_block_times = [ts for _, ts in matched_roots if ts is not None]
            upgrade_state = "bitcoin_confirmed"
    elif require_bitcoin_verification:
        if not btc_atts:
            return False, "ots_pending_no_bitcoin_attestation"
        return False, "ots_missing_trusted_bitcoin_headers"

    return True, {
        "digest_hex": proof.digest_hex,
        "upgrade_state": upgrade_state,
        "bitcoin_verified": bitcoin_verified,
        "verified_bitcoin_heights": verified_heights,
        "verified_bitcoin_block_times_utc": verified_block_times,
        "verified_bitcoin_block_time_utc": min(verified_block_times) if verified_block_times else None,
        "pending_calendars": list(proof.pending_calendars),
        "bitcoin_heights": [a.bitcoin_height for a in btc_atts],
        "attestation_count": len(proof.attestations),
    }


def build_ots_file_from_timestamp_body(record_sha256: str, timestamp_body: bytes) -> bytes:
    """Wrap a calendar /digest response body into a complete .ots file and verify it."""
    digest_bytes = bytes.fromhex(record_sha256)
    if len(digest_bytes) != 32:
        raise ValueError("record_sha256 must decode to 32 bytes")
    ots_bytes = (
        OTS_HEADER_MAGIC
        + _encode_varuint(OTS_VERSION)
        + bytes([OP_SHA256])
        + digest_bytes
        + timestamp_body
    )
    parse_ots_proof(ots_bytes)
    return ots_bytes


def build_synthetic_ots_fixture(
    record_sha256: str,
    *,
    calendar_uri: str = "https://a.pool.opentimestamps.org",
    bitcoin_height: int | None = None,
    nonce: bytes = b"shadow-n-sh1-ots",
) -> tuple[bytes, str | None]:
    """Build a valid offline .ots proof (pending or upgraded Bitcoin) for test fixtures."""
    digest_bytes = bytes.fromhex(record_sha256)
    if len(digest_bytes) != 32:
        raise ValueError("record_sha256 must decode to 32 bytes")

    # Path: append(nonce) -> sha256 -> commitment
    c1 = hashlib.sha256(digest_bytes + nonce).digest()
    pending_payload = _encode_varbytes(calendar_uri.encode("utf-8"))
    pending_branch = (
        bytes([OP_APPEND])
        + _encode_varbytes(nonce)
        + bytes([OP_SHA256])
        + bytes([TAG_ATTESTATION])
        + PENDING_ATTESTATION_TAG
        + _encode_varbytes(pending_payload)
    )

    if bitcoin_height is None:
        ots_bytes = build_ots_file_from_timestamp_body(record_sha256, pending_branch)
        return ots_bytes, None

    # Fork with Bitcoin Merkle branch: prepend(sibling) -> sha256 -> sha256 -> BTC header attestation
    sibling = hashlib.sha256(b"btc-merkle-sibling").digest()
    merkle_root_bytes = hashlib.sha256(hashlib.sha256(sibling + c1).digest()).digest()
    btc_payload = _encode_varuint(bitcoin_height)
    btc_branch = (
        bytes([OP_APPEND])
        + _encode_varbytes(nonce)
        + bytes([OP_SHA256])
        + bytes([OP_PREPEND])
        + _encode_varbytes(sibling)
        + bytes([OP_SHA256, OP_SHA256])
        + bytes([TAG_ATTESTATION])
        + BITCOIN_ATTESTATION_TAG
        + _encode_varbytes(btc_payload)
    )
    combined_body = bytes([TAG_FORK]) + pending_branch + btc_branch
    ots_bytes = build_ots_file_from_timestamp_body(record_sha256, combined_body)
    return ots_bytes, merkle_root_bytes.hex()


class _NoOtsRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Fail closed on HTTP 3xx redirects so calendar URI allowlists cannot be bypassed."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        raise OtsSubmitError(f"ots_redirect_rejected:{code}:{newurl}")


DEFAULT_MAX_OTS_RESPONSE_BYTES = 262_144


def urllib_ots_transport(
    method: str,
    url: str,
    body: bytes | None,
    headers: Mapping[str, str],
    timeout: float,
    *,
    opener: urllib.request.OpenerDirector | None = None,
    max_bytes: int = DEFAULT_MAX_OTS_RESPONSE_BYTES,
    deadline_monotonic: float | None = None,
) -> tuple[int, bytes]:
    """Perform stdlib HTTP POST/GET for OpenTimestamps calendar servers without following redirects."""
    now = time.monotonic()
    req_deadline = now + float(timeout)
    eff_deadline = (
        min(req_deadline, deadline_monotonic)
        if deadline_monotonic is not None
        else req_deadline
    )
    if eff_deadline <= now:
        raise OtsSubmitError("ots_transport_error:deadline_exceeded")
    req = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    if opener is None:
        http_handler, https_handler = make_deadline_http_handlers(eff_deadline)
        active_opener = urllib.request.build_opener(
            http_handler,
            https_handler,
            _NoOtsRedirectHandler(),
        )
    else:
        active_opener = opener
    try:
        remaining = max(0.001, eff_deadline - time.monotonic())
        with active_opener.open(req, timeout=remaining) as resp:
            content_len = (
                resp.headers.get("Content-Length") if getattr(resp, "headers", None) is not None else None
            )
            payload = read_bounded_http_body(
                resp,
                max_bytes=max_bytes,
                error_cls=OtsSubmitError,
                error_prefix="ots_response_too_large",
                content_length_header=content_len,
                deadline_monotonic=eff_deadline,
            )
            return int(resp.status), payload
    except urllib.error.HTTPError as exc:
        err_body = read_bounded_error_body(
            exc,
            max_bytes=max_bytes,
            deadline_monotonic=eff_deadline,
        )
        return int(exc.code), err_body
    except (urllib.error.URLError, http.client.HTTPException, TimeoutError, OSError) as exc:
        raise OtsSubmitError(f"ots_transport_error:{type(exc).__name__}") from exc


class OpenTimestampsClient:
    """Detached OpenTimestamps client with injectable transport for offline verification."""

    def __init__(
        self,
        *,
        calendars: Sequence[str] = DEFAULT_OTS_CALENDARS,
        transport: OtsTransport | None = None,
        timeout: float = 10.0,
        deadline_monotonic: float | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.calendars = tuple(c.rstrip("/") for c in calendars)
        if not self.calendars:
            raise ValueError("At least one OpenTimestamps calendar URL is required")
        self._transport = transport if transport is not None else urllib_ots_transport
        self.timeout = float(timeout)
        self.deadline_monotonic = deadline_monotonic
        self._clock = clock if clock is not None else time.monotonic

    def _remaining_request_timeout(self) -> float | None:
        if self.deadline_monotonic is None:
            return self.timeout
        rem = self.deadline_monotonic - self._clock()
        if rem <= 0:
            return None
        return min(self.timeout, rem)

    def stamp_digest(self, record_sha256: str) -> tuple[bytes, dict]:
        """Submit record_sha256 to calendars and return (ots_bytes, proof_metadata)."""
        digest_bytes = bytes.fromhex(record_sha256)
        if len(digest_bytes) != 32:
            raise ValueError("record_sha256 must decode to 32 bytes")

        headers = {
            "Accept": "application/vnd.opentimestamps.v1",
            "User-Agent": "ShadowResearch-SH1/1.0",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        timestamp_bodies: list[bytes] = []
        used_calendars: list[str] = []
        errors: list[str] = []

        for cal_url in self.calendars:
            req_timeout = self._remaining_request_timeout()
            if req_timeout is None:
                errors.append(f"{cal_url}:ots_budget_exhausted")
                break
            endpoint = f"{cal_url}/digest"
            try:
                status, resp_bytes = self._transport("POST", endpoint, digest_bytes, headers, req_timeout)
            except Exception as exc:
                errors.append(f"{cal_url}:{exc}")
                continue
            if status != 200 or not resp_bytes:
                errors.append(f"{cal_url}:http_{status}")
                continue
            try:
                candidate_ots = build_ots_file_from_timestamp_body(record_sha256, resp_bytes)
            except OtsFormatError as exc:
                errors.append(f"{cal_url}:invalid_ots_{exc}")
                continue
            timestamp_bodies.append(resp_bytes)
            used_calendars.append(cal_url)
            if len(timestamp_bodies) >= 2:
                break

        if not timestamp_bodies:
            raise OtsSubmitError("all_ots_calendars_failed:" + ";".join(errors))

        if len(timestamp_bodies) == 1:
            combined_body = timestamp_bodies[0]
        else:
            combined_body = b""
            for branch in timestamp_bodies[:-1]:
                combined_body += bytes([TAG_FORK]) + branch
            combined_body += timestamp_bodies[-1]

        ots_bytes = build_ots_file_from_timestamp_body(record_sha256, combined_body)
        ok, meta = verify_ots_bytes(ots_bytes, record_sha256)
        if not ok or not isinstance(meta, dict):
            raise OtsSubmitError(f"ots_self_verification_failed:{meta}")
        meta["submitted_calendars"] = used_calendars
        meta["ots_sha256"] = hashlib.sha256(ots_bytes).hexdigest()
        return ots_bytes, meta

    def upgrade_ots(self, ots_bytes: bytes) -> tuple[bytes, bool]:
        """Query calendar /timestamp/<commitment> for pending attestations; return (ots, upgraded)."""
        proof, spans = _parse_proof_with_spans(ots_bytes)
        if proof.bitcoin_attestations:
            return ots_bytes, False

        headers = {"Accept": "application/vnd.opentimestamps.v1"}
        allowed_calendars = set(self.calendars)
        replacements: list[tuple[int, int, bytes]] = []
        for att, start_idx, end_idx in spans:
            if att.kind != "pending" or not att.calendar_uri:
                continue
            cal_url = att.calendar_uri.rstrip("/")
            if cal_url not in allowed_calendars:
                continue
            commit_hex = att.commitment_bytes.hex()
            endpoint = f"{cal_url}/timestamp/{commit_hex}"
            req_timeout = self._remaining_request_timeout()
            if req_timeout is None:
                break
            try:
                status, resp_bytes = self._transport("GET", endpoint, None, headers, req_timeout)
            except Exception:
                continue
            if status != 200 or not resp_bytes:
                continue
            try:
                cur = _ByteCursor(resp_bytes)
                parsed_atts: list[tuple[OtsAttestation, int, int]] = []
                _parse_timestamp_tree(cur, att.commitment_bytes, parsed_atts)
            except OtsFormatError:
                continue
            if cur.remaining == 0 and any(a.kind == "bitcoin" for a, _, _ in parsed_atts):
                replacements.append((start_idx, end_idx, resp_bytes))

        if not replacements:
            return ots_bytes, False

        updated = bytearray(ots_bytes)
        for start_idx, end_idx, resp_bytes in sorted(replacements, reverse=True):
            updated[start_idx:end_idx] = resp_bytes
        final_bytes = bytes(updated)
        parse_ots_proof(final_bytes)
        return final_bytes, True
