"""Offline unit tests for OpenTimestamps (.ots) proof parsing, client, upgrade, and verification."""

from __future__ import annotations

import hashlib
import http.client
import io
import json
import struct
import sys
import tempfile
import unittest
import urllib.request
from pathlib import Path

_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.ots import (
    BITCOIN_ATTESTATION_TAG,
    OTS_HEADER_MAGIC,
    OP_HEXLIFY,
    OP_SHA256,
    OpenTimestampsClient,
    OtsFormatError,
    OtsSubmitError,
    TAG_ATTESTATION,
    _NoOtsRedirectHandler,
    _encode_varbytes,
    _encode_varuint,
    build_ots_file_from_timestamp_body,
    build_synthetic_ots_fixture,
    load_trusted_bitcoin_headers,
    parse_ots_proof,
    urllib_ots_transport,
    verify_ots_bytes,
)


class OpenTimestampsTest(unittest.TestCase):
    """Verify .ots binary serialization, offline Merkle verification, stamping, and upgrading."""

    def test_pending_and_bitcoin_ots_fixtures_verify_offline(self) -> None:
        digest = hashlib.sha256(b"shadow-ledger-record-0").hexdigest()
        pending_ots, root_none = build_synthetic_ots_fixture(digest)
        self.assertIsNone(root_none)
        self.assertTrue(pending_ots.startswith(OTS_HEADER_MAGIC))

        ok, meta = verify_ots_bytes(pending_ots, digest)
        self.assertTrue(ok, meta)
        assert isinstance(meta, dict)
        self.assertEqual(meta["upgrade_state"], "pending_calendar")
        self.assertEqual(meta["pending_calendars"], ["https://a.pool.opentimestamps.org"])

        btc_ots, merkle_root_hex = build_synthetic_ots_fixture(digest, bitcoin_height=870123)
        assert merkle_root_hex is not None
        ok_btc, meta_btc = verify_ots_bytes(
            btc_ots,
            digest,
            bitcoin_merkle_roots={870123: merkle_root_hex},
        )
        self.assertTrue(ok_btc, meta_btc)
        assert isinstance(meta_btc, dict)
        self.assertEqual(meta_btc["upgrade_state"], "bitcoin_confirmed")
        self.assertEqual(meta_btc["bitcoin_heights"], [870123])

        # Wrong Merkle root fails verification
        ok_bad_root, err_bad_root = verify_ots_bytes(
            btc_ots,
            digest,
            bitcoin_merkle_roots={870123: "0" * 64},
        )
        self.assertFalse(ok_bad_root)
        self.assertEqual(err_bad_root, "ots_bitcoin_merkle_root_mismatch")

        # Wrong digest fails verification
        wrong_digest = hashlib.sha256(b"other-record").hexdigest()
        ok_wrong_dig, err_wrong_dig = verify_ots_bytes(pending_ots, wrong_digest)
        self.assertFalse(ok_wrong_dig)
        self.assertIn("ots_digest_mismatch", str(err_wrong_dig))

    def test_tampered_and_malformed_ots_rejected(self) -> None:
        digest = hashlib.sha256(b"shadow-ledger-record-1").hexdigest()
        ots_bytes, _ = build_synthetic_ots_fixture(digest)

        with self.assertRaises(OtsFormatError):
            parse_ots_proof(b"not-an-ots-file")

        with self.assertRaises(OtsFormatError):
            parse_ots_proof(ots_bytes + b"\x00")

        tampered = bytearray(ots_bytes)
        tampered[-1] ^= 0xFF
        ok, _ = verify_ots_bytes(bytes(tampered), digest)
        self.assertFalse(ok)

    def test_client_stamp_and_upgrade_with_mocked_transport(self) -> None:
        digest = hashlib.sha256(b"shadow-ledger-record-2").hexdigest()
        cal_a_ots, _ = build_synthetic_ots_fixture(
            digest, calendar_uri="https://a.pool.opentimestamps.org", nonce=b"nonce-a"
        )
        cal_b_ots, _ = build_synthetic_ots_fixture(
            digest, calendar_uri="https://b.pool.opentimestamps.org", nonce=b"nonce-b"
        )
        header_len = len(OTS_HEADER_MAGIC) + 2 + 32
        body_a = cal_a_ots[header_len:]
        body_b = cal_b_ots[header_len:]

        # Build upgraded Bitcoin branch body starting at commitment c1 = sha256(digest + nonce-a)
        # Matching build_synthetic_ots_fixture(bitcoin_height=870200)
        btc_full, btc_merkle = build_synthetic_ots_fixture(
            digest,
            calendar_uri="https://a.pool.opentimestamps.org",
            bitcoin_height=870200,
            nonce=b"nonce-a",
        )
        assert btc_merkle is not None
        # Extract the BTC sub-branch starting after append(nonce)+sha256:
        proof_a = parse_ots_proof(cal_a_ots)
        commit_hex_a = proof_a.attestations[0].commitment_bytes.hex()
        # Construct calendar /timestamp response: prepend(sibling)+sha256+sha256+attestation(BTC)
        sibling = hashlib.sha256(b"btc-merkle-sibling").digest()
        from shadow.ots import OP_PREPEND, OP_SHA256, TAG_ATTESTATION, _encode_varbytes, _encode_varuint

        upgrade_resp_body = (
            bytes([OP_PREPEND])
            + _encode_varbytes(sibling)
            + bytes([OP_SHA256, OP_SHA256, TAG_ATTESTATION])
            + BITCOIN_ATTESTATION_TAG
            + _encode_varbytes(_encode_varuint(870200))
        )

        calls: list[tuple[str, str]] = []

        def mock_transport(method: str, url: str, body: bytes | None, headers: dict, timeout: float) -> tuple[int, bytes]:
            calls.append((method, url))
            if url == "https://a.pool.opentimestamps.org/digest":
                return 200, body_a
            if url == "https://b.pool.opentimestamps.org/digest":
                return 200, body_b
            if url == f"https://a.pool.opentimestamps.org/timestamp/{commit_hex_a}":
                return 200, upgrade_resp_body
            return 404, b""

        client = OpenTimestampsClient(
            calendars=(
                "https://a.pool.opentimestamps.org",
                "https://b.pool.opentimestamps.org",
            ),
            transport=mock_transport,
        )
        ots_bytes, meta = client.stamp_digest(digest)
        self.assertEqual(meta["submitted_calendars"], [
            "https://a.pool.opentimestamps.org",
            "https://b.pool.opentimestamps.org",
        ])
        self.assertEqual(meta["upgrade_state"], "pending_calendar")
        self.assertEqual(len(meta["pending_calendars"]), 2)

        upgraded_ots, did_upgrade = client.upgrade_ots(ots_bytes)
        self.assertTrue(did_upgrade)
        ok_upg, meta_upg = verify_ots_bytes(
            upgraded_ots,
            digest,
            bitcoin_merkle_roots={870200: btc_merkle},
        )
        self.assertTrue(ok_upg, meta_upg)
        assert isinstance(meta_upg, dict)
        self.assertEqual(meta_upg["upgrade_state"], "bitcoin_confirmed")
        self.assertEqual(meta_upg["bitcoin_heights"], [870200])

        # Failing all calendars raises OtsSubmitError
        failing_client = OpenTimestampsClient(
            calendars=("https://a.pool.opentimestamps.org",),
            transport=lambda m, u, b, h, t: (503, b"unavailable"),
        )
        with self.assertRaises(OtsSubmitError):
            failing_client.stamp_digest(digest)

    def test_bitcoin_attestation_requires_trusted_merkle_root_verification(self) -> None:
        # Finding #3 regression: strictly distinguish pending / bitcoin_unverified / bitcoin_confirmed
        digest = hashlib.sha256(b"shadow-ledger-record-creditability").hexdigest()
        pending_ots, _ = build_synthetic_ots_fixture(digest)
        btc_ots, btc_root = build_synthetic_ots_fixture(digest, bitcoin_height=870333)
        assert btc_root is not None

        # Pending-only OTS passes structural format check, fails require_bitcoin_verification
        ok_fmt, meta_fmt = verify_ots_bytes(pending_ots, digest, require_bitcoin_verification=False)
        self.assertTrue(ok_fmt)
        assert isinstance(meta_fmt, dict)
        self.assertEqual(meta_fmt["upgrade_state"], "pending_calendar")
        self.assertFalse(meta_fmt["bitcoin_verified"])

        ok_req, err_req = verify_ots_bytes(pending_ots, digest, require_bitcoin_verification=True)
        self.assertFalse(ok_req)
        self.assertEqual(err_req, "ots_pending_no_bitcoin_attestation")

        # Bitcoin-attested OTS WITHOUT trusted header material stays bitcoin_unverified & unverified
        ok_unv, meta_unv = verify_ots_bytes(btc_ots, digest, require_bitcoin_verification=False)
        self.assertTrue(ok_unv)
        assert isinstance(meta_unv, dict)
        self.assertEqual(meta_unv["upgrade_state"], "bitcoin_unverified")
        self.assertFalse(meta_unv["bitcoin_verified"])

        ok_btc_req, err_btc_req = verify_ots_bytes(btc_ots, digest, require_bitcoin_verification=True)
        self.assertFalse(ok_btc_req)
        self.assertEqual(err_btc_req, "ots_missing_trusted_bitcoin_headers")

        # Loading trusted headers JSON confirms bitcoin_confirmed
        with tempfile.TemporaryDirectory() as tmpdir:
            hdr_path = Path(tmpdir) / "headers.json"
            hdr_path.write_text(
                json.dumps({"870333": {"merkle_root": btc_root}}),
                encoding="utf-8",
            )
            roots = load_trusted_bitcoin_headers(hdr_path)
            ok_conf, meta_conf = verify_ots_bytes(
                btc_ots,
                digest,
                bitcoin_merkle_roots=roots,
                require_bitcoin_verification=True,
            )
            self.assertTrue(ok_conf, meta_conf)
            assert isinstance(meta_conf, dict)
            self.assertEqual(meta_conf["upgrade_state"], "bitcoin_confirmed")
            self.assertTrue(meta_conf["bitcoin_verified"])

    def test_bitcoin_header_endianness_4096_cap_and_calendar_allowlist(self) -> None:
        # Item #3 negative-control: reversed Merkle root MUST fail; 80-byte raw header internal bytes pass
        digest = hashlib.sha256(b"shadow-ledger-ots-endian-check").hexdigest()
        btc_ots, internal_root_hex = build_synthetic_ots_fixture(digest, bitcoin_height=870600)
        assert internal_root_hex is not None
        internal_root_bytes = bytes.fromhex(internal_root_hex)
        # Construct an 80-byte raw Bitcoin block header: version(4) + prev(32) + merkle_root(32) + nTime(4 LE) + bits(4) + nonce(4)
        ntime_bytes = struct.pack("<I", 1732225200)  # 2024-11-21T21:40:00.000Z
        raw_header_80 = (b"\x00\x00\x00\x20" + b"\x11" * 32 + internal_root_bytes + ntime_bytes + b"\x22" * 8).hex()

        with tempfile.TemporaryDirectory() as tmpdir:
            hdr_path = Path(tmpdir) / "raw_headers.json"
            hdr_path.write_text(json.dumps({"870600": raw_header_80}), encoding="utf-8")
            roots = load_trusted_bitcoin_headers(hdr_path)
            self.assertEqual(roots[870600]["merkle_root"], internal_root_hex)
            self.assertEqual(roots[870600]["block_time_utc"], "2024-11-21T21:40:00.000Z")
            ok_hdr, _ = verify_ots_bytes(
                btc_ots,
                digest,
                bitcoin_merkle_roots=roots,
                require_bitcoin_verification=True,
                require_bitcoin_timestamp=True,
                max_block_time_utc="2024-11-22T04:30:00.000Z",
            )
            self.assertTrue(ok_hdr)

        # Bare 64-hex root without timestamp MUST FAIL when require_bitcoin_timestamp=True
        ok_no_ts, err_no_ts = verify_ots_bytes(
            btc_ots,
            digest,
            bitcoin_merkle_roots={870600: internal_root_hex},
            require_bitcoin_verification=True,
            require_bitcoin_timestamp=True,
        )
        self.assertFalse(ok_no_ts)
        self.assertEqual(err_no_ts, "ots_missing_trusted_bitcoin_block_timestamp")

        # Reversed display-order hex MUST FAIL exact OpenTimestamps internal byte-order verification
        reversed_hex = internal_root_bytes[::-1].hex()
        ok_rev, err_rev = verify_ots_bytes(
            btc_ots,
            digest,
            bitcoin_merkle_roots={870600: reversed_hex},
            require_bitcoin_verification=True,
        )
        self.assertFalse(ok_rev)
        self.assertEqual(err_rev, "ots_bitcoin_merkle_root_mismatch")

        # 4096-byte intermediate result cap: 8 x OP_HEXLIFY yields 32 * 2^8 = 8192 > 4096 bytes -> OtsFormatError
        hexlify_bomb_body = (
            bytes([OP_HEXLIFY] * 8)
            + bytes([TAG_ATTESTATION])
            + BITCOIN_ATTESTATION_TAG
            + _encode_varbytes(_encode_varuint(870601))
        )
        with self.assertRaises(OtsFormatError) as ctx_bomb:
            build_ots_file_from_timestamp_body(digest, hexlify_bomb_body)
        self.assertIn("exceeds_4096_bytes", str(ctx_bomb.exception))

        # upgrade_ots rejects non-allowlisted / attacker-injected calendar URIs with 0 outbound calls
        evil_ots, _ = build_synthetic_ots_fixture(digest, calendar_uri="https://evil-ssrf.example/cal")
        seen_urls: list[str] = []

        def tracking_transport(method: str, url: str, body: bytes | None, headers: dict, timeout: float) -> tuple[int, bytes]:
            seen_urls.append(url)
            return 200, b""

        strict_client = OpenTimestampsClient(
            calendars=("https://a.pool.opentimestamps.org",),
            transport=tracking_transport,
        )
        res_ots, upgraded = strict_client.upgrade_ots(evil_ots)
        self.assertFalse(upgraded)
        self.assertEqual(res_ots, evil_ots)
        self.assertEqual(seen_urls, [])

    def test_independent_bitcoin_genesis_block_header_vector_and_http_redirect_rejection(self) -> None:
        # Independent real Bitcoin mainnet Genesis Block #0 raw 80-byte header + tx0 SHA-256 (NOT build_synthetic_ots_fixture)
        genesis_header_hex = (
            "01000000"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a"
            "29ab5f49"
            "ffff001d"
            "1dac2b7c"
        )
        genesis_header = bytes.fromhex(genesis_header_hex)
        genesis_block_hash_display = hashlib.sha256(hashlib.sha256(genesis_header).digest()).digest()[::-1].hex()
        self.assertEqual(
            genesis_block_hash_display,
            "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
        )
        # Genesis block tx0 raw SHA-256 -> single OP_SHA256 equals internal Merkle root bytes[36:68]
        genesis_tx0_sha256 = "27362e66e032c731c1c8519f43063fe0e5d070db1c0c3552bb04afa18a31c6bf"
        ts_body = (
            bytes([OP_SHA256, TAG_ATTESTATION])
            + BITCOIN_ATTESTATION_TAG
            + _encode_varbytes(_encode_varuint(0))
        )
        real_ots = build_ots_file_from_timestamp_body(genesis_tx0_sha256, ts_body)

        ok_gen, meta_gen = verify_ots_bytes(
            real_ots,
            genesis_tx0_sha256,
            bitcoin_merkle_roots={0: genesis_header_hex},
            require_bitcoin_verification=True,
            require_bitcoin_timestamp=True,
            max_block_time_utc="2009-01-03T18:15:05.000Z",
        )
        self.assertTrue(ok_gen, meta_gen)
        assert isinstance(meta_gen, dict)
        self.assertEqual(meta_gen["verified_bitcoin_block_time_utc"], "2009-01-03T18:15:05.000Z")

        # Late block timestamp fails even with valid Merkle root
        ok_late_gen, err_late_gen = verify_ots_bytes(
            real_ots,
            genesis_tx0_sha256,
            bitcoin_merkle_roots={0: genesis_header_hex},
            require_bitcoin_verification=True,
            require_bitcoin_timestamp=True,
            max_block_time_utc="2009-01-03T18:00:00.000Z",
        )
        self.assertFalse(ok_late_gen)
        self.assertEqual(err_late_gen, "ots_bitcoin_block_time_after_commitment_deadline")

        # Display-order Merkle root rejected on real Bitcoin header vector
        display_merkle = genesis_header[36:68][::-1].hex()
        ok_disp, err_disp = verify_ots_bytes(
            real_ots,
            genesis_tx0_sha256,
            bitcoin_merkle_roots={0: display_merkle},
            require_bitcoin_verification=True,
        )
        self.assertFalse(ok_disp)
        self.assertEqual(err_disp, "ots_bitcoin_merkle_root_mismatch")

        # HTTP 302 redirect from OTS calendar MUST raise OtsSubmitError via _NoOtsRedirectHandler (zero TCP sockets)
        class _Mock302HttpsHandler(urllib.request.HTTPSHandler):
            def https_open(self, req):  # type: ignore[override]
                hdrs = http.client.HTTPMessage()
                hdrs["Location"] = "https://untrusted-offlist.example/timestamp"
                resp = urllib.response.addinfourl(io.BytesIO(b""), hdrs, req.full_url, code=302)
                resp.msg = "Found"
                return resp

        opener = urllib.request.build_opener(_Mock302HttpsHandler(), _NoOtsRedirectHandler())
        with self.assertRaises(OtsSubmitError) as ctx_redir:
            urllib_ots_transport(
                "GET",
                "https://a.pool.opentimestamps.org/timestamp/00",
                None,
                {"Accept": "application/vnd.opentimestamps.v1"},
                5.0,
                opener=opener,
            )
        self.assertIn("ots_redirect_rejected", str(ctx_redir.exception))


if __name__ == "__main__":
    unittest.main()
