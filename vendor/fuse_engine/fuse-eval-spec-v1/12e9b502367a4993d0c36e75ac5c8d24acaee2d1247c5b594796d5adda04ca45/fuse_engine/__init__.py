"""FUSE-1 reference engine package.

This package implements the pinned evaluator interface (BRIEF-FUSE-1 §5.1):

    evaluate(spec_version, engine_hash, policy_hash, session, as_of, mode,
             validated_inputs, prior_row_state, authorized_events)
    -> (rows_by_ID, effective_state_by_scope, next_row_state)

The engine is a pure function: no I/O, no wall-clock, no randomness, no environment reads.
Identical inputs produce byte-identical outputs under canonical serialization.

Row modules:
- rows_loss.py: L0-L6 (Loss rows)
- rows_vehicle.py: V1-V7 (Vehicle rows)
- rows_ops.py: O1-O9, J1 (Data/operations rows)
"""

import hashlib
import importlib.util
import marshal
import re
import sys
import types
from pathlib import Path

_EXPECTED_ENGINE_FILES = (
    '__init__.py',
    'canonical.py',
    'canonical_state.py',
    'evaluator.py',
    'rows_loss.py',
    'rows_ops.py',
    'rows_vehicle.py',
    'state.py',
)
_EXPECTED_ENGINE_STEMS = frozenset(Path(name).stem for name in _EXPECTED_ENGINE_FILES)
_PYCACHE_RE = re.compile(r'^([A-Za-z0-9_]+)\.(cpython-\d+(?:t)?)(?:\.opt-([12]))?\.pyc$')


def _verify_pyc_matches_source(pyc_path: Path, src_path: Path, src_bytes: bytes, rel: str, cache_tag: str, optimize: int) -> None:
    """Verify a cached .pyc matches expected magic/tag, source hash/size, and compiled bytecode."""
    pyc_bytes = pyc_path.read_bytes()
    if len(pyc_bytes) < 16 or pyc_bytes[:4] != importlib.util.MAGIC_NUMBER:
        raise RuntimeError(f"Invalid or mismatched .pyc magic header in engine directory: {rel!r}")
    if sys.implementation.cache_tag and cache_tag != sys.implementation.cache_tag:
        raise RuntimeError(f"Unexpected Python cache tag in engine directory: {rel!r}")

    bitfield = int.from_bytes(pyc_bytes[4:8], 'little')
    if bitfield & ~0b11:
        raise RuntimeError(f"Invalid .pyc bitfield in engine directory: {rel!r}")
    if bitfield & 0b01:
        if pyc_bytes[8:16] != importlib.util.source_hash(src_bytes):
            raise RuntimeError(f"Tampered hash-based .pyc in engine directory: {rel!r}")
    else:
        pyc_size = int.from_bytes(pyc_bytes[12:16], 'little')
        if pyc_size != (len(src_bytes) & 0xffffffff):
            raise RuntimeError(f"Mismatched .pyc source size in engine directory: {rel!r}")

    try:
        pyc_code = marshal.loads(pyc_bytes[16:])
    except Exception as exc:
        raise RuntimeError(f"Corrupt .pyc code payload in engine directory: {rel!r}") from exc

    if not isinstance(pyc_code, types.CodeType) or not isinstance(pyc_code.co_filename, str):
        raise RuntimeError(f"Invalid .pyc code object in engine directory: {rel!r}")
    norm_fn = pyc_code.co_filename.replace('\\', '/')
    if norm_fn != src_path.name and not norm_fn.endswith('/' + src_path.name):
        raise RuntimeError(f"Unexpected .pyc co_filename in engine directory: {rel!r}")

    expected_code = compile(src_bytes, pyc_code.co_filename, 'exec', optimize=optimize)
    if marshal.dumps(pyc_code) != marshal.dumps(expected_code):
        raise RuntimeError(f"Tampered .pyc bytecode mismatch in engine directory: {rel!r}")


def _verify_and_hash_engine_dir(engine_dir: Path) -> str:
    """Verify engine files & __pycache__ bytecode before submodule imports; return framed SHA-256."""
    py_files: list[Path] = []
    rel_py_paths: list[str] = []
    pyc_entries: list[tuple[Path, str, str, str, int]] = []
    for entry in sorted(engine_dir.rglob('*'), key=lambda p: p.relative_to(engine_dir).as_posix()):
        rel = entry.relative_to(engine_dir).as_posix()
        if entry.is_symlink():
            raise RuntimeError(f"Unexpected symlink in engine directory: {rel!r}")
        if entry.is_dir():
            if rel != '__pycache__':
                raise RuntimeError(f"Unexpected subdirectory in engine directory: {rel!r}")
            continue
        if not entry.is_file():
            raise RuntimeError(f"Unexpected non-regular file in engine directory: {rel!r}")
        if entry.parent == engine_dir:
            if rel not in _EXPECTED_ENGINE_FILES:
                raise RuntimeError(f"Unexpected artifact in engine directory: {rel!r}")
            py_files.append(entry)
            rel_py_paths.append(rel)
        elif entry.parent == engine_dir / '__pycache__':
            m = _PYCACHE_RE.match(entry.name)
            if not m or m.group(1) not in _EXPECTED_ENGINE_STEMS:
                raise RuntimeError(f"Unexpected __pycache__ artifact in engine directory: {rel!r}")
            opt = int(m.group(3)) if m.group(3) else 0
            pyc_entries.append((entry, rel, m.group(1), m.group(2), opt))
        else:
            raise RuntimeError(f"Unexpected nested artifact in engine directory: {rel!r}")

    if rel_py_paths != list(_EXPECTED_ENGINE_FILES):
        raise RuntimeError(
            f"Unexpected engine file set: expected={list(_EXPECTED_ENGINE_FILES)!r}, got={rel_py_paths!r}"
        )

    src_by_stem: dict[str, tuple[Path, bytes]] = {}
    hasher = hashlib.sha256()
    for py_file, rel_path in zip(py_files, rel_py_paths):
        rel_bytes = rel_path.encode('utf-8')
        data = py_file.read_bytes()
        src_by_stem[py_file.stem] = (py_file, data)
        hasher.update(len(rel_bytes).to_bytes(4, 'big'))
        hasher.update(rel_bytes)
        hasher.update(len(data).to_bytes(8, 'big'))
        hasher.update(data)

    for pyc_file, rel, stem, cache_tag, opt in pyc_entries:
        py_file, src_bytes = src_by_stem[stem]
        _verify_pyc_matches_source(pyc_file, py_file, src_bytes, rel, cache_tag, opt)

    return hasher.hexdigest()


# Verify directory, bytecode cache, and source integrity BEFORE importing evaluator or submodules
_CANONICAL_ENGINE_HASH = _verify_and_hash_engine_dir(Path(__file__).resolve().parent)

from .evaluator import evaluate

__all__ = ['evaluate']
__version__ = 'fuse-eval-spec/v1'
