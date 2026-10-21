"""Crash-safe tick recorder.

Writes every frame a live socket delivers to a gzipped file, so a month of MCX
and NSE order flow can be replayed later. The engine cannot be backtested on
real order flow without this: Angel One's historical API serves candles only,
and a footprint cannot be rebuilt from OHLCV. The only source of real bid/ask
at price is the live socket, so it has to be captured as it arrives.

Design, and why:

* **Binary, gzipped, framed.** Each record is a length-prefixed frame with a
  1-byte type tag. Hex text (the format `record_frames` writes) costs two bytes
  per byte of payload; gzipped binary measured 146x smaller on a real packet.
  A month of one instrument is tens of MB rather than tens of GB.

* **Flush and fsync per batch, not per frame.** A crash must lose at most the
  in-flight batch. Fsyncing every frame would put a disk barrier on the socket
  reader and drop ticks under load, which is a worse failure than losing a
  second of data.

* **Hourly rotation.** One file per instrument per hour. A multi-gigabyte file
  is unreadable by tools and unrecoverable if its index is damaged; an hour is
  a natural unit for both replay and cleanup.

* **A `.part` suffix while writing.** A file that ends in `.part` was not
  closed cleanly. On startup, `finalize_parts` renames or repairs it, so a
  kill -9 does not leave a file that looks complete but is not.

* **Length-prefixed records make a torn tail recoverable.** The reader stops
  at the first record it cannot read in full and returns what it has. A
  half-written final record from a power loss therefore costs that one record,
  not the file.

Record layout, little endian:

    magic   4 bytes   b"OFT1"
    type    1 byte    0 = binary frame, 1 = text frame
    length  4 bytes   uint32, payload length
    payload N bytes

The magic is repeated at the head of the file. It is not a substitute for the
per-record length; it makes a file that is not a recording fail immediately
with a clear message instead of being parsed as noise.
"""

from __future__ import annotations

import gzip
import os
import struct
import threading
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

MAGIC = b"OFT1"
RECORD_HEADER = struct.Struct("<BI")  # type, length
TYPE_BINARY = 0
TYPE_TEXT = 1


class RecorderError(RuntimeError):
    pass


def _utc_stamp(ts: float | None = None) -> str:
    t = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else datetime.now(timezone.utc)
    return t.strftime("%Y%m%dT%H")


def record_path(root: str | Path, key: str, ts: float | None = None) -> Path:
    """`<root>/<key>/<YYYYMMDDTHH>.oft.gz`, the hourly shard."""
    return Path(root) / key / f"{_utc_stamp(ts)}.oft.gz"


@dataclass
class ShardRecorder:
    """Appends frames to one hourly shard, rotating on the hour."""

    root: str | Path
    key: str
    fsync_every: int = 200
    _path: Path | None = None
    _fh: gzip.GzipFile | None = None
    _raw = None
    _since_sync: int = 0
    _hour: str | None = None
    frames: int = 0
    bytes_in: int = 0
    rotations: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def _open(self, ts: float) -> None:
        hour = _utc_stamp(ts)
        final = record_path(self.root, self.key, ts)
        part = final.with_suffix(final.suffix + ".part")
        part.parent.mkdir(parents=True, exist_ok=True)
        # Line-buffered raw handle under the gzip layer: gzip needs a binary
        # file object, and we control flushing explicitly.
        self._raw = open(part, "wb", buffering=0)
        self._raw.write(MAGIC)
        self._fh = gzip.GzipFile(fileobj=self._raw, mode="wb", compresslevel=6)
        self._path = part
        self._hour = hour
        self._since_sync = 0

    def _close(self) -> None:
        if self._fh is not None:
            try:
                self._fh.close()
            except Exception:
                pass
            self._fh = None
        if self._raw is not None:
            try:
                self._raw.flush()
                os.fsync(self._raw.fileno())
                self._raw.close()
            except Exception:
                pass
            self._raw = None
        # Rename only after the data is on disk, so a `.part` file always means
        # "not durable yet".
        if self._path is not None:
            final = self._path.with_suffix("")  # drop the trailing .part
            try:
                os.replace(self._path, final)
            except OSError:
                pass
            self._path = None

    def write(self, frame: bytes | str, ts: float | None = None) -> None:
        ts = ts if ts is not None else datetime.now(timezone.utc).timestamp()
        with self._lock:
            hour = _utc_stamp(ts)
            if self._fh is None or hour != self._hour:
                self._close()
                self._open(ts)
                self.rotations += 1
            payload = frame.encode() if isinstance(frame, str) else bytes(frame)
            rtype = TYPE_TEXT if isinstance(frame, str) else TYPE_BINARY
            self._fh.write(RECORD_HEADER.pack(rtype, len(payload)))
            self._fh.write(payload)
            self.frames += 1
            self.bytes_in += len(payload)
            self._since_sync += 1
            if self._since_sync >= self.fsync_every:
                self._flush_locked()

    def _flush_locked(self) -> None:
        """Push the gzip stream and raw handle to disk, then fsync.

        Caller holds the lock. Kept separate from `flush` so the write path,
        which already holds it, does not try to take it twice.
        """
        if self._fh is None:
            return
        self._fh.flush()
        if self._raw is not None:
            self._raw.flush()
            try:
                os.fsync(self._raw.fileno())
            except (OSError, ValueError):
                pass
        self._since_sync = 0

    def flush(self) -> None:
        with self._lock:
            self._flush_locked()

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                self._fh.flush()
            self._close()


@dataclass
class MultiRecorder:
    """One `ShardRecorder` per instrument key, keyed by token or symbol."""

    root: str | Path
    fsync_every: int = 200
    _recorders: dict[str, ShardRecorder] = field(default_factory=dict)

    def for_key(self, key: str) -> ShardRecorder:
        rec = self._recorders.get(key)
        if rec is None:
            rec = ShardRecorder(self.root, key, fsync_every=self.fsync_every)
            self._recorders[key] = rec
        return rec

    def write(self, key: str, frame: bytes | str, ts: float | None = None) -> None:
        self.for_key(key).write(frame, ts)

    def flush_all(self) -> None:
        for rec in self._recorders.values():
            rec.flush()

    def close_all(self) -> None:
        for rec in self._recorders.values():
            rec.close()

    def stats(self) -> dict[str, dict[str, int]]:
        return {
            k: {"frames": r.frames, "bytes": r.bytes_in, "rotations": r.rotations}
            for k, r in self._recorders.items()
        }


# ------------------------------------------------------------------ reading
@dataclass(frozen=True)
class Record:
    type: int
    payload: bytes
    index: int


def read_shard(path: str | Path) -> list[Record]:
    """Read a shard, tolerating a torn final record.

    A power loss can leave the last record half-written. Rather than raising,
    this stops at the first record that cannot be read in full and returns what
    is intact, which is the behaviour a replay needs: lose the last record, not
    the session.
    """
    p = Path(path)
    with open(p, "rb") as fh:
        head = fh.read(len(MAGIC))
        if head != MAGIC:
            raise RecorderError(f"{p} is not a recording (bad magic {head!r})")
        blob = fh.read()
    try:
        data = gzip.decompress(blob)
    except (OSError, EOFError, zlib.error):
        # A truncated gzip stream raises EOFError (end-of-stream marker
        # missing), not OSError. Both mean "the tail is gone": fall back to a
        # tolerant inflate and keep the prefix, so a killed process costs the
        # last record rather than the whole shard.
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        try:
            data = d.decompress(blob) + d.flush()
        except zlib.error as exc:
            raise RecorderError(f"{p} is corrupt: {exc}") from exc

    out: list[Record] = []
    i = 0
    n = len(data)
    while i + RECORD_HEADER.size <= n:
        rtype, length = RECORD_HEADER.unpack_from(data, i)
        start = i + RECORD_HEADER.size
        end = start + length
        if end > n:
            break  # torn tail
        out.append(Record(type=rtype, payload=data[start:end], index=len(out)))
        i = end
    return out


def iter_shards(root: str | Path, key: str | None = None):
    """Yield every shard under root, optionally for one instrument."""
    base = Path(root)
    pattern = f"{key}/*.oft.gz" if key else "*/*.oft.gz"
    yield from sorted(base.glob(pattern))


def finalize_parts(root: str | Path) -> list[Path]:
    """Rename `*.part` files left by a crash to their final name.

    A `.part` file holds a complete gzip member for every batch that was
    flushed, because the gzip stream is flushed with Z_SYNC_FLUSH. So a killed
    process leaves a readable file, and promoting it recovers the data.
    """
    fixed = []
    for part in sorted(Path(root).glob("*/*.oft.gz.part")):
        target = part.with_suffix("")
        if target.exists():
            # Do not overwrite a completed shard from the same hour.
            part.rename(part.with_suffix(".part.orphan"))
            continue
        try:
            os.replace(part, target)
            fixed.append(target)
        except OSError:
            continue
    return fixed


def shard_summary(path: str | Path) -> dict:
    records = read_shard(path)
    return {
        "path": str(path),
        "records": len(records),
        "binary": sum(1 for r in records if r.type == TYPE_BINARY),
        "text": sum(1 for r in records if r.type == TYPE_TEXT),
        "bytes": sum(len(r.payload) for r in records),
    }
