"""Recorder and replay tests.

The recorder is the only source of real order-flow data, so its failure modes
matter more than its happy path. These tests cover the ones that cost data:
a crash mid-write, a torn gzip tail, and an hour boundary that must rotate.

The replay test asserts the recorded pipeline produces the same bar as the live
one. If a decoder or chart change breaks that, a month of recordings becomes
worthless, so it is worth a test of its own.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from orderflow.feed import TokenNamespace
from orderflow.feed.protocol import ExchangeType, SubscriptionMode
from orderflow.feed.simulator import TickSpec, encode_tick
from orderflow.instruments import (
    Exchange,
    Instrument,
    Segment,
    SessionKind,
    TokenMapper,
)
from orderflow.recorder import (
    MAGIC,
    TYPE_BINARY,
    TYPE_TEXT,
    MultiRecorder,
    RecorderError,
    ShardRecorder,
    finalize_parts,
    iter_shards,
    read_shard,
    shard_summary,
)
from orderflow.session import Market, MarketCalendar

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def _frame(px: float, vol: int, ms: int, token: str = "48704") -> bytes:
    return encode_tick(TickSpec(
        mode=SubscriptionMode.SNAP_QUOTE, exchange_type=ExchangeType.NSE_FO,
        token=token, exchange_timestamp_ms=ms, last_traded_price=px,
        volume_trade_for_the_day=vol, open_interest=16_300_000,
        bids=((round(px - 0.10, 2), 65),), asks=((round(px + 0.05, 2), 65),),
    ))


def _base_ms() -> int:
    return int(dt.datetime(2026, 10, 21, 10, 30, tzinfo=IST).timestamp() * 1000)


# ------------------------------------------------------------------ writing
class TestShardRecorder:
    def test_roundtrip_preserves_payload_and_type(self, tmp_path):
        r = ShardRecorder(tmp_path, "K", fsync_every=4)
        frames = [b"\x01\x02\x03" * 40, b"\xff" * 100, "hb-text"]
        for f in frames * 20:
            r.write(f, ts=_base_ms() / 1000)
        r.close()
        shard = next(iter_shards(tmp_path))
        recs = read_shard(shard)
        assert len(recs) == 60
        assert recs[0].payload == frames[0]
        assert recs[0].type == TYPE_BINARY
        assert recs[2].type == TYPE_TEXT
        assert recs[2].payload == b"hb-text"

    def test_file_starts_with_magic(self, tmp_path):
        r = ShardRecorder(tmp_path, "K")
        r.write(b"x" * 10, ts=_base_ms() / 1000)
        r.close()
        shard = next(iter_shards(tmp_path))
        assert shard.read_bytes()[: len(MAGIC)] == MAGIC

    def test_no_part_file_after_clean_close(self, tmp_path):
        r = ShardRecorder(tmp_path, "K")
        r.write(b"x" * 10, ts=_base_ms() / 1000)
        r.close()
        assert not list(tmp_path.glob("*/*.part"))
        assert list(tmp_path.glob("*/*.oft.gz"))

    def test_rotates_on_the_hour(self, tmp_path):
        r = ShardRecorder(tmp_path, "K")
        t = _base_ms() / 1000
        r.write(b"a" * 10, ts=t)
        r.write(b"b" * 10, ts=t + 3600)  # next hour
        r.write(b"c" * 10, ts=t + 7200)
        r.close()
        shards = list(iter_shards(tmp_path))
        assert len(shards) == 3, [s.name for s in shards]
        for shard in shards:
            assert len(read_shard(shard)) == 1

    def test_compresses_well(self, tmp_path):
        """A month of data has to fit on a disk, so the ratio is a feature."""
        r = ShardRecorder(tmp_path, "K", fsync_every=500)
        payload = _frame(22895.0, 1000, _base_ms())
        n = 500
        for _ in range(n):
            r.write(payload, ts=_base_ms() / 1000)
        r.close()
        shard = next(iter_shards(tmp_path))
        raw = len(payload) * n
        assert shard.stat().st_size < raw / 5, (
            f"poor compression: {raw} raw -> {shard.stat().st_size}")

    def test_heartbeat_text_is_tagged(self, tmp_path):
        r = ShardRecorder(tmp_path, "K")
        r.write("a text heartbeat", ts=_base_ms() / 1000)
        r.close()
        recs = read_shard(next(iter_shards(tmp_path)))
        assert recs[0].type == TYPE_TEXT


class TestMultiRecorder:
    def test_keys_are_separated(self, tmp_path):
        m = MultiRecorder(tmp_path)
        for i in range(5):
            m.write("conn0", _frame(100.0, i, _base_ms()), ts=_base_ms() / 1000)
            m.write("conn1", _frame(200.0, i, _base_ms()), ts=_base_ms() / 1000)
        m.close_all()
        assert len(list(iter_shards(tmp_path, "conn0"))) == 1
        assert len(list(iter_shards(tmp_path, "conn1"))) == 1
        assert len(read_shard(next(iter_shards(tmp_path, "conn0")))) == 5
        assert len(read_shard(next(iter_shards(tmp_path, "conn1")))) == 5

    def test_stats_count_frames_and_bytes(self, tmp_path):
        m = MultiRecorder(tmp_path)
        for i in range(7):
            m.write("K", b"y" * 50, ts=_base_ms() / 1000)
        stats = m.stats()["K"]
        m.close_all()
        assert stats["frames"] == 7
        assert stats["bytes"] == 350


# ------------------------------------------------------------- failure modes
class TestCrashSafety:
    def test_torn_tail_recovers_the_prefix(self, tmp_path):
        """A power loss must cost the last record, not the whole shard."""
        r = ShardRecorder(tmp_path, "K", fsync_every=10)
        for i in range(200):
            r.write(b"z" * 80, ts=_base_ms() / 1000)
        r.close()
        shard = next(iter_shards(tmp_path))
        full = len(read_shard(shard))
        assert full == 200

        blob = shard.read_bytes()
        torn = tmp_path / "torn.oft.gz"
        torn.write_bytes(blob[: int(len(blob) * 0.6)])
        recs = read_shard(torn)  # must not raise
        assert 0 < len(recs) < full
        assert all(r.payload == b"z" * 80 for r in recs)

    def test_bad_magic_is_rejected_loudly(self, tmp_path):
        bad = tmp_path / "bad.oft.gz"
        bad.write_bytes(b"NOPE" + b"\x00" * 40)
        with pytest.raises(RecorderError):
            read_shard(bad)

    def test_finalize_promotes_a_part_file(self, tmp_path):
        r = ShardRecorder(tmp_path, "K")
        r.write(b"q" * 100, ts=_base_ms() / 1000)
        r.flush()
        # Simulate a kill: the .part file is on disk and never renamed.
        part = next(tmp_path.glob("*/*.part"))
        r._fh = None
        r._raw = None
        assert part.exists()
        fixed = finalize_parts(tmp_path)
        assert len(fixed) == 1
        assert not list(tmp_path.glob("*/*.part"))
        assert len(read_shard(fixed[0])) == 1

    def test_finalize_does_not_clobber_a_complete_shard(self, tmp_path):
        r = ShardRecorder(tmp_path, "K")
        r.write(b"first", ts=_base_ms() / 1000)
        r.close()
        complete = next(iter_shards(tmp_path))
        # A second writer left a .part for the same hour.
        stray = complete.with_suffix(complete.suffix + ".part")
        stray.write_bytes(complete.read_bytes())
        finalize_parts(tmp_path)
        assert len(read_shard(complete)) == 1
        assert not stray.exists()

    def test_recorder_write_error_does_not_escape(self, tmp_path, monkeypatch):
        """The tee must not kill the feed when the disk fails."""
        from orderflow.feed.transports import RecordingTransport

        class Boom:
            def write(self, *a, **k):
                raise OSError("no space left on device")

        class Inner:
            is_live = True

            def connect(self):
                pass

            def recv(self, timeout=None):
                return b"\x01\x02\x03"

            def send(self, *a, **k):
                pass

            def close(self):
                pass

        t = RecordingTransport(Inner(), Boom(), key="K")
        t.connect()
        assert t.recv() == b"\x01\x02\x03"  # returned despite the failure
        assert t.recorded == 0


# ------------------------------------------------------------------- replay
class TestShardTransport:
    def _record(self, root: Path, n: int = 40) -> None:
        rec = MultiRecorder(root, fsync_every=10)
        vol, px = 1000, 22895.0
        base = _base_ms()
        for i in range(n):
            px += 0.05 if i % 3 else -0.05
            vol += 65
            rec.write("conn0", _frame(round(px, 2), vol, base + i * 1000),
                      ts=base / 1000 + i)
        rec.close_all()

    def _mapper(self) -> TokenMapper:
        m = TokenMapper()
        m.register(TokenNamespace.NSE_FO, 48704, Instrument(
            symbol="NIFTY27OCT26FUT", exchange=Exchange.NSE, segment=Segment.FUT,
            tick_size=0.05, session=SessionKind.NSE_FNO,
        ))
        return m

    def test_records_are_loaded_in_order(self, tmp_path):
        from orderflow.feed import ShardTransport

        self._record(tmp_path)
        t = ShardTransport(tmp_path, "conn0")
        t.connect()
        assert len(t._records) == 40
        assert t.recv() == t._records[0].payload

    def test_unreadable_shard_does_not_stop_the_replay(self, tmp_path):
        from orderflow.feed import ShardTransport

        self._record(tmp_path)
        (tmp_path / "conn0" / "20261021T99.oft.gz").write_bytes(b"junk")
        t = ShardTransport(tmp_path, "conn0")
        t.connect()
        assert t.torn_shards, "the junk shard should be reported"
        assert len(t._records) == 40, "the good shard must still load"

    def test_replay_produces_the_same_bar_as_live(self, tmp_path):
        """Record then replay must yield the identical footprint bar."""
        from orderflow.feed import ShardTransport, SmartStreamConnector, build_plan
        from orderflow.session_chart import SessionChart
        from orderflow.ticks import TickGrid

        self._record(tmp_path)
        mapper = self._mapper()
        plan = build_plan([(TokenNamespace.NSE_FO, 48704)],
                          mode=SubscriptionMode.SNAP_QUOTE)
        chart = SessionChart(TickGrid(0.05),
                             MarketCalendar(Market.NSE, segment="NSE_FNO"),
                             bar_seconds=300)
        transport = ShardTransport(tmp_path, "conn0")
        conn = SmartStreamConnector(plan, mapper=mapper,
                                    transport_factory=lambda i: transport)
        stats = conn.run(on_trade=lambda rt: chart.add(rt.trade), reconnect=False)

        assert stats.frames == 40
        assert stats.protocol_errors == 0
        assert len(chart.bars) == 1
        bar = chart.bars[-1]
        # Same numbers the live capture produced: the first print carries the
        # whole day's cumulative volume, so it counts as its own increment.
        assert bar.total_volume == 1065 + 39 * 65
        assert bar.delta == 845
        assert bar.poc == 22894.95


class TestShardSummary:
    def test_summary_counts_records(self, tmp_path):
        r = ShardRecorder(tmp_path, "K")
        r.write(b"a" * 10, ts=_base_ms() / 1000)
        r.write("t", ts=_base_ms() / 1000)
        r.close()
        s = shard_summary(next(iter_shards(tmp_path)))
        assert s["records"] == 2
        assert s["binary"] == 1
        assert s["text"] == 1
        assert s["bytes"] == 11
