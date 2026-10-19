"""Ingestion layer tests: SmartStream protocol, tick reader, connector.

Everything here runs offline. The encoder in `orderflow.feed.simulator` produces
real binary packets, so the decoder, the reader and the connector are exercised
on byte-accurate data with no broker and no network.

Where a test documents a production failure mode (scaled prices not divided,
cumulative volume not differenced, a heartbeat parsed as a tick) it says so, so a
future refactor that reintroduces the bug fails here first.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from orderflow import (
    IST,
    NIFTY_FUT,
    MCX_GOLD_FUT,
    Market,
    MarketCalendar,
    OffGridTick,
    SessionChart,
    Side,
    TickGrid,
    build_indian_token_map,
)
from orderflow.feed import (
    DEFAULT_PRICE_SCALE,
    MAX_TOKENS_PER_SESSION,
    ExchangeType,
    ProtocolError,
    RawTick,
    ReplayTransport,
    SideSource,
    SmartStreamConnector,
    SnapshotModeUnsupported,
    SubscriptionMode,
    TickReader,
    TickSpec,
    TokenNamespace,
    build_plan,
    decode_packet,
    decode_stream,
    encode_tick,
    is_heartbeat,
    price_scale,
    require_volume_capable_mode,
    subscribe_message,
)


# ---------------------------------------------------------------- fixtures
def snap_spec(**over) -> TickSpec:
    base = dict(
        mode=SubscriptionMode.SNAP_QUOTE,
        exchange_type=ExchangeType.NSE_FO,
        token="26000",
        exchange_timestamp_ms=1784000000000,
        last_traded_price=24000.05,
        volume_trade_for_the_day=1000,
        open_interest=12000000,
        bids=((24000.00, 150),),
        asks=((24000.05, 75),),
    )
    base.update(over)
    return TickSpec(**base)


def quote_spec(**over) -> TickSpec:
    base = dict(
        mode=SubscriptionMode.QUOTE,
        exchange_type=ExchangeType.NSE_FO,
        token="26000",
        exchange_timestamp_ms=1784000000000,
        last_traded_price=24000.05,
        volume_trade_for_the_day=1000,
    )
    base.update(over)
    return TickSpec(**base)


# ================================================================ protocol
class TestProtocolRoundTrip:
    def test_snap_quote_length_and_fields(self):
        raw = encode_tick(snap_spec())
        assert len(raw) == 379
        t = decode_packet(raw)
        assert t.mode is SubscriptionMode.SNAP_QUOTE
        assert t.exchange_type is ExchangeType.NSE_FO
        assert t.token == "26000"
        assert t.last_traded_price == 2400005  # scaled by 100
        assert t.volume_trade_for_the_day == 1000
        assert t.open_interest == 12000000
        assert t.exchange_timestamp == 1784000000000

    def test_quote_length_and_fields(self):
        raw = encode_tick(quote_spec())
        assert len(raw) == 347
        t = decode_packet(raw)
        assert t.mode is SubscriptionMode.QUOTE
        assert t.open_interest is None  # SNAP_QUOTE-only field

    def test_ltp_length(self):
        raw = encode_tick(
            TickSpec(
                mode=SubscriptionMode.LTP,
                exchange_type=ExchangeType.NSE_FO,
                token="26000",
                exchange_timestamp_ms=1784000000000,
                last_traded_price=24000.05,
            )
        )
        assert len(raw) == 51
        t = decode_packet(raw)
        assert t.last_traded_price == 2400005
        assert t.volume_trade_for_the_day is None

    def test_depth_round_trip(self):
        bids = tuple((24000.0 - i * 0.05, 100 + i) for i in range(20))
        asks = tuple((24000.05 + i * 0.05, 200 + i) for i in range(20))
        raw = encode_tick(
            TickSpec(
                mode=SubscriptionMode.DEPTH,
                exchange_type=ExchangeType.NSE_FO,
                token="26000",
                exchange_timestamp_ms=1784000000000,
                last_traded_price=24000.05,
                bids=bids,
                asks=asks,
            )
        )
        assert len(raw) == 443
        t = decode_packet(raw)
        assert t.mode is SubscriptionMode.DEPTH
        assert len(t.bids) == 20
        assert len(t.asks) == 20
        assert t.bids[0].price_raw == 2400000
        assert t.bids[0].quantity == 100
        assert t.asks[0].price_raw == 2400005

    def test_price_scaling_is_exact_for_a_5_paise_tick(self):
        """24000.05 must scale to exactly 2400005, not 2400004.9999."""
        raw = encode_tick(snap_spec(last_traded_price=24000.05))
        t = decode_packet(raw)
        assert t.last_traded_price == 2400005
        assert t.last_traded_price / t.scale == 24000.05

    def test_mcx_gold_one_rupee_prices(self):
        raw = encode_tick(
            snap_spec(exchange_type=ExchangeType.MCX_FO, token="42809",
                      last_traded_price=71001.0, bids=((71000.0, 5),),
                      asks=((71001.0, 3),))
        )
        t = decode_packet(raw)
        assert t.exchange_type is ExchangeType.MCX_FO
        assert t.last_traded_price / t.scale == 71001.0
        assert t.bids[0].price_raw / t.scale == 71000.0

    def test_bids_and_asks_are_not_swapped(self):
        """The upstream SDK swaps its buy/sell keys; guard against copying that."""
        raw = encode_tick(
            snap_spec(bids=((24000.00, 111),), asks=((24000.05, 222),))
        )
        t = decode_packet(raw)
        assert t.bids[0].quantity == 111
        assert t.asks[0].quantity == 222

    def test_token_is_nul_terminated_string(self):
        raw = encode_tick(snap_spec(token="99926000"))
        assert decode_packet(raw).token == "99926000"

    def test_cds_uses_larger_scale(self):
        assert price_scale(ExchangeType.CDE_FO) == 10_000_000
        assert price_scale(ExchangeType.NSE_FO) == DEFAULT_PRICE_SCALE


class TestProtocolErrors:
    def test_truncated_packet_raises(self):
        raw = encode_tick(snap_spec())
        with pytest.raises(ProtocolError, match="truncated"):
            decode_packet(raw[:200])

    def test_too_short_raises(self):
        with pytest.raises(ProtocolError, match="too short"):
            decode_packet(b"\x01\x02")

    def test_unknown_mode_raises(self):
        raw = bytearray(encode_tick(snap_spec()))
        raw[0] = 9
        with pytest.raises(ProtocolError, match="unknown subscription mode"):
            decode_packet(bytes(raw))

    def test_unknown_exchange_raises(self):
        raw = bytearray(encode_tick(snap_spec()))
        raw[1] = 99
        with pytest.raises(ProtocolError, match="unknown exchange type"):
            decode_packet(bytes(raw))

    def test_decoder_never_guesses_a_bad_offset(self):
        """A malformed frame must raise, not return plausible nonsense."""
        with pytest.raises(ProtocolError):
            decode_packet(b"\x03\x02" + b"\x00" * 50)


class TestHeartbeat:
    def test_short_binary_is_heartbeat(self):
        assert is_heartbeat(b"") is True
        assert is_heartbeat(b"\x01") is True

    def test_string_message_is_heartbeat(self):
        assert is_heartbeat("pong") is True
        assert is_heartbeat(None) is True

    def test_real_packet_is_not_heartbeat(self):
        assert is_heartbeat(encode_tick(snap_spec())) is False


class TestDecodeStream:
    def test_multiple_packets_in_one_buffer(self):
        data = encode_tick(snap_spec()) + encode_tick(quote_spec())
        ticks = decode_stream(data)
        assert len(ticks) == 2
        assert ticks[0].mode is SubscriptionMode.SNAP_QUOTE
        assert ticks[1].mode is SubscriptionMode.QUOTE

    def test_trailing_truncation_raises(self):
        data = encode_tick(snap_spec()) + encode_tick(quote_spec())[:100]
        with pytest.raises(ProtocolError, match="truncated"):
            decode_stream(data)


# ============================================================== tick reader
class TestTickReaderVolumeDifferencing:
    def test_cumulative_volume_becomes_incremental(self):
        """Feeding cumulative volume straight in puts the whole day in one bar."""
        r = TickReader("26000")
        t1 = r.read(decode_packet(encode_tick(snap_spec(volume_trade_for_the_day=1000))))
        t2 = r.read(decode_packet(encode_tick(snap_spec(volume_trade_for_the_day=1075))))
        assert t1.trade.size == 1000.0  # first print, no prior baseline
        assert t2.trade.size == 75.0  # 1075 - 1000, not 1075

    def test_unchanged_volume_emits_no_trade(self):
        """A quote update with no fill must not create a zero-size print."""
        r = TickReader("26000")
        r.read(decode_packet(encode_tick(snap_spec(volume_trade_for_the_day=1000))))
        again = r.read(decode_packet(encode_tick(snap_spec(volume_trade_for_the_day=1000))))
        assert again is None
        assert r.state.prints == 1

    def test_volume_reset_at_session_open_is_not_negative(self):
        """Day volume restarting at 0 must not inject a huge negative print."""
        r = TickReader("26000")
        r.read(decode_packet(encode_tick(snap_spec(volume_trade_for_the_day=50000))))
        after = r.read(decode_packet(encode_tick(snap_spec(volume_trade_for_the_day=50))))
        assert after.trade.size == 50.0  # the new absolute value
        assert r.state.volume_resets == 1

    def test_price_is_unscaled(self):
        r = TickReader("26000")
        tt = r.read(decode_packet(encode_tick(snap_spec(last_traded_price=24000.05))))
        assert tt.trade.price == 24000.05

    def test_timestamp_is_seconds(self):
        r = TickReader("26000")
        tt = r.read(decode_packet(encode_tick(snap_spec(exchange_timestamp_ms=1784000000000))))
        assert tt.trade.ts == 1784000000.0


class TestTickReaderSideInference:
    def test_quote_rule_buy_when_at_ask(self):
        r = TickReader("26000")
        tt = r.read(
            decode_packet(
                encode_tick(
                    snap_spec(last_traded_price=24000.05,
                              bids=((24000.00, 150),), asks=((24000.05, 75),))
                )
            )
        )
        assert tt.trade.side is Side.BUY
        assert tt.source is SideSource.QUOTE_RULE

    def test_quote_rule_sell_when_at_bid(self):
        r = TickReader("26000")
        tt = r.read(
            decode_packet(
                encode_tick(
                    snap_spec(last_traded_price=24000.00,
                              bids=((24000.00, 150),), asks=((24000.05, 75),))
                )
            )
        )
        assert tt.trade.side is Side.SELL

    def test_tick_rule_when_no_book(self):
        """LTP-style ticks carry no book, so only the tick rule is available."""
        r = TickReader("26000")
        r.read(decode_packet(encode_tick(quote_spec(last_traded_price=24000.00,
                                                    volume_trade_for_the_day=100))))
        up = r.read(decode_packet(encode_tick(quote_spec(last_traded_price=24000.05,
                                                         volume_trade_for_the_day=200))))
        assert up.trade.side is Side.BUY
        assert up.source is SideSource.TICK_RULE

    def test_tick_rule_down_is_sell(self):
        r = TickReader("26000")
        r.read(decode_packet(encode_tick(quote_spec(last_traded_price=24000.10,
                                                    volume_trade_for_the_day=100))))
        down = r.read(decode_packet(encode_tick(quote_spec(last_traded_price=24000.05,
                                                           volume_trade_for_the_day=200))))
        assert down.trade.side is Side.SELL

    def test_both_rules_agreeing_is_marked_both(self):
        r = TickReader("26000")
        r.read(decode_packet(encode_tick(snap_spec(last_traded_price=24000.00,
                                                   volume_trade_for_the_day=100,
                                                   bids=((24000.00, 10),),
                                                   asks=((24000.05, 10),)))))
        tt = r.read(decode_packet(encode_tick(snap_spec(last_traded_price=24000.05,
                                                        volume_trade_for_the_day=200,
                                                        bids=((24000.00, 10),),
                                                        asks=((24000.05, 10),)))))
        assert tt.trade.side is Side.BUY
        assert tt.source is SideSource.BOTH

    def test_book_disagrees_with_tick_rule_book_wins(self):
        """Price rose but the print is still at the bid: trust the book."""
        r = TickReader("26000")
        r.read(decode_packet(encode_tick(snap_spec(last_traded_price=24000.00,
                                                   volume_trade_for_the_day=100,
                                                   bids=((24000.00, 10),),
                                                   asks=((24000.10, 10),)))))
        tt = r.read(decode_packet(encode_tick(snap_spec(last_traded_price=24000.05,
                                                        volume_trade_for_the_day=200,
                                                        bids=((24000.05, 10),),
                                                        asks=((24000.10, 10),)))))
        assert tt.trade.side is Side.SELL
        assert tt.source is SideSource.QUOTE_RULE

    def test_unchanged_price_without_book_is_unknown(self):
        """No book, no price change: the side is genuinely unknowable."""
        r = TickReader("26000")
        r.read(decode_packet(encode_tick(quote_spec(last_traded_price=24000.05,
                                                    volume_trade_for_the_day=100))))
        tt = r.read(decode_packet(encode_tick(quote_spec(last_traded_price=24000.05,
                                                         volume_trade_for_the_day=200))))
        assert tt.trade.side is Side.UNKNOWN
        assert tt.source is SideSource.UNKNOWN

    def test_wrong_token_rejected(self):
        r = TickReader("26000")
        with pytest.raises(ValueError):
            r.read(decode_packet(encode_tick(snap_spec(token="99999"))))


class TestModeGuard:
    def test_ltp_mode_rejected_for_footprint(self):
        """LTP has no volume; subscribing in it yields a silently empty chart."""
        with pytest.raises(SnapshotModeUnsupported):
            require_volume_capable_mode(SubscriptionMode.LTP)

    def test_quote_and_snapquote_accepted(self):
        require_volume_capable_mode(SubscriptionMode.QUOTE)
        require_volume_capable_mode(SubscriptionMode.SNAP_QUOTE)


# =========================================================== subscription plan
class TestSubscriptionPlan:
    def test_deduplicates_identical_subscriptions(self):
        plan = build_plan(
            [(TokenNamespace.NSE_FO, 26000), (TokenNamespace.NSE_FO, 26000)]
        )
        assert plan.total == 1

    def test_same_token_two_namespaces_are_two_subscriptions(self):
        plan = build_plan(
            [(TokenNamespace.NSE_EQ, 26000), (TokenNamespace.NSE_FO, 26000)]
        )
        assert plan.total == 2

    def test_splits_across_sessions_at_1000(self):
        insts = [(TokenNamespace.NSE_FO, i) for i in range(2500)]
        plan = build_plan(insts)
        assert [len(s) for s in plan.sessions] == [1000, 1000, 500]

    def test_more_than_three_connections_rejected(self):
        insts = [(TokenNamespace.NSE_FO, i) for i in range(3001)]
        with pytest.raises(ValueError, match="3"):
            build_plan(insts)

    def test_limit_constant_is_1000(self):
        assert MAX_TOKENS_PER_SESSION == 1000


class TestSubscribeMessage:
    def test_groups_by_exchange_type(self):
        plan = build_plan(
            [(TokenNamespace.NSE_FO, 26000), (TokenNamespace.MCX_FO, 42809)]
        )
        msg = subscribe_message(1, plan.sessions[0]).decode()
        assert '"action": 1' in msg
        assert '"mode": 3' in msg
        assert '"exchangeType": 2' in msg  # NSE_FO
        assert '"exchangeType": 5' in msg  # MCX_FO

    def test_unsubscribe_action_is_zero(self):
        plan = build_plan([(TokenNamespace.NSE_FO, 26000)])
        assert '"action": 0' in subscribe_message(0, plan.sessions[0]).decode()

    def test_mixed_modes_in_one_frame_rejected(self):
        from orderflow.feed import Subscription

        subs = [
            Subscription(TokenNamespace.NSE_FO, 1, SubscriptionMode.QUOTE),
            Subscription(TokenNamespace.NSE_FO, 2, SubscriptionMode.SNAP_QUOTE),
        ]
        with pytest.raises(ValueError, match="share a mode"):
            subscribe_message(1, subs)

    def test_correlation_id_length_enforced(self):
        plan = build_plan([(TokenNamespace.NSE_FO, 26000)])
        with pytest.raises(ValueError):
            subscribe_message(1, plan.sessions[0], correlation_id="x" * 11)


# ================================================================== connector
class TestConnector:
    def _connector(self, frames, plan=None):
        mapper = build_indian_token_map()
        plan = plan or build_plan([(TokenNamespace.NSE_FO, 26000)])
        transport = ReplayTransport(frames)
        conn = SmartStreamConnector(plan, mapper, lambda i: transport)
        return conn, transport

    def test_emits_trades_from_real_packets(self):
        frames = [
            encode_tick(snap_spec(volume_trade_for_the_day=1000)),
            encode_tick(snap_spec(volume_trade_for_the_day=1075)),
        ]
        conn, _ = self._connector(frames)
        got = []
        conn.run(on_trade=got.append)
        assert len(got) == 2
        assert got[1].trade.size == 75.0
        assert got[1].instrument is NIFTY_FUT

    def test_heartbeats_are_skipped_not_parsed(self):
        """Heartbeats must not be parsed as ticks; None ends the stream."""
        frames = ["ping", b"", encode_tick(snap_spec()), "ping"]
        conn, _ = self._connector(frames)
        got = []
        stats = conn.run(on_trade=got.append)
        assert len(got) == 1
        assert stats.heartbeats == 3  # two leading + one trailing; None ends the stream
        assert stats.protocol_errors == 0

    def test_subscriptions_sent_on_connect(self):
        conn, transport = self._connector([encode_tick(snap_spec())])
        conn.run(on_trade=lambda t: None)
        assert len(transport.sent) == 1
        assert b"subscribe" not in transport.sent[0]  # action is numeric, not a verb
        assert b'"action": 1' in transport.sent[0]

    def test_protocol_error_counted_not_raised(self):
        bad = bytearray(encode_tick(snap_spec()))
        bad[0] = 9
        conn, _ = self._connector([bytes(bad), encode_tick(snap_spec())])
        got = []
        stats = conn.run(on_trade=got.append)
        assert stats.protocol_errors == 1
        assert len(got) == 1  # the good packet still lands

    def test_unmapped_token_counted(self):
        conn, _ = self._connector([encode_tick(snap_spec(token="777777"))])
        got = []
        stats = conn.run(on_trade=got.append)
        assert stats.unmapped == 1
        assert got == []

    def test_max_frames_stops_consumption(self):
        frames = [encode_tick(snap_spec(volume_trade_for_the_day=100 + i)) for i in range(50)]
        conn, _ = self._connector(frames)
        got = []
        conn.run(on_trade=got.append, max_frames=5)
        assert len(got) == 5

    def test_transport_closed(self):
        conn, transport = self._connector([encode_tick(snap_spec())])
        conn.run(on_trade=lambda t: None)
        assert transport.closed is True


# ======================================================= end-to-end ingestion
class TestEndToEndIngestion:
    """Bytes -> decode -> reader -> router -> session chart -> footprint."""

    def _feed(self, day: date, prices_volumes, token="26000"):
        """`prices_volumes` entries are (price, cumulative_volume, aggressor).

        The book is built so the print lands on the requested side: a BUY print
        sits at the ask, a SELL print at the bid. That keeps the test about the
        ingestion path rather than about the side inference, which has its own
        tests above.
        """
        mapper = build_indian_token_map()
        plan = build_plan([(TokenNamespace.NSE_FO, int(token))])
        base_ms = int(datetime(day.year, day.month, day.day, 9, 20, tzinfo=IST).timestamp() * 1000)
        frames = []
        for i, (p, v, side) in enumerate(prices_volumes):
            if side is Side.BUY:
                bid, ask = round(p - 0.05, 2), p
            else:
                bid, ask = p, round(p + 0.05, 2)
            frames.append(
                encode_tick(
                    snap_spec(
                        token=token,
                        last_traded_price=p,
                        volume_trade_for_the_day=v,
                        exchange_timestamp_ms=base_ms + i * 30000,
                        bids=((bid, 100),),
                        asks=((ask, 100),),
                    )
                )
            )
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        chart = SessionChart(NIFTY_FUT.grid(), cal, bar_seconds=300)
        transport = ReplayTransport(frames)
        conn = SmartStreamConnector(plan, mapper, lambda i: transport)
        conn.run(on_trade=lambda rt: chart.add(rt.trade))
        return chart, conn

    def test_footprint_built_from_wire_bytes(self):
        chart, conn = self._feed(
            date(2026, 10, 14),
            [(24000.05, 100, Side.BUY), (24000.10, 175, Side.BUY), (24000.05, 250, Side.BUY)],
        )
        assert len(chart.bars) == 1
        bar = chart.bars[0]
        # 100 then 75 then 75 = 250 total, all at two adjacent ticks.
        assert bar.total_volume == 250.0
        assert bar.buy_volume == 250.0  # every print was at the ask
        assert conn.stats.trades == 3

    def test_delta_is_correct_from_the_wire(self):
        chart, _ = self._feed(
            date(2026, 10, 14),
            [(24000.05, 100, Side.BUY), (24000.05, 200, Side.BUY), (24000.00, 300, Side.SELL)],
        )
        # Increments: 100 buy, 100 buy, 100 sell -> +100.
        assert chart.cumulative_delta == pytest.approx(100.0)

    def test_two_days_reset_cumulative_delta(self):
        mapper = build_indian_token_map()
        plan = build_plan([(TokenNamespace.NSE_FO, 26000)])
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        chart = SessionChart(NIFTY_FUT.grid(), cal, bar_seconds=300)

        def day_frames(day, base_vol):
            base_ms = int(datetime(day.year, day.month, day.day, 9, 20, tzinfo=IST).timestamp() * 1000)
            return [
                encode_tick(snap_spec(last_traded_price=24000.05, volume_trade_for_the_day=base_vol + i * 100,
                                      exchange_timestamp_ms=base_ms + i * 60000,
                                      bids=((24000.00, 10),), asks=((24000.05, 10),)))
                for i in range(1, 4)
            ]

        for day in (date(2026, 10, 14), date(2026, 10, 15)):
            transport = ReplayTransport(day_frames(day, 0))
            conn = SmartStreamConnector(plan, mapper, lambda i: transport)
            conn.run(on_trade=lambda rt: chart.add(rt.trade))

        # Day 1 accumulates, day 2 starts from zero. Three 100-lot buys each day.
        assert chart.cumulative_delta == pytest.approx(300.0)
        assert len(chart.closed_sessions) == 1
        assert chart.closed_sessions[0].cumulative_delta == pytest.approx(300.0)

    def test_grid_does_not_catch_a_100x_scale_error(self):
        """Why the reader needs its own scale guard.

        2400005 is an exact multiple of 0.05, so an un-divided scaled price is
        perfectly grid-aligned. The grid cannot see the error: it would build a
        clean-looking footprint 100x too high.
        """
        grid = TickGrid(0.05)
        assert grid.is_aligned(24000.05) is True
        assert grid.is_aligned(2400005) is True  # looks fine, is wrong

    def test_reader_raises_on_implausible_price(self):
        """The guard that does catch it."""
        from orderflow.feed import PriceScaleError

        r = TickReader("26000", scale=1)  # pretend the caller forgot to divide
        with pytest.raises(PriceScaleError):
            r.read(decode_packet(encode_tick(snap_spec(last_traded_price=2400005.0))))

    def test_mcx_gold_ingestion_on_one_rupee_grid(self):
        mapper = build_indian_token_map()
        plan = build_plan([(TokenNamespace.MCX_FO, 42809)])
        cal = MarketCalendar(Market.MCX)
        chart = SessionChart(MCX_GOLD_FUT.grid(), cal, bar_seconds=300)
        base_ms = int(datetime(2026, 11, 9, 22, 0, tzinfo=IST).timestamp() * 1000)
        frames = [
            encode_tick(
                snap_spec(exchange_type=ExchangeType.MCX_FO, token="42809",
                          last_traded_price=71001.0, volume_trade_for_the_day=100 * (i + 1),
                          exchange_timestamp_ms=base_ms + i * 60000,
                          bids=((71000.0, 5),), asks=((71001.0, 5),))
            )
            for i in range(3)
        ]
        conn = SmartStreamConnector(plan, mapper, lambda i: ReplayTransport(frames))
        conn.run(on_trade=lambda rt: chart.add(rt.trade))
        assert chart.session_volume == 300.0
        assert chart.bars[0].sorted_levels[0].price == 71001.0

    def test_session_reset_survives_a_feed_volume_reset(self):
        """End of day 1 volume 50000, day 2 opens at 25: no negative print."""
        mapper = build_indian_token_map()
        plan = build_plan([(TokenNamespace.NSE_FO, 26000)])
        cal = MarketCalendar(Market.NSE, segment="NSE_FNO")
        chart = SessionChart(NIFTY_FUT.grid(), cal, bar_seconds=300)
        d1 = int(datetime(2026, 10, 14, 15, 0, tzinfo=IST).timestamp() * 1000)
        d2 = int(datetime(2026, 10, 15, 9, 20, tzinfo=IST).timestamp() * 1000)
        frames = [
            encode_tick(snap_spec(volume_trade_for_the_day=50000, exchange_timestamp_ms=d1,
                                  last_traded_price=24000.05, bids=((24000.00, 1),),
                                  asks=((24000.05, 1),))),
            encode_tick(snap_spec(volume_trade_for_the_day=25, exchange_timestamp_ms=d2,
                                  last_traded_price=24000.10, bids=((24000.05, 1),),
                                  asks=((24000.10, 1),))),
        ]
        conn = SmartStreamConnector(plan, mapper, lambda i: ReplayTransport(frames))
        conn.run(on_trade=lambda rt: chart.add(rt.trade))
        assert chart.session_volume == 25.0
        assert chart.cumulative_delta == pytest.approx(25.0)



# ====================================================== live transport semantics
class _FakeTransport:
    """Fake socket. `is_live` decides whether idle means 'wait' or 'done'."""

    def __init__(self, script, is_live=True, clock=None):
        self.script = list(script)
        self.is_live = is_live
        self._clock = clock
        self.i = 0
        self.sent = []
        self.closed = False
        self.connected = False

    def connect(self):
        self.connected = True

    def send(self, data):
        self.sent.append(data)

    def recv(self, timeout=None):
        if self._clock is not None:
            self._clock[0] += 5.0
        if self.i < len(self.script):
            f = self.script[self.i]
            self.i += 1
            return f
        return None  # idle

    def close(self):
        self.closed = True


class TestLiveTransportSemantics:
    def _conn(self, transport):
        mapper = build_indian_token_map()
        plan = build_plan([(TokenNamespace.NSE_FO, 26000)])
        return SmartStreamConnector(plan, mapper, lambda i: transport)

    def test_idle_timeout_does_not_end_a_live_stream(self):
        """A quiet patch on a live socket must not stop the feed.

        Replay: None means 'exhausted', stop. Live: None means 'no data yet',
        keep waiting. Treating them the same kills the feed on the first quiet
        minute, which looks exactly like a market outage.
        """
        clock = [0.0]
        t = _FakeTransport([None, None, encode_tick(snap_spec()), None], is_live=True, clock=clock)
        got = []
        self._conn(t).run(on_trade=got.append, max_seconds=100, clock=lambda: clock[0])
        assert len(got) == 1, "the trade after the idle period must still arrive"

    def test_replay_none_still_ends_the_stream(self):
        """The offline path must keep terminating on exhaustion."""
        t = _FakeTransport([encode_tick(snap_spec()), None], is_live=False)
        got = []
        self._conn(t).run(on_trade=got.append)
        assert len(got) == 1
        assert t.closed is True

    def test_replay_is_not_retried(self):
        """A replay must not enter the reconnect backoff (which sleeps for real)."""
        t = _FakeTransport([encode_tick(snap_spec())], is_live=False)
        clock = [0.0]
        stats = self._conn(t).run(on_trade=lambda rt: None, clock=lambda: clock[0])
        assert stats.reconnects == 0

    def test_live_heartbeat_sent_while_idle(self):
        clock = [0.0]
        t = _FakeTransport([None] * 20, is_live=True, clock=clock)
        conn = SmartStreamConnector(
            build_plan([(TokenNamespace.NSE_FO, 26000)]),
            build_indian_token_map(),
            lambda i: t,
            heartbeat_interval=10.0,
        )
        conn.run(on_trade=lambda rt: None, max_seconds=30, clock=lambda: clock[0])
        assert any(b"ping" in s for s in t.sent)

    def test_max_seconds_bounds_a_live_run(self):
        clock = [0.0]
        t = _FakeTransport([encode_tick(snap_spec())] * 1000, is_live=True, clock=clock)
        self._conn(t).run(on_trade=lambda rt: None, max_seconds=20, clock=lambda: clock[0])
        assert t.closed is True


# ==================================================================== totp
class TestTOTP:
    """RFC 6238 Appendix B vectors. These prove the login code is correct."""

    SECRET = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"  # base32 of "12345678901234567890"

    @pytest.mark.parametrize(
        "at_time,expected",
        [
            (59, "94287082"),
            (1111111109, "07081804"),
            (1111111111, "14050471"),
            (1234567890, "89005924"),
            (2000000000, "69279037"),
            (20000000000, "65353130"),
        ],
    )
    def test_rfc6238_vectors(self, at_time, expected):
        from orderflow.feed import totp

        assert totp(self.SECRET, at_time=at_time, digits=8) == expected

    def test_six_digits_and_zero_padding(self):
        from orderflow.feed import totp

        code = totp(self.SECRET, at_time=1111111109)
        assert len(code) == 6 and code.isdigit()

    def test_secret_with_spaces_and_padding_tolerated(self):
        from orderflow.feed import totp

        spaced = "GEZD GNBV GY3T QOJQ GEZD GNBV GY3T QOJQ"
        assert totp(spaced, at_time=59, digits=8) == totp(self.SECRET, at_time=59, digits=8)

    def test_bad_secret_raises(self):
        from orderflow.feed import totp

        with pytest.raises(ValueError, match="base32"):
            totp("not!valid!base32!!", at_time=59)
