"""Regression tests against a real SmartStream packet from Angel One.

`tests/data/nifty_live_snap_quote.bin` is one SNAP_QUOTE frame captured from the
live socket on 2026-10-21 while the NIFTY 27OCT26 future (token 48704) was
subscribed. It was taken outside market hours, so the prices are the last
snapshot of the previous session and the volume is that session's total. That
does not matter: the point of the fixture is the book layout, which is fixed
regardless of when the packet arrives.

This file exists because the bid/ask convention was wrong for a long time and
no synthetic test could catch it. The encoder and decoder agreed with each
other, so round-trip tests passed while the live mapping was inverted. Only a
real packet settles it, so one is checked in.
"""

from __future__ import annotations

from pathlib import Path

from orderflow.feed.protocol import decode_packet

FIXTURE = Path(__file__).parent / "data" / "nifty_live_snap_quote.bin"


def live_tick():
    return decode_packet(FIXTURE.read_bytes())


class TestRealPacketDecodes:
    def test_token_and_mode(self):
        t = live_tick()
        assert t.token == "48704"
        assert int(t.mode) == 3  # SNAP_QUOTE

    def test_book_is_not_crossed(self):
        """The whole reason this fixture exists.

        On the wire, flag==0 marks an ask and flag==1 marks a bid. Reading it
        the other way puts the asks below the bids, which is a crossed book:
        the decoder would hand out a bid above the ask and every inferred trade
        side flips.
        """
        t = live_tick()
        best_bid = t.bids[0].price_raw
        best_ask = t.asks[0].price_raw
        assert best_bid < best_ask, (
            f"crossed book: bid {best_bid/100} >= ask {best_ask/100}"
        )

    def test_flag_zero_levels_are_asks(self):
        """The flag==0 levels sat above the flag==1 levels in this packet.

        A bid is always below an ask, so flag==0 cannot be a bid.
        """
        t = live_tick()
        assert min(l.price_raw for l in t.asks) > max(l.price_raw for l in t.bids)

    def test_expected_price_bands(self):
        """Values from the capture, so a scale or offset change is caught."""
        t = live_tick()
        assert [round(l.price_raw / 100, 1) for l in t.bids] == [
            22893.1, 22893.0, 22892.7, 22892.5, 22892.0]
        assert [round(l.price_raw / 100, 1) for l in t.asks] == [
            22904.8, 22905.0, 22906.0, 22906.7, 22907.0]

    def test_five_levels_each_side(self):
        t = live_tick()
        assert len(t.bids) == 5
        assert len(t.asks) == 5

    def test_no_zero_price_levels(self):
        """The block is padded with zeros; a zero price is not a level.

        If a padded level survives, best_ask becomes 0 and every print looks
        like a buy at the ask.
        """
        t = live_tick()
        for level in t.bids + t.asks:
            assert level.price_raw > 0

    def test_last_traded_price_is_inside_the_spread(self):
        """A print must sit within the quoted market.

        This is a cheap, strong check: it fails if either side is inverted or
        if the price offsets are wrong.
        """
        t = live_tick()
        best_bid = t.bids[0].price_raw
        best_ask = t.asks[0].price_raw
        assert best_bid <= t.last_traded_price <= best_ask

    def test_fields_are_plausible(self):
        t = live_tick()
        assert t.volume_trade_for_the_day > 0
        assert t.open_interest > 0
        assert t.lower_circuit_limit < t.upper_circuit_limit

    def test_quantity_and_order_counts_present(self):
        t = live_tick()
        for level in t.bids + t.asks:
            assert level.quantity > 0
            assert level.orders >= 1


class TestSideInferenceOnTheRealBook:
    def test_quote_rule_uses_the_real_best_bid_and_ask(self):
        """The reader must latch the best bid and ask from the real frame.

        With the mapping inverted, best_ask_raw would hold 2289310 (the bid)
        and best_bid_raw would hold 2290480 (the ask), so the book would be
        crossed and the quote rule would flip every side.
        """
        from orderflow.feed.tick_reader import TickReader

        t = live_tick()
        r = TickReader(t.token, scale=t.scale)
        r.read(t)
        assert r.state.best_bid_raw == t.bids[0].price_raw
        assert r.state.best_ask_raw == t.asks[0].price_raw
        assert r.state.best_bid_raw < r.state.best_ask_raw

    def test_a_print_at_the_ask_is_a_buy(self):
        """Re-encode the real book with the print moved onto the ask.

        This exercises the quote rule end to end on the real level layout
        rather than a synthetic one.
        """
        from orderflow.core import Side
        from orderflow.feed.simulator import TickSpec, encode_tick
        from orderflow.feed.tick_reader import TickReader, SideSource

        t = live_tick()
        real_bid = t.bids[0].price_raw / t.scale
        real_ask = t.asks[0].price_raw / t.scale
        spec = TickSpec(
            mode=t.mode,
            exchange_type=t.exchange_type,
            token=t.token,
            exchange_timestamp_ms=t.exchange_timestamp,
            last_traded_price=real_ask,
            volume_trade_for_the_day=1_000,
            bids=((real_bid, 65),),
            asks=((real_ask, 65),),
            scale=t.scale,
        )
        r = TickReader(t.token, scale=t.scale)
        tt = r.read(decode_packet(encode_tick(spec)))
        assert tt is not None
        assert tt.trade.side is Side.BUY
        assert tt.source is SideSource.QUOTE_RULE

    def test_a_print_at_the_bid_is_a_sell(self):
        from orderflow.core import Side
        from orderflow.feed.simulator import TickSpec, encode_tick
        from orderflow.feed.tick_reader import TickReader, SideSource

        t = live_tick()
        real_bid = t.bids[0].price_raw / t.scale
        real_ask = t.asks[0].price_raw / t.scale
        spec = TickSpec(
            mode=t.mode,
            exchange_type=t.exchange_type,
            token=t.token,
            exchange_timestamp_ms=t.exchange_timestamp,
            last_traded_price=real_bid,
            volume_trade_for_the_day=1_000,
            bids=((real_bid, 65),),
            asks=((real_ask, 65),),
            scale=t.scale,
        )
        r = TickReader(t.token, scale=t.scale)
        tt = r.read(decode_packet(encode_tick(spec)))
        assert tt is not None
        assert tt.trade.side is Side.SELL
        assert tt.source is SideSource.QUOTE_RULE
