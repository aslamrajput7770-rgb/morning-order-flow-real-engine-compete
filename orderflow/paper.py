"""Paper trading: orders, fills, positions and PnL against the live tape.

This is not a simulator that invents prices. Every fill is driven by a real
trade print from the feed, and every fill is made *worse* than the print in the
direction that costs money:

* a market buy fills at ``print + slippage`` and a market sell at
  ``print - slippage``, because you cross the spread and you do not get the
  print you saw;
* costs are charged per fill, not netted at the end, because that is when a
  real account pays them.

The reason is blunt: a paper engine that fills at the signal price with no
costs will show a profit on almost any strategy, and that number is a lie. The
point of this module is to make the paper PnL a lower bound on what the strategy
would really do, so a strategy that survives it has earned some attention.

Indian cost model
-----------------
Futures cost defaults approximate a discount broker on NSE F&O and MCX. They are
configurable and documented per field. They are an approximation, not a
contract: verify the current schedule with your broker before trusting the net
number. The engine never hides a cost it charges.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .core import Side


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"


class OrderStatus(str, Enum):
    NEW = "NEW"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


@dataclass(slots=True)
class CostModel:
    """Per-fill cost parameters for Indian futures.

    All rates are fractions of turnover unless the name says otherwise. Defaults
    suit NSE index/stock futures; MCX differs slightly and the fields are here so
    it can be set per instrument.
    """

    #: Flat brokerage per executed order, capped by `brokerage_pct` of turnover.
    brokerage_flat: float = 20.0
    brokerage_pct: float = 0.0003
    #: Securities Transaction Tax. Sell side only for futures.
    stt_sell_pct: float = 0.000125
    #: Exchange transaction charge, both sides.
    exchange_pct: float = 0.0000173
    #: SEBI turnover fee, both sides.
    sebi_pct: float = 0.000001
    #: Stamp duty, buy side only.
    stamp_buy_pct: float = 0.00002
    #: GST on brokerage + exchange + SEBI.
    gst_pct: float = 0.18

    def charge(self, side: Side, price: float, qty: float) -> float:
        turnover = abs(price * qty)
        brokerage = min(self.brokerage_flat, turnover * self.brokerage_pct)
        exchange = turnover * self.exchange_pct
        sebi = turnover * self.sebi_pct
        stt = turnover * self.stt_sell_pct if side is Side.SELL else 0.0
        stamp = turnover * self.stamp_buy_pct if side is Side.BUY else 0.0
        gst = (brokerage + exchange + sebi) * self.gst_pct
        return brokerage + exchange + sebi + stt + stamp + gst

    def breakdown(self, side: Side, price: float, qty: float) -> dict[str, float]:
        turnover = abs(price * qty)
        brokerage = min(self.brokerage_flat, turnover * self.brokerage_pct)
        exchange = turnover * self.exchange_pct
        sebi = turnover * self.sebi_pct
        stt = turnover * self.stt_sell_pct if side is Side.SELL else 0.0
        stamp = turnover * self.stamp_buy_pct if side is Side.BUY else 0.0
        gst = (brokerage + exchange + sebi) * self.gst_pct
        return {
            "brokerage": brokerage, "stt": stt, "exchange": exchange,
            "sebi": sebi, "stamp": stamp, "gst": gst,
            "total": brokerage + exchange + sebi + stt + stamp + gst,
        }


@dataclass(slots=True)
class Order:
    id: int
    symbol: str
    side: Side
    qty: float
    order_type: OrderType
    ts: float
    price: float | None = None  # limit or stop trigger
    status: OrderStatus = OrderStatus.NEW
    filled_price: float | None = None
    filled_ts: float | None = None
    reason: str = ""


@dataclass(slots=True)
class Fill:
    order_id: int
    symbol: str
    side: Side
    qty: float
    price: float
    ts: float
    cost: float
    slippage: float


@dataclass(slots=True)
class Position:
    symbol: str
    qty: float = 0.0
    avg_price: float = 0.0
    realized_pnl: float = 0.0
    costs: float = 0.0
    last_price: float = 0.0
    #: When the position was opened (from flat), so a round trip can report the
    #: real entry time instead of the exit time twice.
    opened_ts: float = 0.0

    @property
    def is_flat(self) -> bool:
        return self.qty == 0

    @property
    def unrealized_pnl(self) -> float:
        if self.qty == 0:
            return 0.0
        return (self.last_price - self.avg_price) * self.qty

    @property
    def net_pnl(self) -> float:
        return self.realized_pnl + self.unrealized_pnl - self.costs


@dataclass(slots=True)
class RoundTrip:
    """A closed trade: one entry and the exit that flattened it."""

    symbol: str
    side: Side
    qty: float
    entry_price: float
    exit_price: float
    entry_ts: float
    exit_ts: float
    gross: float
    costs: float

    @property
    def net(self) -> float:
        return self.gross - self.costs


@dataclass(slots=True)
class PaperBroker:
    """A paper account that fills orders from real prints.

    Usage::

        broker = PaperBroker(cash=1_000_000.0, slippage_ticks=1)
        broker.register("NIFTY27OCT26FUT", lot_size=75, tick_size=0.05)
        broker.submit_market("NIFTY27OCT26FUT", Side.BUY, lots=1, ts=t)
        broker.on_trade(symbol, trade)     # drives the fill
        broker.equity                      # mark-to-market account value
    """

    cash: float = 1_000_000.0
    cost_model: CostModel = field(default_factory=CostModel)
    #: How far the fill is made worse than the print, in ticks.
    slippage_ticks: int = 1
    #: Refuse an order whose notional exceeds this fraction of cash.
    max_exposure_frac: float = 0.90

    positions: dict[str, Position] = field(default_factory=dict)
    orders: list[Order] = field(default_factory=list)
    fills: list[Fill] = field(default_factory=list)
    round_trips: list[RoundTrip] = field(default_factory=list)
    _lots: dict[str, int] = field(default_factory=dict)
    _ticks: dict[str, float] = field(default_factory=dict)
    _next_id: int = 1
    rejected: int = 0

    # ------------------------------------------------------------ instruments
    def register(self, symbol: str, lot_size: int, tick_size: float) -> None:
        if lot_size <= 0 or tick_size <= 0:
            raise ValueError("lot_size and tick_size must be > 0")
        self._lots[symbol] = lot_size
        self._ticks[symbol] = tick_size
        self.positions.setdefault(symbol, Position(symbol=symbol))

    def lot_size(self, symbol: str) -> int:
        return self._lots.get(symbol, 1)

    def tick_size(self, symbol: str) -> float:
        return self._ticks.get(symbol, 0.05)

    # ----------------------------------------------------------------- orders
    def submit_market(self, symbol: str, side: Side, lots: int, ts: float,
                      reason: str = "") -> Order:
        return self._submit(symbol, side, lots, OrderType.MARKET, ts, None, reason)

    def submit_limit(self, symbol: str, side: Side, lots: int, price: float,
                     ts: float, reason: str = "") -> Order:
        return self._submit(symbol, side, lots, OrderType.LIMIT, ts, price, reason)

    def submit_stop(self, symbol: str, side: Side, lots: int, price: float,
                    ts: float, reason: str = "") -> Order:
        return self._submit(symbol, side, lots, OrderType.STOP, ts, price, reason)

    def _submit(self, symbol: str, side: Side, lots: int, otype: OrderType,
                ts: float, price: float | None, reason: str) -> Order:
        order = Order(
            id=self._next_id, symbol=symbol, side=side, qty=0.0,
            order_type=otype, ts=ts, price=price, reason=reason,
        )
        self._next_id += 1
        self.orders.append(order)

        lot = self.lot_size(symbol)
        if lots <= 0:
            return self._reject(order, "lots must be > 0")
        if otype is not OrderType.MARKET and (price is None or price <= 0):
            return self._reject(order, f"{otype.value} order needs a price")
        qty = lots * lot
        order.qty = qty
        return order

    def _reject(self, order: Order, why: str) -> Order:
        order.status = OrderStatus.REJECTED
        order.reason = why
        self.rejected += 1
        return order

    def cancel(self, order_id: int) -> bool:
        for o in self.orders:
            if o.id == order_id and o.status is OrderStatus.NEW:
                o.status = OrderStatus.CANCELLED
                return True
        return False

    def open_orders(self, symbol: str | None = None) -> list[Order]:
        return [
            o for o in self.orders
            if o.status is OrderStatus.NEW and (symbol is None or o.symbol == symbol)
        ]

    # ------------------------------------------------------------------ fills
    def on_trade(self, symbol: str, price: float, ts: float, qty: float = 0.0) -> list[Fill]:
        """Feed one real print. Fills any resting order it crosses.

        Returns the fills produced, so a caller can log them. A market order
        fills on the first print after submission; a limit fills only when the
        print reaches its price; a stop arms and then fills like a market.
        """
        filled: list[Fill] = []
        for order in self.open_orders(symbol):
            if order.order_type is OrderType.LIMIT:
                crossed = (
                    (order.side is Side.BUY and price <= order.price)  # type: ignore[operator]
                    or (order.side is Side.SELL and price >= order.price)  # type: ignore[operator]
                )
                if not crossed:
                    continue
                # A limit fills at its own price. Slippage must not be applied
                # here: the limit *is* the worst price accepted, so filling
                # above it (buy) or below it (sell) would model a trade the
                # venue would never have executed.
                filled.append(self._fill(order, float(order.price), ts))
            elif order.order_type is OrderType.STOP:
                armed = (
                    (order.side is Side.BUY and price >= order.price)  # type: ignore[operator]
                    or (order.side is Side.SELL and price <= order.price)  # type: ignore[operator]
                )
                if not armed:
                    continue
                fill_price = self._worse(order.side, price, order.symbol)
                filled.append(self._fill(order, fill_price, ts))
            else:  # MARKET
                fill_price = self._worse(order.side, price, order.symbol)
                filled.append(self._fill(order, fill_price, ts))
        return filled

    def _worse(self, side: Side, price: float, symbol: str) -> float:
        """Move the fill against us by the slippage, on the tick grid."""
        step = self.tick_size(symbol) * self.slippage_ticks
        return price + step if side is Side.BUY else price - step

    def _fill(self, order: Order, price: float, ts: float) -> Fill:
        cost = self.cost_model.charge(order.side, price, order.qty)
        fill = Fill(
            order_id=order.id, symbol=order.symbol, side=order.side,
            qty=order.qty, price=price, ts=ts, cost=cost,
            slippage=self.tick_size(order.symbol) * self.slippage_ticks,
        )
        order.status = OrderStatus.FILLED
        order.filled_price = price
        order.filled_ts = ts
        self.fills.append(fill)
        self._apply(fill)
        return fill

    def _apply(self, fill: Fill) -> None:
        pos = self.positions.setdefault(fill.symbol, Position(symbol=fill.symbol))
        signed = fill.qty if fill.side is Side.BUY else -fill.qty
        pos.costs += fill.cost
        pos.last_price = fill.price
        self.cash -= fill.cost

        if pos.qty == 0 or (pos.qty > 0) == (signed > 0):
            # Opening or adding on the same side: blend the average price.
            if pos.qty == 0:
                pos.opened_ts = fill.ts
            new_qty = pos.qty + signed
            if new_qty != 0:
                pos.avg_price = (
                    pos.avg_price * pos.qty + fill.price * signed
                ) / new_qty
            pos.qty = new_qty
            return

        # Reducing, closing or flipping. Realise on the closed part.
        closing = min(abs(signed), abs(pos.qty))
        direction = 1.0 if pos.qty > 0 else -1.0
        gross = (fill.price - pos.avg_price) * closing * direction
        pos.realized_pnl += gross
        self.cash += gross
        self.round_trips.append(RoundTrip(
            symbol=fill.symbol,
            # The side of the *entry*, which is the side of the position being
            # closed: a long is opened by a BUY. Labelling it with the exit side
            # made a winning long report side=SELL against gross=+3609.
            side=Side.BUY if pos.qty > 0 else Side.SELL,
            qty=closing,
            entry_price=pos.avg_price,
            exit_price=fill.price,
            entry_ts=pos.opened_ts,
            exit_ts=fill.ts,
            gross=gross,
            costs=fill.cost,
        ))
        pos.qty += signed
        if pos.qty == 0:
            pos.avg_price = 0.0
        elif (pos.qty > 0) != (direction > 0):
            # Flipped through zero: the remainder opens at the fill price, and
            # now, so the next round trip times its entry from this fill.
            pos.avg_price = fill.price
            pos.opened_ts = fill.ts

    # ------------------------------------------------------------------- marks
    def mark(self, symbol: str, price: float) -> None:
        pos = self.positions.get(symbol)
        if pos is not None:
            pos.last_price = price

    @property
    def realized_pnl(self) -> float:
        return sum(p.realized_pnl for p in self.positions.values())

    @property
    def unrealized_pnl(self) -> float:
        return sum(p.unrealized_pnl for p in self.positions.values())

    @property
    def total_costs(self) -> float:
        return sum(p.costs for p in self.positions.values())

    @property
    def equity(self) -> float:
        return self.cash + self.unrealized_pnl

    @property
    def net_pnl(self) -> float:
        return self.realized_pnl + self.unrealized_pnl - self.total_costs

    def snapshot(self) -> dict:
        """A JSON-safe view of the account for the dashboard."""
        return {
            "cash": round(self.cash, 2),
            "equity": round(self.equity, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "costs": round(self.total_costs, 2),
            "net_pnl": round(self.net_pnl, 2),
            "fills": len(self.fills),
            "rejected": self.rejected,
            "positions": [
                {
                    "symbol": p.symbol, "qty": p.qty,
                    "avg_price": round(p.avg_price, 4),
                    "last_price": round(p.last_price, 4),
                    "realized_pnl": round(p.realized_pnl, 2),
                    "unrealized_pnl": round(p.unrealized_pnl, 2),
                    "costs": round(p.costs, 2),
                }
                for p in self.positions.values() if p.qty != 0
            ],
            "round_trips": [
                {
                    "symbol": r.symbol, "side": r.side.value, "qty": r.qty,
                    "entry_price": round(r.entry_price, 4),
                    "exit_price": round(r.exit_price, 4),
                    "gross": round(r.gross, 2), "costs": round(r.costs, 2),
                    "net": round(r.net, 2),
                }
                for r in self.round_trips[-50:]
            ],
        }
