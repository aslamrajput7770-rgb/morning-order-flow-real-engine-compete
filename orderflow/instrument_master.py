"""Load the Angel One instrument master into a `TokenMapper`.

The master file
---------------
`https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json`
is a JSON array of ~150k records, one per tradable contract, refreshed daily.
Each record carries `token`, `symbol`, `name`, `expiry`, `strike`, `lotsize`,
`instrumenttype`, `exch_seg`, `tick_size`, `freeze_qty`.

Why the token numbers matter
----------------------------
`build_indian_token_map()` registers placeholder ids so the engine runs offline.
Those ids are NOT real. On the live feed they fail silently: the subscription is
accepted, no data ever arrives, and nothing reports an error. Two of the five
placeholders do not exist in the master at all. Subscribe with a real token from
this file.

Why `tick_size` is ignored
--------------------------
The master's `tick_size` field is not the traded tick. It reports 10.0 for
RELIANCE, 5.0 for INFY and 10.0 for a NIFTY future, when the real tick is 0.05.
Trusting it builds a footprint on a 10-rupee grid: every level collapses, and
because the number came from the broker's own file it looks authoritative. Tick
sizes come from exchange rules, which is what the engine already encodes. Only
tokens, symbols, expiries, lots and segment are taken from here.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from .instruments import (
    Exchange,
    Instrument,
    Segment,
    SessionKind,
    TokenMapper,
    TokenNamespace,
)

#: exch_seg -> (namespace, exchange, default session kind).
#: CDS is the currency derivatives segment: 09:00-17:00 IST, distinct from both
#: NSE equity derivatives and MCX. Its prices carry the 1e7 scale.
_SEGMENT_MAP: dict[str, tuple[TokenNamespace, Exchange, SessionKind]] = {
    "NSE": (TokenNamespace.NSE_EQ, Exchange.NSE, SessionKind.NSE_EQUITY),
    "NFO": (TokenNamespace.NSE_FO, Exchange.NSE, SessionKind.NSE_FNO),
    "BSE": (TokenNamespace.BSE_EQ, Exchange.BSE, SessionKind.NSE_EQUITY),
    "BFO": (TokenNamespace.BSE_FO, Exchange.BSE, SessionKind.NSE_FNO),
    "MCX": (TokenNamespace.MCX_FO, Exchange.MCX, SessionKind.MCX_NONAGRI),
    "NCDEX": (TokenNamespace.NCDEX_FO, Exchange.NCDEX, SessionKind.MCX_NONAGRI),
    "CDS": (TokenNamespace.CDS_FO, Exchange.NSE, SessionKind.NSE_CURRENCY),
}

#: Exchange-published tick sizes. NOT read from the master file (see module doc).
_TICK_BY_SEGMENT: dict[str, float] = {
    "NSE": 0.05,
    "NFO": 0.05,
    "BSE": 0.05,
    "BFO": 0.05,
    "MCX": 0.05,  # per-contract overrides below
    "NCDEX": 0.05,
}

#: MCX is not uniform. Bullion and most metals quote on a 1.00 tick; energy and
#: agri differ. Keyed on the instrument's `name` prefix.
_MCX_TICK_BY_NAME: dict[str, float] = {
    "GOLD": 1.0,
    "GOLDM": 0.5,
    "GOLDGUINEA": 1.0,
    "GOLDPETAL": 0.1,
    "SILVER": 1.0,
    "SILVERM": 1.0,
    "SILVERMIC": 0.1,
    "CRUDEOIL": 1.0,
    "CRUDEOILM": 1.0,
    "NATURALGAS": 0.1,
    "NATGASMINI": 0.1,
    "COPPER": 0.05,
    "ZINC": 0.05,
    "LEAD": 0.05,
    "ALUMINIUM": 0.05,
    "NICKEL": 0.1,
}


def _segment_of(record: dict) -> tuple[TokenNamespace, Exchange, SessionKind] | None:
    return _SEGMENT_MAP.get((record.get("exch_seg") or "").upper())


def _parse_expiry(raw: str) -> str | None:
    """Master expiries look like `27OCT2026`. Return an ISO date or None."""
    raw = (raw or "").strip()
    if not raw:
        return None
    for fmt in ("%d%b%Y", "%d-%b-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw.upper(), fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _tick_for(seg: str, name: str, instrumenttype: str) -> float:
    """Exchange tick size for a contract, from rules rather than the master.

    Matched longest-prefix-first. A plain prefix scan returns the wrong tick for
    SILVERMIC (0.1), because "SILVER" (1.0) is a prefix of it and the dict
    iteration order decides. That is the same trap as matching NIFTY against
    NIFTYNXT50, so it is resolved the same way: most specific name wins.
    """
    if seg == "MCX":
        best_prefix = ""
        best_tick = 1.0  # MCX default when the commodity is unknown
        for prefix, tick in _MCX_TICK_BY_NAME.items():
            if name.startswith(prefix) and len(prefix) > len(best_prefix):
                best_prefix, best_tick = prefix, tick
        return best_tick
    return _TICK_BY_SEGMENT.get(seg, 0.05)


def _classify(instrumenttype: str) -> Segment:
    t = (instrumenttype or "").upper()
    if t in ("FUTIDX", "FUTSTK", "FUTCOM", "FUTCUR", "FUTIRD"):
        return Segment.FUT
    if t in ("OPTIDX", "OPTSTK", "OPTCOM", "OPTCUR", "OPTIRD", "OPTFUT"):
        return Segment.OPT
    if t in ("AMXIDX", "INDEX", "IDX"):
        return Segment.IDX
    return Segment.EQ


@dataclass
class MasterStats:
    """What the loader did, so a caller can sanity-check a fresh file.

    The skip reasons are counted separately on purpose. Lumping a malformed
    token in with an unknown segment hides a real data problem behind a number
    that looks like normal filtering.
    """

    total_records: int = 0
    loaded: int = 0
    skipped_unknown_segment: int = 0
    skipped_bad_token: int = 0
    skipped_blank_symbol: int = 0
    skipped_duplicate: int = 0
    by_namespace: dict[str, int] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.by_namespace is None:
            self.by_namespace = {}

    @property
    def skipped(self) -> int:
        return (
            self.skipped_unknown_segment
            + self.skipped_bad_token
            + self.skipped_blank_symbol
            + self.skipped_duplicate
        )

    def __str__(self) -> str:
        segs = ", ".join(f"{k}={v}" for k, v in sorted(self.by_namespace.items()))
        return (
            f"{self.loaded}/{self.total_records} loaded, {self.skipped} skipped "
            f"[unknown_seg={self.skipped_unknown_segment} "
            f"bad_token={self.skipped_bad_token} "
            f"blank_symbol={self.skipped_blank_symbol} "
            f"duplicate={self.skipped_duplicate}]  [{segs}]"
        )


def parse_master(records: list[dict]) -> tuple[TokenMapper, MasterStats]:
    """Build a `TokenMapper` from parsed master records.

    Duplicate `(namespace, token)` pairs are skipped rather than overwriting:
    `TokenMapper.register` refuses to rebind a live token, and a master file
    that rebinds one is telling you something is wrong with the file.
    """
    mapper = TokenMapper()
    stats = MasterStats(total_records=len(records))

    for rec in records:
        seg_info = _segment_of(rec)
        if seg_info is None:
            stats.skipped_unknown_segment += 1
            continue
        namespace, exchange, session = seg_info

        try:
            token = int(rec["token"])
        except (KeyError, TypeError, ValueError):
            stats.skipped_bad_token += 1
            continue

        seg = rec.get("exch_seg", "").upper()
        name = (rec.get("name") or "").strip()
        symbol = (rec.get("symbol") or "").strip()
        if not symbol:
            stats.skipped_blank_symbol += 1
            continue

        instrument = Instrument(
            symbol=symbol,
            underlying=name or None,
            exchange=exchange,
            segment=_classify(rec.get("instrumenttype", "")),
            tick_size=_tick_for(seg, name, rec.get("instrumenttype", "")),
            lot_size=_as_int(rec.get("lotsize"), 1),
            session=session,
            expiry=_parse_expiry(rec.get("expiry", "")),
        )

        key = (namespace, token)
        if key in mapper._by_token:
            stats.skipped_duplicate += 1
            continue
        try:
            mapper.register(namespace, token, instrument)
        except ValueError:
            stats.skipped_duplicate += 1
            continue

        stats.loaded += 1
        stats.by_namespace[namespace.value] = stats.by_namespace.get(namespace.value, 0) + 1

    return mapper, stats


def _as_int(value, default: int) -> int:
    try:
        n = int(float(value))
    except (TypeError, ValueError):
        return default
    return n if n > 0 else default


def load_master(path: str | Path) -> tuple[TokenMapper, MasterStats]:
    """Load a downloaded instrument master file."""
    records = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise ValueError("instrument master must be a JSON array")
    return parse_master(records)


def fetch_master(dest: str | Path, url: str | None = None, timeout: float = 120.0) -> Path:
    """Download the master file. Standard library only (`urllib`).

    This is a plain HTTPS GET of a public file, so it is written without any
    HTTP library. It is the only network call in the package outside the feed.
    """
    import urllib.request

    url = url or DEFAULT_MASTER_URL
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310 - fixed https URL
        data = resp.read()
    dest.write_bytes(data)
    return dest


DEFAULT_MASTER_URL = (
    "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
)


def find_instrument(mapper: TokenMapper, symbol: str) -> Instrument | None:
    """Case-insensitive symbol lookup, for scripting against the master."""
    want = symbol.strip().upper()
    try:
        return mapper.by_symbol(symbol)
    except KeyError:
        pass
    for inst in mapper._by_symbol.values():
        if inst.symbol.upper() == want:
            return inst
    return None


def upcoming_futures(
    mapper: TokenMapper, underlying: str, today: date | None = None, limit: int = 1
) -> list[tuple[TokenNamespace, int, Instrument]]:
    """Nearest futures for an underlying, e.g. `upcoming_futures(m, "NIFTY")`.

    Matches the master's `underlying` field, NOT the symbol prefix. Symbol
    prefix matching is wrong: "NIFTY" is a prefix of "NIFTYNXT50" and
    "NIFTYFPI", so a prefix search for the Nifty future happily returns a
    Nifty Next 50 contract. Subscribing to it streams a different instrument and
    reports no error.

    Returns `(namespace, token, instrument)` sorted by expiry. The nearest
    contract is what a live feed should subscribe to.
    """
    want = underlying.strip().upper()
    today = today or date.today()
    out = []
    for (ns, tok), inst in mapper._by_token.items():
        if inst.segment is not Segment.FUT:
            continue
        if (inst.underlying or inst.symbol).upper() != want:
            continue
        if inst.expiry is None:
            continue
        exp = date.fromisoformat(inst.expiry)
        if exp < today:
            continue
        out.append((ns, tok, inst, exp))
    out.sort(key=lambda t: (t[3], t[2].symbol))
    return [(ns, tok, inst) for ns, tok, inst, _ in out[:limit]]


def build_live_token_map(
    master_path: str | Path | None = None,
) -> tuple[TokenMapper, MasterStats]:
    """Master-backed mapper, falling back to the offline placeholder map.

    The fallback keeps the offline demos and tests running when the master file
    has not been downloaded. It is NOT suitable for live trading: the
    placeholders are not real tokens, and a live subscription to them returns
    nothing at all while reporting no error.
    """
    if master_path is None:
        return build_placeholder_map(), MasterStats()

    path = Path(master_path)
    if not path.exists():
        raise FileNotFoundError(
            f"instrument master not found at {path}. Download it with "
            f"`python3 fetch_instruments.py` before going live."
        )
    return load_master(path)


def build_placeholder_map() -> TokenMapper:
    """The offline map. Placeholder ids; never use these against a live feed."""
    from .instruments import build_indian_token_map

    return build_indian_token_map()
