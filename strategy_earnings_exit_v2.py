from typing import Optional
from utcxchangelib import XChangeClient, Side
import asyncio
import time
import csv
import os
import atexit

ALL_SYMBOLS = [
    "A", "B", "C", "ETF",
    "B_C_950", "B_C_1000", "B_C_1050",
    "B_P_950", "B_P_1000", "B_P_1050",
    # "R_CUT", "R_HOLD", "R_HIKE",
]

# ---------- MM params ----------
QUOTE_SIZE_A = 5
QUOTE_SIZE_B = 3
QUOTE_SIZE_C = 5
SOFT_CAP = {"A": 10, "B": 10, "C": 10}
MAX_POSITION = 200
MIN_SPREAD_TO_QUOTE = 5
REQUOTE_COOLDOWN = 0.3
NON_EARNINGS_PAUSE = 5.0
SKEW_SHIFT_FACTOR = 4.0
PENNY_MM_ENABLED = False

# ---------- Earnings burst params ----------
MAX_ORDER_SIZE = 40
MAX_OPEN_ORDERS = 50
MAX_OUTSTANDING_VOL = 120
HOLD_DURATION = 5.0

# Per-symbol burst sizing (fraction of max burst).
# A is most volatile -> full size. C moves less -> scale down.
BURST_SIZE_MULT = {"A": 1.0, "ETF": 0.5, "C": 0.35}

# ---------- ETF arb params ----------
ETF_FEE = 5           # swap fee assumption (tune)
ETF_ARB_BUFFER = 3    # extra edge required beyond fee
ETF_ARB_SIZE = 2

# ---------- Options params ----------
STRIKES = [950, 1000, 1050]
PCP_BUFFER = 4        # min edge in pts to trade PCP
BOX_BUFFER = 3        # min edge for box arb
OPTION_ARB_SIZE = 2

# ---------- Prediction market params ----------
# PRED_SYMS = ["R_CUT", "R_HOLD", "R_HIKE"]
# PRED_ARB_BUFFER = 2   # require sum mispriced by >2 pts (pts ~ 0.01)
# PRED_ARB_SIZE = 3

# ---------- Logging ----------
FILL_LOG = "fill_log.csv"
EVENT_LOG = "event_log.csv"
ALL_TICKS_LOG = "all_ticks.csv"
ALL_NEWS_LOG = "all_news.csv"
NEWS_PNL_LOG = "news_pnl_log.csv"
ALL_TICK_INTERVAL = 0.5
NEWS_PNL_SNAPSHOT_OFFSETS = [0, 2, 5, 10, 20, 30]
RESET_NEWS_PNL_EACH_ITERATION = False
NEWS_PNL_HEADER = [
    "event_id", "news_wall_time", "snapshot_wall_time", "offset_s",
    "kind", "subtype", "asset", "symbol_field", "value_or_text",
    "pnl_A", "pnl_B", "pnl_C", "pnl_ETF", "pnl_TOTAL"
]

# ---------- Unwind / fade params ----------
UNWIND_DURATION = 8.0
UNWIND_CHUNK = 20
UNWIND_PASSIVE_SECONDS = 0.0
FADE_ENABLED = False
FADE_DELAY = 1.5
FADE_FRACTION = 0.25
FADE_FRACTION_BY_SYM = {"A": 0.25, "ETF": 0.15}
FADE_HOLD = 2.0
FADE_SYMBOLS = {"A", "ETF"}

# ---------- Lowered arb buffers ----------
PCP_BUFFER = 1
BOX_BUFFER = 1
MONO_BUFFER = 2

PNL_LOG = "pnl_log.csv"  # in case it was missing

STRAT_ETF_ARB = "ETF ARB"
STRAT_A_EARN = "Stock A Earnings"
STRAT_C_EARN = "Stock C Earnings"
STRAT_PENNY_MM = "Penny MM"
STRAT_FADE = "Fade"
STRAT_UNWIND = "Unwind"
STRAT_PCP = "PCP Arb"
STRAT_BOX = "Box Arb"
STRAT_MONO = "Option Monotonicity"
STRATEGY_SUMMARY_ORDER = [
    STRAT_ETF_ARB,
    STRAT_A_EARN,
    STRAT_C_EARN,
    STRAT_PENNY_MM,
    STRAT_FADE,
    STRAT_UNWIND,
    STRAT_PCP,
    STRAT_BOX,
    STRAT_MONO,
    "UNLABELED",
]


class StrategyBot(XChangeClient):
    def __init__(self, host, username, password):
        super().__init__(host, username, password)

        self.open_order_sides: dict = {}
        self.open_orders_meta: dict = {}
        # per-symbol resting quotes: {sym: {"bid": oid, "ask": oid}}
        self.my_quotes = {s: {"bid": None, "ask": None} for s in ("A", "B", "C")}

        self.phase: str = "PENNY"
        self.phase_started: float = 0.0
        self.last_eps_A: Optional[float] = None
        self.last_eps_C: Optional[float] = None
        self.pause_until: float = 0.0
        self.last_requote_ts: dict = {"A": 0.0, "B": 0.0, "C": 0.0}

        # Fed prediction market state (disabled)
        # self.implied_delta_r: float = 0.0  # expected bp change
        # self.y0: Optional[float] = None    # baseline yield proxy (mid of HOLD? just track)

        self._fill_count = 0
        self._first_news_logged = False
        self._news_track_id = 0

        self._burst_symbol = None
        self._burst_direction = None
        self._burst_size = 0
        self._burst_symbols = []
        self._burst_sizes = {}
        self._faded = False
        self._unwind_started = 0.0
        self._pcp_stats = {"total": 0, "books": 0, "triggered": 0}
        self._box_stats = {"total": 0, "books": 0, "triggered": 0}
        self._mono_stats = {"total": 0, "triggered": 0}

        # Per-symbol PnL ledger (tracked from fills).
        self.pnl_position = {s: 0 for s in ALL_SYMBOLS}
        self.avg_cost = {s: 0.0 for s in ALL_SYMBOLS}
        self.realized_pnl = {s: 0.0 for s in ALL_SYMBOLS}
        self.last_mid = {s: None for s in ALL_SYMBOLS}

        # Per-strategy PnL ledger (tracked from fills tagged by strategy).
        self.strategy_position = {}
        self.strategy_avg_cost = {}
        self.strategy_realized = {}

        self._news_pnl_lock = asyncio.Lock()
        self._news_pnl_epoch = 0

        atexit.register(self._write_final_pnl_log)

        self._init_logs()

    # ---------- logging ----------
    def _init_logs(self):
        new = not os.path.exists(FILL_LOG)
        self._fill_file = open(FILL_LOG, "a", newline="", encoding="utf-8")
        self._fill_writer = csv.writer(self._fill_file)
        if new:
            self._fill_writer.writerow(["fill_idx", "wall_time", "symbol", "side", "qty", "price",
                                        "phase_at_fill", "position_after", "mid_at_fill"])
            self._fill_file.flush()

        new = not os.path.exists(EVENT_LOG)
        self._event_file = open(EVENT_LOG, "a", newline="", encoding="utf-8")
        self._event_writer = csv.writer(self._event_file)
        if new:
            self._event_writer.writerow(["wall_time", "event_type", "details", "phase_after"])
            self._event_file.flush()

        new = not os.path.exists(ALL_TICKS_LOG)
        self._all_tick_file = open(ALL_TICKS_LOG, "a", newline="", encoding="utf-8")
        self._all_tick_writer = csv.writer(self._all_tick_file)
        if new:
            cols = ["wall_time"]
            for s in ALL_SYMBOLS:
                cols += [f"{s}_bid", f"{s}_ask", f"{s}_mid", f"{s}_spread"]
            self._all_tick_writer.writerow(cols)
            self._all_tick_file.flush()

        new = not os.path.exists(ALL_NEWS_LOG)
        self._all_news_file = open(ALL_NEWS_LOG, "a", newline="", encoding="utf-8")
        self._all_news_writer = csv.writer(self._all_news_file)
        if new:
            self._all_news_writer.writerow(["wall_time", "kind", "subtype", "asset_field",
                                            "symbol_field", "value_or_text"])
            self._all_news_file.flush()

        # Reset news-pnl log at the start of each run.
        self._news_pnl_file = open(NEWS_PNL_LOG, "w", newline="", encoding="utf-8")
        self._news_pnl_writer = csv.writer(self._news_pnl_file)
        self._news_pnl_writer.writerow(NEWS_PNL_HEADER)
        self._news_pnl_file.flush()

        if not os.path.exists(PNL_LOG):
            with open(PNL_LOG, "w", newline="", encoding="utf-8") as pnl_file:
                pnl_writer = csv.writer(pnl_file)
                pnl_writer.writerow(["category", "label", "pnl"])

    def _log_event(self, event_type, details):
        self._event_writer.writerow([time.time(), event_type, details, self.phase])
        self._event_file.flush()

    def _update_symbol_pnl_on_fill(self, symbol: str, side: str, qty: int, price: float):
        if symbol not in self.pnl_position or side not in ("BUY", "SELL") or qty <= 0:
            return

        prev_pos = self.pnl_position[symbol]
        prev_avg = self.avg_cost[symbol]
        signed_qty = qty if side == "BUY" else -qty
        new_pos = prev_pos + signed_qty

        same_direction = (
            prev_pos == 0
            or (prev_pos > 0 and signed_qty > 0)
            or (prev_pos < 0 and signed_qty < 0)
        )

        if same_direction:
            if new_pos != 0:
                prior_notional = abs(prev_pos) * prev_avg
                trade_notional = abs(signed_qty) * price
                self.avg_cost[symbol] = (prior_notional + trade_notional) / abs(new_pos)
        else:
            close_qty = min(abs(prev_pos), abs(signed_qty))
            if prev_pos > 0:
                self.realized_pnl[symbol] += (price - prev_avg) * close_qty
            else:
                self.realized_pnl[symbol] += (prev_avg - price) * close_qty

            if abs(signed_qty) > abs(prev_pos):
                self.avg_cost[symbol] = float(price)
            elif new_pos == 0:
                self.avg_cost[symbol] = 0.0

        if new_pos == 0:
            self.avg_cost[symbol] = 0.0

        self.pnl_position[symbol] = new_pos

    def _ensure_strategy_ledger(self, strategy_label: str):
        if strategy_label in self.strategy_position:
            return
        self.strategy_position[strategy_label] = {s: 0 for s in ALL_SYMBOLS}
        self.strategy_avg_cost[strategy_label] = {s: 0.0 for s in ALL_SYMBOLS}
        self.strategy_realized[strategy_label] = 0.0

    def _update_strategy_pnl_on_fill(self, strategy_label: str, symbol: str, side: str, qty: int, price: float):
        if symbol not in ALL_SYMBOLS or side not in ("BUY", "SELL") or qty <= 0:
            return

        self._ensure_strategy_ledger(strategy_label)
        pos_map = self.strategy_position[strategy_label]
        avg_map = self.strategy_avg_cost[strategy_label]

        prev_pos = pos_map[symbol]
        prev_avg = avg_map[symbol]
        signed_qty = qty if side == "BUY" else -qty
        new_pos = prev_pos + signed_qty

        same_direction = (
            prev_pos == 0
            or (prev_pos > 0 and signed_qty > 0)
            or (prev_pos < 0 and signed_qty < 0)
        )

        if same_direction:
            if new_pos != 0:
                prior_notional = abs(prev_pos) * prev_avg
                trade_notional = abs(signed_qty) * price
                avg_map[symbol] = (prior_notional + trade_notional) / abs(new_pos)
        else:
            close_qty = min(abs(prev_pos), abs(signed_qty))
            if prev_pos > 0:
                self.strategy_realized[strategy_label] += (price - prev_avg) * close_qty
            else:
                self.strategy_realized[strategy_label] += (prev_avg - price) * close_qty

            if abs(signed_qty) > abs(prev_pos):
                avg_map[symbol] = float(price)
            elif new_pos == 0:
                avg_map[symbol] = 0.0

        if new_pos == 0:
            avg_map[symbol] = 0.0

        pos_map[symbol] = new_pos

    def _write_final_pnl_log(self):
        rows = []
        asset_totals = {}
        portfolio_total = 0.0
        mids = {}

        for symbol in ALL_SYMBOLS:
            pos = self.pnl_position.get(symbol, 0)
            avg = self.avg_cost.get(symbol, 0.0)
            realized = self.realized_pnl.get(symbol, 0.0)

            mid = self._mid(symbol)
            if mid is not None:
                self.last_mid[symbol] = mid
            else:
                mid = self.last_mid[symbol]
            mids[symbol] = mid

            unrealized = (mid - avg) * pos if mid is not None else 0.0
            total = realized + unrealized
            portfolio_total += total
            asset_totals[symbol] = total
            rows.append(["ASSET", symbol, int(round(total))])

        rows.append(["ASSET", "TOTAL", int(round(portfolio_total))])

        strategy_labels = list(STRATEGY_SUMMARY_ORDER)
        for label in self.strategy_position.keys():
            if label not in strategy_labels:
                strategy_labels.append(label)

        strategy_portfolio_total = 0.0
        for label in strategy_labels:
            self._ensure_strategy_ledger(label)
            realized = self.strategy_realized.get(label, 0.0)
            unrealized = 0.0
            pos_map = self.strategy_position.get(label, {})
            avg_map = self.strategy_avg_cost.get(label, {})
            for symbol in ALL_SYMBOLS:
                pos = pos_map.get(symbol, 0)
                avg = avg_map.get(symbol, 0.0)
                mid = mids.get(symbol)
                if mid is not None:
                    unrealized += (mid - avg) * pos
            total = realized + unrealized
            strategy_portfolio_total += total
            rows.append(["STRATEGY", label, int(round(total))])

        rows.append(["STRATEGY", "TOTAL", int(round(strategy_portfolio_total))])

        with open(PNL_LOG, "w", newline="", encoding="utf-8") as pnl_file:
            pnl_writer = csv.writer(pnl_file)
            pnl_writer.writerow(["category", "label", "pnl"])
            pnl_writer.writerows(rows)

    def _compute_asset_pnl_totals(self):
        totals = {}
        portfolio_total = 0.0
        for symbol in ALL_SYMBOLS:
            pos = self.pnl_position.get(symbol, 0)
            avg = self.avg_cost.get(symbol, 0.0)
            realized = self.realized_pnl.get(symbol, 0.0)

            mid = self._mid(symbol)
            if mid is not None:
                self.last_mid[symbol] = mid
            else:
                mid = self.last_mid[symbol]

            unrealized = (mid - avg) * pos if mid is not None else 0.0
            total = realized + unrealized
            totals[symbol] = total
            portfolio_total += total

        totals["TOTAL"] = portfolio_total
        return totals

    async def _reset_news_pnl_log(self):
        async with self._news_pnl_lock:
            self._news_pnl_epoch += 1
            self._news_track_id = 0
            self._news_pnl_file.seek(0)
            self._news_pnl_file.truncate()
            self._news_pnl_writer.writerow(NEWS_PNL_HEADER)
            self._news_pnl_file.flush()

    async def _track_news_pnl_snapshots(
        self,
        event_id: int,
        epoch: int,
        news_wall_time: float,
        kind: str,
        subtype: str,
        asset: str,
        symbol_field: str,
        text: str,
    ):
        start = time.monotonic()
        prev_offset = 0.0
        for offset in NEWS_PNL_SNAPSHOT_OFFSETS:
            wait_for = max(0.0, float(offset) - prev_offset)
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            prev_offset = float(offset)

            if epoch != self._news_pnl_epoch:
                return

            totals = self._compute_asset_pnl_totals()
            row = [
                event_id,
                news_wall_time,
                time.time(),
                offset,
                kind,
                subtype,
                asset,
                symbol_field,
                text,
                round(totals.get("A", 0.0), 2),
                round(totals.get("B", 0.0), 2),
                round(totals.get("C", 0.0), 2),
                round(totals.get("ETF", 0.0), 2),
                round(totals.get("TOTAL", 0.0), 2),
            ]
            async with self._news_pnl_lock:
                self._news_pnl_writer.writerow(row)
                self._news_pnl_file.flush()

    # ---------- order helpers ----------
    def _open_orders_count(self) -> int:
        return len(self.open_orders_meta)

    def _outstanding_volume(self, symbol: str) -> int:
        total = 0
        for meta in self.open_orders_meta.values():
            if meta.get("symbol") == symbol:
                total += int(meta.get("qty_remaining", 0))
        return total

    async def _place(self, symbol, qty, side, price=None, strategy_label="UNLABELED"):
        if qty <= 0:
            return None

        # Respect hard exchange size limits per order.
        qty = int(min(qty, MAX_ORDER_SIZE))

        if self._open_orders_count() >= MAX_OPEN_ORDERS:
            print(f"[PLACE SKIP] {symbol} max open orders reached ({MAX_OPEN_ORDERS})")
            return None

        headroom = MAX_OUTSTANDING_VOL - self._outstanding_volume(symbol)
        if headroom <= 0:
            print(f"[PLACE SKIP] {symbol} outstanding volume limit reached ({MAX_OUTSTANDING_VOL})")
            return None

        qty = int(min(qty, headroom))
        if qty <= 0:
            return None

        try:
            oid = await self.place_order(symbol, qty, side, price)
        except Exception as e:
            print(f"[PLACE ERR] {symbol} {e}")
            return None
        if oid:
            self._ensure_strategy_ledger(strategy_label)
            self.open_order_sides[oid] = ("BUY" if side == Side.BUY else "SELL", symbol)
            self.open_orders_meta[oid] = {
                "symbol": symbol,
                "qty_remaining": qty,
                "strategy": strategy_label,
            }
        return oid

    async def _cancel(self, oid):
        try:
            await self.cancel_order(oid)
        except Exception:
            pass

    async def _cancel_sym_quotes(self, symbol):
        q = self.my_quotes.get(symbol)
        if not q:
            return
        for k in ("bid", "ask"):
            if q[k] is not None:
                await self._cancel(q[k])
                q[k] = None

    async def _cancel_all_quotes(self):
        for s in list(self.my_quotes.keys()):
            await self._cancel_sym_quotes(s)

    def _bbo(self, symbol):
        book = self.order_books.get(symbol)
        if not book:
            return None, None
        bids = [p for p, q in book.bids.items() if q > 0]
        asks = [p for p, q in book.asks.items() if q > 0]
        return (max(bids) if bids else None, min(asks) if asks else None)

    def _mid(self, symbol):
        b, a = self._bbo(symbol)
        if b is None or a is None:
            return None
        return (b + a) / 2

    # ---------- sampler ----------
    async def _all_asset_sampler(self):
        await asyncio.sleep(2)
        while True:
            row = [time.time()]
            for s in ALL_SYMBOLS:
                bid, ask = self._bbo(s)
                if bid is not None and ask is not None:
                    row += [bid, ask, (bid + ask) / 2, ask - bid]
                else:
                    row += ["", "", "", ""]
            self._all_tick_writer.writerow(row)
            self._all_tick_file.flush()
            await asyncio.sleep(ALL_TICK_INTERVAL)

    def _enter_phase(self, phase):
        old = self.phase
        self.phase = phase
        self.phase_started = time.monotonic()
        print(f"[PHASE] {old} -> {phase}")
        self._log_event("phase_change", f"{old}->{phase}")

    # ---------- earnings directional burst (A) ----------
    async def _enter_directional(self, symbol, direction, eps_delta):
        # Decide which symbols to burst. A earnings -> also burst ETF.
        burst_syms = [symbol]
        if symbol == "A":
            burst_syms.append("ETF")

        await self._cancel_all_quotes()
        self._burst_symbols = []
        self._burst_direction = direction
        self._burst_sizes = {}
        self._faded = False

        for sym in burst_syms:
            bid, ask = self._bbo(sym)
            if bid is None or ask is None:
                continue
            cur_pos = self.positions.get(sym, 0)
            mult = BURST_SIZE_MULT.get(sym, 1.0)
            sym_max = int(MAX_OUTSTANDING_VOL * mult)
            sym_pos_cap = int(MAX_POSITION * mult)
            if direction == "LONG":
                burst = min(max(0, sym_pos_cap - cur_pos), sym_max)
                side, px = Side.BUY, ask
            else:
                burst = min(max(0, cur_pos + sym_pos_cap), sym_max)
                side, px = Side.SELL, bid
            if burst <= 0:
                continue
            self._burst_symbols.append(sym)
            self._burst_sizes[sym] = burst
            print(f"[DIRECTIONAL {sym}] {direction} {burst} @ {px} eps_delta={eps_delta:+.2f}")
            self._log_event(f"directional_{direction}", f"{sym} qty={burst} eps_delta={eps_delta}")
            burst_label = STRAT_A_EARN if symbol == "A" else STRAT_C_EARN
            rem = burst
            while rem > 0:
                chunk = min(MAX_ORDER_SIZE, rem)
                await self._place(sym, chunk, side, px, strategy_label=burst_label)
                rem -= chunk

        # keep legacy single-symbol fields in sync for anything that still reads them
        self._burst_symbol = self._burst_symbols[0] if self._burst_symbols else symbol
        self._burst_size = sum(self._burst_sizes.values())
        self._enter_phase("HOLD_LONG" if direction == "LONG" else "HOLD_SHORT")

    async def _fade_burst(self):
        if not FADE_ENABLED or not self._burst_symbols:
            return
        for sym in self._burst_symbols:
            if sym not in FADE_SYMBOLS:
                continue
            size = self._burst_sizes.get(sym, 0)
            if size == 0:
                continue
            bid, ask = self._bbo(sym)
            if bid is None or ask is None:
                continue
            fade_fraction = FADE_FRACTION_BY_SYM.get(sym, FADE_FRACTION)
            fade_qty = int(size * fade_fraction)
            if fade_qty <= 0:
                continue
            if self._burst_direction == "LONG":
                side, px = Side.SELL, bid
            else:
                side, px = Side.BUY, ask
            print(f"[FADE] {sym} {side} {fade_qty} @ {px}")
            self._log_event("fade", f"{sym} qty={fade_qty}")
            rem = fade_qty
            while rem > 0:
                chunk = min(MAX_ORDER_SIZE, rem)
                await self._place(sym, chunk, side, px, strategy_label=STRAT_FADE)
                rem -= chunk

    async def _force_unwind(self, symbol):
        pos = self.positions.get(symbol, 0)
        if pos == 0:
            return True
        bid, ask = self._bbo(symbol)
        if bid is None or ask is None:
            return False
        elapsed_unwind = time.monotonic() - self._unwind_started
        if elapsed_unwind < UNWIND_PASSIVE_SECONDS:
            mid = (bid + ask) / 2
            px = int(round(mid))
        else:
            px = bid if pos > 0 else ask
        chunk_cap = 15 if symbol == "ETF" else UNWIND_CHUNK
        if pos > 0:
            qty = min(pos, chunk_cap, MAX_ORDER_SIZE)
            await self._place(symbol, qty, Side.SELL, px, strategy_label=STRAT_UNWIND)
        else:
            qty = min(-pos, chunk_cap, MAX_ORDER_SIZE)
            await self._place(symbol, qty, Side.BUY, px, strategy_label=STRAT_UNWIND)
        return False

    async def _option_monotonicity_arb(self):
        self._mono_stats["total"] += 1
        fired = False
        for lo, hi in [(950, 1000), (1000, 1050)]:
            lb, la = self._bbo(f"B_C_{lo}")
            hb, ha = self._bbo(f"B_C_{hi}")
            if None not in (lb, la, hb, ha) and hb > la + MONO_BUFFER:
                print(f"[MONO C] sell C{hi}@{hb} buy C{lo}@{la}")
                self._log_event("mono_call", f"lo={lo} hi={hi} edge={hb-la}")
                await self._place(f"B_C_{hi}", OPTION_ARB_SIZE, Side.SELL, hb, strategy_label=STRAT_MONO)
                await self._place(f"B_C_{lo}", OPTION_ARB_SIZE, Side.BUY, la, strategy_label=STRAT_MONO)
                fired = True
            lb, la = self._bbo(f"B_P_{lo}")
            hb, ha = self._bbo(f"B_P_{hi}")
            if None not in (lb, la, hb, ha) and lb > ha + MONO_BUFFER:
                print(f"[MONO P] sell P{lo}@{lb} buy P{hi}@{ha}")
                self._log_event("mono_put", f"lo={lo} hi={hi} edge={lb-ha}")
                await self._place(f"B_P_{lo}", OPTION_ARB_SIZE, Side.SELL, lb, strategy_label=STRAT_MONO)
                await self._place(f"B_P_{hi}", OPTION_ARB_SIZE, Side.BUY, ha, strategy_label=STRAT_MONO)
                fired = True
        if fired:
            self._mono_stats["triggered"] += 1

    # ---------- generic penny MM ----------
    async def _penny_requote(self, symbol, quote_size, soft_cap):
        if self.phase != "PENNY":
            return
        now = time.monotonic()
        if now < self.pause_until:
            await self._cancel_sym_quotes(symbol)
            return
        if (now - self.last_requote_ts[symbol]) < REQUOTE_COOLDOWN:
            return
        self.last_requote_ts[symbol] = now

        bid, ask = self._bbo(symbol)
        if bid is None or ask is None:
            return
        spread = ask - bid
        if spread < MIN_SPREAD_TO_QUOTE:
            await self._cancel_sym_quotes(symbol)
            return

        pos = self.positions.get(symbol, 0)
        fair = (bid + ask) / 2.0
        half = spread / 2.0
        skew = (pos / float(soft_cap)) * SKEW_SHIFT_FACTOR

        my_bid = min(int(round(fair - half - skew)), bid)
        my_ask = max(int(round(fair + half - skew)), ask)
        if my_bid >= my_ask:
            await self._cancel_sym_quotes(symbol)
            return

        await self._cancel_sym_quotes(symbol)
        # neutrality: don't quote side that would grow inventory past soft cap
        if pos < soft_cap:
            self.my_quotes[symbol]["bid"] = await self._place(symbol, quote_size, Side.BUY, my_bid, strategy_label=STRAT_PENNY_MM)
        if pos > -soft_cap:
            self.my_quotes[symbol]["ask"] = await self._place(symbol, quote_size, Side.SELL, my_ask, strategy_label=STRAT_PENNY_MM)

    # ---------- ETF arbitrage ----------
    async def _etf_arb(self):
        a_b, a_a = self._bbo("A")
        b_b, b_a = self._bbo("B")
        c_b, c_a = self._bbo("C")
        e_b, e_a = self._bbo("ETF")
        if None in (a_b, a_a, b_b, b_a, c_b, c_a, e_b, e_a):
            return

        # Case 1: ETF overpriced -> sell ETF, buy basket
        basket_cost = a_a + b_a + c_a  # lift all asks
        if e_b > basket_cost + ETF_FEE + ETF_ARB_BUFFER:
            print(f"[ETF ARB] ETF rich: sell ETF@{e_b} buy basket@{basket_cost}")
            self._log_event("etf_arb_sell", f"etf_bid={e_b} basket={basket_cost}")
            await self._place("ETF", ETF_ARB_SIZE, Side.SELL, e_b, strategy_label=STRAT_ETF_ARB)
            await self._place("A", ETF_ARB_SIZE, Side.BUY, a_a, strategy_label=STRAT_ETF_ARB)
            await self._place("B", ETF_ARB_SIZE, Side.BUY, b_a, strategy_label=STRAT_ETF_ARB)
            await self._place("C", ETF_ARB_SIZE, Side.BUY, c_a, strategy_label=STRAT_ETF_ARB)
            return

        # Case 2: ETF cheap -> buy ETF, sell basket
        basket_bid = a_b + b_b + c_b
        if e_a < basket_bid - ETF_FEE - ETF_ARB_BUFFER:
            print(f"[ETF ARB] ETF cheap: buy ETF@{e_a} sell basket@{basket_bid}")
            self._log_event("etf_arb_buy", f"etf_ask={e_a} basket={basket_bid}")
            await self._place("ETF", ETF_ARB_SIZE, Side.BUY, e_a, strategy_label=STRAT_ETF_ARB)
            await self._place("A", ETF_ARB_SIZE, Side.SELL, a_b, strategy_label=STRAT_ETF_ARB)
            await self._place("B", ETF_ARB_SIZE, Side.SELL, b_b, strategy_label=STRAT_ETF_ARB)
            await self._place("C", ETF_ARB_SIZE, Side.SELL, c_b, strategy_label=STRAT_ETF_ARB)

    # ---------- Put-call parity arb on B options ----------
    async def _pcp_arb(self):
        self._pcp_stats["total"] += 1
        b_b, b_a = self._bbo("B")
        if b_b is None or b_a is None: return
        for K in STRIKES:
            cb, ca = self._bbo(f"B_C_{K}")
            pb, pa = self._bbo(f"B_P_{K}")
            # one-sided fallback: fill missing side with same-side opposite leg
            if ca is None and cb is not None: ca = cb + 2
            if cb is None and ca is not None: cb = ca - 2
            if pa is None and pb is not None: pa = pb + 2
            if pb is None and pa is not None: pb = pa - 2
            if None in (cb, ca, pb, pa): continue
            self._pcp_stats["books"] += 1
            syn_long_cost = ca - pb + K
            syn_short_proceeds = cb - pa + K
            if syn_long_cost + PCP_BUFFER < b_b:
                self._pcp_stats["triggered"] += 1
                print(f"[PCP {K}] buy syn/sell B: syn={syn_long_cost} B_bid={b_b}")
                self._log_event("pcp_long_syn", f"K={K} edge={b_b-syn_long_cost}")
                await self._place(f"B_C_{K}", OPTION_ARB_SIZE, Side.BUY, ca, strategy_label=STRAT_PCP)
                await self._place(f"B_P_{K}", OPTION_ARB_SIZE, Side.SELL, pb, strategy_label=STRAT_PCP)
                await self._place("B", OPTION_ARB_SIZE, Side.SELL, b_b, strategy_label=STRAT_PCP)
            elif syn_short_proceeds - PCP_BUFFER > b_a:
                self._pcp_stats["triggered"] += 1
                print(f"[PCP {K}] sell syn/buy B: syn={syn_short_proceeds} B_ask={b_a}")
                self._log_event("pcp_short_syn", f"K={K} edge={syn_short_proceeds-b_a}")
                await self._place(f"B_C_{K}", OPTION_ARB_SIZE, Side.SELL, cb, strategy_label=STRAT_PCP)
                await self._place(f"B_P_{K}", OPTION_ARB_SIZE, Side.BUY, pa, strategy_label=STRAT_PCP)
                await self._place("B", OPTION_ARB_SIZE, Side.BUY, b_a, strategy_label=STRAT_PCP)
        if self._pcp_stats["total"] % 100 == 0:
            print(f"[PCP STATS] {self._pcp_stats}")

    # ---------- Box spread arb (950/1050) ----------
    async def _box_arb(self):
        self._box_stats["total"] += 1
        K1, K2 = 950, 1050
        c1b, c1a = self._bbo(f"B_C_{K1}")
        c2b, c2a = self._bbo(f"B_C_{K2}")
        p1b, p1a = self._bbo(f"B_P_{K1}")
        p2b, p2a = self._bbo(f"B_P_{K2}")
        if None in (c1b, c1a, c2b, c2a, p1b, p1a, p2b, p2a):
            return
        self._box_stats["books"] += 1
        target = K2 - K1  # 100
        # Long box: buy C(K1), sell C(K2), sell P(K1), buy P(K2) -> cost = c1a - c2b - p1b + p2a
        long_cost = c1a - c2b - p1b + p2a
        if long_cost + BOX_BUFFER < target:
            self._box_stats["triggered"] += 1
            print(f"[BOX] long box cost={long_cost} < {target}")
            self._log_event("box_long", f"cost={long_cost}")
            await self._place(f"B_C_{K1}", OPTION_ARB_SIZE, Side.BUY, c1a, strategy_label=STRAT_BOX)
            await self._place(f"B_C_{K2}", OPTION_ARB_SIZE, Side.SELL, c2b, strategy_label=STRAT_BOX)
            await self._place(f"B_P_{K1}", OPTION_ARB_SIZE, Side.SELL, p1b, strategy_label=STRAT_BOX)
            await self._place(f"B_P_{K2}", OPTION_ARB_SIZE, Side.BUY, p2a, strategy_label=STRAT_BOX)
            return
        # Short box proceeds = c1b - c2a - p1a + p2b
        short_proc = c1b - c2a - p1a + p2b
        if short_proc - BOX_BUFFER > target:
            self._box_stats["triggered"] += 1
            print(f"[BOX] short box proceeds={short_proc} > {target}")
            self._log_event("box_short", f"proc={short_proc}")
            await self._place(f"B_C_{K1}", OPTION_ARB_SIZE, Side.SELL, c1b, strategy_label=STRAT_BOX)
            await self._place(f"B_C_{K2}", OPTION_ARB_SIZE, Side.BUY, c2a, strategy_label=STRAT_BOX)
            await self._place(f"B_P_{K1}", OPTION_ARB_SIZE, Side.BUY, p1a, strategy_label=STRAT_BOX)
            await self._place(f"B_P_{K2}", OPTION_ARB_SIZE, Side.SELL, p2b, strategy_label=STRAT_BOX)
        if self._box_stats["total"] % 100 == 0:
            print(f"[BOX STATS] {self._box_stats}")

    # ---------- Fed prediction sum-to-1 arb (disabled) ----------
    # async def _pred_arb(self):
    #     quotes = {}
    #     for s in PRED_SYMS:
    #         b, a = self._bbo(s)
    #         if b is None or a is None:
    #             return
    #         quotes[s] = (b, a)
    #     sum_asks = sum(q[1] for q in quotes.values())
    #     sum_bids = sum(q[0] for q in quotes.values())

    #     # Prices are in pts out of 100 (per packet). If we can buy all three for <100, free money.
    #     if sum_asks + PRED_ARB_BUFFER < 100:
    #         print(f"[PRED ARB] buy all three, sum_ask={sum_asks}")
    #         self._log_event("pred_arb_buy_all", f"sum={sum_asks}")
    #         for s in PRED_SYMS:
    #             await self._place(s, PRED_ARB_SIZE, Side.BUY, quotes[s][1])
    #     elif sum_bids - PRED_ARB_BUFFER > 100:
    #         print(f"[PRED ARB] sell all three, sum_bid={sum_bids}")
    #         self._log_event("pred_arb_sell_all", f"sum={sum_bids}")
    #         for s in PRED_SYMS:
    #             await self._place(s, PRED_ARB_SIZE, Side.SELL, quotes[s][0])

    #     # Track implied delta r for C fair-value model
    #     total = sum((q[0] + q[1]) / 2 for q in quotes.values())
    #     if total > 0:
    #         p_cut = ((quotes["R_CUT"][0] + quotes["R_CUT"][1]) / 2) / total
    #         p_hike = ((quotes["R_HIKE"][0] + quotes["R_HIKE"][1]) / 2) / total
    #         self.implied_delta_r = 25 * p_hike - 25 * p_cut

    # ---------- main loop ----------
    async def _strategy_loop(self):
        await asyncio.sleep(2)
        while True:
            now = time.monotonic()
            elapsed = now - self.phase_started
            try:
                if self.phase == "PENNY":
                    if PENNY_MM_ENABLED:
                        await self._penny_requote("A", QUOTE_SIZE_A, SOFT_CAP["A"])
                        await self._penny_requote("B", QUOTE_SIZE_B, SOFT_CAP["B"])
                        await self._penny_requote("C", QUOTE_SIZE_C, SOFT_CAP["C"])
                    await self._etf_arb()
                    await self._pcp_arb()
                    await self._box_arb()
                    await self._option_monotonicity_arb()
                    # await self._pred_arb()
                elif self.phase in ("HOLD_LONG", "HOLD_SHORT"):
                    if elapsed >= FADE_DELAY and not self._faded:
                        await self._fade_burst()
                        self._faded = True
                    if elapsed >= FADE_DELAY + FADE_HOLD:
                        await self._cancel_all_quotes()
                        self._unwind_started = time.monotonic()
                        self._enter_phase("UNWIND")
                elif self.phase == "UNWIND":
                    syms = self._burst_symbols or ["A"]
                    all_flat = True
                    for sym in syms:
                        flat = await self._force_unwind(sym)
                        if not flat:
                            all_flat = False
                    if all_flat or (time.monotonic() - self._unwind_started) > UNWIND_DURATION:
                        pos_str = {s: self.positions.get(s, 0) for s in syms}
                        print(f"[UNWIND DONE] {pos_str}")
                        self._log_event("unwind_done", str(pos_str))
                        self._burst_symbols = []
                        self._burst_sizes = {}
                        self._burst_size = 0
                        self._burst_symbol = None
                        self._faded = False
                        self._enter_phase("PENNY")
            except Exception as e:
                print(f"[LOOP ERR] {e}")
            await asyncio.sleep(0.2 if self.phase in ("UNWIND", "HOLD_LONG", "HOLD_SHORT") else 0.5)

    # ---------- news handler ----------
    async def bot_handle_news(self, news_release: dict):
        if not self._first_news_logged:
            print(f"[NEWS DEBUG] {news_release}")
            self._first_news_logged = True

        nd = news_release.get("new_data", {})
        kind = news_release.get("kind", "")
        subtype = nd.get("structured_subtype", "")
        asset = nd.get("asset", "")
        symbol_field = news_release.get("symbol", "")

        print(
            f"[NEWS POS] phase={self.phase} "
            f"A={self.positions.get('A', 0)} "
            f"ETF={self.positions.get('ETF', 0)} "
            f"B={self.positions.get('B', 0)} "
            f"C={self.positions.get('C', 0)}"
        )

        if kind == "structured" and subtype == "earnings":
            text = str(nd.get("value", ""))
        elif kind == "structured" and subtype == "cpi_print":
            text = f"{nd.get('actual')} vs {nd.get('forecast')}"
        else:
            text = nd.get("content", "")
        news_wall_time = time.time()
        self._all_news_writer.writerow([news_wall_time, kind, subtype, asset, symbol_field, text])
        self._all_news_file.flush()

        track_news = (
            subtype == "earnings"
            or asset == "A"
            or symbol_field == "A"
        )
        if track_news:
            if RESET_NEWS_PNL_EACH_ITERATION:
                await self._reset_news_pnl_log()

            self._news_track_id += 1
            current_epoch = self._news_pnl_epoch
            asyncio.create_task(
                self._track_news_pnl_snapshots(
                    self._news_track_id,
                    current_epoch,
                    news_wall_time,
                    kind,
                    subtype,
                    asset,
                    symbol_field,
                    text,
                )
            )

        if (asset == "A" or symbol_field == "A") and self.phase == "PENNY":
            a_pos = self.positions.get("A", 0)
            etf_pos = self.positions.get("ETF", 0)
            if abs(a_pos) > 5 or abs(etf_pos) > 5:
                print(f"[EMERGENCY FLATTEN] A news with residual A={a_pos} ETF={etf_pos}")
                self._log_event("emergency_flatten", f"A={a_pos} ETF={etf_pos}")
                self._burst_symbols = [s for s in ("A", "ETF") if abs(self.positions.get(s, 0)) > 0]
                self._burst_sizes = {s: abs(self.positions.get(s, 0)) for s in self._burst_symbols}
                self._unwind_started = time.monotonic()
                self._enter_phase("UNWIND")

        # A earnings
        if kind == "structured" and subtype == "earnings" and asset == "A":
            new_eps = float(nd["value"])
            print(f"[A EPS] {new_eps} (prev {self.last_eps_A})")
            if self.last_eps_A is None:
                self.last_eps_A = new_eps
                return
            delta = new_eps - self.last_eps_A
            self.last_eps_A = new_eps
            if delta > 0.001:
                await self._enter_directional("A", "LONG", delta)
            elif delta < -0.001:
                await self._enter_directional("A", "SHORT", delta)
            return

        # C earnings - also directional but smaller
        if kind == "structured" and subtype == "earnings" and asset == "C":
            new_eps = float(nd["value"])
            print(f"[C EPS] {new_eps} (prev {self.last_eps_C})")
            if self.last_eps_C is None:
                self.last_eps_C = new_eps
                return
            delta = new_eps - self.last_eps_C
            self.last_eps_C = new_eps
            if delta > 0.001:
                await self._enter_directional("C", "LONG", delta)
            elif delta < -0.001:
                await self._enter_directional("C", "SHORT", delta)
            return

        # CPI print -> trade Fed prediction market (disabled)
        # if kind == "structured" and subtype == "cpi_print":
        #     try:
        #         actual = float(nd.get("actual"))
        #         forecast = float(nd.get("forecast"))
        #         surprise = actual - forecast
        #         print(f"[CPI] actual={actual} forecast={forecast} surprise={surprise:+.2f}")
        #         self._log_event("cpi", f"surprise={surprise}")
        #         # hot -> hike more likely; buy HIKE, sell CUT
        #         if surprise > 0.05:
        #             hb, ha = self._bbo("R_HIKE")
        #             cb, ca = self._bbo("R_CUT")
        #             if ha is not None:
        #                 await self._place("R_HIKE", PRED_ARB_SIZE, Side.BUY, ha)
        #             if cb is not None:
        #                 await self._place("R_CUT", PRED_ARB_SIZE, Side.SELL, cb)
        #         elif surprise < -0.05:
        #             hb, ha = self._bbo("R_HIKE")
        #             cb, ca = self._bbo("R_CUT")
        #             if hb is not None:
        #                 await self._place("R_HIKE", PRED_ARB_SIZE, Side.SELL, hb)
        #             if ca is not None:
        #                 await self._place("R_CUT", PRED_ARB_SIZE, Side.BUY, ca)
        #     except Exception as e:
        #         print(f"[CPI ERR] {e}")
        #     return

        # other news: brief MM pause
        if self.phase == "PENNY":
            self.pause_until = time.monotonic() + NON_EARNINGS_PAUSE
            await self._cancel_all_quotes()

    async def bot_handle_book_update(self, symbol):
        if PENNY_MM_ENABLED and self.phase == "PENNY" and symbol in ("A", "B", "C"):
            cap = SOFT_CAP[symbol]
            qsize = {"A": QUOTE_SIZE_A, "B": QUOTE_SIZE_B, "C": QUOTE_SIZE_C}[symbol]
            await self._penny_requote(symbol, qsize, cap)

    async def bot_handle_order_fill(self, order_id, qty, price):
        self._fill_count += 1
        info = self.open_order_sides.get(order_id, ("?", "?"))
        side, sym = info if isinstance(info, tuple) else (info, "?")

        meta = self.open_orders_meta.get(order_id)
        strategy_label = "UNLABELED"
        if meta is not None:
            strategy_label = meta.get("strategy", "UNLABELED")
            meta["qty_remaining"] = max(0, int(meta.get("qty_remaining", 0)) - int(qty))
            if meta["qty_remaining"] == 0:
                self.open_orders_meta.pop(order_id, None)
                self.open_order_sides.pop(order_id, None)

        if side in ("BUY", "SELL") and sym in self.pnl_position:
            self._update_symbol_pnl_on_fill(sym, side, int(qty), float(price))
            self._update_strategy_pnl_on_fill(strategy_label, sym, side, int(qty), float(price))

        pos = self.positions.get(sym, 0)
        mid = self._mid(sym)
        print(f"FILL #{self._fill_count} {sym} {side} qty={qty} px={price} pos={pos} phase={self.phase}")
        self._fill_writer.writerow([self._fill_count, time.time(), sym, side, qty, price,
                                    self.phase, pos, mid if mid is not None else ""])
        self._fill_file.flush()
        for s, q in self.my_quotes.items():
            if q["bid"] == order_id: q["bid"] = None
            if q["ask"] == order_id: q["ask"] = None

    async def bot_handle_order_rejected(self, order_id, reason):
        print(f"REJECTED {order_id}: {reason}")
        self.open_order_sides.pop(order_id, None)
        self.open_orders_meta.pop(order_id, None)

    async def bot_handle_cancel_response(self, order_id, success, error=None):
        if success:
            self.open_order_sides.pop(order_id, None)
            self.open_orders_meta.pop(order_id, None)

    async def bot_handle_trade_msg(self, symbol, price, qty): pass
    async def bot_handle_swap_response(self, swap, qty, success): pass

    async def trade(self):
        asyncio.create_task(self._strategy_loop())
        asyncio.create_task(self._all_asset_sampler())
        while True:
            await asyncio.sleep(60)

    async def start(self):
        asyncio.create_task(self.trade())
        try:
            await self.connect()
        finally:
            self._write_final_pnl_log()


async def main():
    SERVER = "34.197.188.76:3333"
    client = StrategyBot(SERVER, "caltech_stanford_yale", "lunar-forge-vapor")
    await client.start()


if __name__ == "__main__":
    asyncio.run(main())
