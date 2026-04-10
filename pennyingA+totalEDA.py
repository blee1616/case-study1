from typing import Optional
from utcxchangelib import XChangeClient, Side
import asyncio
import time
import csv
import os

SYMBOL = "A"

ALL_SYMBOLS = [
    "A", "B", "C", "ETF",
    "B_C_950", "B_C_1000", "B_C_1050",
    "B_P_950", "B_P_1000", "B_P_1050",
    # "R_CUT", "R_HOLD", "R_HIKE",
]

# Penny phase params
QUOTE_SIZE = 20
MAX_POSITION = 200
MIN_SPREAD_TO_QUOTE = 1
REQUOTE_COOLDOWN = 0.3
NON_EARNINGS_PAUSE = 3.0
SKEW_SHIFT_FACTOR = 2.0

# Directional bet params
MAX_ORDER_SIZE = 40
MAX_OUTSTANDING_VOL = 120
HOLD_DURATION = 5.0

# Logging
FILL_LOG = "fill_log.csv"
EVENT_LOG = "event_log.csv"
ALL_TICKS_LOG = "all_ticks.csv"
ALL_NEWS_LOG = "all_news.csv"
ALL_TICK_INTERVAL = 0.5  # seconds


class StrategyBot(XChangeClient):
    def __init__(self, host, username, password):
        super().__init__(host, username, password)

        # Order tracking
        self.open_order_sides: dict = {}
        self.my_bid_id: Optional[str] = None
        self.my_ask_id: Optional[str] = None

        # Strategy state
        self.phase: str = "PENNY"
        self.phase_started: float = 0.0
        self.last_eps: Optional[float] = None
        self.last_requote_ts: float = 0.0
        self.pause_until: float = 0.0

        # Counters
        self._fill_count = 0
        self._first_news_logged = False

        self._init_logs()

    def _init_logs(self):
        # Fill log (A trades only)
        new = not os.path.exists(FILL_LOG)
        self._fill_file = open(FILL_LOG, "a", newline="", encoding="utf-8")
        self._fill_writer = csv.writer(self._fill_file)
        if new:
            self._fill_writer.writerow([
                "fill_idx", "wall_time", "side", "qty", "price",
                "phase_at_fill", "position_after", "mid_at_fill"
            ])
            self._fill_file.flush()

        # Event log (phase transitions, EPS events)
        new = not os.path.exists(EVENT_LOG)
        self._event_file = open(EVENT_LOG, "a", newline="", encoding="utf-8")
        self._event_writer = csv.writer(self._event_file)
        if new:
            self._event_writer.writerow([
                "wall_time", "event_type", "details", "phase_after",
                "position", "mid"
            ])
            self._event_file.flush()

        # All-asset tick log
        new = not os.path.exists(ALL_TICKS_LOG)
        self._all_tick_file = open(ALL_TICKS_LOG, "a", newline="", encoding="utf-8")
        self._all_tick_writer = csv.writer(self._all_tick_file)
        if new:
            cols = ["wall_time"]
            for s in ALL_SYMBOLS:
                cols += [f"{s}_bid", f"{s}_ask", f"{s}_mid", f"{s}_spread"]
            self._all_tick_writer.writerow(cols)
            self._all_tick_file.flush()

        # All-asset news log
        new = not os.path.exists(ALL_NEWS_LOG)
        self._all_news_file = open(ALL_NEWS_LOG, "a", newline="", encoding="utf-8")
        self._all_news_writer = csv.writer(self._all_news_file)
        if new:
            self._all_news_writer.writerow([
                "wall_time", "kind", "subtype", "asset_field",
                "symbol_field", "value_or_text"
            ])
            self._all_news_file.flush()

    def _log_event(self, event_type, details):
        mid = self._mid(SYMBOL)
        self._event_writer.writerow([
            time.time(), event_type, details, self.phase,
            self.positions.get(SYMBOL, 0),
            mid if mid is not None else ""
        ])
        self._event_file.flush()

    # ---------- order helpers ----------
    async def _place(self, symbol, qty, side, price=None):
        oid = await self.place_order(symbol, qty, side, price)
        if oid:
            self.open_order_sides[oid] = "BUY" if side == Side.BUY else "SELL"
        return oid

    async def _cancel(self, oid):
        try:
            await self.cancel_order(oid)
        except Exception:
            pass

    async def _cancel_my_quotes(self):
        for attr in ("my_bid_id", "my_ask_id"):
            oid = getattr(self, attr)
            if oid is not None:
                await self._cancel(oid)
                setattr(self, attr, None)

    def _bbo(self, symbol):
        book = self.order_books.get(symbol)
        if not book:
            return None, None
        bids = [p for p, q in book.bids.items() if q > 0]
        asks = [p for p, q in book.asks.items() if q > 0]
        return (max(bids) if bids else None,
                min(asks) if asks else None)

    def _mid(self, symbol):
        b, a = self._bbo(symbol)
        if b is None or a is None:
            return None
        return (b + a) / 2

    # ---------- background sampler for all assets ----------
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

    # ---------- phase transitions ----------
    def _enter_phase(self, phase):
        old = self.phase
        self.phase = phase
        self.phase_started = time.monotonic()
        print(f"[PHASE] {old} -> {phase}")
        self._log_event("phase_change", f"{old}->{phase}")

    async def _enter_directional(self, direction: str, eps_delta: float):
        bid, ask = self._bbo(SYMBOL)
        if bid is None or ask is None:
            print("[DIRECTIONAL] no book, aborting")
            return

        # Clear resting passive quotes before sending aggressive burst orders.
        await self._cancel_my_quotes()

        cur_pos = self.positions.get(SYMBOL, 0)
        if direction == "LONG":
            qty_needed = max(0, MAX_POSITION - cur_pos)
            burst_qty = min(qty_needed, MAX_OUTSTANDING_VOL)
            if burst_qty > 0:
                print(
                    f"[DIRECTIONAL] AGGRESSIVE BUY {burst_qty} @ ask={ask} "
                    f"(eps_delta={eps_delta:+.2f})"
                )
                self._log_event("directional_buy_max", f"qty={burst_qty} eps_delta={eps_delta}")
                remaining = burst_qty
                while remaining > 0:
                    chunk = min(MAX_ORDER_SIZE, remaining)
                    await self._place(SYMBOL, chunk, Side.BUY, ask)
                    remaining -= chunk
            self._enter_phase("HOLD_LONG")
        else:
            qty_needed = max(0, cur_pos + MAX_POSITION)
            burst_qty = min(qty_needed, MAX_OUTSTANDING_VOL)
            if burst_qty > 0:
                print(
                    f"[DIRECTIONAL] AGGRESSIVE SELL {burst_qty} @ bid={bid} "
                    f"(eps_delta={eps_delta:+.2f})"
                )
                self._log_event("directional_sell_max", f"qty={burst_qty} eps_delta={eps_delta}")
                remaining = burst_qty
                while remaining > 0:
                    chunk = min(MAX_ORDER_SIZE, remaining)
                    await self._place(SYMBOL, chunk, Side.SELL, bid)
                    remaining -= chunk
            self._enter_phase("HOLD_SHORT")

    # ---------- penny phase ----------
    async def _penny_requote(self, force=False):
        if self.phase != "PENNY":
            return
        now = time.monotonic()
        if now < self.pause_until:
            await self._cancel_my_quotes()
            return
        if not force and (now - self.last_requote_ts) < REQUOTE_COOLDOWN:
            return
        self.last_requote_ts = now

        bid, ask = self._bbo(SYMBOL)
        if bid is None or ask is None:
            return
        spread = ask - bid
        if spread < MIN_SPREAD_TO_QUOTE:
            await self._cancel_my_quotes()
            return

        position = self.positions.get(SYMBOL, 0)

        # Skew quotes toward reducing inventory while staying passive.
        fair_value = (bid + ask) / 2.0
        half_spread = spread / 2.0
        skew = (position / float(MAX_POSITION)) * SKEW_SHIFT_FACTOR

        my_bid = int(round(fair_value - half_spread - skew))
        my_ask = int(round(fair_value + half_spread - skew))

        my_bid = min(my_bid, bid)
        my_ask = max(my_ask, ask)
        if my_bid >= my_ask:
            await self._cancel_my_quotes()
            return

        await self._cancel_my_quotes()
        if position < MAX_POSITION:
            self.my_bid_id = await self._place(SYMBOL, QUOTE_SIZE, Side.BUY, my_bid)
        if position > -MAX_POSITION:
            self.my_ask_id = await self._place(SYMBOL, QUOTE_SIZE, Side.SELL, my_ask)

    # ---------- main strategy loop ----------
    async def _strategy_loop(self):
        await asyncio.sleep(2)
        while True:
            now = time.monotonic()
            elapsed = now - self.phase_started

            if self.phase == "PENNY":
                await self._penny_requote()
            elif self.phase in ("HOLD_LONG", "HOLD_SHORT"):
                if elapsed >= HOLD_DURATION:
                    print(
                        "[STRATEGY] Volatility settled. Returning to PENNY "
                        f"to safely unwind position: {self.positions.get(SYMBOL, 0)}"
                    )
                    self._enter_phase("PENNY")

            await asyncio.sleep(0.5)

    # ---------- handlers ----------
    async def bot_handle_news(self, news_release: dict):
        if not self._first_news_logged:
            print(f"[NEWS DEBUG] {news_release}")
            self._first_news_logged = True

        nd = news_release.get("new_data", {})
        kind = news_release.get("kind", "")
        subtype = nd.get("structured_subtype", "")
        asset = nd.get("asset", "")
        symbol_field = news_release.get("symbol", "")

        # Log all news to the all-news file
        if kind == "structured" and subtype == "earnings":
            text = str(nd.get("value", ""))
        elif kind == "structured" and subtype == "cpi_print":
            text = f"{nd.get('actual')} vs {nd.get('forecast')}"
        else:
            text = nd.get("content", "")
        self._all_news_writer.writerow([
            time.time(), kind, subtype, asset, symbol_field, text
        ])
        self._all_news_file.flush()

        # A earnings: trigger directional bet
        if kind == "structured" and subtype == "earnings" and asset == "A":
            new_eps = float(nd["value"])
            print(f"[A EPS] {new_eps} (prev: {self.last_eps})")
            self._log_event("a_earnings", f"eps={new_eps} prev={self.last_eps}")

            if self.last_eps is None:
                print("[A EPS] first print, recording baseline only")
                self.last_eps = new_eps
                return

            delta = new_eps - self.last_eps
            self.last_eps = new_eps

            if delta > 0.001:
                await self._enter_directional("LONG", delta)
            elif delta < -0.001:
                await self._enter_directional("SHORT", delta)
            else:
                print("[A EPS] flat print, no action")
            return

        # All other news: brief penny pause
        if self.phase == "PENNY":
            self.pause_until = time.monotonic() + NON_EARNINGS_PAUSE
            await self._cancel_my_quotes()

    async def bot_handle_book_update(self, symbol):
        if symbol != SYMBOL:
            return
        if self.phase == "PENNY":
            await self._penny_requote()

    async def bot_handle_order_fill(self, order_id, qty, price):
        self._fill_count += 1
        side = self.open_order_sides.get(order_id, "?")
        pos = self.positions.get(SYMBOL, 0)
        mid = self._mid(SYMBOL)
        print(f"FILL #{self._fill_count} {side} qty={qty} px={price} pos={pos} phase={self.phase}")
        self._fill_writer.writerow([
            self._fill_count, time.time(), side, qty, price,
            self.phase, pos, mid if mid is not None else ""
        ])
        self._fill_file.flush()

        if order_id == self.my_bid_id:
            self.my_bid_id = None
        if order_id == self.my_ask_id:
            self.my_ask_id = None

    async def bot_handle_order_rejected(self, order_id, reason):
        print(f"REJECTED {order_id}: {reason}")
        self.open_order_sides.pop(order_id, None)

    async def bot_handle_cancel_response(self, order_id, success, error=None):
        if success:
            self.open_order_sides.pop(order_id, None)

    async def bot_handle_trade_msg(self, symbol, price, qty):
        pass

    async def bot_handle_swap_response(self, swap, qty, success):
        pass

    async def trade(self):
        asyncio.create_task(self._strategy_loop())
        asyncio.create_task(self._all_asset_sampler())
        while True:
            await asyncio.sleep(60)

    async def start(self):
        asyncio.create_task(self.trade())
        await self.connect()


async def main():
    SERVER = "34.197.188.76:3333"
    client = StrategyBot(SERVER, "caltech_stanford_yale", "lunar-forge-vapor")
    await client.start()


if __name__ == "__main__":
    asyncio.run(main())