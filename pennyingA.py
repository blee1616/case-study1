from typing import Optional
from utcxchangelib import XChangeClient, Side
import asyncio
import time
import csv
import os

SYMBOL = "A"

# Risk
MAX_POSITION = 15
SOFT_POSITION = 8
QUOTE_SIZE = 5
FLATTEN_CHUNK = 5

# Quoting
MIN_SPREAD_TO_QUOTE = 5
REQUOTE_COOLDOWN = 0.3

# News pause
NEWS_PAUSE_DURATION = 2.0

# Logging
FILL_LOG_PATH = "fill_log.csv"
NEWS_LOG_PATH = "news_log.csv"
TICK_LOG_PATH = "tick_log.csv"
TICK_SAMPLE_INTERVAL = 0.2


class PennyBotLogged(XChangeClient):
    def __init__(self, host, username, password):
        super().__init__(host, username, password)
        self.my_bid_id: Optional[str] = None
        self.my_ask_id: Optional[str] = None
        self.last_requote_ts: float = 0.0
        self.pause_until: float = 0.0
        self.last_news_ts: float = 0.0  # Track most recent news for attribution
        self._fill_count = 0
        self._pending_fill_followups: list = []  # (fill_row_idx, deadline)
        self._fill_rows: list = []  # holds row dicts pending followup snapshots
        self._news_logged_once = False
        self._init_logs()

    def _init_logs(self):
        # Fill log
        new_fill = not os.path.exists(FILL_LOG_PATH)
        self._fill_file = open(FILL_LOG_PATH, "a", newline="", encoding="utf-8")
        self._fill_writer = csv.writer(self._fill_file)
        if new_fill:
            self._fill_writer.writerow([
                "fill_idx", "wall_time", "side", "qty", "price",
                "position_after", "mid_at_fill", "spread_at_fill",
                "secs_since_news", "mid_5s_after", "mid_15s_after", "mid_30s_after",
            ])
            self._fill_file.flush()

        # News log
        new_news = not os.path.exists(NEWS_LOG_PATH)
        self._news_file = open(NEWS_LOG_PATH, "a", newline="", encoding="utf-8")
        self._news_writer = csv.writer(self._news_file)
        if new_news:
            self._news_writer.writerow([
                "wall_time", "kind", "subtype", "asset_field",
                "symbol_field", "value_or_text", "mid_at_news"
            ])
            self._news_file.flush()

        # Tick log (continuous)
        new_tick = not os.path.exists(TICK_LOG_PATH)
        self._tick_file = open(TICK_LOG_PATH, "a", newline="", encoding="utf-8")
        self._tick_writer = csv.writer(self._tick_file)
        if new_tick:
            self._tick_writer.writerow([
                "wall_time", "best_bid", "best_ask", "mid", "spread", "position"
            ])
            self._tick_file.flush()

    def _book_bbo(self):
        book = self.order_books.get(SYMBOL)
        if not book:
            return None, None
        bids = [p for p, q in book.bids.items() if q > 0]
        asks = [p for p, q in book.asks.items() if q > 0]
        return (max(bids) if bids else None,
                min(asks) if asks else None)

    def _mid(self):
        b, a = self._book_bbo()
        if b is None or a is None:
            return None
        return (b + a) / 2

    async def _cancel_my_quotes(self):
        for attr in ("my_bid_id", "my_ask_id"):
            oid = getattr(self, attr)
            if oid is not None:
                try:
                    await self.cancel_order(oid)
                except Exception:
                    pass
                setattr(self, attr, None)

    async def _flatten_if_needed(self):
        position = self.positions.get(SYMBOL, 0)
        if abs(position) < MAX_POSITION:
            return
        bid, ask = self._book_bbo()
        if bid is None or ask is None:
            return

        if position >= MAX_POSITION:
            size = min(FLATTEN_CHUNK, position)
            print(f"[FLATTEN] pos={position}, selling {size} @ {bid}")
            await self.place_order(SYMBOL, size, Side.SELL, bid)
        elif position <= -MAX_POSITION:
            size = min(FLATTEN_CHUNK, -position)
            print(f"[FLATTEN] pos={position}, buying {size} @ {ask}")
            await self.place_order(SYMBOL, size, Side.BUY, ask)

    async def _requote(self, force=False):
        now = time.monotonic()
        if now < self.pause_until:
            await self._cancel_my_quotes()
            return
        if not force and (now - self.last_requote_ts) < REQUOTE_COOLDOWN:
            return
        self.last_requote_ts = now

        await self._flatten_if_needed()

        bid, ask = self._book_bbo()
        if bid is None or ask is None:
            return
        spread = ask - bid
        if spread < MIN_SPREAD_TO_QUOTE:
            await self._cancel_my_quotes()
            return

        position = self.positions.get(SYMBOL, 0)
        quote_bid = position < SOFT_POSITION
        quote_ask = position > -SOFT_POSITION
        if position >= SOFT_POSITION:
            quote_bid = False
            quote_ask = True
        elif position <= -SOFT_POSITION:
            quote_bid = True
            quote_ask = False

        my_bid = bid + 1
        my_ask = ask - 1
        if my_bid >= my_ask:
            return

        await self._cancel_my_quotes()
        if quote_bid:
            self.my_bid_id = await self.place_order(SYMBOL, QUOTE_SIZE, Side.BUY, my_bid)
        if quote_ask:
            self.my_ask_id = await self.place_order(SYMBOL, QUOTE_SIZE, Side.SELL, my_ask)

    async def _tick_sampler(self):
        while True:
            mid = self._mid()
            bid, ask = self._book_bbo()
            if mid is not None:
                pos = self.positions.get(SYMBOL, 0)
                self._tick_writer.writerow([
                    time.time(), bid, ask, mid, ask - bid, pos
                ])
                self._tick_file.flush()
            await asyncio.sleep(TICK_SAMPLE_INTERVAL)

    async def _followup_worker(self):
        """Snapshot mid 5/15/30s after each fill and write completed rows."""
        while True:
            now = time.monotonic()
            still_pending = []
            for entry in self._pending_fill_followups:
                row, deadline_5, deadline_15, deadline_30 = entry
                if now >= deadline_5 and row.get("mid_5s_after") is None:
                    row["mid_5s_after"] = self._mid()
                if now >= deadline_15 and row.get("mid_15s_after") is None:
                    row["mid_15s_after"] = self._mid()
                if now >= deadline_30 and row.get("mid_30s_after") is None:
                    row["mid_30s_after"] = self._mid()
                if row.get("mid_30s_after") is not None:
                    # Done — write
                    self._fill_writer.writerow([
                        row["fill_idx"], row["wall_time"], row["side"],
                        row["qty"], row["price"], row["position_after"],
                        row["mid_at_fill"], row["spread_at_fill"],
                        row["secs_since_news"],
                        row["mid_5s_after"] if row["mid_5s_after"] is not None else "",
                        row["mid_15s_after"] if row["mid_15s_after"] is not None else "",
                        row["mid_30s_after"] if row["mid_30s_after"] is not None else "",
                    ])
                    self._fill_file.flush()
                else:
                    still_pending.append(entry)
            self._pending_fill_followups = still_pending
            await asyncio.sleep(0.5)

    # ---------- handlers ----------
    async def bot_handle_news(self, news_release: dict):
        if not self._news_logged_once:
            print(f"[NEWS DEBUG] {news_release}")
            self._news_logged_once = True

        nd = news_release.get("new_data", {})
        kind = news_release.get("kind", "")
        symbol_field = news_release.get("symbol", "")
        subtype = nd.get("structured_subtype", "")
        asset = nd.get("asset", "")
        if kind == "structured" and subtype == "earnings":
            text = str(nd.get("value", ""))
        elif kind == "structured" and subtype == "cpi_print":
            text = f"{nd.get('actual')} vs {nd.get('forecast')}"
        else:
            text = nd.get("content", "")

        mid_now = self._mid()
        self._news_writer.writerow([
            time.time(), kind, subtype, asset, symbol_field, text,
            mid_now if mid_now is not None else ""
        ])
        self._news_file.flush()

        self.last_news_ts = time.monotonic()
        self.pause_until = time.monotonic() + NEWS_PAUSE_DURATION
        await self._cancel_my_quotes()
        print(f"[NEWS] {kind}/{subtype} sym={symbol_field} asset={asset}: {text[:50]}")

    async def bot_handle_book_update(self, symbol):
        if symbol != SYMBOL:
            return
        await self._requote()

    async def bot_handle_order_fill(self, order_id, qty, price):
        self._fill_count += 1
        pos = self.positions.get(SYMBOL, 0)
        side = "BUY" if order_id == self.my_bid_id else "SELL"

        bid, ask = self._book_bbo()
        mid_now = (bid + ask) / 2 if bid is not None and ask is not None else None
        spread_now = (ask - bid) if bid is not None and ask is not None else None

        secs_since_news = time.monotonic() - self.last_news_ts if self.last_news_ts > 0 else 9999

        print(f"FILL #{self._fill_count} {side} qty={qty} px={price} pos={pos} "
              f"news_age={secs_since_news:.1f}s")

        # Schedule followup snapshots
        row = {
            "fill_idx": self._fill_count,
            "wall_time": time.time(),
            "side": side,
            "qty": qty,
            "price": price,
            "position_after": pos,
            "mid_at_fill": mid_now,
            "spread_at_fill": spread_now,
            "secs_since_news": round(secs_since_news, 1),
            "mid_5s_after": None,
            "mid_15s_after": None,
            "mid_30s_after": None,
        }
        now = time.monotonic()
        self._pending_fill_followups.append(
            (row, now + 5, now + 15, now + 30)
        )

        if order_id == self.my_bid_id:
            self.my_bid_id = None
        if order_id == self.my_ask_id:
            self.my_ask_id = None

    async def bot_handle_order_rejected(self, order_id, reason):
        print(f"REJECTED {order_id}: {reason}")

    async def bot_handle_cancel_response(self, order_id, success, error=None):
        pass

    async def bot_handle_trade_msg(self, symbol, price, qty):
        pass

    async def bot_handle_swap_response(self, swap, qty, success):
        pass

    async def trade(self):
        await asyncio.sleep(2)
        asyncio.create_task(self._tick_sampler())
        asyncio.create_task(self._followup_worker())
        while True:
            await self._requote()
            await asyncio.sleep(0.5)

    async def start(self):
        asyncio.create_task(self.trade())
        await self.connect()


async def main():
    SERVER = "34.197.188.76:3333"
    client = PennyBotLogged(SERVER, "caltech_stanford_yale", "lunar-forge-vapor")
    await client.start()


if __name__ == "__main__":
    asyncio.run(main())