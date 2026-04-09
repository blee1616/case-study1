from typing import Optional
from utcxchangelib import XChangeClient, Side
import asyncio
import time
import csv
import os

SYMBOL = "A"
TICK_LOG_PATH = "tick_log.csv"
NEWS_LOG_PATH = "news_log.csv"
SAMPLE_INTERVAL = 0.2  # seconds between mid samples


class DataCollectorClient(XChangeClient):
    def __init__(self, host, username, password):
        super().__init__(host, username, password)
        self._init_tick_log()
        self._init_news_log()
        self._news_logged_once = False

    def _init_tick_log(self):
        new_file = not os.path.exists(TICK_LOG_PATH)
        self._tick_file = open(TICK_LOG_PATH, "a", newline="")
        self._tick_writer = csv.writer(self._tick_file)
        if new_file:
            self._tick_writer.writerow([
                "wall_time", "best_bid", "best_ask", "mid", "spread",
                "bid_depth", "ask_depth"
            ])
            self._tick_file.flush()

    def _init_news_log(self):
        new_file = not os.path.exists(NEWS_LOG_PATH)
        self._news_file = open(NEWS_LOG_PATH, "a", newline="")
        self._news_writer = csv.writer(self._news_file)
        if new_file:
            self._news_writer.writerow([
                "wall_time", "kind", "subtype", "asset_field",
                "symbol_field", "value_or_text"
            ])
            self._news_file.flush()

    def _book_snapshot(self):
        book = self.order_books.get(SYMBOL)
        if not book:
            return None
        bids = [(p, q) for p, q in book.bids.items() if q > 0]
        asks = [(p, q) for p, q in book.asks.items() if q > 0]
        if not bids or not asks:
            return None
        best_bid = max(p for p, _ in bids)
        best_ask = min(p for p, _ in asks)
        bid_depth = sum(q for p, q in bids if p >= best_bid - 5)
        ask_depth = sum(q for p, q in asks if p <= best_ask + 5)
        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid": (best_bid + best_ask) / 2,
            "spread": best_ask - best_bid,
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
        }

    async def _sampler(self):
        await asyncio.sleep(2)
        while True:
            snap = self._book_snapshot()
            if snap:
                self._tick_writer.writerow([
                    time.time(),
                    snap["best_bid"], snap["best_ask"], snap["mid"],
                    snap["spread"], snap["bid_depth"], snap["ask_depth"],
                ])
                self._tick_file.flush()
            await asyncio.sleep(SAMPLE_INTERVAL)

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
        self._news_writer.writerow([
            time.time(), kind, subtype, asset, symbol_field, text
        ])
        self._news_file.flush()
        print(f"[NEWS] {kind}/{subtype} asset={asset} sym={symbol_field}: {text[:60]}")

    async def bot_handle_book_update(self, symbol):
        pass

    async def bot_handle_order_fill(self, order_id, qty, price):
        pass

    async def bot_handle_order_rejected(self, order_id, reason):
        pass

    async def bot_handle_cancel_response(self, order_id, success, error=None):
        pass

    async def bot_handle_trade_msg(self, symbol, price, qty):
        pass

    async def bot_handle_swap_response(self, swap, qty, success):
        pass

    async def trade(self):
        asyncio.create_task(self._sampler())
        # Idle forever — we are NOT trading
        while True:
            await asyncio.sleep(60)

    async def start(self):
        asyncio.create_task(self.trade())
        await self.connect()


async def main():
    SERVER = "34.197.188.76:3333"
    client = DataCollectorClient(SERVER, "caltech_stanford_yale", "lunar-forge-vapor")
    await client.start()


if __name__ == "__main__":
    asyncio.run(main())