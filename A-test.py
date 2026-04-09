import pandas as pd
import matplotlib.pyplot as plt

ticks = pd.read_csv("tick_log.csv")
news = pd.read_csv("news_log.csv", encoding="cp1252")

# Convert wall_time to seconds since start for easier plotting
t0 = ticks["wall_time"].iloc[0]
ticks["t"] = ticks["wall_time"] - t0
news["t"] = news["wall_time"] - t0

fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)

# Plot 1: mid price with news event markers
axes[0].plot(ticks["t"], ticks["mid"], linewidth=0.8, color="black")
axes[0].set_ylabel("Mid price (A)")
axes[0].set_title("A mid price with news events")
for _, n in news.iterrows():
    color = {"structured": "blue", "unstructured": "orange"}.get(n["kind"], "gray")
    if n["asset_field"] == "A" or n["symbol_field"] == "A":
        color = "red"
    axes[0].axvline(n["t"], color=color, alpha=0.4, linewidth=0.8)

# Plot 2: spread
axes[1].plot(ticks["t"], ticks["spread"], linewidth=0.8, color="purple")
axes[1].set_ylabel("Spread (ticks)")
axes[1].set_title("Bid-ask spread")

# Plot 3: depth
axes[2].plot(ticks["t"], ticks["bid_depth"], label="bid depth", color="green", linewidth=0.8)
axes[2].plot(ticks["t"], ticks["ask_depth"], label="ask depth", color="red", linewidth=0.8)
axes[2].set_ylabel("Depth (top 5 levels)")
axes[2].set_xlabel("Time (seconds)")
axes[2].set_title("Order book depth")
axes[2].legend()

plt.tight_layout()
plt.savefig("market_overview.png", dpi=120)
plt.show()

# Quick stats
print("Mid statistics:")
print(ticks["mid"].describe())
print(f"\nMid std (volatility proxy): {ticks['mid'].std():.1f}")
print(f"Max single-tick mid jump: {ticks['mid'].diff().abs().max():.1f}")
print(f"\nSpread statistics:")
print(ticks["spread"].describe())

# News impact: for A-specific news, look at price 30s before vs 30s after
print("\nA-specific news events:")
a_news = news[(news["asset_field"] == "A") | (news["symbol_field"] == "A")]
for _, n in a_news.iterrows():
    before = ticks[(ticks["t"] >= n["t"] - 30) & (ticks["t"] < n["t"])]["mid"].mean()
    after = ticks[(ticks["t"] > n["t"]) & (ticks["t"] <= n["t"] + 30)]["mid"].mean()
    if pd.notna(before) and pd.notna(after):
        print(f"  t={n['t']:.0f} {n['kind']}/{n['subtype']}: {before:.0f} -> {after:.0f} (Δ={after-before:+.0f}) [{str(n['value_or_text'])[:40]}]")