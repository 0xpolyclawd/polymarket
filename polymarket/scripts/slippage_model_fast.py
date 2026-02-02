#!/usr/bin/env python3
"""
Fast Slippage Model - uses sampling for quick analysis
"""

import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
import json
import warnings
warnings.filterwarnings('ignore')

DB_CONFIG = {'dbname': 'polymarket', 'host': '/tmp'}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

print("🔮 POLYMARKET SLIPPAGE MODEL (Fast)")
print("=" * 60)

conn = get_connection()

# 1. PRICE IMPACT BY TRADE SIZE (sample 1M trades)
print("\n📊 PRICE IMPACT BY TRADE SIZE")
print("-" * 60)

query = """
WITH sampled AS (
    SELECT * FROM processed_trades TABLESAMPLE SYSTEM(1) -- ~1% sample
    WHERE usd_amount > 0 AND price > 0 AND price < 1
    LIMIT 1000000
),
trade_pairs AS (
    SELECT 
        usd_amount,
        price,
        LAG(price) OVER (PARTITION BY market_id ORDER BY timestamp) as prev_price
    FROM sampled
)
SELECT 
    CASE 
        WHEN usd_amount < 10 THEN '$0-10'
        WHEN usd_amount < 50 THEN '$10-50'
        WHEN usd_amount < 100 THEN '$50-100'
        WHEN usd_amount < 500 THEN '$100-500'
        WHEN usd_amount < 1000 THEN '$500-1K'
        WHEN usd_amount < 5000 THEN '$1K-5K'
        WHEN usd_amount < 10000 THEN '$5K-10K'
        ELSE '$10K+'
    END as size_bucket,
    AVG(ABS(price - prev_price)) as avg_impact,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(price - prev_price)) as med_impact,
    COUNT(*) as trades
FROM trade_pairs
WHERE prev_price IS NOT NULL AND ABS(price - prev_price) < 0.3
GROUP BY 1
ORDER BY MIN(usd_amount)
"""

df = pd.read_sql(query, conn)
print(f"{'Size':<12} {'Avg Impact':>12} {'Med Impact':>12} {'Trades':>10}")
print("-" * 50)
for _, r in df.iterrows():
    if r['avg_impact']:
        print(f"{r['size_bucket']:<12} {r['avg_impact']:>11.3%} {r['med_impact']:>11.3%} {r['trades']:>10,}")

# 2. MARKET LIQUIDITY SUMMARY  
print("\n📊 MARKET LIQUIDITY SUMMARY")
print("-" * 60)

query = """
SELECT 
    COUNT(DISTINCT market_id) as markets,
    SUM(trade_count) as total_trades,
    SUM(total_vol) as total_volume,
    AVG(daily_vol) as avg_daily_vol,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY daily_vol) as med_daily_vol
FROM (
    SELECT 
        market_id,
        COUNT(*) as trade_count,
        SUM(usd_amount) as total_vol,
        SUM(usd_amount) / GREATEST(1, EXTRACT(EPOCH FROM MAX(timestamp) - MIN(timestamp)) / 86400) as daily_vol
    FROM processed_trades
    WHERE usd_amount > 0
    GROUP BY market_id
    HAVING COUNT(*) > 10
) sub
"""

stats = pd.read_sql(query, conn).iloc[0]
print(f"Total markets: {stats['markets']:,.0f}")
print(f"Total trades: {stats['total_trades']:,.0f}")  
print(f"Total volume: ${stats['total_volume']/1e9:.2f}B")
print(f"Avg daily volume/market: ${stats['avg_daily_vol']:,.0f}")
print(f"Median daily volume/market: ${stats['med_daily_vol']:,.0f}")

# 3. LIQUIDITY TIERS
print("\n📊 LIQUIDITY TIER DISTRIBUTION")
print("-" * 60)

query = """
SELECT 
    tier,
    COUNT(*) as markets,
    SUM(total_vol) as volume,
    AVG(avg_trade) as avg_trade_size
FROM (
    SELECT 
        market_id,
        SUM(usd_amount) as total_vol,
        AVG(usd_amount) as avg_trade,
        CASE 
            WHEN SUM(usd_amount) / GREATEST(1, EXTRACT(EPOCH FROM MAX(timestamp) - MIN(timestamp)) / 86400) < 1000 THEN 'Very Low (<$1K/day)'
            WHEN SUM(usd_amount) / GREATEST(1, EXTRACT(EPOCH FROM MAX(timestamp) - MIN(timestamp)) / 86400) < 10000 THEN 'Low ($1K-10K/day)'
            WHEN SUM(usd_amount) / GREATEST(1, EXTRACT(EPOCH FROM MAX(timestamp) - MIN(timestamp)) / 86400) < 100000 THEN 'Medium ($10K-100K/day)'
            WHEN SUM(usd_amount) / GREATEST(1, EXTRACT(EPOCH FROM MAX(timestamp) - MIN(timestamp)) / 86400) < 1000000 THEN 'High ($100K-1M/day)'
            ELSE 'Very High (>$1M/day)'
        END as tier
    FROM processed_trades
    WHERE usd_amount > 0
    GROUP BY market_id
    HAVING COUNT(*) > 10
) sub
GROUP BY tier
ORDER BY MIN(total_vol)
"""

tiers = pd.read_sql(query, conn)
print(f"{'Tier':<25} {'Markets':>10} {'Volume':>15} {'Avg Trade':>12}")
print("-" * 65)
for _, r in tiers.iterrows():
    vol_str = f"${r['volume']/1e9:.2f}B" if r['volume'] > 1e9 else f"${r['volume']/1e6:.0f}M"
    print(f"{r['tier']:<25} {r['markets']:>10,} {vol_str:>15} ${r['avg_trade_size']:>10,.0f}")

# 4. BUILD SLIPPAGE MODEL PARAMETERS
print("\n📊 SLIPPAGE MODEL")
print("-" * 60)
print("Model: slippage = base_spread + impact_coef × √(order_size / daily_vol)")

query = """
SELECT 
    tier,
    COUNT(*) as n,
    AVG(volatility) as avg_vol,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY daily_vol) as med_daily_vol,
    AVG(volatility) * 2 as est_spread,
    AVG(volatility) / SQRT(GREATEST(1, AVG(daily_vol))) as impact_coef
FROM (
    SELECT 
        market_id,
        STDDEV(price) as volatility,
        SUM(usd_amount) / GREATEST(1, EXTRACT(EPOCH FROM MAX(timestamp) - MIN(timestamp)) / 86400) as daily_vol,
        CASE 
            WHEN SUM(usd_amount) / GREATEST(1, EXTRACT(EPOCH FROM MAX(timestamp) - MIN(timestamp)) / 86400) < 10000 THEN 'Low'
            WHEN SUM(usd_amount) / GREATEST(1, EXTRACT(EPOCH FROM MAX(timestamp) - MIN(timestamp)) / 86400) < 100000 THEN 'Medium'
            ELSE 'High'
        END as tier
    FROM processed_trades
    WHERE usd_amount > 0 AND price > 0 AND price < 1
    GROUP BY market_id
    HAVING COUNT(*) > 100 AND STDDEV(price) IS NOT NULL
) sub
GROUP BY tier
"""

model = pd.read_sql(query, conn)
print(f"\n{'Tier':<10} {'Base Spread':>12} {'Impact Coef':>14} {'Daily Vol':>14}")
print("-" * 55)
for _, r in model.iterrows():
    vol_str = f"${r['med_daily_vol']/1000:.0f}K"
    print(f"{r['tier']:<10} {r['est_spread']:>11.3%} {r['impact_coef']:>14.6f} {vol_str:>14}")

# 5. EXAMPLE ESTIMATES
print("\n📊 SLIPPAGE ESTIMATES (High Liquidity Market)")
print("-" * 60)
high = model[model['tier'] == 'High'].iloc[0]
for size in [100, 500, 1000, 5000, 10000, 50000]:
    slip = high['est_spread'] + high['impact_coef'] * np.sqrt(size / high['med_daily_vol'])
    cost = size * slip
    print(f"  ${size:>6,} order → {slip:>6.2%} slippage → ${cost:>8,.2f} cost")

print("\n📊 SLIPPAGE ESTIMATES (Low Liquidity Market)")
print("-" * 60)
low = model[model['tier'] == 'Low'].iloc[0]
for size in [100, 500, 1000, 5000]:
    slip = low['est_spread'] + low['impact_coef'] * np.sqrt(size / low['med_daily_vol'])
    cost = size * slip
    print(f"  ${size:>6,} order → {slip:>6.2%} slippage → ${cost:>8,.2f} cost")

# 6. CHECK ORDERBOOK DATA
print("\n📊 LIVE ORDERBOOK DATA")
print("-" * 60)
ob_stats = pd.read_sql("""
    SELECT COUNT(*) as snapshots, 
           COUNT(DISTINCT token_id) as tokens,
           AVG(spread) as avg_spread,
           AVG(bid_depth + ask_depth) as avg_total_depth
    FROM orderbook_snapshots
""", conn).iloc[0]

print(f"Snapshots: {ob_stats['snapshots']:,}")
print(f"Tokens: {ob_stats['tokens']:.0f}")
if ob_stats['avg_spread']:
    print(f"Avg spread: {ob_stats['avg_spread']:.4f}")
    print(f"Avg total depth: ${ob_stats['avg_total_depth']:,.0f}")

# Save model
model_dict = model.set_index('tier')[['est_spread', 'impact_coef', 'med_daily_vol']].to_dict('index')
with open('/Users/polyclawd/clawd/polymarket/data/slippage_model_v1.json', 'w') as f:
    json.dump({
        'model': 'sqrt_market_impact',
        'formula': 'slippage = spread + impact_coef * sqrt(order_size / daily_vol)',
        'parameters': model_dict,
        'generated': datetime.now().isoformat()
    }, f, indent=2, default=str)

print("\n✅ Model saved to data/slippage_model_v1.json")
print("\n" + "="*60)
print("NEXT STEPS")
print("="*60)
print("""
1. Orderbook collectors running - will calibrate spreads in 24h
2. Integrate slippage into backtest.py
3. Add time-of-day adjustments
4. Test on paper trades before live
""")

conn.close()
