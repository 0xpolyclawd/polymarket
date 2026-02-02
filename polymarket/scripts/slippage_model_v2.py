#!/usr/bin/env python3
"""
Slippage Model v2 - Uses ACTUAL orderbook spreads, not volatility proxy
"""

import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
import json

DB_CONFIG = {'dbname': 'polymarket', 'host': '/tmp'}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

print("🔮 POLYMARKET SLIPPAGE MODEL v2")
print("=" * 60)

conn = get_connection()

# 1. ACTUAL SPREADS FROM ORDERBOOK DATA
print("\n📊 ACTUAL SPREADS FROM LIVE ORDERBOOK")
print("-" * 60)

ob_query = """
SELECT 
    AVG(spread) as avg_spread,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY spread) as median_spread,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY spread) as p25_spread,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY spread) as p75_spread,
    AVG(bid_depth) as avg_bid_depth,
    AVG(ask_depth) as avg_ask_depth,
    COUNT(*) as snapshots
FROM orderbook_snapshots
WHERE spread IS NOT NULL AND spread < 1 AND spread > 0
"""

ob = pd.read_sql(ob_query, conn).iloc[0]
print(f"Snapshots analyzed: {ob['snapshots']:,.0f}")
print(f"Average spread: {ob['avg_spread']:.4f} ({ob['avg_spread']*100:.2f}%)")
print(f"Median spread: {ob['median_spread']:.4f} ({ob['median_spread']*100:.2f}%)")
print(f"25th percentile: {ob['p25_spread']:.4f}")
print(f"75th percentile: {ob['p75_spread']:.4f}")
print(f"Avg bid depth: ${ob['avg_bid_depth']:,.0f}")
print(f"Avg ask depth: ${ob['avg_ask_depth']:,.0f}")

# 2. PRICE IMPACT FROM CONSECUTIVE TRADES
print("\n📊 PRICE IMPACT FROM TRADE DATA")
print("-" * 60)

impact_query = """
WITH trade_pairs AS (
    SELECT 
        usd_amount,
        ABS(price - LAG(price) OVER (PARTITION BY market_id ORDER BY timestamp)) as price_change
    FROM processed_trades
    WHERE usd_amount > 0 AND price > 0 AND price < 1
)
SELECT 
    CASE 
        WHEN usd_amount < 100 THEN 'Small (<$100)'
        WHEN usd_amount < 1000 THEN 'Medium ($100-1K)'
        WHEN usd_amount < 10000 THEN 'Large ($1K-10K)'
        ELSE 'Very Large (>$10K)'
    END as size_bucket,
    AVG(price_change) as avg_impact,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_change) as med_impact,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY price_change) as p90_impact,
    COUNT(*) as trades,
    AVG(usd_amount) as avg_size
FROM trade_pairs
WHERE price_change IS NOT NULL AND price_change < 0.2
GROUP BY 1
ORDER BY MIN(usd_amount)
"""

impact = pd.read_sql(impact_query, conn)
print(f"{'Size':<20} {'Avg Impact':>12} {'Med Impact':>12} {'P90 Impact':>12} {'Trades':>12}")
print("-" * 72)
for _, r in impact.iterrows():
    print(f"{r['size_bucket']:<20} {r['avg_impact']:>11.4f} {r['med_impact']:>11.4f} {r['p90_impact']:>11.4f} {r['trades']:>12,}")

# 3. BUILD REALISTIC MODEL
print("\n📊 REALISTIC SLIPPAGE MODEL")
print("-" * 60)

# Use median spread as base (more robust than mean)
base_spread = ob['median_spread'] if ob['median_spread'] else 0.01

# Calculate impact coefficient from trade data
# Impact scales with sqrt of relative order size
med_impact = impact[impact['size_bucket'] == 'Medium ($100-1K)']['med_impact'].iloc[0]
avg_size_medium = impact[impact['size_bucket'] == 'Medium ($100-1K)']['avg_size'].iloc[0]

# impact = coef * sqrt(order_size / reference_size)
# coef = impact / sqrt(1) when order = reference
impact_coef = med_impact

print(f"\nModel Parameters:")
print(f"  Base spread (bid-ask): {base_spread:.4f} ({base_spread*100:.2f}%)")
print(f"  Impact coefficient: {impact_coef:.4f}")
print(f"  Reference size: ${avg_size_medium:.0f}")

print(f"\nFormula: slippage = {base_spread:.4f} + {impact_coef:.4f} × √(order_size / {avg_size_medium:.0f})")

# 4. SLIPPAGE ESTIMATES
print("\n📊 ESTIMATED SLIPPAGE BY ORDER SIZE")
print("-" * 60)
print(f"{'Order Size':>12} {'Spread':>10} {'Impact':>10} {'Total':>10} {'$ Cost':>12}")
print("-" * 60)

for size in [50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000]:
    spread_cost = base_spread / 2  # Half spread for market order
    impact_cost = impact_coef * np.sqrt(size / avg_size_medium)
    total = spread_cost + impact_cost
    dollar_cost = size * total
    print(f"${size:>11,} {spread_cost:>9.3%} {impact_cost:>9.3%} {total:>9.3%} ${dollar_cost:>11,.2f}")

# 5. MARKET LIQUIDITY TIERS
print("\n📊 LIQUIDITY TIERS FOR BACKTESTING")
print("-" * 60)

tier_query = """
SELECT 
    CASE 
        WHEN daily_vol < 5000 THEN 'ILLIQUID'
        WHEN daily_vol < 50000 THEN 'LOW'
        WHEN daily_vol < 500000 THEN 'MEDIUM'
        ELSE 'HIGH'
    END as tier,
    COUNT(*) as markets,
    AVG(daily_vol) as avg_daily_vol,
    SUM(total_vol) as total_vol
FROM (
    SELECT 
        market_id,
        SUM(usd_amount) as total_vol,
        SUM(usd_amount) / GREATEST(1, EXTRACT(EPOCH FROM MAX(timestamp) - MIN(timestamp)) / 86400) as daily_vol
    FROM processed_trades
    WHERE usd_amount > 0
    GROUP BY market_id
    HAVING COUNT(*) > 50
) sub
GROUP BY 1
ORDER BY MIN(daily_vol)
"""

tiers = pd.read_sql(tier_query, conn)
print(f"{'Tier':<12} {'Markets':>10} {'Avg Daily Vol':>15} {'Total Vol':>15}")
print("-" * 55)

tier_params = {}
for _, r in tiers.iterrows():
    vol_str = f"${r['avg_daily_vol']/1000:.1f}K" if r['avg_daily_vol'] < 1e6 else f"${r['avg_daily_vol']/1e6:.2f}M"
    total_str = f"${r['total_vol']/1e9:.2f}B"
    print(f"{r['tier']:<12} {r['markets']:>10,} {vol_str:>15} {total_str:>15}")
    
    # Adjust impact coef by tier (less liquid = more impact)
    tier_multiplier = {'ILLIQUID': 3.0, 'LOW': 2.0, 'MEDIUM': 1.0, 'HIGH': 0.5}
    tier_params[r['tier']] = {
        'spread': base_spread,
        'impact_coef': impact_coef * tier_multiplier.get(r['tier'], 1.0),
        'daily_vol': r['avg_daily_vol']
    }

# Save model
model_output = {
    'version': 2,
    'model': 'spread_plus_sqrt_impact',
    'formula': f'slippage = spread/2 + {impact_coef:.4f} * sqrt(order_size / {avg_size_medium:.0f})',
    'base_spread': float(base_spread),
    'impact_coefficient': float(impact_coef),
    'reference_size': float(avg_size_medium),
    'tier_parameters': tier_params,
    'orderbook_snapshots_used': int(ob['snapshots']),
    'generated': datetime.now().isoformat()
}

with open('/Users/polyclawd/clawd/polymarket/data/slippage_model_v2.json', 'w') as f:
    json.dump(model_output, f, indent=2, default=str)

print(f"\n✅ Model v2 saved to data/slippage_model_v2.json")

# Summary
print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"""
Realistic slippage estimates:
- $100 trade: ~{(base_spread/2 + impact_coef*np.sqrt(100/avg_size_medium))*100:.2f}% slippage
- $1,000 trade: ~{(base_spread/2 + impact_coef*np.sqrt(1000/avg_size_medium))*100:.2f}% slippage  
- $10,000 trade: ~{(base_spread/2 + impact_coef*np.sqrt(10000/avg_size_medium))*100:.2f}% slippage

Key findings:
- Actual bid-ask spread: {base_spread*100:.2f}%
- Price impact scales with √(order_size)
- ~{tiers[tiers['tier']=='HIGH']['markets'].iloc[0] if len(tiers[tiers['tier']=='HIGH']) > 0 else 0:,} high-liquidity markets suitable for larger trades

For backtesting: Use tier-adjusted slippage based on market's daily volume.
""")

conn.close()
