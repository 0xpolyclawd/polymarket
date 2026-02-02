#!/usr/bin/env python3
"""Quick slippage estimates using sampling"""
import psycopg2
import numpy as np
from datetime import datetime
import json

conn = psycopg2.connect(dbname='polymarket', host='/tmp')
cur = conn.cursor()

print("🔮 SLIPPAGE MODEL (Quick Sampled)")
print("=" * 60)

# 1. Get actual spreads from orderbook
print("\n📊 ACTUAL ORDERBOOK SPREADS")
cur.execute("""
    SELECT AVG(spread), 
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY spread),
           COUNT(*)
    FROM orderbook_snapshots 
    WHERE spread > 0 AND spread < 0.5
""")
avg_spread, med_spread, ob_count = cur.fetchone()
print(f"Snapshots: {ob_count:,}")
print(f"Avg spread: {avg_spread:.4f} ({avg_spread*100:.2f}%)" if avg_spread else "No spread data yet")
print(f"Median spread: {med_spread:.4f}" if med_spread else "")

# 2. Sample price impacts from a few high-volume markets
print("\n📊 PRICE IMPACT (sampled from top markets)")
cur.execute("""
    WITH top_markets AS (
        SELECT market_id, COUNT(*) as cnt
        FROM processed_trades
        GROUP BY market_id
        ORDER BY cnt DESC
        LIMIT 50
    ),
    sampled AS (
        SELECT t.market_id, t.timestamp, t.price, t.usd_amount
        FROM processed_trades t
        JOIN top_markets tm ON t.market_id = tm.market_id
        WHERE t.usd_amount > 0 AND t.price > 0.01 AND t.price < 0.99
        ORDER BY t.market_id, t.timestamp
    ),
    with_lag AS (
        SELECT 
            usd_amount,
            ABS(price - LAG(price) OVER (PARTITION BY market_id ORDER BY timestamp)) as impact
        FROM sampled
    )
    SELECT 
        CASE WHEN usd_amount < 100 THEN 'S' WHEN usd_amount < 1000 THEN 'M' WHEN usd_amount < 10000 THEN 'L' ELSE 'XL' END,
        AVG(impact), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY impact), COUNT(*)
    FROM with_lag
    WHERE impact IS NOT NULL AND impact < 0.1
    GROUP BY 1
    ORDER BY MIN(usd_amount)
""")

results = cur.fetchall()
print(f"{'Size':<5} {'Avg Impact':>12} {'Med Impact':>12} {'Trades':>10}")
print("-" * 45)
for bucket, avg_imp, med_imp, cnt in results:
    size_label = {'S': '<$100', 'M': '$100-1K', 'L': '$1K-10K', 'XL': '>$10K'}[bucket]
    print(f"{size_label:<12} {avg_imp:>10.4f} {med_imp:>10.4f} {cnt:>10,}")

# 3. Build model
med_imp_m = [r[2] for r in results if r[0] == 'M'][0] if any(r[0] == 'M' for r in results) else 0.005
base_spread = med_spread if med_spread else 0.01

print(f"\n📊 MODEL PARAMETERS")
print("-" * 45)
print(f"Base spread (half bid-ask): {base_spread/2:.4f} ({base_spread/2*100:.2f}%)")
print(f"Impact coefficient: {med_imp_m:.4f}")

print(f"\n📊 SLIPPAGE ESTIMATES")
print("-" * 45)
print(f"{'Order':>10} {'Spread':>10} {'Impact':>10} {'Total':>10} {'Cost':>12}")
for size in [100, 500, 1000, 5000, 10000]:
    spread_c = base_spread / 2
    impact_c = med_imp_m * np.sqrt(size / 500)  # Scale from $500 reference
    total = spread_c + impact_c
    cost = size * total
    print(f"${size:>9,} {spread_c:>9.2%} {impact_c:>9.2%} {total:>9.2%} ${cost:>11,.2f}")

# Save
model = {
    'base_spread': float(base_spread) if base_spread else 0.01,
    'impact_coef': float(med_imp_m),
    'reference_size': 500,
    'formula': 'slippage = spread/2 + impact_coef * sqrt(size/500)',
    'generated': datetime.now().isoformat()
}
with open('/Users/polyclawd/clawd/polymarket/data/slippage_model_v2.json', 'w') as f:
    json.dump(model, f, indent=2)
print(f"\n✅ Saved to data/slippage_model_v2.json")

conn.close()
