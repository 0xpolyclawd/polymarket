#!/usr/bin/env python3
"""
Slippage/Liquidity Model Builder

Analyzes historical trades to estimate:
- Price impact as a function of trade size
- Liquidity metrics per market
- Slippage prediction model

This will be calibrated against live orderbook data.
"""

import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import warnings
warnings.filterwarnings('ignore')

DB_CONFIG = {
    'dbname': 'polymarket',
    'host': '/tmp',
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def analyze_price_impact():
    """
    Analyze price impact: how much does price move per $ traded?
    """
    print("\n" + "="*60)
    print("PRICE IMPACT ANALYSIS")
    print("="*60)
    
    conn = get_connection()
    
    # Get consecutive trades and calculate price changes
    query = """
    WITH trade_pairs AS (
        SELECT 
            market_id,
            timestamp,
            price,
            usd_amount,
            LAG(price) OVER (PARTITION BY market_id ORDER BY timestamp) as prev_price,
            LAG(timestamp) OVER (PARTITION BY market_id ORDER BY timestamp) as prev_timestamp
        FROM processed_trades
        WHERE usd_amount > 0 AND price > 0 AND price < 1
    )
    SELECT 
        market_id,
        usd_amount,
        ABS(price - prev_price) as price_change,
        price,
        EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) as time_gap_sec
    FROM trade_pairs
    WHERE prev_price IS NOT NULL 
      AND ABS(price - prev_price) < 0.5  -- Filter outliers
      AND EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) < 3600  -- Within 1 hour
    LIMIT 5000000
    """
    
    print("\nLoading trade pairs for price impact analysis...")
    df = pd.read_sql(query, conn)
    print(f"Loaded {len(df):,} trade pairs")
    
    # Bucket by trade size
    df['size_bucket'] = pd.cut(df['usd_amount'], 
                               bins=[0, 10, 50, 100, 500, 1000, 5000, 10000, 50000, float('inf')],
                               labels=['$0-10', '$10-50', '$50-100', '$100-500', '$500-1K', 
                                      '$1K-5K', '$5K-10K', '$10K-50K', '$50K+'])
    
    # Calculate impact per bucket
    impact_by_size = df.groupby('size_bucket').agg({
        'price_change': ['mean', 'median', 'std', 'count'],
        'usd_amount': 'mean'
    }).round(6)
    
    print("\n📊 PRICE IMPACT BY TRADE SIZE")
    print("-" * 60)
    print(f"{'Size Bucket':<15} {'Avg Impact':<12} {'Med Impact':<12} {'Trades':>10}")
    print("-" * 60)
    
    for bucket in impact_by_size.index:
        avg_impact = impact_by_size.loc[bucket, ('price_change', 'mean')]
        med_impact = impact_by_size.loc[bucket, ('price_change', 'median')]
        count = impact_by_size.loc[bucket, ('price_change', 'count')]
        print(f"{bucket:<15} {avg_impact:>10.4%} {med_impact:>10.4%} {count:>10,.0f}")
    
    conn.close()
    return impact_by_size


def analyze_market_liquidity():
    """
    Calculate liquidity metrics per market
    """
    print("\n" + "="*60)
    print("MARKET LIQUIDITY ANALYSIS")
    print("="*60)
    
    conn = get_connection()
    
    query = """
    SELECT 
        m.id as market_id,
        m.question,
        m.volume,
        COUNT(t.transaction_hash) as trade_count,
        AVG(t.usd_amount) as avg_trade_size,
        STDDEV(t.usd_amount) as std_trade_size,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.usd_amount) as median_trade_size,
        MAX(t.usd_amount) as max_trade_size,
        MIN(t.timestamp) as first_trade,
        MAX(t.timestamp) as last_trade
    FROM markets_full m
    JOIN processed_trades t ON t.market_id = m.id
    WHERE t.usd_amount > 0
    GROUP BY m.id, m.question, m.volume
    HAVING COUNT(*) > 100
    ORDER BY m.volume DESC NULLS LAST
    LIMIT 500
    """
    
    print("\nCalculating liquidity metrics per market...")
    df = pd.read_sql(query, conn)
    print(f"Analyzed {len(df)} markets with >100 trades")
    
    # Calculate additional metrics
    df['trade_duration_days'] = (df['last_trade'] - df['first_trade']).dt.total_seconds() / 86400
    df['trades_per_day'] = df['trade_count'] / df['trade_duration_days'].replace(0, 1)
    df['volume_per_trade'] = df['volume'] / df['trade_count']
    
    print("\n📊 TOP 20 MARKETS BY LIQUIDITY")
    print("-" * 80)
    print(f"{'Market':<40} {'Volume':>12} {'Trades':>10} {'Avg Size':>10}")
    print("-" * 80)
    
    for _, row in df.head(20).iterrows():
        question = row['question'][:38] + '..' if len(str(row['question'])) > 40 else row['question']
        vol = f"${row['volume']/1e6:.1f}M" if row['volume'] > 1e6 else f"${row['volume']/1e3:.0f}K"
        print(f"{question:<40} {vol:>12} {row['trade_count']:>10,.0f} ${row['avg_trade_size']:>8,.0f}")
    
    # Liquidity tiers
    print("\n📊 LIQUIDITY DISTRIBUTION")
    df['liquidity_tier'] = pd.cut(df['trades_per_day'],
                                  bins=[0, 10, 50, 200, 1000, float('inf')],
                                  labels=['Very Low (<10/day)', 'Low (10-50/day)', 
                                         'Medium (50-200/day)', 'High (200-1K/day)', 
                                         'Very High (>1K/day)'])
    
    tier_summary = df.groupby('liquidity_tier').agg({
        'market_id': 'count',
        'volume': 'sum',
        'avg_trade_size': 'mean'
    }).round(0)
    
    print("-" * 60)
    for tier, row in tier_summary.iterrows():
        print(f"{tier}: {row['market_id']:.0f} markets, ${row['volume']/1e9:.2f}B volume")
    
    conn.close()
    return df


def build_slippage_model():
    """
    Build a simple slippage estimation model:
    slippage = base_spread + impact_coefficient * sqrt(order_size / avg_daily_volume)
    
    This is a simplified Almgren-Chriss style model.
    """
    print("\n" + "="*60)
    print("SLIPPAGE MODEL CONSTRUCTION")
    print("="*60)
    
    conn = get_connection()
    
    # Get market-level stats
    query = """
    WITH market_stats AS (
        SELECT 
            market_id,
            COUNT(*) as trade_count,
            SUM(usd_amount) as total_volume,
            AVG(usd_amount) as avg_trade_size,
            STDDEV(price) as price_volatility
        FROM processed_trades
        WHERE usd_amount > 0 AND price > 0 AND price < 1
        GROUP BY market_id
        HAVING COUNT(*) > 500
    ),
    price_impacts AS (
        SELECT 
            t1.market_id,
            t1.usd_amount,
            ABS(t1.price - t2.price) as price_change
        FROM processed_trades t1
        JOIN processed_trades t2 ON t1.market_id = t2.market_id 
            AND t2.timestamp = (
                SELECT MAX(timestamp) FROM processed_trades 
                WHERE market_id = t1.market_id AND timestamp < t1.timestamp
            )
        WHERE t1.usd_amount > 100
        LIMIT 100000
    )
    SELECT 
        ms.market_id,
        ms.trade_count,
        ms.total_volume,
        ms.avg_trade_size,
        ms.price_volatility,
        AVG(pi.price_change) as avg_impact,
        CORR(pi.usd_amount, pi.price_change) as size_impact_corr
    FROM market_stats ms
    LEFT JOIN price_impacts pi ON ms.market_id = pi.market_id
    GROUP BY ms.market_id, ms.trade_count, ms.total_volume, ms.avg_trade_size, ms.price_volatility
    """
    
    print("\nBuilding model from trade data...")
    
    # Simpler approach: just get summary stats
    simple_query = """
    SELECT 
        market_id,
        COUNT(*) as trade_count,
        SUM(usd_amount) as total_volume,
        AVG(usd_amount) as avg_trade_size,
        STDDEV(price) as price_volatility,
        MAX(timestamp) - MIN(timestamp) as trading_period
    FROM processed_trades
    WHERE usd_amount > 0 AND price > 0 AND price < 1
    GROUP BY market_id
    HAVING COUNT(*) > 100
    """
    
    df = pd.read_sql(simple_query, conn)
    print(f"Loaded stats for {len(df)} markets")
    
    # Calculate derived metrics
    df['trading_days'] = df['trading_period'].dt.total_seconds() / 86400
    df['daily_volume'] = df['total_volume'] / df['trading_days'].replace(0, 1)
    df['daily_trades'] = df['trade_count'] / df['trading_days'].replace(0, 1)
    
    # Estimate base spread from volatility (rough proxy)
    df['estimated_spread'] = df['price_volatility'] * 2  # 2x vol as spread proxy
    
    # Impact coefficient estimation (simplified)
    # Using square-root market impact model: impact = sigma * sqrt(Q/V)
    # where sigma = volatility, Q = order size, V = daily volume
    df['impact_coefficient'] = df['price_volatility'] / np.sqrt(df['daily_volume'].replace(0, 1))
    
    print("\n📊 SLIPPAGE MODEL PARAMETERS")
    print("-" * 60)
    print("\nModel: slippage = spread + impact_coef * sqrt(order_size / daily_volume)")
    print("\nBy liquidity tier:")
    
    df['liquidity_tier'] = pd.qcut(df['daily_volume'], q=5, 
                                    labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'])
    
    model_params = df.groupby('liquidity_tier').agg({
        'estimated_spread': 'median',
        'impact_coefficient': 'median', 
        'daily_volume': 'median',
        'market_id': 'count'
    }).round(6)
    
    print(f"\n{'Tier':<12} {'Spread':>10} {'Impact Coef':>12} {'Daily Vol':>12} {'Markets':>8}")
    print("-" * 60)
    for tier, row in model_params.iterrows():
        vol_str = f"${row['daily_volume']/1000:.0f}K" if row['daily_volume'] > 1000 else f"${row['daily_volume']:.0f}"
        print(f"{tier:<12} {row['estimated_spread']:>10.4f} {row['impact_coefficient']:>12.6f} {vol_str:>12} {row['market_id']:>8.0f}")
    
    # Example slippage estimates
    print("\n📊 EXAMPLE SLIPPAGE ESTIMATES")
    print("-" * 60)
    print("For a HIGH liquidity market (daily vol ~$500K):")
    
    high_liq = model_params.loc['High']
    for order_size in [100, 500, 1000, 5000, 10000]:
        slippage = high_liq['estimated_spread'] + high_liq['impact_coefficient'] * np.sqrt(order_size / high_liq['daily_volume'])
        print(f"  ${order_size:>6,} order → {slippage:>6.2%} slippage (${order_size * slippage:>6.2f} cost)")
    
    print("\nFor a LOW liquidity market (daily vol ~$10K):")
    low_liq = model_params.loc['Low']
    for order_size in [100, 500, 1000, 5000]:
        slippage = low_liq['estimated_spread'] + low_liq['impact_coefficient'] * np.sqrt(order_size / low_liq['daily_volume'])
        print(f"  ${order_size:>6,} order → {slippage:>6.2%} slippage (${order_size * slippage:>6.2f} cost)")
    
    conn.close()
    
    # Save model parameters
    model_output = {
        'model_type': 'square_root_market_impact',
        'formula': 'slippage = spread + impact_coef * sqrt(order_size / daily_volume)',
        'parameters_by_tier': model_params.to_dict(),
        'generated_at': datetime.now().isoformat(),
        'note': 'Preliminary model - will calibrate against live orderbook data'
    }
    
    with open('/Users/polyclawd/clawd/polymarket/data/slippage_model_v1.json', 'w') as f:
        json.dump(model_output, f, indent=2, default=str)
    
    print("\n✅ Model saved to data/slippage_model_v1.json")
    
    return model_params


def check_orderbook_data():
    """Check if we have enough orderbook data to calibrate"""
    print("\n" + "="*60)
    print("LIVE ORDERBOOK DATA STATUS")
    print("="*60)
    
    conn = get_connection()
    
    query = """
    SELECT 
        COUNT(*) as total_snapshots,
        COUNT(DISTINCT token_id) as unique_tokens,
        MIN(captured_at) as first_capture,
        MAX(captured_at) as last_capture,
        AVG(spread) as avg_spread,
        AVG(bid_depth) as avg_bid_depth,
        AVG(ask_depth) as avg_ask_depth
    FROM orderbook_snapshots
    """
    
    result = pd.read_sql(query, conn)
    
    print(f"\nSnapshots collected: {result['total_snapshots'].iloc[0]:,}")
    print(f"Unique tokens: {result['unique_tokens'].iloc[0]}")
    print(f"Collection period: {result['first_capture'].iloc[0]} to {result['last_capture'].iloc[0]}")
    print(f"Average spread: {result['avg_spread'].iloc[0]:.4f}")
    print(f"Average bid depth: ${result['avg_bid_depth'].iloc[0]:,.0f}")
    print(f"Average ask depth: ${result['avg_ask_depth'].iloc[0]:,.0f}")
    
    if result['total_snapshots'].iloc[0] < 1000:
        print("\n⚠️  Need more orderbook data for calibration (target: 10K+ snapshots)")
        print("    Collectors are running - check back in a few hours")
    else:
        print("\n✅ Enough data for preliminary calibration")
    
    conn.close()


if __name__ == "__main__":
    print("🔮 POLYMARKET SLIPPAGE MODEL BUILDER")
    print("=" * 60)
    print(f"Started: {datetime.now()}")
    
    # Run analyses
    impact_by_size = analyze_price_impact()
    market_liquidity = analyze_market_liquidity()
    model_params = build_slippage_model()
    check_orderbook_data()
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print("""
Next steps:
1. Let orderbook collectors run for 24-48 hours
2. Calibrate spread estimates against actual bid-ask spreads
3. Validate impact model against orderbook depth
4. Add time-of-day and market-event adjustments
5. Backtest strategies WITH slippage costs
""")
