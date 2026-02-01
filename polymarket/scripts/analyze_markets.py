#!/usr/bin/env python3
"""
Analyze Polymarket data for strategy research.
"""

import sqlite3
import json
import pandas as pd
import numpy as np
from pathlib import Path
from collections import Counter

DB_PATH = Path(__file__).parent.parent / "data" / "polymarket.db"


def load_data():
    """Load data from database."""
    conn = sqlite3.connect(DB_PATH)
    
    markets_df = pd.read_sql_query("""
        SELECT * FROM markets ORDER BY volume DESC
    """, conn)
    
    price_history_df = pd.read_sql_query("""
        SELECT * FROM price_history ORDER BY market_id, timestamp
    """, conn)
    
    conn.close()
    return markets_df, price_history_df


def analyze_market_structure(markets_df):
    """Analyze market structure."""
    print("=" * 60)
    print("MARKET STRUCTURE ANALYSIS")
    print("=" * 60)
    
    # Volume distribution
    print(f"\n📊 Volume Distribution:")
    print(f"   Total markets: {len(markets_df)}")
    print(f"   Total volume: ${markets_df['volume'].sum():,.0f}")
    print(f"   Mean volume: ${markets_df['volume'].mean():,.0f}")
    print(f"   Median volume: ${markets_df['volume'].median():,.0f}")
    print(f"   Max volume: ${markets_df['volume'].max():,.0f}")
    
    # Top markets by volume
    print(f"\n📈 Top 5 Markets by Volume:")
    for _, m in markets_df.head(5).iterrows():
        q = m['question'][:50] if m['question'] else 'N/A'
        print(f"   ${m['volume']:,.0f} - {q}...")
    
    # Category analysis
    if 'category' in markets_df.columns:
        categories = markets_df['category'].value_counts()
        print(f"\n🏷️ Categories:")
        for cat, count in categories.head(10).items():
            print(f"   {cat or 'Unknown'}: {count}")
    
    # Active vs Closed
    print(f"\n📊 Market Status:")
    print(f"   Active: {markets_df['active'].sum()}")
    print(f"   Closed: {markets_df['closed'].sum()}")


def analyze_price_behavior(markets_df, price_history_df):
    """Analyze price behavior patterns."""
    print("\n" + "=" * 60)
    print("PRICE BEHAVIOR ANALYSIS")
    print("=" * 60)
    
    if price_history_df.empty:
        print("No price history data available")
        return
    
    # Group by market
    market_groups = price_history_df.groupby('market_id')
    
    stats = []
    for market_id, group in market_groups:
        if len(group) < 5:
            continue
            
        prices = group['price'].values
        timestamps = group['timestamp'].values
        
        # Calculate metrics
        price_range = prices.max() - prices.min()
        volatility = np.std(prices)
        
        # Price movement direction
        if len(prices) > 1:
            start_price = prices[0]
            end_price = prices[-1]
            price_change = end_price - start_price
            
            # Did it trend toward 0 or 1?
            if end_price > 0.9:
                resolution = "YES"
            elif end_price < 0.1:
                resolution = "NO"
            else:
                resolution = "UNRESOLVED"
        else:
            price_change = 0
            resolution = "UNKNOWN"
        
        # Duration in hours
        duration_hours = (timestamps[-1] - timestamps[0]) / 3600 if len(timestamps) > 1 else 0
        
        stats.append({
            'market_id': market_id,
            'num_points': len(prices),
            'start_price': prices[0],
            'end_price': prices[-1],
            'price_change': price_change,
            'price_range': price_range,
            'volatility': volatility,
            'resolution': resolution,
            'duration_hours': duration_hours
        })
    
    if not stats:
        print("No markets with sufficient price history")
        return
    
    stats_df = pd.DataFrame(stats)
    
    print(f"\n📊 Price History Statistics:")
    print(f"   Markets with history: {len(stats_df)}")
    print(f"   Avg price points: {stats_df['num_points'].mean():.0f}")
    print(f"   Avg volatility: {stats_df['volatility'].mean():.3f}")
    print(f"   Avg price range: {stats_df['price_range'].mean():.3f}")
    
    # Resolution analysis
    print(f"\n📈 Resolution Distribution:")
    for res, count in stats_df['resolution'].value_counts().items():
        pct = count / len(stats_df) * 100
        print(f"   {res}: {count} ({pct:.1f}%)")
    
    # Starting price analysis
    print(f"\n🎯 Starting Price Distribution:")
    price_bins = pd.cut(stats_df['start_price'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1.0])
    for bin_range, count in price_bins.value_counts().sort_index().items():
        print(f"   {bin_range}: {count}")
    
    return stats_df


def find_patterns(markets_df, price_history_df):
    """Find potential alpha patterns."""
    print("\n" + "=" * 60)
    print("PATTERN ANALYSIS (Alpha Hunting)")
    print("=" * 60)
    
    if price_history_df.empty:
        print("No price history for pattern analysis")
        return
    
    patterns = []
    
    # Look for mispriced markets (starting far from 0.5 but resolving opposite)
    market_groups = price_history_df.groupby('market_id')
    
    reversals = 0
    momentum = 0
    
    for market_id, group in market_groups:
        if len(group) < 10:
            continue
        
        prices = group['price'].values
        start_p = prices[0]
        end_p = prices[-1]
        
        # Check for reversal (started >0.6 but resolved NO, or started <0.4 but resolved YES)
        if start_p > 0.6 and end_p < 0.2:
            reversals += 1
            patterns.append(f"Reversal HIGH→NO: start={start_p:.2f}, end={end_p:.2f}")
        elif start_p < 0.4 and end_p > 0.8:
            reversals += 1
            patterns.append(f"Reversal LOW→YES: start={start_p:.2f}, end={end_p:.2f}")
        
        # Momentum (price continued in initial direction)
        if start_p > 0.6 and end_p > 0.9:
            momentum += 1
        elif start_p < 0.4 and end_p < 0.1:
            momentum += 1
    
    print(f"\n📊 Price Pattern Observations:")
    print(f"   Reversals (started wrong): {reversals}")
    print(f"   Momentum (started right): {momentum}")
    
    if patterns:
        print(f"\n⚠️ Notable Reversals (potential alpha):")
        for p in patterns[:5]:
            print(f"   {p}")
    
    # Spread analysis opportunity
    print(f"\n💰 Potential Strategy Ideas:")
    print("   1. Early mispricing: Markets that start far from 0.5 may be mispriced")
    print("   2. Momentum: High-probability events tend to resolve as expected")
    print("   3. Mean reversion: Markets near 0.5 have higher uncertainty")
    print("   4. Volume-based: High volume = more information incorporated")


def main():
    print("\n🔮 POLYMARKET DATA ANALYSIS")
    print(f"   Database: {DB_PATH}")
    print()
    
    markets_df, price_history_df = load_data()
    
    if markets_df.empty:
        print("No data in database. Run data_collector.py first.")
        return
    
    analyze_market_structure(markets_df)
    stats_df = analyze_price_behavior(markets_df, price_history_df)
    find_patterns(markets_df, price_history_df)
    
    print("\n" + "=" * 60)
    print("NEXT STEPS FOR RESEARCH")
    print("=" * 60)
    print("""
1. Collect more historical data (1000+ resolved markets)
2. Analyze categories separately (politics vs crypto vs sports)
3. Build calibration curves (predicted vs actual outcomes)
4. Look for systematic biases (overconfidence, recency bias)
5. Backtest simple strategies:
   - Fade extreme prices (mean reversion)
   - Follow momentum in high-volume markets
   - Arbitrage multi-outcome events
    """)


if __name__ == "__main__":
    main()
