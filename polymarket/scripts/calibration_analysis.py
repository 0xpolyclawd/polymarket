#!/usr/bin/env python3
"""
Calibration Analysis - Check if Polymarket prices are well-calibrated.

A well-calibrated market means:
- Events priced at 70% should resolve YES ~70% of the time
- Events priced at 30% should resolve YES ~30% of the time

If markets are miscalibrated, there's systematic alpha available.
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "polymarket.db"


def load_resolved_markets():
    """Load markets that have resolved (price went to 0 or 1)."""
    conn = sqlite3.connect(DB_PATH)
    
    # Get price history
    price_df = pd.read_sql_query("""
        SELECT market_id, token_id, timestamp, price
        FROM price_history
        ORDER BY market_id, timestamp
    """, conn)
    
    markets_df = pd.read_sql_query("""
        SELECT id, question, volume, category
        FROM markets
    """, conn)
    
    conn.close()
    return price_df, markets_df


def get_final_resolution(prices):
    """Determine final resolution from price series."""
    if len(prices) == 0:
        return None, None
    
    final_price = prices.iloc[-1]
    
    if final_price > 0.95:
        return "YES", final_price
    elif final_price < 0.05:
        return "NO", final_price
    else:
        return None, final_price  # Not resolved


def analyze_calibration(price_df, markets_df):
    """Analyze market calibration."""
    print("=" * 60)
    print("CALIBRATION ANALYSIS")
    print("=" * 60)
    
    results = []
    
    for market_id, group in price_df.groupby('market_id'):
        if len(group) < 10:  # Need enough data points
            continue
        
        prices = group.sort_values('timestamp')['price']
        resolution, final = get_final_resolution(prices)
        
        if resolution is None:
            continue
        
        # Get prices at different time points
        n = len(prices)
        
        # Sample at 25%, 50%, 75% of market lifetime
        for pct_idx, pct_name in [(n//4, "early_25%"), (n//2, "mid_50%"), (3*n//4, "late_75%")]:
            if pct_idx < len(prices):
                predicted_prob = prices.iloc[pct_idx]
                actual = 1 if resolution == "YES" else 0
                
                results.append({
                    'market_id': market_id,
                    'time_point': pct_name,
                    'predicted_prob': predicted_prob,
                    'actual_outcome': actual,
                    'resolution': resolution
                })
    
    if not results:
        print("Not enough resolved markets for calibration analysis")
        return None
    
    df = pd.DataFrame(results)
    
    # Bin predictions
    bins = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    df['prob_bin'] = pd.cut(df['predicted_prob'], bins=bins)
    
    print(f"\nAnalyzing {len(df)} price points from resolved markets\n")
    
    # Calibration by probability bin
    print("📊 Calibration by Probability Bin:")
    print("-" * 50)
    print(f"{'Bin':<15} {'Predicted':<12} {'Actual':<12} {'N':<6} {'Diff':<10}")
    print("-" * 50)
    
    calibration_data = []
    for bin_range, bin_df in df.groupby('prob_bin', observed=True):
        if len(bin_df) < 3:
            continue
        
        predicted = bin_df['predicted_prob'].mean()
        actual = bin_df['actual_outcome'].mean()
        n = len(bin_df)
        diff = actual - predicted
        
        calibration_data.append({
            'bin': str(bin_range),
            'predicted': predicted,
            'actual': actual,
            'n': n,
            'diff': diff
        })
        
        diff_str = f"{diff:+.1%}" if diff >= 0 else f"{diff:.1%}"
        print(f"{str(bin_range):<15} {predicted:.1%}          {actual:.1%}         {n:<6} {diff_str}")
    
    if calibration_data:
        cal_df = pd.DataFrame(calibration_data)
        
        # Overall metrics
        print("\n📈 Calibration Summary:")
        
        # Mean absolute calibration error
        mace = np.abs(cal_df['diff']).mean()
        print(f"   Mean Absolute Calibration Error: {mace:.1%}")
        
        # Brier score proxy
        weighted_error = (cal_df['diff'].abs() * cal_df['n']).sum() / cal_df['n'].sum()
        print(f"   Weighted Calibration Error: {weighted_error:.1%}")
        
        # Check for systematic biases
        print("\n🔍 Potential Alpha Signals:")
        
        for _, row in cal_df.iterrows():
            if abs(row['diff']) > 0.10 and row['n'] >= 5:  # >10% miscalibration
                direction = "OVERCONFIDENT" if row['diff'] < 0 else "UNDERCONFIDENT"
                print(f"   {row['bin']}: {direction} by {abs(row['diff']):.1%} (n={row['n']})")
    
    return df


def main():
    print("\n🎯 POLYMARKET CALIBRATION STUDY")
    print()
    
    price_df, markets_df = load_resolved_markets()
    
    if price_df.empty:
        print("No data. Run data_collector.py first.")
        return
    
    print(f"Loaded {len(price_df)} price points from {price_df['market_id'].nunique()} markets")
    
    df = analyze_calibration(price_df, markets_df)
    
    print("\n" + "=" * 60)
    print("INTERPRETATION")
    print("=" * 60)
    print("""
If a bin shows:
  - OVERCONFIDENT: Market thinks YES is more likely than reality
    → Strategy: BET NO when price is in this range
    
  - UNDERCONFIDENT: Market thinks YES is less likely than reality  
    → Strategy: BET YES when price is in this range

NOTE: Need 1000+ resolved markets for statistical significance.
Current analysis is preliminary.
    """)


if __name__ == "__main__":
    main()
