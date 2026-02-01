#!/usr/bin/env python3
"""
Simple Backtesting Framework for Polymarket Strategies.

Strategies tested:
1. Calibration Arb: Bet against miscalibrated extreme prices
2. Momentum: Follow recent price direction
3. Mean Reversion: Fade extreme moves
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict

DB_PATH = Path(__file__).parent.parent / "data" / "polymarket.db"


@dataclass
class Trade:
    """Represents a trade."""
    market_id: str
    entry_time: int
    entry_price: float
    side: str  # "YES" or "NO"
    size: float
    exit_time: int = None
    exit_price: float = None
    pnl: float = None


class Backtest:
    """Simple backtesting engine."""
    
    def __init__(self, initial_capital=10000):
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.trades: List[Trade] = []
        self.equity_curve = []
    
    def open_trade(self, market_id: str, time: int, price: float, side: str, size: float) -> Trade:
        """Open a new trade."""
        trade = Trade(
            market_id=market_id,
            entry_time=time,
            entry_price=price,
            side=side,
            size=size
        )
        self.trades.append(trade)
        return trade
    
    def close_trade(self, trade: Trade, time: int, price: float):
        """Close a trade and calculate PnL."""
        trade.exit_time = time
        trade.exit_price = price
        
        # Calculate PnL
        if trade.side == "YES":
            # Bought YES at entry_price, selling at exit_price
            # If resolved YES (price -> 1): PnL = (1 - entry_price) * size
            # If resolved NO (price -> 0): PnL = (0 - entry_price) * size
            trade.pnl = (price - trade.entry_price) * trade.size
        else:
            # Bought NO at (1 - entry_price), selling at (1 - exit_price)
            trade.pnl = ((1 - price) - (1 - trade.entry_price)) * trade.size
        
        self.capital += trade.pnl
        self.equity_curve.append(self.capital)
        return trade
    
    def get_results(self) -> Dict:
        """Get backtest results."""
        if not self.trades:
            return {"error": "No trades"}
        
        closed_trades = [t for t in self.trades if t.pnl is not None]
        
        if not closed_trades:
            return {"error": "No closed trades"}
        
        pnls = [t.pnl for t in closed_trades]
        
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        
        return {
            "total_trades": len(closed_trades),
            "winning_trades": len(wins),
            "losing_trades": len(losses),
            "win_rate": len(wins) / len(closed_trades) if closed_trades else 0,
            "total_pnl": sum(pnls),
            "avg_pnl": np.mean(pnls),
            "max_pnl": max(pnls) if pnls else 0,
            "min_pnl": min(pnls) if pnls else 0,
            "sharpe": np.mean(pnls) / np.std(pnls) if np.std(pnls) > 0 else 0,
            "final_capital": self.capital,
            "return_pct": (self.capital - self.initial_capital) / self.initial_capital * 100
        }


def load_price_data():
    """Load price history from database."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT market_id, token_id, timestamp, price
        FROM price_history
        ORDER BY market_id, timestamp
    """, conn)
    conn.close()
    return df


def strategy_calibration_arb(price_df, bt: Backtest, entry_threshold=0.15, bet_size=100):
    """
    Calibration Arbitrage Strategy
    
    Based on preliminary findings:
    - When price is 10-30%: Market may be overconfident → Bet NO
    - When price is 70-90%: Market may be underconfident → Bet YES (inverse)
    """
    print("\n📈 STRATEGY: Calibration Arbitrage")
    print(f"   Entry threshold: {entry_threshold:.0%}")
    print(f"   Bet size: ${bet_size}")
    
    for market_id, group in price_df.groupby('market_id'):
        prices = group.sort_values('timestamp')
        
        if len(prices) < 20:  # Need enough history
            continue
        
        # Get early price (25% into market)
        early_idx = len(prices) // 4
        early_price = prices.iloc[early_idx]['price']
        early_time = prices.iloc[early_idx]['timestamp']
        
        # Get final price (resolution)
        final_price = prices.iloc[-1]['price']
        final_time = prices.iloc[-1]['timestamp']
        
        # Skip unresolved markets
        if 0.1 < final_price < 0.9:
            continue
        
        # Entry logic based on calibration findings
        if entry_threshold < early_price < 0.35:
            # Price is 15-35%, market may be overconfident
            # Bet NO (buy NO which is cheap)
            trade = bt.open_trade(market_id, early_time, early_price, "NO", bet_size)
            bt.close_trade(trade, final_time, final_price)
        
        elif 0.65 < early_price < (1 - entry_threshold):
            # Price is 65-85%, inverse signal
            # Bet YES
            trade = bt.open_trade(market_id, early_time, early_price, "YES", bet_size)
            bt.close_trade(trade, final_time, final_price)
    
    return bt.get_results()


def strategy_momentum(price_df, bt: Backtest, lookback=5, bet_size=100):
    """
    Momentum Strategy
    
    If price has been trending up, bet YES (expect continuation).
    If price has been trending down, bet NO.
    """
    print("\n📈 STRATEGY: Momentum")
    print(f"   Lookback periods: {lookback}")
    print(f"   Bet size: ${bet_size}")
    
    for market_id, group in price_df.groupby('market_id'):
        prices = group.sort_values('timestamp')
        
        if len(prices) < lookback + 10:
            continue
        
        # Calculate momentum at midpoint
        mid_idx = len(prices) // 2
        
        if mid_idx < lookback:
            continue
        
        current_price = prices.iloc[mid_idx]['price']
        past_price = prices.iloc[mid_idx - lookback]['price']
        momentum = current_price - past_price
        
        entry_time = prices.iloc[mid_idx]['timestamp']
        final_price = prices.iloc[-1]['price']
        final_time = prices.iloc[-1]['timestamp']
        
        # Skip unresolved
        if 0.1 < final_price < 0.9:
            continue
        
        # Entry logic
        if momentum > 0.05:  # Upward momentum
            trade = bt.open_trade(market_id, entry_time, current_price, "YES", bet_size)
            bt.close_trade(trade, final_time, final_price)
        elif momentum < -0.05:  # Downward momentum
            trade = bt.open_trade(market_id, entry_time, current_price, "NO", bet_size)
            bt.close_trade(trade, final_time, final_price)
    
    return bt.get_results()


def main():
    print("=" * 60)
    print("POLYMARKET BACKTEST")
    print("=" * 60)
    
    price_df = load_price_data()
    
    if price_df.empty:
        print("No data. Run data_collector.py first.")
        return
    
    print(f"\nLoaded {len(price_df)} price points from {price_df['market_id'].nunique()} markets")
    
    # Test calibration strategy
    bt1 = Backtest(initial_capital=10000)
    results1 = strategy_calibration_arb(price_df, bt1)
    
    print("\n📊 Results:")
    for k, v in results1.items():
        if isinstance(v, float):
            print(f"   {k}: {v:.2f}")
        else:
            print(f"   {k}: {v}")
    
    # Test momentum strategy
    bt2 = Backtest(initial_capital=10000)
    results2 = strategy_momentum(price_df, bt2)
    
    print("\n📊 Results:")
    for k, v in results2.items():
        if isinstance(v, float):
            print(f"   {k}: {v:.2f}")
        else:
            print(f"   {k}: {v}")
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"""
Calibration Strategy: {results1.get('return_pct', 0):.1f}% return, {results1.get('total_trades', 0)} trades
Momentum Strategy: {results2.get('return_pct', 0):.1f}% return, {results2.get('total_trades', 0)} trades

⚠️ NOTE: These are preliminary results with limited data.
Need 1000+ resolved markets for statistical significance.
    """)


if __name__ == "__main__":
    main()
