# Initial Research Findings - 2026-02-01

## Data Infrastructure ✅

Built working data collection pipeline:
- `scripts/data_collector.py` - Collects markets and price history
- `scripts/analyze_markets.py` - Analyzes patterns
- SQLite database for storage
- Gamma API for market metadata
- CLOB API for price history

## Key Technical Findings

### API Structure
1. **Gamma API** (`gamma-api.polymarket.com`)
   - Market metadata (question, outcomes, dates)
   - Volume, liquidity stats
   - Tags/categories
   
2. **CLOB API** (`clob.polymarket.com`)
   - `/prices-history` - Historical prices (key for backtesting!)
   - `/book` - Order book (requires auth for trading)
   - `/midpoint` - Current midpoint price

3. **Fees**: Currently 0 bps for maker AND taker (!!)
   - This is unusual - typical exchanges charge 5-50 bps
   - Opportunity: No execution cost friction

### Market Mechanics
- Binary outcomes only (YES/NO or A/B)
- Prices 0.00-1.00 = probability
- $1.00 USDC payout for correct outcome
- Settlement via UMA oracle

## Initial Data Analysis (50 markets sample)

### Structure
- High variance in volume ($100 to $100k)
- Most markets start near 0.5 (uncertain)
- Categories: Politics, Crypto, Sports, World Events

### Price Behavior
- Avg volatility: ~0.10 (prices move 10% on average)
- Resolution: 53% YES, 40% NO (slight YES bias in sample?)
- Most starting prices: 0.4-0.6 range

## Potential Alpha Hypotheses

### 1. Calibration Arbitrage
If markets are miscalibrated (e.g., 70% prices resolve YES 80% of the time),
there's systematic alpha in betting against miscalibrated odds.

**Need:** Large dataset of resolved markets with final prices before resolution.

### 2. Momentum in High-Volume Markets
High-volume markets may incorporate information faster. If price moves are 
sticky (momentum), following recent direction could work.

**Need:** Tick-by-tick data, volume analysis.

### 3. Mean Reversion in Low-Volume Markets
Thin markets may have temporary mispricings that revert.

**Need:** Order book depth analysis, spread data.

### 4. Event Category Specialization
Different categories may have different biases:
- Politics: Overconfidence bias?
- Sports: Sharp money vs retail?
- Crypto: Correlation with underlying?

**Need:** Category-specific analysis.

### 5. Multi-Outcome Arbitrage
Events with multiple markets (e.g., "Who wins election?") may have 
prices that don't sum to 1.00, creating arbitrage.

**Need:** Event-level analysis, position netting.

## Next Steps

1. **Collect more data** - Need 1000+ resolved markets
2. **Build calibration analysis** - Predicted vs actual outcomes
3. **Backtest framework** - Simulate strategy returns
4. **Category deep-dives** - Politics vs Sports vs Crypto
5. **Real-time monitoring** - Detect mispricings as they happen

## Questions for Marco

1. Any categories of particular interest?
2. Time horizon preference (scalping vs positional)?
3. Risk tolerance for backtests?
4. Access to more compute for data collection?
