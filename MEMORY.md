# MEMORY.md - Polyclawd Long-Term Memory

## Identity
- **Name:** Polyclawd 🔮
- **Role:** Autonomous systematic trading researcher for Polymarket
- **Human:** Marco Antonio (@marcoantonioribeiro) - crypto trader, EST timezone
- **Philosophy:** No vibes, only backtested edge. Systematic > discretionary.

## Mission
Find provably profitable alpha in Polymarket prediction markets through:
1. Rigorous data analysis
2. Strategy backtesting with realistic slippage
3. Independent research and iteration
4. Only request wallet access once alpha is proven

## Infrastructure Built

### Data Pipeline
- **PostgreSQL 16** running on Mac Studio
- **Database:** `polymarket` (27 GB)
  - `processed_trades`: 120M historical trades ($15.8B volume)
  - `markets_full`: 120K markets (104K resolved)
  - `orderbook_snapshots`: Live depth collection
  - `price_changes`: Real-time price deltas
  - `realtime_trades`: Live trade feed

### Live Collectors (launchd daemons, auto-restart)
- `websocket_collector.py` - Real-time price changes (~154 MB/day)
- `orderbook_poller.py` - Full orderbook snapshots (~36 MB/day)

### Analysis Scripts
| Script | Purpose |
|--------|---------|
| `data_collector.py` | Historical data from Gamma/CLOB APIs |
| `extract_trades.py` | Process raw trade data |
| `fetch_markets.py` | Market metadata sync |
| `calibration_analysis.py` | Market calibration study |
| `backtest.py` | Backtesting framework |
| `slippage_model_v2.py` | Liquidity/slippage estimation |

### Slippage Model v1
```
slippage = 0.50% base + impact × √(order_size / $500)
```
Conservative estimates:
- $100 trade → ~0.5% slippage
- $1K trade → ~0.7% slippage
- $10K trade → ~1.0% slippage

## Accounts
- **Gmail:** 0xpolyclawd@gmail.com
- **GitHub:** 0xpolyclawd (repo: polymarket)

## Research Agenda

### Active Hypotheses
1. **Calibration Arbitrage** - Bet against miscalibrated extreme prices
2. **Momentum** - Follow trends in high-volume markets
3. **Mean Reversion** - Fade mispricings in thin markets
4. **Category Bias** - Different biases by topic (politics/sports/crypto)
5. **Multi-outcome Arbitrage** - Related market price inconsistencies
6. **News Alpha** - Price reaction to external events (needs web search)

### Backtest Queue
- [ ] Calibration strategy with slippage
- [ ] Momentum on >$100K daily volume markets
- [ ] Mean reversion on 20-80% price range
- [ ] Category-specific calibration

### Data Gaps
- [ ] Web search API (Brave key needed) - BLOCKER for news alpha
- [ ] More orderbook data (collecting now, need 24h+)
- [ ] News/event API integration

## Learnings

### Polymarket Mechanics
- Binary YES/NO outcomes, prices = probabilities
- 0 bps fees (maker AND taker)
- $1.00 USDC payout for correct outcome
- Settlement via UMA oracle, Polygon chain
- CLOB API for trading, Gamma API for metadata

### Key Findings (Preliminary)
- Mean calibration error: ~10.9% (small sample)
- Most trades don't move price (median impact = 0)
- Average impact: 0.3-0.6% depending on size
- High liquidity markets: ~3K with >$100K daily volume
- Extreme price markets (>90% or <10%) have huge spreads

## Blockers
1. **Brave API key** - Need for web research
2. **Wallet** - Not needed until alpha proven

## Communication
- Telegram group: polyclawd
- Update humans on significant findings
- Don't spam with routine progress
