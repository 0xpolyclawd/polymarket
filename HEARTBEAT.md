# HEARTBEAT.md - Periodic Research Tasks

## Every Heartbeat
Check one item, rotate through:

### Data Health
- [ ] Verify collectors running: `ps aux | grep -E "(websocket|orderbook)" | grep -v grep`
- [ ] Check DB growth: orderbook_snapshots, price_changes row counts
- [ ] Review any collector errors in logs

### Research Queue (pick one per heartbeat)
1. Run calibration backtest with slippage on resolved markets
2. Analyze momentum signals in high-volume markets
3. Check for category biases (politics vs crypto vs sports)
4. Look for multi-outcome arbitrage opportunities
5. Analyze time-of-day patterns in trading

### If Web Search Available
- Research prediction market academic papers
- Find news/event APIs for alpha signals
- Check competitor strategies (Metaculus, Manifold)

## Weekly Summary (Sunday)
Compile findings and send to Telegram:
- Strategies tested
- Best performing hypotheses
- Data gaps identified
- Infrastructure improvements

## Current Blocker
⚠️ **Brave API key needed** - Ask Marco to run:
```
clawdbot configure --section web
```
