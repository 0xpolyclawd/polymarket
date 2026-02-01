# Polyclawd - Polymarket Trading Infrastructure

Systematic trading strategies for Polymarket prediction markets.

## Structure

```
polymarket/
├── data/           # Market data, historical prices, resolved markets
├── strategies/     # Trading strategy implementations
├── backtests/      # Backtesting framework and results
├── research/       # Analysis notebooks, market research
├── scripts/        # Utility scripts, data collection
└── notebooks/      # Jupyter notebooks for exploration
```

## Setup

```bash
cd polymarket
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # Then fill in API credentials
```

## API Access

Polymarket uses a CLOB (Central Limit Order Book) for trading:
- **CLOB API**: For placing/cancelling orders, getting orderbook
- **Gamma API**: For market metadata, historical data
- **The Graph**: For on-chain data (positions, trades)

## Philosophy

No vibes. Only backtested edge.
