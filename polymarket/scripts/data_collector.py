#!/usr/bin/env python3
"""
Polymarket Data Collector
Collects market data and price history for backtesting.
"""

import requests
import json
import time
import sqlite3
from datetime import datetime
from pathlib import Path

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

DB_PATH = Path(__file__).parent.parent / "data" / "polymarket.db"


def init_db():
    """Initialize SQLite database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Markets table
    c.execute('''
        CREATE TABLE IF NOT EXISTS markets (
            id TEXT PRIMARY KEY,
            question TEXT,
            condition_id TEXT,
            slug TEXT,
            category TEXT,
            end_date TEXT,
            resolution_source TEXT,
            outcomes TEXT,  -- JSON array
            outcome_prices TEXT,  -- JSON array
            volume REAL,
            liquidity REAL,
            active INTEGER,
            closed INTEGER,
            clob_token_ids TEXT,  -- JSON array
            created_at TEXT,
            updated_at TEXT,
            fetched_at TEXT
        )
    ''')
    
    # Price history table
    c.execute('''
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id TEXT,
            token_id TEXT,
            timestamp INTEGER,
            price REAL,
            UNIQUE(market_id, token_id, timestamp)
        )
    ''')
    
    # Events table
    c.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            title TEXT,
            slug TEXT,
            description TEXT,
            category TEXT,
            volume REAL,
            liquidity REAL,
            active INTEGER,
            closed INTEGER,
            market_ids TEXT,  -- JSON array
            fetched_at TEXT
        )
    ''')
    
    conn.commit()
    return conn


def fetch_markets(limit=100, offset=0, closed=None, active=None):
    """Fetch markets from Gamma API."""
    params = {
        "limit": limit,
        "offset": offset,
        "order": "volume",
        "ascending": "false"
    }
    if closed is not None:
        params["closed"] = str(closed).lower()
    if active is not None:
        params["active"] = str(active).lower()
    
    resp = requests.get(f"{GAMMA_API}/markets", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_price_history(token_id, interval="max"):
    """Fetch price history from CLOB API."""
    try:
        resp = requests.get(
            f"{CLOB_API}/prices-history",
            params={"market": token_id, "interval": interval},
            timeout=30
        )
        if resp.ok:
            return resp.json().get("history", [])
    except Exception as e:
        print(f"  Error fetching price history: {e}")
    return []


def save_market(conn, market):
    """Save market to database."""
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO markets 
        (id, question, condition_id, slug, category, end_date, resolution_source,
         outcomes, outcome_prices, volume, liquidity, active, closed, 
         clob_token_ids, created_at, updated_at, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        market.get('id'),
        market.get('question'),
        market.get('conditionId'),
        market.get('slug'),
        market.get('category'),
        market.get('endDate'),
        market.get('resolutionSource'),
        market.get('outcomes'),
        market.get('outcomePrices'),
        float(market.get('volume', 0) or 0),
        float(market.get('liquidity', 0) or 0),
        1 if market.get('active') else 0,
        1 if market.get('closed') else 0,
        market.get('clobTokenIds'),
        market.get('createdAt'),
        market.get('updatedAt'),
        datetime.utcnow().isoformat()
    ))
    conn.commit()


def save_price_history(conn, market_id, token_id, history):
    """Save price history to database."""
    if not history:
        return
    
    c = conn.cursor()
    for point in history:
        try:
            c.execute('''
                INSERT OR IGNORE INTO price_history 
                (market_id, token_id, timestamp, price)
                VALUES (?, ?, ?, ?)
            ''', (market_id, token_id, point['t'], point['p']))
        except Exception as e:
            pass  # Ignore duplicates
    conn.commit()


def collect_all_markets(conn, include_closed=True, include_active=True, max_markets=None):
    """Collect all markets and their price history."""
    print("=" * 60)
    print(f"Starting data collection - {datetime.now().isoformat()}")
    print("=" * 60)
    
    total_collected = 0
    offset = 0
    limit = 100
    
    # Collect closed markets (for backtesting)
    if include_closed:
        print("\n📊 Fetching CLOSED markets (for backtesting)...")
        while True:
            markets = fetch_markets(limit=limit, offset=offset, closed=True)
            if not markets:
                break
            
            for market in markets:
                if max_markets and total_collected >= max_markets:
                    break
                
                save_market(conn, market)
                
                # Fetch price history for each token
                token_ids_str = market.get('clobTokenIds', '[]')
                try:
                    token_ids = json.loads(token_ids_str) if isinstance(token_ids_str, str) else token_ids_str
                except:
                    token_ids = []
                
                for token_id in token_ids[:1]:  # Just first token (YES)
                    history = fetch_price_history(token_id)
                    save_price_history(conn, market.get('id'), token_id, history)
                    
                total_collected += 1
                if total_collected % 10 == 0:
                    print(f"  Collected {total_collected} markets...")
                
                time.sleep(0.1)  # Rate limiting
            
            if max_markets and total_collected >= max_markets:
                break
            offset += limit
    
    # Collect active markets
    if include_active:
        print("\n📈 Fetching ACTIVE markets...")
        offset = 0
        while True:
            markets = fetch_markets(limit=limit, offset=offset, active=True)
            if not markets:
                break
            
            for market in markets:
                if max_markets and total_collected >= max_markets:
                    break
                    
                save_market(conn, market)
                
                token_ids_str = market.get('clobTokenIds', '[]')
                try:
                    token_ids = json.loads(token_ids_str) if isinstance(token_ids_str, str) else token_ids_str
                except:
                    token_ids = []
                
                for token_id in token_ids[:1]:
                    history = fetch_price_history(token_id)
                    save_price_history(conn, market.get('id'), token_id, history)
                
                total_collected += 1
                if total_collected % 10 == 0:
                    print(f"  Collected {total_collected} markets...")
                
                time.sleep(0.1)
            
            if max_markets and total_collected >= max_markets:
                break
            offset += limit
    
    print(f"\n✅ Collection complete! Total markets: {total_collected}")
    return total_collected


def get_stats(conn):
    """Get database statistics."""
    c = conn.cursor()
    
    c.execute("SELECT COUNT(*) FROM markets")
    total_markets = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM markets WHERE closed = 1")
    closed_markets = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM markets WHERE active = 1")
    active_markets = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM price_history")
    price_points = c.fetchone()[0]
    
    c.execute("SELECT SUM(volume) FROM markets")
    total_volume = c.fetchone()[0] or 0
    
    return {
        "total_markets": total_markets,
        "closed_markets": closed_markets,
        "active_markets": active_markets,
        "price_history_points": price_points,
        "total_volume": total_volume
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Collect Polymarket data")
    parser.add_argument("--max", type=int, default=100, help="Max markets to collect")
    parser.add_argument("--stats", action="store_true", help="Show stats only")
    args = parser.parse_args()
    
    conn = init_db()
    
    if args.stats:
        stats = get_stats(conn)
        print(f"\n📊 Database Statistics:")
        print(f"   Total markets: {stats['total_markets']}")
        print(f"   Closed markets: {stats['closed_markets']}")
        print(f"   Active markets: {stats['active_markets']}")
        print(f"   Price history points: {stats['price_history_points']}")
        print(f"   Total volume: ${stats['total_volume']:,.0f}")
    else:
        collect_all_markets(conn, max_markets=args.max)
        stats = get_stats(conn)
        print(f"\n📊 Final Statistics:")
        print(f"   Total markets: {stats['total_markets']}")
        print(f"   Price history points: {stats['price_history_points']}")
    
    conn.close()
