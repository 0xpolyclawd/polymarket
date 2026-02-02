#!/usr/bin/env python3
"""
Fast trade extraction from Polymarket Goldsky subgraph.
Extracts all OrderFilledEvent records and stores in PostgreSQL.
"""

import requests
import psycopg2
from psycopg2.extras import execute_values
import time
import sys
from datetime import datetime

# Goldsky endpoints
ORDERBOOK_ENDPOINT = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/prod/gn"
ACTIVITY_ENDPOINT = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/activity-subgraph/0.0.4/gn"

# PostgreSQL connection
DB_CONFIG = {
    'dbname': 'polymarket',
    'host': '/tmp',  # Unix socket
}

BATCH_SIZE = 1000  # Max allowed by subgraph


def query_subgraph(endpoint: str, query: str) -> dict:
    """Execute GraphQL query against subgraph"""
    for attempt in range(3):
        try:
            resp = requests.post(
                endpoint,
                json={'query': query},
                headers={'Content-Type': 'application/json'},
                timeout=60
            )
            resp.raise_for_status()
            data = resp.json()
            if 'errors' in data:
                print(f"GraphQL errors: {data['errors']}")
                return None
            return data.get('data')
        except Exception as e:
            print(f"Query attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)
    return None


def get_total_trades() -> int:
    """Get total trade count from global stats"""
    query = '{ ordersMatchedGlobal(id: "") { tradesQuantity } }'
    data = query_subgraph(ORDERBOOK_ENDPOINT, query)
    if data and data.get('ordersMatchedGlobal'):
        return int(data['ordersMatchedGlobal']['tradesQuantity'])
    return 0


def extract_trades_batch(last_id: str = "") -> list:
    """Extract a batch of trades using cursor pagination"""
    where_clause = f'where: {{ id_gt: "{last_id}" }}' if last_id else ""
    
    query = f"""{{
        orderFilledEvents(
            first: {BATCH_SIZE}
            orderBy: id
            orderDirection: asc
            {where_clause}
        ) {{
            id
            transactionHash
            timestamp
            maker
            taker
            makerAssetId
            takerAssetId
            makerAmountFilled
            takerAmountFilled
            fee
        }}
    }}"""
    
    data = query_subgraph(ORDERBOOK_ENDPOINT, query)
    if data:
        return data.get('orderFilledEvents', [])
    return []


def insert_trades(conn, trades: list):
    """Bulk insert trades into PostgreSQL"""
    if not trades:
        return 0
        
    values = [
        (
            t['id'],
            t['transactionHash'],
            int(t['timestamp']),
            t['maker'],
            t['taker'],
            t['makerAssetId'],
            t['takerAssetId'],
            int(t['makerAmountFilled']),
            int(t['takerAmountFilled']),
            int(t['fee'])
        )
        for t in trades
    ]
    
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO trades (id, tx_hash, timestamp, maker, taker, 
               maker_asset_id, taker_asset_id, maker_amount, taker_amount, fee)
               VALUES %s
               ON CONFLICT (id) DO NOTHING""",
            values,
            page_size=1000
        )
    conn.commit()
    return len(values)


def get_last_trade_id(conn) -> str:
    """Get the last trade ID we've stored (for resuming)"""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(id) FROM trades")
        result = cur.fetchone()[0]
        return result or ""


def get_trade_count(conn) -> int:
    """Get current trade count in DB"""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM trades")
        return cur.fetchone()[0]


def main():
    print("=" * 60)
    print("Polymarket Trade Extraction Pipeline")
    print("=" * 60)
    
    # Connect to PostgreSQL
    print("\nConnecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Get current state
    db_count = get_trade_count(conn)
    total_trades = get_total_trades()
    
    print(f"Total trades in subgraph: {total_trades:,}")
    print(f"Trades already in DB: {db_count:,}")
    print(f"Trades to fetch: {total_trades - db_count:,}")
    
    if db_count >= total_trades:
        print("\nDatabase is up to date!")
        conn.close()
        return
    
    # Resume from last ID
    last_id = get_last_trade_id(conn)
    if last_id:
        print(f"\nResuming from ID: {last_id[:50]}...")
    else:
        print("\nStarting fresh extraction...")
    
    # Extract in batches
    start_time = time.time()
    batch_num = 0
    total_inserted = 0
    
    while True:
        batch_num += 1
        trades = extract_trades_batch(last_id)
        
        if not trades:
            print("\nNo more trades to fetch!")
            break
            
        inserted = insert_trades(conn, trades)
        total_inserted += inserted
        last_id = trades[-1]['id']
        
        # Progress update
        elapsed = time.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0
        
        current_count = db_count + total_inserted
        pct = (current_count / total_trades) * 100 if total_trades > 0 else 0
        
        eta_seconds = (total_trades - current_count) / rate if rate > 0 else 0
        eta_str = f"{eta_seconds/3600:.1f}h" if eta_seconds > 3600 else f"{eta_seconds/60:.1f}m"
        
        if batch_num % 10 == 0:
            print(f"Batch {batch_num}: {current_count:,}/{total_trades:,} ({pct:.1f}%) | "
                  f"Rate: {rate:.0f}/s | ETA: {eta_str}")
        
        # Rate limiting
        time.sleep(0.1)
    
    # Final stats
    elapsed = time.time() - start_time
    final_count = get_trade_count(conn)
    
    print("\n" + "=" * 60)
    print("Extraction Complete!")
    print(f"Total trades in DB: {final_count:,}")
    print(f"Inserted this run: {total_inserted:,}")
    print(f"Time elapsed: {elapsed/60:.1f} minutes")
    print(f"Average rate: {total_inserted/elapsed:.0f} trades/second")
    print("=" * 60)
    
    conn.close()


if __name__ == "__main__":
    main()
