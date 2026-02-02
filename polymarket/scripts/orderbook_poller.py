#!/usr/bin/env python3
"""
Periodic orderbook snapshot collector via REST API.
Complements websocket collector by capturing full book depth.
"""

import asyncio
import aiohttp
import json
import psycopg2
from datetime import datetime
import time
import signal
import sys
import requests

CLOB_API = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

DB_CONFIG = {
    'dbname': 'polymarket',
    'host': '/tmp',
}

POLL_INTERVAL = 60  # seconds between snapshots per token
BATCH_SIZE = 20     # tokens per batch
BATCH_DELAY = 2     # seconds between batches


class OrderbookPoller:
    def __init__(self):
        self.conn = None
        self.running = True
        self.stats = {'snapshots': 0, 'errors': 0, 'start_time': time.time()}
        
    def setup_db(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Database connected")
        
    def get_top_markets(self, limit=100):
        """Get top markets by volume, filtering for mid-range prices"""
        try:
            resp = requests.get(
                f"{GAMMA_API}/markets",
                params={'closed': 'false', 'limit': 500, 'order': 'volume24hr', 'ascending': 'false'},
                timeout=30
            )
            resp.raise_for_status()
            markets = resp.json()
            
            tokens = []
            for m in markets:
                if len(tokens) >= limit:
                    break
                    
                # Filter for mid-range prices (more meaningful spreads)
                try:
                    best_bid = float(m.get('bestBid', 0) or 0)
                    best_ask = float(m.get('bestAsk', 1) or 1)
                    mid_price = (best_bid + best_ask) / 2
                    
                    # Skip extreme prices (< 10% or > 90%)
                    if mid_price < 0.10 or mid_price > 0.90:
                        continue
                except:
                    pass
                
                token_ids = m.get('clobTokenIds')
                if token_ids:
                    try:
                        ids = json.loads(token_ids)
                        if ids:
                            tokens.append({
                                'token': ids[0], 
                                'slug': m.get('slug', 'unknown'),
                                'question': m.get('question', '')[:50]
                            })
                    except:
                        pass
            return tokens
        except Exception as e:
            print(f"Error fetching markets: {e}")
            return []
    
    async def fetch_orderbook(self, session, token_id):
        """Fetch orderbook for a single token"""
        try:
            url = f"{CLOB_API}/book"
            params = {'token_id': token_id}
            
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    return None
        except Exception as e:
            return None
    
    def store_snapshot(self, token_id, data):
        """Store orderbook snapshot"""
        try:
            bids = data.get('bids', [])
            asks = data.get('asks', [])
            
            bid_depth = sum(float(b.get('size', 0)) * float(b.get('price', 0)) for b in bids)
            ask_depth = sum(float(a.get('size', 0)) * float(a.get('price', 0)) for a in asks)
            
            best_bid = float(bids[0]['price']) if bids else None
            best_ask = float(asks[0]['price']) if asks else None
            spread = best_ask - best_bid if (best_bid and best_ask) else None
            
            timestamp = int(time.time() * 1000)
            
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO orderbook_snapshots 
                    (token_id, timestamp, bids, asks, best_bid, best_ask, spread, bid_depth, ask_depth)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    token_id, timestamp, 
                    json.dumps(bids), json.dumps(asks),
                    best_bid, best_ask, spread,
                    bid_depth, ask_depth
                ))
            self.conn.commit()
            self.stats['snapshots'] += 1
            return True
        except Exception as e:
            print(f"Error storing: {e}")
            self.conn.rollback()
            self.stats['errors'] += 1
            return False
    
    def print_stats(self):
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['snapshots'] / (elapsed / 3600) if elapsed > 0 else 0
        print(f"\n📊 Stats ({elapsed/60:.1f} min): {self.stats['snapshots']} snapshots, {self.stats['errors']} errors, {rate:.0f}/hr")
    
    async def run(self):
        self.setup_db()
        
        print("🔍 Fetching top markets by volume...")
        tokens = self.get_top_markets(limit=100)
        print(f"   Monitoring {len(tokens)} markets")
        
        if not tokens:
            print("❌ No tokens found!")
            return
        
        print(f"\n🔄 Starting polling loop (every {POLL_INTERVAL}s)...")
        
        async with aiohttp.ClientSession() as session:
            while self.running:
                cycle_start = time.time()
                
                # Process in batches
                for i in range(0, len(tokens), BATCH_SIZE):
                    if not self.running:
                        break
                        
                    batch = tokens[i:i+BATCH_SIZE]
                    tasks = [self.fetch_orderbook(session, t['token']) for t in batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for t, result in zip(batch, results):
                        if result and not isinstance(result, Exception):
                            self.store_snapshot(t['token'], result)
                    
                    await asyncio.sleep(BATCH_DELAY)
                
                self.print_stats()
                
                # Wait for next cycle
                elapsed = time.time() - cycle_start
                wait_time = max(0, POLL_INTERVAL - elapsed)
                if wait_time > 0 and self.running:
                    await asyncio.sleep(wait_time)
    
    def stop(self):
        print("\n🛑 Stopping poller...")
        self.running = False
        self.print_stats()
        if self.conn:
            self.conn.close()


def main():
    poller = OrderbookPoller()
    
    def signal_handler(sig, frame):
        poller.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("=" * 60)
    print("Polymarket Orderbook Poller")
    print("=" * 60)
    print("Polling REST API for full orderbook depth")
    print("Press Ctrl+C to stop")
    print("=" * 60)
    
    asyncio.run(poller.run())


if __name__ == "__main__":
    main()
