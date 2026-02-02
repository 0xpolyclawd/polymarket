#!/usr/bin/env python3
"""
Real-time WebSocket collector for Polymarket order book data.
Captures order book snapshots, price changes, and trades.
"""

import asyncio
import websockets
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import time
import signal
import sys
import requests

# WebSocket endpoint
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# PostgreSQL connection
DB_CONFIG = {
    'dbname': 'polymarket',
    'host': '/tmp',
}

# Gamma API for market discovery
GAMMA_API = "https://gamma-api.polymarket.com"


class WebSocketCollector:
    def __init__(self):
        self.conn = None
        self.running = True
        self.subscribed_tokens = set()
        self.stats = {
            'books': 0,
            'price_changes': 0,
            'trades': 0,
            'start_time': time.time()
        }
        
    def setup_db(self):
        """Create tables for real-time data"""
        self.conn = psycopg2.connect(**DB_CONFIG)
        
        with self.conn.cursor() as cur:
            # Order book snapshots
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                    id SERIAL PRIMARY KEY,
                    token_id TEXT,
                    timestamp BIGINT,
                    captured_at TIMESTAMP DEFAULT NOW(),
                    bids JSONB,
                    asks JSONB,
                    best_bid NUMERIC,
                    best_ask NUMERIC,
                    spread NUMERIC,
                    bid_depth NUMERIC,
                    ask_depth NUMERIC
                );
                
                CREATE INDEX IF NOT EXISTS idx_ob_snap_token_ts 
                ON orderbook_snapshots(token_id, timestamp);
            """)
            
            # Real-time trades from WebSocket
            cur.execute("""
                CREATE TABLE IF NOT EXISTS realtime_trades (
                    id SERIAL PRIMARY KEY,
                    token_id TEXT,
                    timestamp BIGINT,
                    price NUMERIC,
                    size NUMERIC,
                    side TEXT,
                    fee_rate_bps TEXT
                );
                
                CREATE INDEX IF NOT EXISTS idx_rt_trades_token_ts 
                ON realtime_trades(token_id, timestamp);
            """)
            
            # Price changes (order additions/cancellations)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS price_changes (
                    id SERIAL PRIMARY KEY,
                    token_id TEXT,
                    timestamp BIGINT,
                    price NUMERIC,
                    size NUMERIC,
                    side TEXT,
                    best_bid NUMERIC,
                    best_ask NUMERIC
                );
                
                CREATE INDEX IF NOT EXISTS idx_pc_token_ts 
                ON price_changes(token_id, timestamp);
            """)
            
        self.conn.commit()
        print("✅ Database tables ready")
        
    def get_active_markets(self, limit=100):
        """Fetch active markets from Gamma API"""
        try:
            resp = requests.get(
                f"{GAMMA_API}/markets",
                params={'closed': 'false', 'limit': limit},
                timeout=30
            )
            resp.raise_for_status()
            markets = resp.json()
            
            tokens = []
            for m in markets:
                token_ids = m.get('clobTokenIds')
                if token_ids:
                    try:
                        ids = json.loads(token_ids)
                        tokens.extend(ids)
                    except:
                        pass
            return tokens
        except Exception as e:
            print(f"Error fetching markets: {e}")
            return []
    
    def store_book_snapshot(self, data):
        """Store order book snapshot"""
        try:
            token_id = data.get('asset_id')
            timestamp = int(data.get('timestamp', 0))
            bids = data.get('bids', [])
            asks = data.get('asks', [])
            
            # Calculate depth
            bid_depth = sum(float(b.get('size', 0)) * float(b.get('price', 0)) for b in bids)
            ask_depth = sum(float(a.get('size', 0)) * float(a.get('price', 0)) for a in asks)
            
            best_bid = float(bids[0]['price']) if bids else None
            best_ask = float(asks[0]['price']) if asks else None
            spread = best_ask - best_bid if (best_bid and best_ask) else None
            
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
            self.stats['books'] += 1
        except Exception as e:
            print(f"Error storing book: {e}")
            self.conn.rollback()
    
    def store_trade(self, data):
        """Store real-time trade"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO realtime_trades 
                    (token_id, timestamp, price, size, side, fee_rate_bps)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    data.get('asset_id'),
                    int(data.get('timestamp', 0)),
                    float(data.get('price', 0)),
                    float(data.get('size', 0)),
                    data.get('side'),
                    data.get('fee_rate_bps')
                ))
            self.conn.commit()
            self.stats['trades'] += 1
        except Exception as e:
            print(f"Error storing trade: {e}")
            self.conn.rollback()
    
    def store_price_change(self, data):
        """Store price change event"""
        try:
            changes = data.get('price_changes', [])
            timestamp = int(data.get('timestamp', 0))
            
            values = []
            for c in changes:
                values.append((
                    c.get('asset_id'),
                    timestamp,
                    float(c.get('price', 0)),
                    float(c.get('size', 0)),
                    c.get('side'),
                    float(c.get('best_bid', 0)) if c.get('best_bid') else None,
                    float(c.get('best_ask', 0)) if c.get('best_ask') else None
                ))
            
            if values:
                with self.conn.cursor() as cur:
                    execute_values(
                        cur,
                        """INSERT INTO price_changes 
                           (token_id, timestamp, price, size, side, best_bid, best_ask)
                           VALUES %s""",
                        values
                    )
                self.conn.commit()
                self.stats['price_changes'] += len(values)
        except Exception as e:
            print(f"Error storing price change: {e}")
            self.conn.rollback()
    
    async def handle_message(self, message):
        """Process incoming WebSocket message"""
        try:
            data = json.loads(message)
            
            # Handle both single messages and arrays of messages
            if isinstance(data, list):
                for item in data:
                    await self._process_event(item)
            else:
                await self._process_event(data)
                
        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"Error handling message: {e}")
    
    async def _process_event(self, data):
        """Process a single event"""
        if not isinstance(data, dict):
            return
            
        event_type = data.get('event_type')
        
        if event_type == 'book':
            self.store_book_snapshot(data)
        elif event_type == 'last_trade_price':
            self.store_trade(data)
        elif event_type == 'price_change':
            self.store_price_change(data)
    
    async def subscribe(self, ws, token_ids):
        """Subscribe to market channels"""
        # Subscribe in batches to avoid overwhelming
        batch_size = 50
        for i in range(0, len(token_ids), batch_size):
            batch = token_ids[i:i+batch_size]
            sub_msg = {
                "type": "market",
                "assets_ids": batch
            }
            await ws.send(json.dumps(sub_msg))
            self.subscribed_tokens.update(batch)
            await asyncio.sleep(0.5)
        
        print(f"✅ Subscribed to {len(self.subscribed_tokens)} tokens")
    
    def print_stats(self):
        """Print collection statistics"""
        elapsed = time.time() - self.stats['start_time']
        rate = (self.stats['books'] + self.stats['trades'] + self.stats['price_changes']) / elapsed
        
        print(f"\n📊 Collection Stats (running {elapsed/60:.1f} min):")
        print(f"   Books: {self.stats['books']:,}")
        print(f"   Trades: {self.stats['trades']:,}")
        print(f"   Price changes: {self.stats['price_changes']:,}")
        print(f"   Rate: {rate:.1f} events/sec")
        print(f"   Tokens: {len(self.subscribed_tokens)}")
    
    async def run(self):
        """Main collection loop"""
        self.setup_db()
        
        # Get active market tokens
        print("\n🔍 Fetching active markets...")
        tokens = self.get_active_markets(limit=200)
        print(f"   Found {len(tokens)} tokens to monitor")
        
        if not tokens:
            print("❌ No tokens found!")
            return
        
        # Connect and subscribe
        print(f"\n🔌 Connecting to {WS_URL}...")
        
        reconnect_delay = 1
        while self.running:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5
                ) as ws:
                    print("✅ Connected!")
                    reconnect_delay = 1
                    
                    await self.subscribe(ws, tokens)
                    
                    last_stats = time.time()
                    
                    while self.running:
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=60)
                            await self.handle_message(message)
                            
                            # Print stats every 60 seconds
                            if time.time() - last_stats > 60:
                                self.print_stats()
                                last_stats = time.time()
                                
                        except asyncio.TimeoutError:
                            # Send ping to keep alive
                            await ws.ping()
                            
            except websockets.exceptions.ConnectionClosed as e:
                print(f"\n⚠️ Connection closed: {e}")
            except Exception as e:
                print(f"\n❌ Error: {e}")
            
            if self.running:
                print(f"🔄 Reconnecting in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)
    
    def stop(self):
        """Stop the collector"""
        print("\n🛑 Stopping collector...")
        self.running = False
        self.print_stats()
        if self.conn:
            self.conn.close()


def main():
    collector = WebSocketCollector()
    
    # Handle shutdown signals
    def signal_handler(sig, frame):
        collector.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("=" * 60)
    print("Polymarket WebSocket Collector")
    print("=" * 60)
    print("Collecting: Order books, trades, price changes")
    print("Press Ctrl+C to stop")
    print("=" * 60)
    
    asyncio.run(collector.run())


if __name__ == "__main__":
    main()
