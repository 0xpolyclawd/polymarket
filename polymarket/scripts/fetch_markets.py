#!/usr/bin/env python3
"""
Fetch active markets from Polymarket Gamma API
"""

import requests
import json
from datetime import datetime

GAMMA_API = "https://gamma-api.polymarket.com"


def fetch_markets(limit=100, active=True, closed=False):
    """Fetch markets from Gamma API"""
    params = {
        "limit": limit,
        "active": str(active).lower(),
        "closed": str(closed).lower(),
    }
    
    response = requests.get(f"{GAMMA_API}/markets", params=params)
    response.raise_for_status()
    return response.json()


def fetch_events(limit=100, active=True):
    """Fetch events (market groups) from Gamma API"""
    params = {
        "limit": limit,
        "active": str(active).lower(),
    }
    
    response = requests.get(f"{GAMMA_API}/events", params=params)
    response.raise_for_status()
    return response.json()


def main():
    print("=" * 60)
    print(f"Polymarket Data Fetch - {datetime.now().isoformat()}")
    print("=" * 60)
    
    # Fetch active markets
    print("\n📊 Fetching active markets...")
    markets = fetch_markets(limit=20)
    
    print(f"\nFound {len(markets)} markets")
    print("-" * 60)
    
    # Sort by volume
    markets_with_volume = [(m, float(m.get('volume', 0) or 0)) for m in markets]
    markets_with_volume.sort(key=lambda x: x[1], reverse=True)
    
    for market, volume in markets_with_volume[:10]:
        question = market.get('question', 'N/A')[:60]
        prices = market.get('outcomePrices', '[]')
        category = market.get('category', 'Unknown')
        
        try:
            prices_list = json.loads(prices) if isinstance(prices, str) else prices
            yes_price = float(prices_list[0]) if prices_list else 0
            no_price = float(prices_list[1]) if len(prices_list) > 1 else 0
        except:
            yes_price, no_price = 0, 0
            
        print(f"\n📈 {question}...")
        print(f"   Category: {category}")
        print(f"   YES: {yes_price:.1%} | NO: {no_price:.1%}")
        print(f"   Volume: ${volume:,.0f}")
    
    # Fetch events
    print("\n\n📅 Fetching active events...")
    events = fetch_events(limit=10)
    
    print(f"\nFound {len(events)} events")
    print("-" * 60)
    
    for event in events[:5]:
        title = event.get('title', 'N/A')[:50]
        volume = event.get('volume', 0) or 0
        market_count = len(event.get('markets', []))
        
        print(f"\n🎯 {title}")
        print(f"   Markets: {market_count} | Volume: ${volume:,.0f}")


if __name__ == "__main__":
    main()
