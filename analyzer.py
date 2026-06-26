import logging
from typing import Dict, List, Optional
from collections import defaultdict, deque
import pandas as pd
from ta.momentum import RSIIndicator
import time

logger = logging.getLogger(__name__)

class Analyzer:
    def __init__(self, max_history: int = 1000):
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_history))
    
    def add_price(self, symbol: str, price: float, timestamp: float):
        self.price_history[symbol].append((timestamp, price))
    
    def calculate_rsi(self, prices: List[float], window: int = 5) -> Optional[float]:
        try:
            if len(prices) < window + 1:
                return None
            series = pd.Series(prices)
            rsi = RSIIndicator(close=series, window=window).rsi().iloc[-1]
            return round(float(rsi), 2) if not pd.isna(rsi) else None
        except Exception as e:
            return None
    
    def analyze_symbol(self, symbol: str, current_price: float, current_time: float,
                      window: int, percent: float, cooldown: int, last_alert: Dict) -> Optional[Dict]:
        if current_price <= 0:
            return None
        
        if symbol not in self.price_history:
            self.price_history[symbol] = deque(maxlen=1000)
        
        self.price_history[symbol].append((current_time, current_price))
        
        if symbol in last_alert and current_time - last_alert[symbol] < cooldown:
            return None
        
        recent = [p for t, p in self.price_history[symbol] if current_time - t <= window]
        if len(recent) < 2:
            return None
        
        old_price = recent[0]
        if old_price <= 0:
            return None
        
        growth = ((current_price - old_price) / old_price) * 100
        if abs(growth) < percent:
            return None
        
        prices = [p for t, p in self.price_history[symbol][-100:]]
        rsi = self.calculate_rsi(prices)
        
        return {
            "symbol": symbol,
            "price": current_price,
            "growth": growth,
            "rsi": rsi,
            "timestamp": current_time
        }
