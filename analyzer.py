import logging
from typing import Dict, List, Optional
from collections import defaultdict, deque
import pandas as pd
from ta.momentum import RSIIndicator
import time

class Analyzer:
    def __init__(self, max_history: int = 1000):
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_history))
    
    def add_price(self, symbol: str, price: float, timestamp: float):
        self.price_history[symbol].append((timestamp, price))
    
    def calculate_rsi(self, prices: List[float], window: int = 5) -> Optional[float]:
        try:
            if len(prices) < window + 1:
                return None
            rsi = RSIIndicator(close=pd.Series(prices), window=window).rsi().iloc[-1]
            return round(float(rsi), 2) if not pd.isna(rsi) else None
        except:
            return None
    
    def analyze_symbol(self, symbol: str, current_price: float, current_time: float,
                      window: int, percent: float, cooldown: int, last_alert: Dict) -> Optional[Dict]:
        if current_price <= 0 or symbol not in self.price_history:
            return None
        
        if symbol in last_alert and current_time - last_alert[symbol] < cooldown:
            return None
        
        recent = [p for t, p in self.price_history[symbol] if current_time - t <= window]
        if len(recent) < 2:
            return None
        
        growth = ((current_price - recent[0]) / recent[0]) * 100
        if abs(growth) < percent:
            return None
        
        prices = [p for t, p in self.price_history[symbol][-100:]]
        rsi = self.calculate_rsi(prices)
        
        return {"symbol": symbol, "price": current_price, "growth": growth, "rsi": rsi}
