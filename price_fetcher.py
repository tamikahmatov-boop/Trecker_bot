import logging
from typing import Dict, Set, Tuple
import aiohttp
from bs4 import BeautifulSoup

class PriceFetcher:
    def __init__(self):
        self.session: aiohttp.ClientSession = None
        self.symbols_cache = set()
        self.last_update = 0
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    def normalize(self, sym: str) -> str:
        return sym.upper().replace("-", "").replace("_", "").replace("/", "")
    
    async def get_symbols(self) -> Set[str]:
        import time
        symbols = set()
        try:
            async with self.session.get("https://public.bybit.com/trading/") as resp:
                if resp.status == 200:
                    soup = BeautifulSoup(await resp.text(), "html.parser")
                    for a in soup.find_all("a"):
                        sym = a.text.strip("/")
                        if sym.endswith(("USDT", "PERP")):
                            symbols.add(sym.replace("/", ""))
        except Exception as e:
            logging.error(f"Bybit error: {e}")
        return symbols
    
    async def get_prices(self, symbols: Set[str]) -> Tuple[Dict[str, float], Dict[str, str]]:
        prices = {}
        sources = {}
        normalized = {self.normalize(s): s for s in symbols}
        
        # OKX
        try:
            async with self.session.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data.get("data", []):
                        sym = self.normalize(item["instId"])
                        price = float(item["last"])
                        if price > 0 and sym in normalized:
                            real = normalized[sym]
                            prices[real] = price
                            sources[real] = "OKX"
        except Exception as e:
            logging.warning(f"OKX error: {e}")
        
        # MEXC
        try:
            async with self.session.get("https://contract.mexc.com/api/v1/contract/ticker") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success"):
                        for item in data["data"]:
                            sym = self.normalize(item["symbol"])
                            price = float(item["lastPrice"])
                            if price > 0 and sym in normalized and sym not in prices:
                                real = normalized[sym]
                                prices[real] = price
                                sources[real] = "MEXC"
        except Exception as e:
            logging.warning(f"MEXC error: {e}")
        
        return prices, sources
