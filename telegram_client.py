import logging
from typing import Optional, Dict, List, Any
import aiohttp

class TelegramClient:
    def __init__(self, token: str, chat_id: int):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.offset = 0
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def send_message(self, text: str, chat_id: Optional[int] = None) -> bool:
        if chat_id is None:
            chat_id = self.chat_id
        try:
            async with self.session.post(
                f"{self.base_url}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=20)
            ) as response:
                return response.status == 200
        except Exception as e:
            logging.error(f"Telegram send error: {e}")
            return False
    
    async def send_keyboard(self, chat_id: int):
        keyboard = {
            "keyboard": [
                ["📈 0.2%", "📈 5%", "📈 10%"],
                ["📈 15%", "📈 20%"],
                ["⏱ 5 мин", "⏱ 1 час"],
                ["⏱ 4 часа", "⏱ 1 день"],
                ["📊 Статистика"],
                ["/status"]
            ],
            "resize_keyboard": True
        }
        await self.send_message("Выберите настройки:", chat_id)
    
    async def get_updates(self) -> List[Dict[str, Any]]:
        try:
            async with self.session.get(
                f"{self.base_url}/getUpdates",
                params={"timeout": 30, "offset": self.offset},
                timeout=aiohttp.ClientTimeout(total=35)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("ok"):
                        updates = data.get("result", [])
                        if updates:
                            self.offset = updates[-1]["update_id"] + 1
                        return updates
                return []
        except Exception as e:
            logging.error(f"Telegram get_updates error: {e}")
            return []
