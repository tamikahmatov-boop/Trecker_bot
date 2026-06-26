import logging
import asyncio
from typing import Optional, Dict, List, Any
import aiohttp

logger = logging.getLogger(__name__)

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
    
    async def send_message(self, text: str, chat_id: Optional[int] = None, 
                          reply_markup: Optional[Dict] = None) -> bool:
        if chat_id is None:
            chat_id = self.chat_id
        
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        
        if reply_markup:
            payload["reply_markup"] = reply_markup
        
        try:
            async with self.session.post(
                f"{self.base_url}/sendMessage",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as response:
                if response.status == 200:
                    logger.info(f"✅ Сообщение отправлено")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Ошибка: {response.status} - {error_text}")
                    return False
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return False
    
    async def get_updates(self) -> List[Dict[str, Any]]:
        try:
            async with self.session.get(
                f"{self.base_url}/getUpdates",
                params={
                    "timeout": 30,
                    "offset": self.offset,
                    "allowed_updates": ["message", "callback_query"]
                },
                timeout=aiohttp.ClientTimeout(total=35)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("ok"):
                        updates = data.get("result", [])
                        if updates:
                            self.offset = updates[-1]["update_id"] + 1
                            logger.info(f"📥 Получено {len(updates)} обновлений")
                            for u in updates:
                                if "callback_query" in u:
                                    logger.info(f"🔘 CALLBACK: {u['callback_query'].get('data')}")
                        return updates
                    return []
                else:
                    logger.error(f"HTTP error: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            return []
    
    async def answer_callback(self, callback_id: str, text: str, show_alert: bool = False) -> bool:
        """ОБЯЗАТЕЛЬНО для работы кнопок"""
        try:
            payload = {
                "callback_query_id": callback_id,
                "text": text,
                "show_alert": show_alert
            }
            
            async with self.session.post(
                f"{self.base_url}/answerCallbackQuery",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    logger.info(f"✅ Ответ на callback: {text}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Ошибка answerCallback: {response.status} - {error_text}")
                    return False
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return False
    
    async def edit_message(self, chat_id: int, message_id: int, text: str) -> bool:
        """Редактирование сообщения"""
        try:
            payload = {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": "HTML"
            }
            
            async with self.session.post(
                f"{self.base_url}/editMessageText",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    logger.info(f"✅ Сообщение обновлено")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Ошибка edit: {response.status} - {error_text}")
                    return False
        except Exception as e:
            logger.error(f"❌ Ошибка edit: {e}")
            return False
    
    async def delete_webhook(self) -> bool:
        """Удаление вебхука (важно для long polling)"""
        try:
            async with self.session.get(
                f"{self.base_url}/deleteWebhook",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"✅ Вебхук удален: {data}")
                    return True
                return False
        except Exception as e:
            logger.error(f"❌ Ошибка delete_webhook: {e}")
            return False
