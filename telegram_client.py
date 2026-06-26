import logging
from typing import Optional, Dict, List, Any
import aiohttp
import json

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
                return response.status == 200
        except Exception as e:
            logging.error(f"Telegram send error: {e}")
            return False
    
    async def send_main_menu(self, chat_id: int):
        """Главное меню с кнопками"""
        keyboard = {
            "keyboard": [
                ["📈 Настройки порога", "⏱ Настройки периода"],
                ["📊 Статистика", "📋 Список монет"],
                ["🔄 Обновить список", "❌ Очистить историю"],
                ["/status", "/help"]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": False
        }
        await self.send_message(
            "🏠 <b>Главное меню</b>\n\n"
            "Выберите действие:",
            chat_id,
            keyboard
        )
    
    async def send_percent_menu(self, chat_id: int):
        """Меню выбора порога"""
        keyboard = {
            "keyboard": [
                ["📈 0.2%", "📈 5%", "📈 10%"],
                ["📈 15%", "📈 20%", "📈 30%"],
                ["📈 50%", "📈 100%", "📈 Пользовательский"],
                ["🔙 Назад"]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": False
        }
        await self.send_message(
            "📈 <b>Выберите порог изменения цены</b>\n\n"
            "Сигнал будет отправлен при достижении выбранного процента:",
            chat_id,
            keyboard
        )
    
    async def send_window_menu(self, chat_id: int):
        """Меню выбора периода"""
        keyboard = {
            "keyboard": [
                ["⏱ 1 минута", "⏱ 5 минут", "⏱ 15 минут"],
                ["⏱ 30 минут", "⏱ 1 час", "⏱ 4 часа"],
                ["⏱ 12 часов", "⏱ 1 день", "⏱ 3 дня"],
                ["🔙 Назад"]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": False
        }
        await self.send_message(
            "⏱ <b>Выберите период анализа</b>\n\n"
            "Бот будет анализировать изменение цены за выбранный период:",
            chat_id,
            keyboard
        )
    
    async def send_inline_keyboard(self, chat_id: int, symbol: str, price: float, growth: float):
        """Инлайн кнопки под сигналом"""
        inline_keyboard = {
            "inline_keyboard": [
                [
                    {"text": "📊 График", "url": f"https://www.tradingview.com/chart/?symbol={symbol}"},
                    {"text": "ℹ️ Инфо", "callback_data": f"info_{symbol}"}
                ],
                [
                    {"text": "🔕 Игнорировать", "callback_data": f"ignore_{symbol}"}
                ]
            ]
        }
        
        # Упрощенная версия без инлайн кнопок (если не нужно)
        return inline_keyboard
    
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
    
    async def answer_callback(self, callback_id: str, text: str, show_alert: bool = False):
        """Ответ на callback запрос"""
        try:
            async with self.session.post(
                f"{self.base_url}/answerCallbackQuery",
                json={
                    "callback_query_id": callback_id,
                    "text": text,
                    "show_alert": show_alert
                }
            ) as response:
                return response.status == 200
        except Exception as e:
            logging.error(f"Callback error: {e}")
            return False
