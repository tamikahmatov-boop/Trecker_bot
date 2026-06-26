import json
import logging
from typing import Dict
from dataclasses import dataclass, asdict
from pathlib import Path

@dataclass
class BotState:
    signals_count: int = 0
    checks_count: int = 0
    start_time: float = 0
    last_alert: Dict[str, float] = None
    last_alert_growth: Dict[str, float] = None
    
    def __post_init__(self):
        if self.last_alert is None:
            self.last_alert = {}
        if self.last_alert_growth is None:
            self.last_alert_growth = {}

class StateManager:
    def __init__(self, state_file: str = "state.json"):
        self.state_file = Path(state_file)
        self.state = BotState()
        self.load()
    
    def load(self):
        try:
            if self.state_file.exists():
                with open(self.state_file, "r") as f:
                    data = json.load(f)
                    self.state = BotState(**data)
                    logging.info(f"Состояние загружено")
        except Exception as e:
            logging.warning(f"Не удалось загрузить состояние: {e}")
    
    def save(self):
        try:
            with open(self.state_file, "w") as f:
                json.dump(asdict(self.state), f, indent=2)
        except Exception as e:
            logging.error(f"Не удалось сохранить состояние: {e}")
    
    def record_signal(self, symbol: str, growth: float):
        import time
        self.state.signals_count += 1
        self.state.last_alert[symbol] = time.time()
        self.state.last_alert_growth[symbol] = growth
    
    def should_alert(self, symbol: str, cooldown: int) -> bool:
        import time
        if symbol not in self.state.last_alert:
            return True
        return time.time() - self.state.last_alert[symbol] >= cooldown
