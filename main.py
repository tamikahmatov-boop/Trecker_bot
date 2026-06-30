"""
Crypto Alert Bot — v20
Полный аудит файла от начала до конца. Исправлен корень проблемы 409 Conflict
на Railway, перекалиброваны уровни уверенности под 20 факторов.

═══════════════════════════════════════════════════════
 НОВОЕ v20 — ПОЛНЫЙ АУДИТ
═══════════════════════════════════════════════════════
• telegram_loop(): ГЛАВНЫЙ ФИКС. При ошибке getUpdates (в т.ч. 409 Conflict)
  бот раньше повторял запрос немедленно — пауза была всего 0.2с независимо
  от результата. При конфликте двух инстансов (например, во время
  Railway-редеплоя, когда старый и новый контейнер на короткое время
  работают одновременно) это означало ~5 запросов/сек к getUpdates,
  что НЕ давало конфликту разрешиться и забивало логи бесконечными 409.
  Теперь при ошибке включается экспоненциальный backoff: 1с → 2с → 4с →
  ... → потолок 30с. Как только Telegram снова отвечает ok=true — backoff
  сразу сбрасывается на 1с. Это не убирает 409 полностью (если второй
  процесс реально жив), но даёт системе шанс на саморазрешение и не
  усугубляет конфликт активным спамом запросов.
• telegram_loop(): после 10 ошибок подряд бот один раз (не на каждую
  ошибку) шлёт владельцу предупреждение через sendMessage — этот метод
  НЕ конфликтует с getUpdates, поэтому сообщение дойдёт даже во время
  активного 409-конфликта на long-polling. Текст прямо объясняет, что
  нужно искать второй процесс с тем же BOT_TOKEN (другой сервер/
  Railway-сервис/локальный запуск), а не молча копить ошибки в логах.
• ВАЖНО про single-instance lock (bot.lock, добавлен в v19): этот лок
  на уровне ОС защищает только от двух процессов В ОДНОМ контейнере/
  файловой системе. На Railway каждый деплой — это отдельный контейнер
  с собственным диском, поэтому bot.lock одного контейнера НЕ видит
  bot.lock другого. Если 409 идёт постоянно (не только в момент
  редеплоя) — следует искать дублирующий Railway-сервис/проект или
  локально запущенный процесс с тем же токеном, lock здесь не поможет
  и не должен восприниматься как полная защита от cross-container
  конфликтов.
• format_reversal_alert и _cmd_reversals: уровни уверенности (🔴🟠🟡)
  были откалиброваны под старый максимум в 16 факторов (10/16≈62%,
  6/16≈37%) и не пересчитаны при добавлении факторов 17-20 в v18.
  Из-за этого 10/20 (всего 50% сработавших факторов) ошибочно
  показывалось как "ВЫСОКАЯ" уверенность. Пересчитано пропорционально
  под 20: высокая ≥12 (60%), средняя ≥7 (35%), слабая <7.
• handle_message: версия бота в приветственных сообщениях (/menu,
  /help, автоподписка для не-владельца) была захардкожена как "v16"
  ещё с ранних версий и не обновлялась — исправлено на v19/v20 по месту.
• detect_short_reversal: убрана мёртвая инициализация stoch_rsi_val=None
  в начале фактора 1, которая немедленно перезаписывалась в факторе 2 —
  не баг, но лишний код, могущий ввести в заблуждение при чтении.

═══════════════════════════════════════════════════════
 v19 — ИСТОРИЯ ИЗМЕНЕНИЙ
═══════════════════════════════════════════════════════
• Single-instance lock через fcntl.flock на файл bot.lock (путь
  настраивается через config.LOCK_FILE). Раньше при случайном запуске
  второй копии бота оба процесса начинали бесконечно конфликтовать за
  getUpdates, Telegram отвечал 409 Conflict, сообщения терялись/дублировались,
  а причина была не очевидна из логов.
  Теперь второй процесс при старте сразу видит занятый лок, печатает
  понятную ошибку с инструкцией (ps aux | grep python3 + что убить) и
  завершается с exit code 1 — вместо тихого зависания в цикле 409-ошибок.
• Лок — на уровне ОС (POSIX advisory lock), не PID-файл: если процесс
  убит любым способом (kill -9, OOM killer, краш сервера), ядро снимает
  лок автоматически. Никаких "залипших" lock-файлов со старым PID,
  которые пришлось бы вручную чистить.
• Лок захватывается ОДИН раз на весь жизненный цикл процесса, до входа
  во внутренний retry-цикл (while True: asyncio.run(main())), а не
  внутри main() — иначе при каждом внутреннем перезапуске после
  необработанного исключения возникало бы окно release→acquire.
• Если fcntl недоступен (не-POSIX окружение) — бот не падает, просто
  логирует предупреждение, что защита от двойного запуска отключена.

═══════════════════════════════════════════════════════
 v18 — ИСТОРИЯ ИЗМЕНЕНИЙ
═══════════════════════════════════════════════════════
• 17. MACD гистограмма падает (Min1) — быстрое подтверждение разворота,
      не дожидаясь Min5-сигнала (фактор 4).
• 18. Цена растянута >1.5% над EMA21 (Min5) — перегрев тренда,
      дополняет крест/gap EMA (фактор 9) количественной мерой растяжения.
• 19. Серия из 3+ красных свечей подряд (Min1) — прямой признак давления
      продавцов, не пересекается с Wick Rejection (фактор 14).
• 20. StochRSI разворачивается вниз из экстремума >0.85 (Min1) — отличается
      от фактора 2 (статичная перекупленность): здесь ловится именно момент
      разворота индикатора, а не просто нахождение в зоне.
  Все 4 фактора используют уже загруженные Min1/Min5 свечи —
  никаких дополнительных HTTP-запросов, скорость не падает.
• max_score теперь 20 везде: кнопки, /rev_score (1–20), /rev_score_delta
  (0–20), _cmd_reversal_settings, format_reversal_alert, лог разворотов.
• Кнопки порога разворота: 3/20, 5/20, 7/20, 9/20, 12/20, 15/20.

═══════════════════════════════════════════════════════
 v17 — ИСТОРИЯ ИЗМЕНЕНИЙ
═══════════════════════════════════════════════════════
• handle_message: первый блок `if text == "/start"` перехватывал /start
  ДО проверки chat_id, из-за чего владелец бота (CHAT_ID) никогда не видел
  полное меню с клавиатурой — получал только короткое сообщение и return.
  Удалён мёртвый/конфликтующий блок, /start теперь всегда даёт владельцу
  полное меню, а подписчикам — приветствие + автоподписку.
• monitor(): стартовое сообщение говорило "v12 — 12-факторный разворот"
  хотя бот уже v16 с 16 факторами — исправлено на v16/16.
• checks_count инкрементировался дважды за итерацию (в начале цикла и в
  блоке периодической очистки) — счётчик циклов был врут в 2 раза чаще
  триггерил периодическую очистку памяти. Дубликат убран.
• log.info при REVERSAL использовал переменную price_change, которая не
  определена если last_info is None (первый сигнал по монете) → NameError,
  тихо проглатываемый внешним try/except, но ломавший лог. Исправлено.
• Умный фильтр повторных разворотов: при пропуске дубликата `continue`
  пропускал ВЕСЬ остаток итерации по монете — включая обычные алерты
  роста/падения. Теперь пропускается только разворотный сигнал.
• Все кнопки и тексты "X/12 факторов" заменены на корректные "X/16"
  (клавиатура порога разворота, /menu, /reversal_settings, /reversals).
• Кнопки порога разворота расширены и переименованы:
  3/16, 5/16, 9/16, 12/16 (агрессивный → стандарт → строгий → очень строгий).
• _cmd_reversals: пороги confidence (🔴🟠🟡) синхронизированы с
  format_reversal_alert (10/6 вместо устаревших 8/5).
• /rev_score_delta: диапазон валидации расширен с 0–12 до 0–16
  (соответствует максимальному скору в 16-факторном детекторе).
• _rev_cmds: сообщение об ошибке для /rev_cooldown показывало текущее
  значение в СЕКУНДАХ при том, что диапазон ввода — в МИНУТАХ.
  Теперь оба значения в одних единицах (минуты).
• Стандартный пресет приведён к единому виду: REVERSAL_MIN_SCORE по
  умолчанию = 5 (как заявлено в пресетах), было 4.

═══════════════════════════════════════════════════════
 НОВЫЕ ВОЗМОЖНОСТИ v17
═══════════════════════════════════════════════════════
• 🔄 Полный перезапуск — новая кнопка и команда /restart. Безопасно
  пересоздаёт asyncio.Task для monitor() БЕЗ потери price_history,
  БД, подписчиков и текущих настроек. Кулдаун 60с защищает от спама.
  Использует общую функцию restart_monitor(), которую теперь вызывает
  и watchdog (устранено дублирование кода).
• 🆘 Помощь — новая кнопка с кратким описанием текущих настроек и кнопок,
  отдельно от полного /menu.
• restart_monitor(reason) — переиспользуемая безопасная функция
  перезапуска цикла мониторинга (раньше код перезапуска дублировался
  только внутри watchdog).

═══════════════════════════════════════════════════════
 v16 — ИСТОРИЯ ИЗМЕНЕНИЙ (см. предыдущие правки)
═══════════════════════════════════════════════════════
• db_connect: не делал commit() → INSERT мог не сохраняться. Добавлен commit/rollback.
• _fetch_mexc_klines: не запрашивал поле "open" → detect_candle_pattern использовал
  closes[-2] как open текущей свечи → Shooting Star никогда не срабатывал.
  Исправлено: opens теперь запрашиваются и передаются в detect_candle_pattern.
• detect_candle_pattern: полностью переписан — реальные opens, добавлен паттерн
  "Вечерняя звезда" (3 бара), исправлена логика тела/тени.
• monitor: get_rsi_trend_from_mexc() делал отдельный HTTP запрос хотя closes уже есть.
  Заменено на calculate_rsi_trend(closes).
• price_change использовалась в log.info но могла быть не определена → NameError.
• import os был внутри db_size_mb() → перемещён на уровень модуля.
• 🧹 БД Очистка: выводила Python dict → заменена на читаемый текст.
• _esc() определялась внутри format_reversal_alert при каждом вызове → перенесена
  на уровень модуля (DRY).
• MACD_SLOPE дефолт -0.000005 слишком мал → никогда не срабатывал slope-ветка.
• get_mexc_klines_multi: fallback dict не содержал "opens" → KeyError в паттернах.
• _kline_cache / _alert_cooldown / price_history: не чистились → утечка памяти.
  Добавлена периодическая очистка в monitor loop.

═══════════════════════════════════════════════════════
 ФАКТОРЫ РАЗВОРОТА (16/16)
═══════════════════════════════════════════════════════
• 13. OBV-дивергенция (Min15): цена растёт, On-Balance Volume падает.
      Сигнал ослабления покупательского давления.
• 14. Wick Rejection Ratio > 0.55 (Min1): среднее отношение верхней тени
      к диапазону за 3 свечи. Указывает на систематическое отталкивание цены от верха.
• 15. RSI старшего TF > 75 (Min15): перекупленность на 15-минутном таймфрейме
      в сочетании с 1m-сигналами даёт подтверждение разворота.
• 16. Lower Highs паттерн (Min5): 3+ последовательных снижения максимумов —
      структурный медвежий разворот.

═══════════════════════════════════════════════════════
 ДАННЫЕ (3 таймфрейма параллельно)
═══════════════════════════════════════════════════════
• Min1  x120: RSI, StochRSI, BB, паттерны (с реальными opens), wick rejection
• Min5  x100: MACD, EMA, ATR, моментум, объём, lower highs
• Min15 x60:  OBV-дивергенция, RSI старшего TF, RSI-дивергенция
  Все три таймфрейма загружаются параллельно через asyncio.gather.

═══════════════════════════════════════════════════════
 ДРУГИЕ ОСОБЕННОСТИ
═══════════════════════════════════════════════════════
• format_reversal_alert: RSI 1m + RSI 15m, Wick ratio, 3 цели Фибо (38.2/50/61.8%)
• Уровни уверенности: слабый <6, средний 6-9, высокий 10+
• _cmd_reversal_settings: описание всех 16 факторов
• Пресеты: агрессивный /rev_score 3, стандарт /rev_score 5, строгий /rev_score 9
• /rev_score принимает значения 1–16

═══════════════════════════════════════════════════════
 О CoinGlass
═══════════════════════════════════════════════════════
Интеграция с CoinGlass (ликвидации, открытый интерес, funding rate, long/short
ratio) НЕ включена в эту версию — требует платного API-ключа CoinGlass.
Архитектура detect_short_reversal() уже модульная: для добавления CoinGlass
как 17-го+ фактора достаточно написать async-функцию получения данных и
добавить её в get_mexc_klines_multi()-подобный параллельный gather, затем
добавить блок в detect_short_reversal(). При наличии ключа — обращайтесь,
добавим этот функционал отдельным патчем.
"""

from __future__ import annotations

import asyncio
import csv
import fcntl
import io
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from contextlib import contextmanager
from typing import Optional

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from ta.momentum import RSIIndicator
from ta.trend import MACD, EMAIndicator
from ta.volatility import BollingerBands, AverageTrueRange

import config

# ── Prometheus (опционально) ──────────────────────────────────────────────────
try:
    from prometheus_client import Counter, Gauge, start_http_server
    PROM_SIGNALS   = Counter("bot_signals_total",   "Всего сигналов")
    PROM_CHECKS    = Counter("bot_checks_total",    "Всего циклов")
    PROM_COINS     = Gauge("bot_tracked_coins",     "Монет в истории")
    PROM_REVERSALS = Counter("bot_reversals_total", "Разворотных сигналов")
    PROM_AVAILABLE = True
except ImportError:
    PROM_AVAILABLE = False

# ================================================================
#  LOGGING
# ================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(funcName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ================================================================
#  SINGLE INSTANCE LOCK
# ================================================================
#  Защита от двойного запуска бота с одним BOT_TOKEN.
#  Без этого второй запущенный процесс получает от Telegram:
#    409 Conflict: terminated by other getUpdates request
#  и оба процесса начинают бесконечно конфликтовать, теряя/дублируя
#  сообщения. Используем POSIX file lock (fcntl.flock) — он атомарный
#  на уровне ОС и автоматически снимается ядром, если процесс убит
#  любым способом (kill -9, OOM killer, падение питания), поэтому
#  никаких "зависших" lock-файлов с устаревшим PID не остаётся.

LOCK_FILE = getattr(config, "LOCK_FILE", "bot.lock")
_lock_fh = None  # держим файловый дескриптор открытым на всё время жизни процесса


def acquire_single_instance_lock() -> None:
    """
    Гарантирует, что одновременно запущен только один процесс бота.
    Если лок уже занят другим процессом — печатает понятную ошибку
    в лог и завершает процесс с кодом 1 (вместо тихого 409-конфликта
    с Telegram, который раньше было сложно диагностировать).
    """
    global _lock_fh
    try:
        _lock_fh = open(LOCK_FILE, "w")
        fcntl.flock(_lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lock_fh.write(str(os.getpid()))
        _lock_fh.flush()
        log.info("Single-instance lock получен (PID=%d, файл=%s)", os.getpid(), LOCK_FILE)
    except BlockingIOError:
        log.error(
            "❌ Бот уже запущен другим процессом! Lock-файл '%s' занят. "
            "Завершаю работу, чтобы не получать 409 Conflict от Telegram. "
            "Проверьте: ps aux | grep python3 — и убейте лишний процесс перед перезапуском.",
            LOCK_FILE,
        )
        sys.exit(1)
    except OSError as e:
        # fcntl недоступен (например Windows) — не блокируем запуск,
        # просто предупреждаем, что защита от двойного запуска отключена.
        log.warning("Single-instance lock недоступен (%s) — защита от двойного запуска отключена", e)


def release_single_instance_lock() -> None:
    global _lock_fh
    if _lock_fh is not None:
        try:
            fcntl.flock(_lock_fh, fcntl.LOCK_UN)
            _lock_fh.close()
        except Exception:
            pass
        try:
            os.remove(LOCK_FILE)
        except OSError:
            pass
        _lock_fh = None

# ================================================================
#  DATABASE
# ================================================================

DB_FILE = "alerts.db"


@contextmanager
def db_connect():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def db_init():
    with db_connect() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS alerts (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol    TEXT    NOT NULL,
                price     REAL    NOT NULL,
                growth    REAL    NOT NULL,
                rsi       REAL,
                macd      REAL,
                source    TEXT,
                ts        REAL    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_alerts_symbol ON alerts(symbol);
            CREATE INDEX IF NOT EXISTS idx_alerts_ts     ON alerts(ts);

            CREATE TABLE IF NOT EXISTS alert_levels (
                symbol       TEXT    PRIMARY KEY,
                alert_price  REAL    NOT NULL,
                direction    INTEGER NOT NULL,
                ts           REAL    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS price_stats (
                symbol   TEXT PRIMARY KEY,
                high24h  REAL,
                low24h   REAL,
                vol24h   REAL,
                updated  REAL
            );

            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id  INTEGER PRIMARY KEY,
                added_ts REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reversal_signals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT    NOT NULL,
                price       REAL    NOT NULL,
                score       INTEGER NOT NULL,
                factors     TEXT    NOT NULL,
                rsi         REAL,
                macd        REAL,
                stoch_rsi   REAL,
                bb_pct      REAL,
                atr         REAL,
                target1     REAL,
                target2     REAL,
                source      TEXT,
                ts          REAL    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_rev_symbol ON reversal_signals(symbol);
            CREATE INDEX IF NOT EXISTS idx_rev_ts     ON reversal_signals(ts);
        """)


db_init()

DB_KEEP_ALERTS_DAYS  = getattr(config, "DB_KEEP_ALERTS_DAYS",  30)
DB_KEEP_LEVELS_DAYS  = getattr(config, "DB_KEEP_LEVELS_DAYS",   7)
DB_VACUUM_INTERVAL_H = getattr(config, "DB_VACUUM_INTERVAL_H", 24)


def db_save_alert(symbol, price, growth, rsi, macd, source):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO alerts (symbol, price, growth, rsi, macd, source, ts) VALUES (?,?,?,?,?,?,?)",
            (symbol, price, growth, rsi, macd, source, time.time()),
        )


def db_save_reversal(symbol, price, score, factors, rsi, macd,
                     stoch_rsi, bb_pct, atr, target1, target2, source):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO reversal_signals "
            "(symbol, price, score, factors, rsi, macd, stoch_rsi, bb_pct, atr, target1, target2, source, ts) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (symbol, price, score, json.dumps(factors, ensure_ascii=False),
             rsi, macd, stoch_rsi, bb_pct, atr, target1, target2, source, time.time()),
        )


def db_recent_reversals(limit: int = 10) -> list[dict]:
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT symbol, price, score, factors, rsi, macd, stoch_rsi, bb_pct, "
            "atr, target1, target2, source, ts "
            "FROM reversal_signals ORDER BY ts DESC LIMIT ?", (limit,),
        ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["factors"] = json.loads(d["factors"])
        except Exception:
            d["factors"] = []
        result.append(d)
    return result


def db_get_alert_level(symbol):
    with db_connect() as conn:
        row = conn.execute(
            "SELECT alert_price, direction, ts FROM alert_levels WHERE symbol=?", (symbol,)
        ).fetchone()
    return dict(row) if row else None


def db_set_alert_level(symbol, alert_price, direction):
    with db_connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO alert_levels (symbol, alert_price, direction, ts) VALUES (?,?,?,?)",
            (symbol, alert_price, direction, time.time()),
        )


def db_clear_alert_levels():
    with db_connect() as conn:
        conn.execute("DELETE FROM alert_levels")


def db_clear_alert_level(symbol):
    with db_connect() as conn:
        conn.execute("DELETE FROM alert_levels WHERE symbol=?", (symbol,))


def db_cleanup() -> dict:
    now = time.time()
    deleted = {}
    with db_connect() as conn:
        cur = conn.execute("DELETE FROM alerts WHERE ts < ?", (now - DB_KEEP_ALERTS_DAYS * 86400,))
        deleted["alerts"] = cur.rowcount
        cur = conn.execute("DELETE FROM alert_levels WHERE ts < ?", (now - DB_KEEP_LEVELS_DAYS * 86400,))
        deleted["alert_levels"] = cur.rowcount
        cur = conn.execute("DELETE FROM price_stats WHERE updated < ?", (now - 86400,))
        deleted["price_stats"] = cur.rowcount
        cur = conn.execute("DELETE FROM reversal_signals WHERE ts < ?", (now - 14 * 86400,))
        deleted["reversal_signals"] = cur.rowcount
    return deleted


def db_vacuum():
    with db_connect() as conn:
        conn.execute("VACUUM")


def db_size_mb() -> float:
    try:
        return os.path.getsize(DB_FILE) / 1_048_576
    except OSError:
        return 0.0


def db_stats() -> dict:
    with db_connect() as conn:
        alerts_count = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        oldest       = conn.execute("SELECT MIN(ts) FROM alerts").fetchone()[0]
        levels_count = conn.execute("SELECT COUNT(*) FROM alert_levels").fetchone()[0]
        rev_count    = conn.execute("SELECT COUNT(*) FROM reversal_signals").fetchone()[0]
    return {
        "alerts":       alerts_count,
        "oldest_alert": time.strftime("%d.%m.%Y", time.localtime(oldest)) if oldest else "—",
        "levels":       levels_count,
        "reversals":    rev_count,
        "size_mb":      db_size_mb(),
    }


def db_recent_alerts(limit: int = 10) -> list[dict]:
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT symbol, price, growth, rsi, macd, source, ts FROM alerts ORDER BY ts DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def db_top_signals(limit: int = 5) -> list[dict]:
    cutoff = time.time() - 86400
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT symbol, growth, price, source, ts FROM alerts "
            "WHERE ts >= ? ORDER BY ABS(growth) DESC LIMIT ?",
            (cutoff, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def db_export_csv(limit: int = 500) -> str:
    rows = db_recent_alerts(limit)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["ts", "symbol", "price", "growth", "rsi", "macd", "source"])
    writer.writeheader()
    for r in rows:
        r["ts"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["ts"]))
        writer.writerow(r)
    return buf.getvalue()


# ── Подписчики ────────────────────────────────────────────────────────────────

def db_add_subscriber(chat_id: int):
    with db_connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO subscribers (chat_id, added_ts) VALUES (?,?)",
            (chat_id, time.time()),
        )


def db_remove_subscriber(chat_id: int):
    with db_connect() as conn:
        conn.execute("DELETE FROM subscribers WHERE chat_id=?", (chat_id,))


def db_get_subscribers() -> list[int]:
    with db_connect() as conn:
        rows = conn.execute("SELECT chat_id FROM subscribers").fetchall()
    ids = {r["chat_id"] for r in rows}
    ids.add(int(config.CHAT_ID))
    return list(ids)


# ================================================================
#  STATE
# ================================================================

STATE_FILE = "state.json"
last_alert_growth: dict = {}


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({"last_alert_growth": last_alert_growth}, f)
    except Exception:
        pass


def load_state():
    global last_alert_growth
    try:
        with open(STATE_FILE) as f:
            last_alert_growth = json.load(f).get("last_alert_growth", {})
    except Exception:
        pass


load_state()

# ================================================================
#  GLOBALS
# ================================================================

TOKEN = config.BOT_TOKEN
TG    = f"https://api.telegram.org/bot{TOKEN}"

offset           = 0
price_history:   dict  = {}
signals_count    = 0
reversal_count   = 0
checks_count     = 0
start_time       = time.time()
last_check_time: float = 0.0

current_percent: float = config.PERCENT
current_window:  int   = config.WINDOW

monitor_paused = False
monitor_task: asyncio.Task | None = None

_session: aiohttp.ClientSession | None = None

_alert_cooldown:    dict[str, float] = {}
_reversal_cooldown: dict[str, float] = {}
_restart_cooldown:  dict[int, float] = {}   # {chat_id: last_restart_ts} — защита от спама кнопкой
RESTART_COOLDOWN_SEC: int = 60
ALERT_COOLDOWN_SEC:    int = getattr(config, "ALERT_COOLDOWN_SEC",    60)
REVERSAL_COOLDOWN_SEC: int = getattr(config, "REVERSAL_COOLDOWN_SEC", 1800)  # 30 мин по умолчанию

# Умный фильтр повторных разворотных сигналов по одной монете:
#   - цена должна измениться минимум на REVERSAL_REPEAT_PRICE_PCT% от прошлого сигнала
#   - или скор должен вырасти минимум на REVERSAL_REPEAT_SCORE_DELTA
_reversal_last: dict[str, dict] = {}   # {sym: {price, score, ts}}
REVERSAL_REPEAT_PRICE_PCT:  float = getattr(config, "REVERSAL_REPEAT_PRICE_PCT",  3.0)   # 3% смены цены
REVERSAL_REPEAT_SCORE_DELTA: int  = getattr(config, "REVERSAL_REPEAT_SCORE_DELTA", 2)    # +2 фактора

_levels_cache: dict[str, dict] = {}

# ── Настройки детектора разворота (все изменяемы через Telegram) ──────────────
REVERSAL_MIN_SCORE:   int   = getattr(config, "REVERSAL_MIN_SCORE",   5)      # из 20
REVERSAL_RSI_OB:      float = getattr(config, "REVERSAL_RSI_OB",      70.0)
REVERSAL_STOCH_OB:    float = getattr(config, "REVERSAL_STOCH_OB",    0.80)
REVERSAL_STOCH_EXT:   float = getattr(config, "REVERSAL_STOCH_EXT",   0.85)
REVERSAL_BB_OB:       float = getattr(config, "REVERSAL_BB_OB",       1.0)
REVERSAL_MACD_SLOPE:  float = getattr(config, "REVERSAL_MACD_SLOPE",  -0.000005)
REVERSAL_ACCEL:       float = getattr(config, "REVERSAL_ACCEL",       0.5)
REVERSAL_HIGH_MARGIN: float = getattr(config, "REVERSAL_HIGH_MARGIN", 0.998)
REVERSAL_MOMENTUM:    float = getattr(config, "REVERSAL_MOMENTUM",    -0.5)
REVERSAL_ATR_MULT:    float = getattr(config, "REVERSAL_ATR_MULT",    3.0)    # ATR перегрев
REVERSAL_VOL_RATIO:   float = getattr(config, "REVERSAL_VOL_RATIO",   0.7)    # объём < 70% средн.

# ── Настраиваемый RSI-фильтр (через кнопки) ────────────────────────────────
# Для обычных сигналов роста/падения:
RSI_SIGNAL_TF:    str   = getattr(config, "RSI_SIGNAL_TF",    "Min1")   # ТФ: Min1/Min5/Min15/Min60/Hour4/Hour12/Day1
RSI_SIGNAL_LEVEL: float = getattr(config, "RSI_SIGNAL_LEVEL", 50.0)     # "не меньше": 50/60/70/80/90/95
# Для разворотных сигналов (фактор 15 + переопределяет REVERSAL_RSI_OB по выбранному ТФ):
RSI_REVERSAL_TF:  str   = getattr(config, "RSI_REVERSAL_TF",  "Min15")

# Минимальный % роста за период для проверки разворота на шорт (кнопки 1/5/10/15/25%)
REVERSAL_GROWTH_MIN_PCT: float = getattr(config, "REVERSAL_GROWTH_MIN_PCT", 5.0)

# Период для расчёта пикового роста разворота (кнопки 5м/30м/1ч/4ч/1д)
REVERSAL_WINDOW_SEC: int = getattr(config, "REVERSAL_WINDOW_SEC", 3600)  # по умолчанию 1 час

# Порог уведомлений о росте/падении (задаётся через /notify_pct или автоматически = current_percent)
NOTIFY_BIG_MOVE_PCT: float = getattr(config, "NOTIFY_BIG_MOVE_PCT", 15.0)
_notify_cooldown: dict[str, float] = {}   # {sym: last_sent_ts}
NOTIFY_BIG_MOVE_COOLDOWN_SEC: int = 3600  # не чаще 1 раза в час


def _cache_load_levels():
    global _levels_cache
    with db_connect() as conn:
        rows = conn.execute("SELECT symbol, alert_price, direction FROM alert_levels").fetchall()
    _levels_cache = {r["symbol"]: {"alert_price": r["alert_price"], "direction": r["direction"]} for r in rows}
    log.info("Загружено уровней в кэш: %d", len(_levels_cache))


def _cache_get_level(symbol):
    return _levels_cache.get(symbol)


def _cache_set_level(symbol, alert_price, direction):
    _levels_cache[symbol] = {"alert_price": alert_price, "direction": direction}
    db_set_alert_level(symbol, alert_price, direction)


def _cache_clear_level(symbol):
    _levels_cache.pop(symbol, None)
    db_clear_alert_level(symbol)


def _cache_clear_all():
    _levels_cache.clear()
    db_clear_alert_levels()


def normalize_symbol(sym: str) -> str:
    return sym.upper().replace("-", "").replace("_", "").replace("/", "")


# ================================================================
#  MEXC KLINE CACHE  (цена + объём)
# ================================================================

_kline_cache: dict[str, dict] = {}
KLINE_CACHE_TTL  = 60
KLINE_LIMIT_1M   = 120   # 120 свечей по 1 мин = 2 часа (RSI, BB, StochRSI, паттерны)
KLINE_LIMIT_5M   = 100   # 100 свечей по 5 мин = 8+ часов (ATR, EMA, MACD, объём)
# Для обратной совместимости
KLINE_LIMIT = KLINE_LIMIT_1M


async def _fetch_mexc_klines(symbol: str, interval: str = "Min1", limit: int = KLINE_LIMIT_1M) -> dict:
    """Возвращает dict с ключами: opens, closes, highs, lows, volumes (все list[float])."""
    empty = {"opens": [], "closes": [], "highs": [], "lows": [], "volumes": []}
    try:
        async with _session.get(
            "https://contract.mexc.com/api/v1/contract/kline",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            data = await r.json()
        if data.get("success") and data.get("data"):
            d = data["data"]
            return {
                "opens":   [float(c) for c in d.get("open",  [])],
                "closes":  [float(c) for c in d.get("close", [])],
                "highs":   [float(c) for c in d.get("high",  [])],
                "lows":    [float(c) for c in d.get("low",   [])],
                "volumes": [float(c) for c in d.get("vol",   [])],
            }
    except Exception as e:
        log.debug("MEXC kline %s %s: %s", symbol, interval, e)
    return empty


async def get_mexc_klines(symbol: str, interval: str = "Min1", limit: int = KLINE_LIMIT_1M) -> dict:
    cache_key = f"{symbol}:{interval}:{limit}"
    now       = time.time()
    cached    = _kline_cache.get(cache_key)
    if cached and now - cached["ts"] < KLINE_CACHE_TTL:
        return cached["data"]
    data = await _fetch_mexc_klines(symbol, interval, limit)
    if data["closes"]:
        _kline_cache[cache_key] = {"ts": now, "data": data}
    # Очищаем устаревшие записи при переполнении (предотвращаем утечку памяти)
    if len(_kline_cache) > 3000:
        stale = [k for k, v in _kline_cache.items() if now - v["ts"] > KLINE_CACHE_TTL * 5]
        for k in stale:
            _kline_cache.pop(k, None)
    return data


KLINE_LIMIT_15M  = 60    # 60 свечей по 15 мин = 15 часов (старший таймфрейм RSI, OBV)


async def get_mexc_klines_multi(symbol: str) -> dict:
    """
    Получает три таймфрейма параллельно:
      - Min1  x120 — RSI, StochRSI, BB, паттерны свечей, wick rejection
      - Min5  x100 — ATR, EMA, MACD, объём, моментум, lower highs
      - Min15 x60  — OBV-дивергенция, RSI старшего TF, структура тренда
    """
    _empty = {"opens": [], "closes": [], "highs": [], "lows": [], "volumes": []}
    results = await asyncio.gather(
        get_mexc_klines(symbol, "Min1",  KLINE_LIMIT_1M),
        get_mexc_klines(symbol, "Min5",  KLINE_LIMIT_5M),
        get_mexc_klines(symbol, "Min15", KLINE_LIMIT_15M),
        return_exceptions=True,
    )
    return {
        "klines_1m":  results[0] if not isinstance(results[0], Exception) else _empty,
        "klines_5m":  results[1] if not isinstance(results[1], Exception) else _empty,
        "klines_15m": results[2] if not isinstance(results[2], Exception) else _empty,
    }


async def get_mexc_closes(symbol: str, interval: str = "Min1") -> list[float]:
    return (await get_mexc_klines(symbol, interval))["closes"]


# ── Настраиваемые таймфреймы RSI (кнопки 5м/15м/1ч/4ч/12ч/1д) ─────────────────
RSI_TF_INTERVALS = {
    "5 мин":  "Min5",
    "15 мин": "Min15",
    "1ч":     "Min60",
    "4ч":     "Hour4",
    "12ч":    "Hour12",
    "1 день": "Day1",
}
RSI_TF_LABELS = {v: k for k, v in RSI_TF_INTERVALS.items()}
RSI_TF_LABELS["Min1"] = "1м"
RSI_TF_LIMIT = {
    "Min1": 120, "Min5": 100, "Min15": 60, "Min60": 60,
    "Hour4": 60, "Hour12": 60, "Day1": 60,
}


def _resample_closes(closes: list[float], group: int) -> list[float]:
    """Грубая агрегация серии закрытий в более крупный ТФ
    (берём цену закрытия последней свечи каждой группы — точное значение,
    просто реже семплированное, без интерполяции/округлений)."""
    if group <= 1 or len(closes) < group:
        return closes
    n = (len(closes) // group) * group
    trimmed = closes[-n:]
    return [trimmed[i + group - 1] for i in range(0, n, group)]


async def get_rsi_for_tf(symbol: str, interval: str, window: int = 14) -> Optional[float]:
    """
    Точный RSI с биржи MEXC (contract klines) для выбранного через кнопки
    таймфрейма. Использует тот же TTL-кэш свечей (60с), поэтому повторное
    обращение в течение минуты не делает лишних HTTP-запросов.
    Hour12 не поддерживается биржей MEXC напрямую — агрегируется из Hour4×3
    (берётся реальная цена закрытия каждой 3-й 4-часовой свечи).
    """
    if interval == "Hour12":
        data = await get_mexc_klines(symbol, "Hour4", 180)
        closes = _resample_closes(data["closes"], 3)
        if len(closes) < window + 1:
            return None
        return calculate_rsi(closes, window=window)
    limit  = RSI_TF_LIMIT.get(interval, 60)
    data   = await get_mexc_klines(symbol, interval, limit)
    closes = data["closes"]
    if len(closes) < window + 1:
        return None
    return calculate_rsi(closes, window=window)


def _to_mexc_symbol(sym: str) -> str:
    if sym.endswith("USDT"):
        return sym[:-4] + "_USDT"
    if sym.endswith("PERP"):
        base = sym[:-4]
        if base.endswith("USDT"):
            return base[:-4] + "_USDT"
    return sym


# ================================================================
#  INDICATORS
# ================================================================

MIN_SAMPLES = 10


def calculate_rsi(prices: list[float], window: int = 14) -> Optional[float]:
    try:
        if len(prices) < window + 1:
            return None
        s   = pd.Series(prices)
        val = RSIIndicator(close=s, window=window).rsi().iloc[-1]
        return None if pd.isna(val) else round(float(val), 2)
    except Exception as e:
        log.error("RSI error: %s", e)
        return None


def calculate_stoch_rsi(prices: list[float], window: int = 14) -> Optional[float]:
    """StochRSI: позиция RSI в его собственном диапазоне. >0.8 = перекупленность."""
    try:
        if len(prices) < window * 2 + 1:
            return None
        s          = pd.Series(prices)
        rsi_series = RSIIndicator(close=s, window=window).rsi().dropna()
        if len(rsi_series) < window:
            return None
        rsi_min = rsi_series.rolling(window).min().iloc[-1]
        rsi_max = rsi_series.rolling(window).max().iloc[-1]
        if pd.isna(rsi_min) or pd.isna(rsi_max) or (rsi_max - rsi_min) == 0:
            return None
        return round(float((rsi_series.iloc[-1] - rsi_min) / (rsi_max - rsi_min)), 4)
    except Exception as e:
        log.error("StochRSI error: %s", e)
        return None


def calculate_bollinger_pct(prices: list[float], window: int = 20) -> Optional[float]:
    """%B Боллинджера. >1.0 = выше верхней полосы."""
    try:
        if len(prices) < window:
            return None
        s     = pd.Series(prices)
        bb    = BollingerBands(close=s, window=window, window_dev=2)
        upper = bb.bollinger_hband().iloc[-1]
        lower = bb.bollinger_lband().iloc[-1]
        if pd.isna(upper) or pd.isna(lower) or (upper - lower) == 0:
            return None
        return round(float((prices[-1] - lower) / (upper - lower)), 4)
    except Exception as e:
        log.error("BB error: %s", e)
        return None


def calculate_macd_full(prices: list[float]) -> dict:
    result = {"histogram": None, "histogram_prev": None, "slope": None, "cross_down": False}
    try:
        if len(prices) < 30:
            return result
        s    = pd.Series(prices)
        hist = MACD(close=s).macd_diff().dropna()
        if len(hist) < 4:
            return result
        result["histogram"]      = round(float(hist.iloc[-1]), 6)
        result["histogram_prev"] = round(float(hist.iloc[-2]), 6)
        result["slope"]          = round(float(hist.iloc[-1] - hist.iloc[-3]), 6)
        result["cross_down"]     = bool(hist.iloc[-2] > 0 and hist.iloc[-1] < 0)
    except Exception as e:
        log.error("MACD full error: %s", e)
    return result


def calculate_ema_cross(prices: list[float], fast: int = 9, slow: int = 21) -> dict:
    """
    EMA крест: если EMA9 только что пересекла EMA21 вниз — медвежий сигнал.
    Возвращает: cross_down (bool), ema_fast, ema_slow, gap_pct (разрыв в %).
    """
    result = {"cross_down": False, "ema_fast": None, "ema_slow": None, "gap_pct": None}
    try:
        if len(prices) < slow + 2:
            return result
        s        = pd.Series(prices)
        ema_fast = EMAIndicator(close=s, window=fast).ema_indicator()
        ema_slow = EMAIndicator(close=s, window=slow).ema_indicator()
        ef_now   = float(ema_fast.iloc[-1])
        ef_prev  = float(ema_fast.iloc[-2])
        es_now   = float(ema_slow.iloc[-1])
        es_prev  = float(ema_slow.iloc[-2])
        # Крест вниз: до — fast > slow, после — fast < slow
        result["cross_down"] = bool(ef_prev >= es_prev and ef_now < es_now)
        result["ema_fast"]   = round(ef_now, 6)
        result["ema_slow"]   = round(es_now, 6)
        result["gap_pct"]    = round((ef_now - es_now) / es_now * 100, 3)
    except Exception as e:
        log.error("EMA cross error: %s", e)
    return result


def calculate_atr(highs: list[float], lows: list[float],
                  closes: list[float], window: int = 14) -> Optional[float]:
    """ATR — средний истинный диапазон. Мера волатильности."""
    try:
        if len(closes) < window + 1:
            return None
        s_h = pd.Series(highs)
        s_l = pd.Series(lows)
        s_c = pd.Series(closes)
        atr = AverageTrueRange(high=s_h, low=s_l, close=s_c, window=window).average_true_range()
        val = atr.iloc[-1]
        return None if pd.isna(val) else round(float(val), 8)
    except Exception as e:
        log.error("ATR error: %s", e)
        return None


def detect_candle_pattern(closes: list[float], highs: list[float],
                           lows: list[float],
                           opens: list[float] | None = None) -> Optional[str]:
    """
    Медвежий свечной паттерн на последних 2 барах.
    opens — реальные цены открытия из MEXC kline (поле 'open').
    Если не переданы, аппроксимируем prev_close.
      - Shooting Star: длинная верхняя тень >2× тела, тело внизу, закрытие < открытие
      - Доджи: тело < 10% диапазона — нерешительность
      - Медвежье поглощение: красная свеча полностью поглощает предыдущую зелёную
      - Вечерняя звезда (3 бара): бычья → доджи/малое тело → красная
    """
    try:
        if len(closes) < 3 or len(highs) < 3 or len(lows) < 3:
            return None

        # Реальные opens из MEXC или аппроксимация
        if opens and len(opens) >= 3:
            o_prev2 = opens[-3]
            o_prev  = opens[-2]
            o_cur   = opens[-1]
        else:
            o_prev2 = closes[-4] if len(closes) >= 4 else closes[-3]
            o_prev  = closes[-3]
            o_cur   = closes[-2]

        c_prev2 = closes[-3]
        c_prev  = closes[-2]
        c_cur   = closes[-1]
        h_cur   = highs[-1]
        l_cur   = lows[-1]
        h_prev  = highs[-2]

        body_cur  = abs(c_cur  - o_cur)
        body_prev = abs(c_prev - o_prev)
        rng_cur   = h_cur - l_cur
        if rng_cur == 0:
            return None

        upper_wick = h_cur - max(c_cur, o_cur)
        lower_wick = min(c_cur, o_cur) - l_cur

        # Shooting Star: длинная верхняя тень >2× тела, нижняя тень < тела, медвежья
        if (body_cur > 0
                and upper_wick > 2 * body_cur
                and lower_wick < body_cur * 0.5
                and c_cur < o_cur):
            return "Shooting Star 🌠"

        # Доджи: тело < 10% диапазона (нерешительность на пике)
        if body_cur < rng_cur * 0.1:
            return "Доджи ✝️"

        # Медвежье поглощение: пред. зелёная, текущая красная и больше
        if (c_prev > o_prev         # предыдущая — зелёная
                and c_cur < o_cur   # текущая — красная
                and o_cur >= c_prev # открытие ≥ закрытие пред.
                and c_cur <= o_prev # закрытие ≤ открытие пред.
                and body_cur > body_prev):
            return "Медвежье поглощение 🐻"

        # Вечерняя звезда (3 бара): бычья → малое тело/доджи → красная
        body_prev2 = abs(c_prev2 - o_prev2)
        if (c_prev2 > o_prev2              # 1й бар — зелёный бычий
                and body_prev < body_prev2 * 0.3  # 2й бар — малое тело
                and c_cur < o_cur          # 3й бар — красный медвежий
                and c_cur < (o_prev2 + c_prev2) / 2):  # закрытие ниже середины 1го бара
            return "Вечерняя звезда 🌆"

    except Exception as e:
        log.error("Candle pattern error: %s", e)
    return None


def calculate_fibonacci_levels(high: float, low: float) -> dict:
    """
    Уровни Фибоначчи от локального хая к лою (зоны коррекции/цели).
    При развороте вниз цели: 0.236, 0.382, 0.5, 0.618 от текущего хая.
    """
    diff = high - low
    return {
        "fib_236": round(high - diff * 0.236, 8),
        "fib_382": round(high - diff * 0.382, 8),
        "fib_500": round(high - diff * 0.500, 8),
        "fib_618": round(high - diff * 0.618, 8),
        "fib_786": round(high - diff * 0.786, 8),
    }


def calculate_rsi_divergence(prices: list[float], window: int = 14,
                              lookback: int = 10) -> bool:
    """Медвежья дивергенция: цена = новый хай, RSI = ниже предыдущего пика."""
    try:
        if len(prices) < window + lookback + 1:
            return False
        s          = pd.Series(prices)
        rsi_series = RSIIndicator(close=s, window=window).rsi().dropna()
        if len(rsi_series) < lookback:
            return False
        price_now  = prices[-1]
        price_prev = max(prices[-(lookback + 1):-1])
        rsi_now    = float(rsi_series.iloc[-1])
        rsi_prev   = float(rsi_series.iloc[-(lookback + 1):].max())
        return (price_now > price_prev) and (rsi_now < rsi_prev - 2)
    except Exception:
        return False


def calculate_price_momentum(prices: list[float],
                              fast: int = 5, slow: int = 20) -> Optional[float]:
    """Разница быстрого и медленного моментума. < 0 = иссякание."""
    try:
        if len(prices) < slow + 1:
            return None
        mom_fast = (prices[-1] - prices[-fast]) / prices[-fast] * 100
        mom_slow = (prices[-1] - prices[-slow]) / prices[-slow] * 100
        return round(mom_fast - mom_slow, 4)
    except Exception:
        return None


def calculate_volume_weakness(volumes: list[float]) -> Optional[str]:
    """
    Реальный объём из свечей MEXC.
    Если последний бар < REVERSAL_VOL_RATIO от среднего — сигнал слабости роста.
    """
    try:
        if len(volumes) < 20:
            return None
        avg_vol  = sum(volumes[-20:-1]) / 19
        last_vol = volumes[-1]
        if avg_vol == 0:
            return None
        ratio = last_vol / avg_vol
        if ratio < REVERSAL_VOL_RATIO:
            return f"📊 Объём слабый ({ratio:.1%} от среднего)"
        return None
    except Exception:
        return None


def calculate_volume_signal(sym: str) -> Optional[str]:
    """Fallback: анализ иссякания по тикам цены (если нет данных свечей)."""
    hist = price_history.get(sym, [])
    now  = time.time()
    if len(hist) < 20:
        return None
    hour_prices = [p for t, p in hist if now - t <= 3600]
    if len(hour_prices) < 10:
        return None
    mid   = len(hour_prices) // 2
    move1 = abs(hour_prices[mid - 1] - hour_prices[0])   / hour_prices[0]   * 100
    move2 = abs(hour_prices[-1]      - hour_prices[mid])  / hour_prices[mid] * 100
    if move1 > 0.5 and move2 < move1 * 0.3:
        return "📊 Движение без импульса (иссякание)"
    return None


async def get_rsi_trend_from_mexc(symbol: str, window: int = 14) -> Optional[str]:
    mexc_sym = _to_mexc_symbol(symbol)
    closes   = await get_mexc_closes(mexc_sym, interval="Min1")
    if len(closes) < window + 3:
        return None
    return calculate_rsi_trend(closes, window=window)


def calculate_rsi_trend(prices: list[float], window: int = 14) -> Optional[str]:
    try:
        if len(prices) < window + 3:
            return None
        s   = pd.Series(prices)
        rsi = RSIIndicator(close=s, window=window).rsi().dropna()
        if len(rsi) < 3:
            return None
        delta = rsi.iloc[-1] - rsi.iloc[-3]
        if delta > 1:
            return "▲"
        if delta < -1:
            return "▼"
        return "—"
    except Exception:
        return None


def calculate_acceleration(recent: list[tuple[float, float]]) -> Optional[float]:
    try:
        if len(recent) < 6:
            return None
        mid    = len(recent) // 2
        half1  = recent[:mid]
        half2  = recent[mid:]
        speed1 = (half1[-1][1] - half1[0][1]) / half1[0][1] * 100 / max(half1[-1][0] - half1[0][0], 1)
        speed2 = (half2[-1][1] - half2[0][1]) / half2[0][1] * 100 / max(half2[-1][0] - half2[0][0], 1)
        if speed1 == 0:
            return None
        return round(speed2 / abs(speed1), 2)
    except Exception:
        return None


def calculate_growth_duration(recent: list[tuple[float, float]]) -> int:
    if len(recent) < 2:
        return 0
    return int(recent[-1][0] - recent[0][0])


def check_24h_breakout(sym: str, price: float) -> Optional[str]:
    hist = price_history.get(sym, [])
    now  = time.time()
    day_prices = [p for t, p in hist if now - t <= 86400]
    if len(day_prices) < 20:
        return None
    high24 = max(day_prices[:-1])
    low24  = min(day_prices[:-1])
    if price > high24:
        return f"🔺 пробой хая 24h ({high24:.4g})"
    if price < low24:
        return f"🔻 пробой лоя 24h ({low24:.4g})"
    return None


def get_24h_context(sym: str, price: float) -> Optional[str]:
    hist = price_history.get(sym, [])
    now  = time.time()
    day_prices = [p for t, p in hist if now - t <= 86400]
    if len(day_prices) < 20:
        return None
    high24    = max(day_prices)
    low24     = min(day_prices)
    dist_high = (price - high24) / high24 * 100
    dist_low  = (price - low24)  / low24  * 100
    return f"📉 от хая: {dist_high:+.1f}%  📈 от лоя: {dist_low:+.1f}%"


def get_peak_growth(sym: str, price: float, window_sec: int = 3600) -> float:
    """
    Максимальный рост за window_sec от минимума к текущей цене.
    Используется для разворотного детектора: монета могла уже откатить,
    но пиковый рост сохраняется в истории.
    """
    hist = price_history.get(sym, [])
    now  = time.time()
    window_prices = [p for t, p in hist if now - t <= window_sec]
    if not window_prices:
        return 0.0
    low = min(window_prices)
    if low <= 0:
        return 0.0
    return (price - low) / low * 100


# Обратная совместимость
def get_peak_growth_24h(sym: str, price: float) -> float:
    return get_peak_growth(sym, price, window_sec=86400)


def calculate_obv_divergence(closes: list[float], volumes: list[float],
                              lookback: int = 15) -> bool:
    """
    OBV-дивергенция: цена растёт, On-Balance Volume падает.
    Надёжный признак ослабления покупательского давления.
    """
    try:
        if len(closes) < lookback + 1 or len(volumes) < lookback + 1:
            return False
        # Считаем OBV
        obv = [0.0]
        for i in range(1, len(closes)):
            if closes[i] > closes[i - 1]:
                obv.append(obv[-1] + volumes[i])
            elif closes[i] < closes[i - 1]:
                obv.append(obv[-1] - volumes[i])
            else:
                obv.append(obv[-1])
        # Цена: новый хай за lookback
        price_rising = closes[-1] > max(closes[-(lookback + 1):-1])
        # OBV: падает (текущий < среднего за lookback)
        obv_now  = obv[-1]
        obv_prev = sum(obv[-(lookback + 1):-1]) / lookback
        obv_falling = obv_now < obv_prev * 0.97  # OBV упал на 3%+
        return price_rising and obv_falling
    except Exception:
        return False


def calculate_wick_rejection(highs: list[float], lows: list[float],
                              opens: list[float], closes: list[float],
                              lookback: int = 3) -> Optional[float]:
    """
    Wick Rejection Ratio: отношение верхней тени к полному диапазону.
    >0.6 = сильное отталкивание от верхней зоны (медвежий признак).
    Усредняется за последние lookback свечей.
    """
    try:
        n = min(lookback, len(closes))
        if n < 1:
            return None
        ratios = []
        for i in range(-n, 0):
            rng = highs[i] - lows[i]
            if rng == 0:
                continue
            upper_wick = highs[i] - max(opens[i], closes[i])
            ratios.append(upper_wick / rng)
        return round(sum(ratios) / len(ratios), 3) if ratios else None
    except Exception:
        return None


def calculate_lower_highs(highs: list[float], window: int = 5) -> bool:
    """
    Паттерн убывающих максимумов (Lower Highs) на последних window свечах.
    3+ последовательных снижения хая = структурный разворот.
    """
    try:
        if len(highs) < window + 1:
            return False
        recent = highs[-window:]
        # Считаем сколько раз хай ниже предыдущего
        lower_count = sum(1 for i in range(1, len(recent)) if recent[i] < recent[i - 1])
        return lower_count >= window - 2  # минимум window-2 убывания из window-1 пар
    except Exception:
        return False


def calculate_rsi_higher_tf(prices_15m: list[float], window: int = 14) -> Optional[float]:
    """
    RSI на 15m свечах — аппроксимация «старшего таймфрейма».
    Перекупленность >75 на 15m при уже высоком 1m RSI — сильный сигнал.
    """
    return calculate_rsi(prices_15m, window=window)


# ================================================================
#  REVERSAL DETECTOR  — 20 факторов
# ================================================================

def _esc(s: str) -> str:
    """Экранирование HTML-символов для Telegram parse_mode=HTML."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def detect_short_reversal(
    sym:        str,
    price:      float,
    klines_1m:  dict,   # Min1  x120 — RSI, StochRSI, BB, паттерны, wick
    klines_5m:  dict,   # Min5  x100 — ATR, EMA, MACD, объём, моментум, lower highs
    klines_15m: dict,   # Min15 x60  — OBV-дивергенция, RSI старшего TF
    recent:     list[tuple[float, float]],
    growth:     float,
    rsi:        Optional[float],
    min_score:  int = 4,
    rsi_reversal_tf:       Optional[float] = None,
    rsi_reversal_tf_label: str             = "Min15",
) -> dict:
    """
    20-факторный детектор разворота на шорт.

    Min1  (краткосрочные): RSI, StochRSI, BB, паттерн, отбой от хая, Wick Rejection,
                            MACD Min1, серия красных свечей, разворот StochRSI
    Min5  (среднесрочные): MACD, EMA, ATR, моментум, Lower Highs, объём, растяжение от EMA21
    Min15 (долгосрочные):  OBV-дивергенция, RSI-дивергенция, RSI старшего TF
    Тики (акселерация):    замедление импульса

    Факторы (+1 каждый):
      1.  RSI перекупленность (Min1 > RSI_OB)
      2.  StochRSI зона перекупленности (Min1)
      3.  Цена выше верхней полосы Боллинджера %B>BB_OB (Min1)
      4.  MACD медвежий крест или убывающий slope (Min5)
      5.  RSI-дивергенция: цена↑, RSI↓ (Min15)
      6.  Замедление импульса accel < ACCEL (тики)
      7.  Отбой от 24h High с подтверждением свечи (Min1)
      8.  Моментум иссякает fast-slow < MOMENTUM (Min5)
      9.  EMA9 пробила EMA21 вниз или ниже EMA21 (Min5)
      10. ATR-перегрев: движение > ATR_MULT × ATR (Min5, 1 час)
      11. Медвежий свечной паттерн (Min1, реальные opens)
      12. Слабый объём при росте vol < VOL_RATIO (Min5 → Min1 → тики)
      13. OBV-дивергенция: цена↑, OBV↓ (Min15)
      14. Wick Rejection Ratio > 0.55 (Min1, 3 последних свечи)
      15. RSI старшего TF > 75 (Min15)
      16. Lower Highs паттерн (Min5, 5 свечей)
      17. MACD гистограмма падает (Min1, быстрое подтверждение разворота)
      18. Цена растянута выше EMA21 на >1.5% (Min5, перегрев)
      19. Серия из 3+ красных свечей подряд (Min1)
      20. StochRSI разворачивается вниз из экстремума >0.85 (Min1)
    """
    score   = 0
    factors: list[str] = []

    # Распаковываем данные по таймфреймам
    closes_1m   = klines_1m.get("closes",  [])
    highs_1m    = klines_1m.get("highs",   [])
    lows_1m     = klines_1m.get("lows",    [])
    opens_1m    = klines_1m.get("opens",   [])
    volumes_1m  = klines_1m.get("volumes", [])

    closes_5m   = klines_5m.get("closes",  [])
    highs_5m    = klines_5m.get("highs",   [])
    lows_5m     = klines_5m.get("lows",    [])
    volumes_5m  = klines_5m.get("volumes", [])

    closes_15m  = klines_15m.get("closes",  [])
    volumes_15m = klines_15m.get("volumes", [])

    # Fallback на тики если нет свечей
    prices_1m  = closes_1m  if len(closes_1m)  >= 20 else [p for _, p in price_history.get(sym, [])[-120:]]
    prices_5m  = closes_5m  if len(closes_5m)  >= 20 else prices_1m
    prices_15m = closes_15m if len(closes_15m) >= 20 else prices_5m

    # ── 1. RSI перекупленность (Min1) ──────────────────────────────────────────
    if rsi is not None and rsi > REVERSAL_RSI_OB:
        score += 1
        factors.append(f"RSI перекуплен ({rsi:.1f} &gt; {REVERSAL_RSI_OB}) Min1")

    # ── 2. StochRSI (Min1) ─────────────────────────────────────────────────────
    stoch_rsi_val = calculate_stoch_rsi(prices_1m)
    if stoch_rsi_val is not None:
        thr = REVERSAL_STOCH_OB if (rsi is not None and rsi > REVERSAL_RSI_OB) else REVERSAL_STOCH_EXT
        if stoch_rsi_val > thr:
            score += 1
            factors.append(f"StochRSI перекупленность ({stoch_rsi_val:.2f} &gt; {thr}) Min1")

    # ── 3. Боллинджер %B (Min1) ────────────────────────────────────────────────
    bb_pct = calculate_bollinger_pct(prices_1m)
    if bb_pct is not None and bb_pct > REVERSAL_BB_OB:
        score += 1
        factors.append(f"Цена выше верхней BB %B={bb_pct:.2f} Min1")

    # ── 4. MACD (Min5) ─────────────────────────────────────────────────────────
    macd_data = calculate_macd_full(prices_5m)
    macd_hist = macd_data["histogram"]
    if macd_data["cross_down"]:
        score += 1
        factors.append("MACD медвежий крест (hist 0→-) Min5")
    elif (macd_hist is not None
          and macd_data["slope"] is not None
          and macd_hist > 0
          and macd_data["slope"] < REVERSAL_MACD_SLOPE):
        score += 1
        factors.append(f"MACD убывает Min5 (slope={macd_data['slope']:+.6f})")

    # ── 5. RSI-дивергенция (Min15 — более надёжный сигнал) ────────────────────
    if len(prices_15m) >= 30 and calculate_rsi_divergence(prices_15m):
        score += 1
        factors.append("RSI-дивергенция Min15 (цена↑, RSI↓)")
    elif len(prices_5m) >= 30 and calculate_rsi_divergence(prices_5m):
        score += 1
        factors.append("RSI-дивергенция Min5 (цена↑, RSI↓)")

    # ── 6. Замедление импульса (тики) ─────────────────────────────────────────
    accel = calculate_acceleration(recent)
    if accel is not None and growth >= current_percent and accel < REVERSAL_ACCEL:
        score += 1
        factors.append(f"Замедление импульса (accel={accel:.2f}x)")

    # ── 7. Отбой от 24h High (Min1 с реальными opens) ────────────────────────
    hist_data  = price_history.get(sym, [])
    now_ts     = time.time()
    day_prices = [p for t, p in hist_data if now_ts - t <= 86400]
    high24 = max(day_prices) if day_prices else price
    low24  = min(day_prices) if day_prices else price
    if len(day_prices) >= 20:
        near_high = price >= high24 * REVERSAL_HIGH_MARGIN
        if near_high:
            bearish = False
            if len(closes_1m) >= 2 and len(opens_1m) >= 1:
                # Реальная красная свеча у хая (close < open из MEXC)
                bearish = (opens_1m[-1] > closes_1m[-1]  # медвежья свеча
                           and highs_1m[-1] >= high24 * REVERSAL_HIGH_MARGIN)
            if not bearish and len(recent) >= 4:
                ticks = [p for _, p in recent[-4:]]
                falling = sum(ticks[i] > ticks[i + 1] for i in range(len(ticks) - 1))
                bearish = falling >= 2
            if bearish:
                pct = (price - high24) / high24 * 100
                score += 1
                factors.append(f"Отбой от 24h High ({pct:+.2f}%) Min1")

    # ── 8. Моментум иссякает (Min5) ───────────────────────────────────────────
    momentum = calculate_price_momentum(prices_5m)
    if momentum is not None and momentum < REVERSAL_MOMENTUM:
        score += 1
        factors.append(f"Моментум иссякает Min5 (diff={momentum:+.2f}%)")

    # ── 9. EMA крест / ниже EMA (Min5) ────────────────────────────────────────
    ema_data = calculate_ema_cross(prices_5m)
    if ema_data["cross_down"]:
        score += 1
        factors.append(
            f"EMA9 пробила EMA21 вниз Min5 "
            f"({ema_data['ema_fast']:.4g} &lt; {ema_data['ema_slow']:.4g})"
        )
    elif ema_data.get("gap_pct") is not None and ema_data["gap_pct"] < -0.05:
        score += 1
        factors.append(f"EMA9 ниже EMA21 Min5 (gap={ema_data['gap_pct']:+.2f}%)")

    # ── 10. ATR-перегрев (Min5, последний час) ────────────────────────────────
    atr = None
    if len(highs_5m) >= 15 and len(lows_5m) >= 15 and len(closes_5m) >= 15:
        atr = calculate_atr(highs_5m, lows_5m, closes_5m)
        if atr is not None and atr > 0:
            lookback    = min(12, len(closes_5m) - 1)
            period_move = abs(price - closes_5m[-lookback])
            atr_ratio   = period_move / atr
            if atr_ratio > REVERSAL_ATR_MULT:
                score += 1
                factors.append(f"ATR-перегрев Min5 ({atr_ratio:.1f}× ATR={atr:.4g})")

    # ── 11. Свечной паттерн (Min1, реальные opens из MEXC) ───────────────────
    candle_pattern = None
    if len(closes_1m) >= 4 and len(highs_1m) >= 4 and len(lows_1m) >= 4:
        candle_pattern = detect_candle_pattern(
            closes_1m, highs_1m, lows_1m,
            opens_1m if len(opens_1m) >= 4 else None,
        )
        if candle_pattern:
            score += 1
            factors.append(f"Паттерн Min1: {candle_pattern}")

    # ── 12. Слабый объём (Min5 → Min1 → тики) ────────────────────────────────
    vol_signal: Optional[str] = None
    if len(volumes_5m) >= 20:
        vol_signal = calculate_volume_weakness(volumes_5m)
    elif len(volumes_1m) >= 20:
        vol_signal = calculate_volume_weakness(volumes_1m)
    else:
        vol_signal = calculate_volume_signal(sym)
    if vol_signal:
        score += 1
        factors.append(vol_signal)

    # ── 13. OBV-дивергенция (Min15) ───────────────────────────────────────────
    if calculate_obv_divergence(prices_15m, volumes_15m if len(volumes_15m) >= 16
                                else volumes_5m):
        score += 1
        factors.append("OBV-дивергенция Min15 (цена↑, OBV↓)")

    # ── 14. Wick Rejection Ratio > 0.55 (Min1) ───────────────────────────────
    wick_ratio = None
    if len(closes_1m) >= 3 and len(opens_1m) >= 3:
        wick_ratio = calculate_wick_rejection(highs_1m, lows_1m, opens_1m, closes_1m, lookback=3)
        if wick_ratio is not None and wick_ratio > 0.55:
            score += 1
            factors.append(f"Wick Rejection {wick_ratio:.2f} (верхняя тень доминирует) Min1")

    # ── 15. RSI настраиваемого старшего ТФ > REVERSAL_RSI_OB (кнопки) ─────────
    if rsi_reversal_tf is not None:
        rsi_15m       = rsi_reversal_tf
        rsi_15m_label = rsi_reversal_tf_label
    else:
        rsi_15m       = calculate_rsi_higher_tf(prices_15m) if len(prices_15m) >= 16 else None
        rsi_15m_label = "Min15"
    if rsi_15m is not None and rsi_15m > REVERSAL_RSI_OB:
        score += 1
        factors.append(f"RSI перекуплен на {rsi_15m_label} ({rsi_15m:.1f} &gt; {REVERSAL_RSI_OB})")

    # ── 16. Lower Highs паттерн (Min5) ────────────────────────────────────────
    if len(highs_5m) >= 6 and calculate_lower_highs(highs_5m, window=5):
        score += 1
        factors.append("Lower Highs паттерн Min5 (убывающие максимумы)")

    # ── 17. MACD гистограмма падает (Min1, быстрое подтверждение) ────────────
    macd_data_1m = calculate_macd_full(prices_1m)
    macd_hist_1m = macd_data_1m["histogram"]
    if (macd_hist_1m is not None
            and macd_data_1m["histogram_prev"] is not None
            and macd_hist_1m > 0
            and macd_hist_1m < macd_data_1m["histogram_prev"]):
        score += 1
        factors.append(f"MACD гистограмма падает Min1 ({macd_hist_1m:+.6f})")

    # ── 18. Цена сильно растянута выше EMA21 (Min5) ───────────────────────────
    ema_extension_pct = None
    if ema_data.get("ema_slow"):
        ema_extension_pct = (price - ema_data["ema_slow"]) / ema_data["ema_slow"] * 100
        if ema_extension_pct > 1.5:
            score += 1
            factors.append(f"Цена растянута над EMA21 Min5 (+{ema_extension_pct:.2f}%)")

    # ── 19. Серия красных свечей (Min1, 3+ подряд) ────────────────────────────
    red_streak = 0
    if len(closes_1m) >= 3 and len(opens_1m) >= 3:
        for i in range(-1, -4, -1):
            if closes_1m[i] < opens_1m[i]:
                red_streak += 1
            else:
                break
        if red_streak >= 3:
            score += 1
            factors.append(f"Серия красных свечей Min1 ({red_streak} подряд)")

    # ── 20. StochRSI разворачивается вниз из экстремума (Min1) ───────────────
    stoch_turn = False
    if len(prices_1m) >= 30:
        stoch_prev = calculate_stoch_rsi(prices_1m[:-1])
        if (stoch_rsi_val is not None and stoch_prev is not None
                and stoch_prev > 0.85 and stoch_rsi_val < stoch_prev - 0.05):
            stoch_turn = True
            score += 1
            factors.append(f"StochRSI разворот вниз Min1 ({stoch_prev:.2f} → {stoch_rsi_val:.2f})")

    # ── Цели по шорту: уровни Фибоначчи от 24h High к Low ────────────────────
    fib     = calculate_fibonacci_levels(high24, low24)
    target1 = fib["fib_382"]
    target2 = fib["fib_618"]

    day_context = get_24h_context(sym, price)

    return {
        "score":          score,
        "max_score":      20,
        "factors":        factors,
        "triggered":      score >= min_score,
        "rsi":            rsi,
        "rsi_15m":        rsi_15m,
        "rsi_15m_label":  rsi_15m_label,
        "stoch_rsi":      stoch_rsi_val,
        "bb_pct":         bb_pct,
        "macd_hist":      macd_hist,
        "macd_hist_1m":   macd_hist_1m,
        "accel":          accel,
        "atr":            atr,
        "wick_ratio":     wick_ratio,
        "ema_extension_pct": ema_extension_pct,
        "red_streak":     red_streak,
        "stoch_turn":     stoch_turn,
        "target1":        target1,
        "target2":        target2,
        "fib":            fib,
        "high24":         high24,
        "low24":          low24,
        "candle_pattern": candle_pattern,
        "ema_gap":        ema_data.get("gap_pct"),
        "vol_signal":     vol_signal,
        "day_context":    day_context,
    }


# ================================================================
#  TELEGRAM HELPERS
# ================================================================

async def _tg_post(method: str, payload: dict) -> Optional[dict]:
    try:
        async with _session.post(
            f"{TG}/{method}", json=payload, timeout=aiohttp.ClientTimeout(total=20)
        ) as resp:
            data = await resp.json()
            if not data.get("ok"):
                log.warning("Telegram %s error: %s", method, data)
            return data
    except Exception as e:
        log.error("Telegram %s: %s", method, e)
        return None


async def send_message(text: str, chat_id, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    await _tg_post("sendMessage", payload)


async def send_document(chat_id, filename: str, content: str, caption: str = ""):
    try:
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id))
        form.add_field("caption", caption)
        form.add_field("document", content.encode(), filename=filename, content_type="text/csv")
        async with _session.post(f"{TG}/sendDocument", data=form,
                                  timeout=aiohttp.ClientTimeout(total=30)) as resp:
            return await resp.json()
    except Exception as e:
        log.error("sendDocument: %s", e)


async def broadcast(text: str, reply_markup=None):
    tasks = [send_message(text, cid, reply_markup) for cid in db_get_subscribers()]
    await asyncio.gather(*tasks, return_exceptions=True)


def reply_keyboard():
    return {
        "keyboard": [
            # Строка 1 — Управление мониторингом
            ["⏸ Пауза", "▶️ Продолжить", "📊 Статус"],
            # Строка 2 — Порог роста/падения для алертов (включая 15%)
            ["📈 0.2%", "📈 5%", "📈 10%", "📈 15%", "📈 20%"],
            # Строка 3 — Временное окно
            ["⏱ 5 мин", "⏱ 1 час", "⏱ 4 ч", "⏱ 1 д"],
            # Строка 4 — Сигналы и история
            ["📋 История", "🏆 Топ-5", "📤 Экспорт"],
            # Строка 5 — Разворот + кулдауны
            ["🔄 Развороты", "⚙️ Настройки разворота", "🗑 Кулдауны"],
            # Строка 6 — Порог разворота (быстрая настройка, из 20 факторов)
            ["🎚 Порог 3/20", "🎚 Порог 5/20", "🎚 Порог 7/20"],
            ["🎚 Порог 9/20", "🎚 Порог 12/20", "🎚 Порог 15/20"],
            # Строка 7 — Процент роста для разворотных сигналов в шорт
            ["📉 Разворот 1%", "📉 Разворот 5%", "📉 Разворот 10%", "📉 Разворот 15%", "📉 Разворот 25%"],
            # Строка 8 — Период окна для расчёта роста разворота
            ["🕐 Разворот 5м", "🕐 Разворот 30м", "🕐 Разворот 1ч", "🕐 Разворот 4ч", "🕐 Разворот 1д"],
            # Строка 9 — Управление процессом
            ["🔄 Полный перезапуск", "🆘 Помощь"],
            # Строка 10 — RSI-фильтр обычных сигналов: таймфрейм
            ["📊 RSI ТФ 5м", "📊 RSI ТФ 15м", "📊 RSI ТФ 1ч", "📊 RSI ТФ 4ч", "📊 RSI ТФ 12ч", "📊 RSI ТФ 1д"],
            # Строка 11 — RSI-фильтр обычных сигналов: порог "не меньше"
            ["📊 RSI ≥50", "📊 RSI ≥60", "📊 RSI ≥70", "📊 RSI ≥80", "📊 RSI ≥90", "📊 RSI ≥95"],
            # Строка 12 — RSI разворота: таймфрейм (отдельно от сигналов роста/падения)
            ["🔄📊 RSI ТФ 5м", "🔄📊 RSI ТФ 15м", "🔄📊 RSI ТФ 1ч", "🔄📊 RSI ТФ 4ч", "🔄📊 RSI ТФ 12ч", "🔄📊 RSI ТФ 1д"],
            # Строка 13 — RSI разворота: порог "не меньше"
            ["🔄📊 RSI ≥50", "🔄📊 RSI ≥60", "🔄📊 RSI ≥70", "🔄📊 RSI ≥80", "🔄📊 RSI ≥90", "🔄📊 RSI ≥95"],
        ],
        "resize_keyboard": True,
        "persistent":      True,
    }


# ================================================================
#  ALERT FORMATTING
# ================================================================

def _fmt_dur(sec: int) -> str:
    if sec >= 3600:
        return f"{sec // 3600}ч {(sec % 3600) // 60}м"
    if sec >= 60:
        return f"{sec // 60}м"
    return f"{sec}с"


def alert_emoji(growth: float) -> str:
    a = abs(growth)
    if a >= 20:
        return "💥"
    if a >= 10:
        return "🔥"
    return "🚀" if growth > 0 else "📉"


def format_growth_alert(
    sym: str, price: float, growth: float,
    rsi: Optional[float], macd: Optional[float], source: str,
    rsi_trend: Optional[str] = None,
    accel: Optional[float]   = None,
    duration_sec: int        = 0,
    breakout: Optional[str]  = None,
    day_context: Optional[str] = None,
    rsi_tf_label: Optional[str] = None,
    rsi_tf_val:   Optional[float] = None,
    rsi_tf_level: Optional[float] = None,
) -> str:
    emoji = alert_emoji(growth)
    sign  = "+" if growth > 0 else ""
    label = "Рост" if growth > 0 else "Падение"

    rsi_s    = f"{rsi:.1f}" if rsi is not None else "—"
    rsi_hint = (" ⚠️ перекуплен" if rsi is not None and rsi >= 70
                else " ⚠️ перепродан" if rsi is not None and rsi <= 30
                else "")
    rsi_t    = f" {rsi_trend}" if rsi_trend else ""
    macd_s   = f"{macd:+.6f}" if macd is not None else "—"

    rsi_tf_s = ""
    if rsi_tf_label and rsi_tf_val is not None:
        lvl_s = f" (порог ≥{rsi_tf_level:g})" if rsi_tf_level is not None else ""
        rsi_tf_s = f"\n📊 RSI {rsi_tf_label}: <code>{rsi_tf_val:.1f}</code>{lvl_s}"

    accel_s = ""
    if accel is not None:
        if accel >= 2.0:
            accel_s = f"\n⚡ Ускорение: <b>×{accel:.1f}</b> 🔥"
        elif accel >= 1.3:
            accel_s = f"\n⚡ Ускорение: ×{accel:.1f}"
        elif accel < 0.7:
            accel_s = f"\n🐢 Замедление: ×{accel:.1f}"

    return (
        f"{emoji} <b>СИГНАЛ — {label.upper()}</b>\n\n"
        f"🪙 <b>{sym}</b>  [{source}]\n"
        f"💵 Цена: <code>{price}</code>\n"
        f"📈 {label}: <b>{sign}{growth:.2f}%</b> за {_fmt_dur(duration_sec)}\n"
        f"📊 RSI: <code>{rsi_s}</code>{rsi_t}{rsi_hint}\n"
        f"〽️ MACD: <code>{macd_s}</code>"
        f"{rsi_tf_s}"
        f"{accel_s}"
        f"{chr(10) + breakout if breakout else ''}"
        f"{chr(10) + day_context if day_context else ''}"
        f"\n\n📋 <code>{sym}</code>"
    )


format_alert = format_growth_alert  # обратная совместимость


def format_drop_alert(
    sym: str, price: float, growth: float,
    rsi: Optional[float], macd: Optional[float], source: str,
    rsi_trend: Optional[str] = None,
    duration_sec: int        = 0,
    breakout: Optional[str]  = None,
    day_context: Optional[str] = None,
    rsi_tf_label: Optional[str] = None,
    rsi_tf_val:   Optional[float] = None,
    rsi_tf_level: Optional[float] = None,
) -> str:
    a     = abs(growth)
    emoji = "💥" if a >= 20 else "🔥" if a >= 10 else "📉"
    rsi_s = f"{rsi:.1f}" if rsi is not None else "—"
    hint  = " ⚠️ перепродан" if (rsi is not None and rsi <= 30) else ""
    rsi_t = f" {rsi_trend}" if rsi_trend else ""
    macd_s = f"{macd:+.6f}" if macd is not None else "—"

    rsi_tf_s = ""
    if rsi_tf_label and rsi_tf_val is not None:
        lvl_s = f" (порог ≤{100 - rsi_tf_level:g})" if rsi_tf_level is not None else ""
        rsi_tf_s = f"\n📊 RSI {rsi_tf_label}: <code>{rsi_tf_val:.1f}</code>{lvl_s}"

    return (
        f"{emoji} <b>ПАДЕНИЕ</b>\n\n"
        f"🪙 <b>{sym}</b>  [{source}]\n"
        f"💵 Цена: <code>{price}</code>\n"
        f"📉 Падение: <b>{growth:.2f}%</b> за {_fmt_dur(duration_sec)}\n"
        f"📊 RSI: <code>{rsi_s}</code>{rsi_t}{hint}\n"
        f"〽️ MACD: <code>{macd_s}</code>"
        f"{rsi_tf_s}"
        f"{chr(10) + breakout if breakout else ''}"
        f"{chr(10) + day_context if day_context else ''}"
        f"\n\n📋 <code>{sym}</code> #падение"
    )


def format_reversal_alert(
    sym:          str,
    price:        float,
    growth:       float,
    source:       str,
    rev:          dict,
    duration_sec: int = 0,
    window_sec:   int = 3600,
) -> str:
    score      = rev["score"]
    max_score  = rev.get("max_score", 20)
    factors    = rev["factors"]
    rsi        = rev.get("rsi")
    rsi_15m    = rev.get("rsi_15m")
    rsi15_lbl  = rev.get("rsi_15m_label", "15m")
    stoch      = rev.get("stoch_rsi")
    bb_pct     = rev.get("bb_pct")
    macd_h     = rev.get("macd_hist")
    atr        = rev.get("atr")
    target1    = rev.get("target1")
    target2    = rev.get("target2")
    candle     = rev.get("candle_pattern")
    ema_gap    = rev.get("ema_gap")
    wick_ratio = rev.get("wick_ratio")
    vol_sig    = rev.get("vol_signal", "")
    day_ctx    = rev.get("day_context", "")

    if score >= 12:
        confidence, hdr = "🔴 ВЫСОКАЯ", "🚨"
    elif score >= 7:
        confidence, hdr = "🟠 СРЕДНЯЯ", "⚠️"
    else:
        confidence, hdr = "🟡 СЛАБАЯ", "🔄"

    rsi_s    = f"{rsi:.1f}"       if rsi        is not None else "—"
    rsi15_s  = f"{rsi_15m:.1f}"   if rsi_15m    is not None else "—"
    stoch_s  = f"{stoch:.2f}"     if stoch      is not None else "—"
    bb_s     = f"{bb_pct:.2f}"    if bb_pct     is not None else "—"
    macd_s   = f"{macd_h:+.6f}"   if macd_h     is not None else "—"
    atr_s    = f"{atr:.6f}"       if atr        is not None else "—"
    ema_s    = f"{ema_gap:+.2f}%" if ema_gap    is not None else "—"
    wick_s   = f"{wick_ratio:.2f}" if wick_ratio is not None else "—"

    factors_s = "\n".join(f"  {i+1}. {_esc(f)}" for i, f in enumerate(factors)) if factors else "  —"

    t1_s      = f"<code>{target1:.4g}</code>" if target1 else "—"
    t2_s      = f"<code>{target2:.4g}</code>" if target2 else "—"
    fib       = rev.get("fib", {})
    t3_s      = f"<code>{fib['fib_500']:.4g}</code>" if fib.get("fib_500") else "—"

    candle_s  = f"\n🕯 Паттерн: <b>{candle}</b>" if candle  else ""
    vol_s     = f"\n{_esc(vol_sig)}"              if vol_sig else ""
    day_ctx_s = f"\n{day_ctx}"                    if day_ctx else ""

    return (
        f"{hdr} <b>РАЗВОРОТ НА ШОРТ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{sym}</b>  [{source}]\n"
        f"💵 Цена: <code>{price}</code>\n"
        f"📈 Рост до разворота: <b>+{growth:.2f}%</b> за {_fmt_dur(window_sec)}\n\n"
        f"🎯 Уверенность: {confidence}  [{score}/{max_score}]\n\n"
        f"<b>Сработавшие факторы ({score}/{max_score}):</b>\n{factors_s}\n\n"
        f"<b>Индикаторы:</b>\n"
        f"  RSI 1m: <code>{rsi_s}</code>  │  RSI {rsi15_lbl}: <code>{rsi15_s}</code> (порог &gt;{REVERSAL_RSI_OB})\n"
        f"  StochRSI: <code>{stoch_s}</code>  │  BB%: <code>{bb_s}</code>\n"
        f"  MACD: <code>{macd_s}</code>  │  ATR: <code>{atr_s}</code>\n"
        f"  EMA gap: <code>{ema_s}</code>  │  Wick: <code>{wick_s}</code>"
        f"{candle_s}"
        f"{vol_s}"
        f"{day_ctx_s}\n\n"
        f"<b>🎯 Цели шорта (Фибо):</b>\n"
        f"  Цель 1 (38.2%): {t1_s}\n"
        f"  Цель 2 (50.0%): {t3_s}\n"
        f"  Цель 3 (61.8%): {t2_s}"
    )


# ================================================================
#  SYMBOLS
# ================================================================

async def get_symbols() -> set[str]:
    symbols: set[str] = set()
    for label, url, suffixes in [
        ("Trading", "https://public.bybit.com/trading/", ("USDT", "PERP")),
        ("Spot",    "https://public.bybit.com/spot/",    ("USDT",)),
    ]:
        try:
            async with _session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                html = await r.text()
            soup  = BeautifulSoup(html, "html.parser")
            count = 0
            for a in soup.find_all("a"):
                sym = a.text.strip("/")
                if sym.endswith(suffixes):
                    symbols.add(sym.replace("/", ""))
                    count += 1
            log.info("%s: %d символов", label, count)
        except Exception as e:
            log.error("get_symbols %s: %s", label, e)
    log.info("Всего монет: %d", len(symbols))
    return symbols


# ================================================================
#  PRICES
# ================================================================

async def _fetch_okx(norm: dict) -> tuple[dict, dict]:
    prices, sources = {}, {}
    try:
        async with _session.get(
            "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            data = await r.json()
        for item in data.get("data", []):
            sym   = normalize_symbol(item["instId"])
            price = float(item["last"])
            if price > 0 and sym in norm:
                real = norm[sym]
                prices[real]  = price
                sources[real] = "OKX"
    except Exception as e:
        log.error("OKX: %s", e)
    return prices, sources


async def _fetch_mexc(norm: dict) -> tuple[dict, dict]:
    prices, sources = {}, {}
    try:
        async with _session.get(
            "https://contract.mexc.com/api/v1/contract/ticker",
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            data = await r.json()
        if data.get("success"):
            for item in data["data"]:
                sym   = normalize_symbol(item["symbol"])
                price = float(item["lastPrice"])
                if price > 0 and sym in norm:
                    real = norm[sym]
                    prices[real]  = price
                    sources[real] = "MEXC"
    except Exception as e:
        log.error("MEXC: %s", e)
    return prices, sources


_norm_cache: dict[str, str] = {}
_norm_symbols_key: frozenset = frozenset()


async def get_prices(symbols: set[str]) -> tuple[dict, dict]:
    global _norm_cache, _norm_symbols_key
    sym_key = frozenset(symbols)
    if sym_key != _norm_symbols_key:
        _norm_cache       = {normalize_symbol(s): s for s in symbols}
        _norm_symbols_key = sym_key
    norm = _norm_cache

    merged: dict = {}
    msrc:   dict = {}
    results = await asyncio.gather(_fetch_mexc(norm), _fetch_okx(norm), return_exceptions=True)
    for res in results:
        if isinstance(res, Exception):
            log.error("Fetch error: %s", res)
            continue
        p, s = res
        for sym, price in p.items():
            if sym not in merged:
                merged[sym] = price
                msrc[sym]   = s[sym]
    return merged, msrc


# ================================================================
#  MONITOR
# ================================================================

# ================================================================
#  ОБРАБОТКА ОДНОЙ МОНЕТЫ (вынесено для конкурентной обработки —
#  чтобы уведомления слались сразу по готовности, не дожидаясь
#  последовательного перебора всех остальных монет)
# ================================================================

# Сколько монет обрабатываем одновременно (ограничивает параллельные HTTP-запросы к MEXC)
MONITOR_CONCURRENCY: int = getattr(config, "MONITOR_CONCURRENCY", 25)


async def _process_one_symbol(sym: str, price: float, now: float, source: str) -> None:
    global signals_count, reversal_count

    if price <= 0:
        return

    hist = price_history.setdefault(sym, [])
    hist.append((now, price))
    cutoff = max(current_window * 2, 86400)
    price_history[sym] = [(t, p) for t, p in hist if now - t <= cutoff]

    recent = [(t, p) for t, p in price_history[sym] if now - t <= current_window]

    if len(recent) < MIN_SAMPLES:
        return

    old_price = recent[0][1]
    if old_price <= 0:
        return

    growth    = (price - old_price) / old_price * 100
    direction = 1 if growth > 0 else -1

    # ── Свечи MEXC: 3 таймфрейма параллельно ──────────────────────
    mexc_sym    = _to_mexc_symbol(sym)
    klines_all  = await get_mexc_klines_multi(mexc_sym)
    klines_1m   = klines_all["klines_1m"]
    klines_5m   = klines_all["klines_5m"]
    klines_15m  = klines_all["klines_15m"]
    closes      = klines_1m["closes"]

    rsi = calculate_rsi(closes) if closes else None
    if rsi is None:
        vals = [p for _, p in price_history[sym][-120:]]
        rsi  = calculate_rsi(vals)

    # ════════════════════════════════════════════════════════════
    #  БЛОК 1: РАЗВОРОТ НА ШОРТ
    #  Условие: пиковый рост за REVERSAL_WINDOW_SEC >= REVERSAL_GROWTH_MIN_PCT
    #  Фильтр дублей: кулдаун + смена цены + рост скора
    # ════════════════════════════════════════════════════════════
    peak_growth = get_peak_growth(sym, price, REVERSAL_WINDOW_SEC)
    if peak_growth >= REVERSAL_GROWTH_MIN_PCT or growth >= REVERSAL_GROWTH_MIN_PCT:
        last_rev_ts = _reversal_cooldown.get(sym, 0)
        if now - last_rev_ts >= REVERSAL_COOLDOWN_SEC:
            rsi_reversal_val = await get_rsi_for_tf(mexc_sym, RSI_REVERSAL_TF)
            rev = detect_short_reversal(
                sym, price, klines_1m, klines_5m, klines_15m,
                recent, growth, rsi, REVERSAL_MIN_SCORE,
                rsi_reversal_tf=rsi_reversal_val,
                rsi_reversal_tf_label=RSI_TF_LABELS.get(RSI_REVERSAL_TF, RSI_REVERSAL_TF),
            )
            if rev["triggered"]:
                # ── Умный фильтр повторов ───────────────────────
                last_info = _reversal_last.get(sym)
                if last_info:
                    price_change = abs(price - last_info["price"]) / last_info["price"] * 100
                    score_delta  = rev["score"] - last_info["score"]
                    # Пропускаем если: цена почти не изменилась И скор не вырос значительно
                    if (price_change < REVERSAL_REPEAT_PRICE_PCT
                            and score_delta < REVERSAL_REPEAT_SCORE_DELTA):
                        log.debug(
                            "REVERSAL skip %s: price_chg=%.2f%% score_delta=%d",
                            sym, price_change, score_delta,
                        )
                        return
                # ── Отправляем сигнал ───────────────────────────
                duration = calculate_growth_duration(recent)
                rev_text = format_reversal_alert(
                    sym, price, max(growth, peak_growth), source, rev,
                    duration_sec=duration,
                    window_sec=REVERSAL_WINDOW_SEC,
                )
                _reversal_cooldown[sym] = now
                _reversal_last[sym] = {"price": price, "score": rev["score"], "ts": now}
                db_save_reversal(
                    sym, price, rev["score"], rev["factors"],
                    rev["rsi"], rev["macd_hist"],
                    rev["stoch_rsi"], rev["bb_pct"],
                    rev["atr"], rev["target1"], rev["target2"], source,
                )
                await broadcast(rev_text)
                reversal_count += 1
                if PROM_AVAILABLE:
                    PROM_REVERSALS.inc()
                log.info("REVERSAL %s score=%d/20 peak=%.2f%% price_chg=%.2f%%",
                         sym, rev["score"], peak_growth,
                         abs(price - last_info["price"]) / last_info["price"] * 100 if last_info else 0.0)
                return  # не дублируем обычным алертом

    # ════════════════════════════════════════════════════════════
    #  БЛОК 1.5: УВЕДОМЛЕНИЕ О БОЛЬШОМ РОСТЕ / ПАДЕНИИ (15% и т.п.)
    # ════════════════════════════════════════════════════════════
    if NOTIFY_BIG_MOVE_PCT > 0 and abs(growth) >= NOTIFY_BIG_MOVE_PCT:
        last_notif = _notify_cooldown.get(sym, 0)
        if now - last_notif >= NOTIFY_BIG_MOVE_COOLDOWN_SEC:
            direction_emoji = "🚀" if growth > 0 else "📉"
            move_label = "РОСТ" if growth > 0 else "ПАДЕНИЕ"
            rsi_str = f"{rsi:.1f}" if rsi is not None else "—"
            dur_str = _fmt_dur(calculate_growth_duration(recent))
            notif_text = (
                f"🔔 <b>КРУПНОЕ ДВИЖЕНИЕ — {move_label}</b>\n\n"
                f"🪙 <b>{sym}</b>  [{source}]\n"
                f"💵 Цена: <code>{price}</code>\n"
                f"{direction_emoji} {move_label}: <b>{growth:+.2f}%</b> "
                f"за {dur_str}\n"
                f"📊 RSI: <code>{rsi_str}</code>\n\n"
                f"📋 <code>{sym}</code> #большое_движение"
            )
            _notify_cooldown[sym] = now
            await broadcast(notif_text)
            log.info("BigMove notify: %s %+.2f%%", sym, growth)

    # ════════════════════════════════════════════════════════════
    #  БЛОК 2: ОБЫЧНЫЕ АЛЕРТЫ (РОСТ / ПАДЕНИЕ)
    # ════════════════════════════════════════════════════════════
    if abs(growth) < current_percent:
        level = _cache_get_level(sym)
        if level and level["direction"] != direction:
            _cache_clear_level(sym)
        return

    # RSI-фильтр направления (настраиваемый ТФ и порог "не меньше" через кнопки)
    rsi_filter_val = rsi if RSI_SIGNAL_TF == "Min1" else await get_rsi_for_tf(mexc_sym, RSI_SIGNAL_TF)
    if rsi_filter_val is not None:
        if direction == 1 and rsi_filter_val < RSI_SIGNAL_LEVEL:
            return
        if direction == -1 and rsi_filter_val > (100 - RSI_SIGNAL_LEVEL):
            return

    last_sent = _alert_cooldown.get(sym, 0)
    if now - last_sent < ALERT_COOLDOWN_SEC:
        return

    level = _cache_get_level(sym)
    if level:
        prev_price = level["alert_price"]
        prev_dir   = level["direction"]
        if prev_dir == direction:
            if abs(price - prev_price) / prev_price * 100 < current_percent:
                return

    vals      = [p for _, p in price_history[sym][-120:]]
    macd_data = calculate_macd_full(vals)
    macd      = macd_data["histogram"]
    # RSI-тренд из уже загруженных свечей — не делаем лишний HTTP запрос
    rsi_trend = calculate_rsi_trend(closes) if len(closes) >= 17 else None
    accel     = calculate_acceleration(recent)
    duration  = calculate_growth_duration(recent)
    breakout  = check_24h_breakout(sym, price)
    day_ctx   = get_24h_context(sym, price)

    rsi_tf_label_show = RSI_TF_LABELS.get(RSI_SIGNAL_TF) if RSI_SIGNAL_TF != "Min1" else None
    if direction == 1:
        text = format_growth_alert(
            sym, price, growth, rsi, macd, source,
            rsi_trend=rsi_trend, accel=accel,
            duration_sec=duration, breakout=breakout, day_context=day_ctx,
            rsi_tf_label=rsi_tf_label_show, rsi_tf_val=rsi_filter_val,
            rsi_tf_level=RSI_SIGNAL_LEVEL if rsi_tf_label_show else None,
        )
    else:
        text = format_drop_alert(
            sym, price, growth, rsi, macd, source,
            rsi_trend=rsi_trend, duration_sec=duration,
            breakout=breakout, day_context=day_ctx,
            rsi_tf_label=rsi_tf_label_show, rsi_tf_val=rsi_filter_val,
            rsi_tf_level=RSI_SIGNAL_LEVEL if rsi_tf_label_show else None,
        )

    _cache_set_level(sym, price, direction)
    _alert_cooldown[sym] = now
    db_save_alert(sym, price, growth, rsi, macd, source)
    await broadcast(text)
    signals_count += 1

    if PROM_AVAILABLE:
        PROM_SIGNALS.inc()

    log.info("Signal: %s %+.2f%% [%s]", sym, growth, source)


async def monitor():
    global current_percent, current_window, signals_count, reversal_count, checks_count, last_check_time
    global REVERSAL_GROWTH_MIN_PCT, NOTIFY_BIG_MOVE_PCT

    symbols          = await get_symbols()
    last_symbols_upd = time.time()
    _cache_load_levels()

    await broadcast("✅ <b>Бот запущен</b> (v17 — 20-факторный разворот)")

    while True:
        if monitor_paused:
            await asyncio.sleep(2)
            continue

        try:
            now             = time.time()
            checks_count   += 1
            last_check_time = now

            if PROM_AVAILABLE:
                PROM_CHECKS.inc()
                PROM_COINS.set(len(price_history))

            if now - last_symbols_upd >= 1800:
                new_sym = await get_symbols()
                if new_sym:
                    symbols = new_sym
                    log.info("Монеты обновлены: %d", len(symbols))
                last_symbols_upd = now

            prices, sources = await get_prices(symbols)

            # ── Конкурентная обработка монет ───────────────────────────────
            # Раньше монеты проверялись строго последовательно (await за
            # await на каждую), из-за чего при сотнях монет уведомление по
            # 50-й монете могло прийти на десятки секунд позже, чем по 1-й,
            # а вся следующая итерация мониторинга начиналась ещё позже.
            # Теперь все монеты обрабатываются параллельно (с ограничением
            # MONITOR_CONCURRENCY одновременных запросов к MEXC), и каждое
            # уведомление отправляется сразу же, как только готово —
            # не дожидаясь обработки остальных монет.
            sem = asyncio.Semaphore(MONITOR_CONCURRENCY)

            async def _bounded(sym: str, price: float) -> None:
                async with sem:
                    try:
                        await _process_one_symbol(sym, price, now, sources.get(sym, "UNKNOWN"))
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        log.exception("process_symbol %s: %s", sym, e)

            await asyncio.gather(*(_bounded(sym, price) for sym, price in prices.items()))

            await asyncio.sleep(config.INTERVAL)

            # ── Периодическая очистка памяти ──────────────────────────────────
            if checks_count % 200 == 0:
                # Удаляем устаревшие кулдауны
                stale_ts = now - max(ALERT_COOLDOWN_SEC, REVERSAL_COOLDOWN_SEC) * 3
                for d in (_alert_cooldown, _reversal_cooldown, _notify_cooldown):
                    stale = [k for k, v in d.items() if v < stale_ts]
                    for k in stale:
                        d.pop(k, None)
            if checks_count % 1000 == 0:
                # Удаляем монеты без свежих данных
                cutoff2 = now - max(current_window * 4, 86400 * 2)
                dead = [s for s, h in price_history.items() if not h or h[-1][0] < cutoff2]
                for s in dead:
                    price_history.pop(s, None)
                if dead:
                    log.info("price_history: удалено %d мёртвых монет", len(dead))

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("monitor error: %s", e)
            await asyncio.sleep(5)


# ================================================================
#  COMMAND HANDLERS
# ================================================================

async def handle_message(msg: dict):
    global current_percent, current_window, monitor_paused
    global REVERSAL_MIN_SCORE, REVERSAL_RSI_OB, REVERSAL_STOCH_OB, REVERSAL_STOCH_EXT
    global REVERSAL_BB_OB, REVERSAL_MACD_SLOPE, REVERSAL_ACCEL, REVERSAL_HIGH_MARGIN
    global REVERSAL_MOMENTUM, REVERSAL_COOLDOWN_SEC, REVERSAL_ATR_MULT, REVERSAL_VOL_RATIO
    global REVERSAL_GROWTH_MIN_PCT, NOTIFY_BIG_MOVE_PCT, REVERSAL_WINDOW_SEC
    global REVERSAL_REPEAT_PRICE_PCT, REVERSAL_REPEAT_SCORE_DELTA
    global RSI_SIGNAL_TF, RSI_SIGNAL_LEVEL, RSI_REVERSAL_TF

    text    = msg.get("text", "").strip()
    chat_id = msg["chat"]["id"]

    if chat_id != int(config.CHAT_ID):
        if text in ("/start", "/subscribe"):
            db_add_subscriber(chat_id)
            await send_message(
                "🚀 <b>Crypto Alert Bot v19</b>\n\n"
                "✅ Вы подписаны на сигналы.\n"
                "Для отписки: /unsubscribe",
                chat_id,
            )
        elif text == "/unsubscribe":
            db_remove_subscriber(chat_id)
            await send_message("❌ Вы отписались от сигналов", chat_id)
        else:
            await send_message("⛔ Нет доступа к управлению. /subscribe — подписаться на алерты.", chat_id)
        return

    if text in ("/start", "/menu"):
        await send_message(
            f"🚀 <b>Crypto Alert Bot v19</b>\n\n"
            f"📈 Порог роста: <b>{current_percent}%</b>\n"
            f"⏱ Период алертов: <b>{current_window // 60} мин</b>\n"
            f"🔄 Порог разворота: <b>{REVERSAL_MIN_SCORE}/20 факторов</b>\n"
            f"📉 Рост для разворота: <b>{REVERSAL_GROWTH_MIN_PCT}%</b> за <b>{_fmt_dur(REVERSAL_WINDOW_SEC)}</b>\n"
            f"🔔 Уведомление движения: <b>{NOTIFY_BIG_MOVE_PCT}%</b>\n"
            f"📊 RSI-фильтр сигналов: ТФ <b>{RSI_TF_LABELS.get(RSI_SIGNAL_TF, RSI_SIGNAL_TF)}</b>, порог ≥<b>{RSI_SIGNAL_LEVEL}</b>\n"
            f"🔄📊 RSI-фильтр разворота: ТФ <b>{RSI_TF_LABELS.get(RSI_REVERSAL_TF, RSI_REVERSAL_TF)}</b>, порог &gt;<b>{REVERSAL_RSI_OB}</b>\n"
            f"{'⏸ Пауза активна' if monitor_paused else '▶️ Мониторинг активен'}\n\n"
            f"<b>Кнопки управления:</b>\n"
            f"  📈 0.2%/5%/10%/15%/20% — порог роста/падения алертов\n"
            f"  📉 Разворот 1-25% — минимальный рост для шорт-разворота\n"
            f"  🕐 Разворот 5м-1д — период расчёта роста разворота\n"
            f"  ⏱ 5 мин/1 час/4 ч/1 д — период окна алертов\n"
            f"  🎚 Порог 3-12/20 — чувствительность (кол-во факторов)\n"
            f"  📊 RSI ТФ 5м-1д — таймфрейм RSI для сигналов роста/падения\n"
            f"  📊 RSI ≥50-95 — порог RSI для сигналов роста/падения\n"
            f"  🔄📊 RSI ТФ 5м-1д — таймфрейм RSI для разворотного сигнала\n"
            f"  🔄📊 RSI ≥50-95 — порог RSI для разворотного сигнала\n"
            f"  ⚙️ Настройки разворота — все параметры\n"
            f"  🔄 Полный перезапуск — перезапуск без потери данных\n"
            f"  🆘 Помощь — краткая справка\n\n"
            f"<b>Команды:</b>\n"
            f"  /set_percent 2.5 — порог алертов (%)\n"
            f"  /set_window 60 — период алертов (мин)\n"
            f"  /rev_score 5 — порог факторов разворота\n"
            f"  /rev_growth 5 — % роста для разворота\n"
            f"  /rev_window 60 — период роста разворота (мин)\n"
            f"  /notify_pct 15 — порог уведомлений движения (%)\n"
            f"  /rev_cooldown 5 — кулдаун разворота (мин)\n"
            f"  /restart — перезапуск мониторинга",
            chat_id,
            reply_markup=reply_keyboard(),
        )
        return

    _pct_map = {
        "📈 0.2%": 0.2, "📈 5%": 5.0, "📈 10%": 10.0, "📈 15%": 15.0, "📈 20%": 20.0,
    }
    if text in _pct_map:
        current_percent = _pct_map[text]
        await send_message(f"✅ Порог роста/падения: <b>{current_percent}%</b>", chat_id)
        return

    _win_map = {
        "⏱ 5 мин": (300,   "5 мин"),
        "⏱ 1 час": (3600,  "1 час"),
        "⏱ 4 ч":   (14400, "4 часа"),
        "⏱ 1 д":   (86400, "1 день"),
    }
    if text in _win_map:
        current_window, label = _win_map[text]
        await send_message(f"✅ Период: <b>{label}</b>", chat_id)
        return

    if text == "⏸ Пауза":
        monitor_paused = True
        await send_message("⏸ Мониторинг приостановлен", chat_id)
        return

    if text == "▶️ Продолжить":
        monitor_paused = False
        await send_message("▶️ Мониторинг возобновлён", chat_id)
        return

    if text in ("🆘 Помощь", "/help"):
        await send_message(
            f"🚀 <b>Crypto Alert Bot v19 — справка</b>\n\n"
            f"📈 Порог роста: <b>{current_percent}%</b>\n"
            f"⏱ Период алертов: <b>{current_window // 60} мин</b>\n"
            f"🔄 Порог разворота: <b>{REVERSAL_MIN_SCORE}/20 факторов</b>\n"
            f"📊 RSI сигналов: ТФ <b>{RSI_TF_LABELS.get(RSI_SIGNAL_TF, RSI_SIGNAL_TF)}</b>, ≥<b>{RSI_SIGNAL_LEVEL}</b>\n"
            f"🔄📊 RSI разворота: ТФ <b>{RSI_TF_LABELS.get(RSI_REVERSAL_TF, RSI_REVERSAL_TF)}</b>, &gt;<b>{REVERSAL_RSI_OB}</b>\n\n"
            f"<b>Кнопки управления:</b>\n"
            f"  📈 0.2%/5%/10%/15%/20% — порог роста/падения алертов\n"
            f"  📉 Разворот 1-25% — мин. рост для шорт-разворота\n"
            f"  🕐 Разворот 5м-1д — период расчёта роста разворота\n"
            f"  ⏱ 5 мин/1 час/4 ч/1 д — период окна алертов\n"
            f"  🎚 Порог 3-12/20 — чувствительность разворота\n"
            f"  📊 RSI ТФ / RSI ≥ — таймфрейм и порог RSI для роста/падения\n"
            f"  🔄📊 RSI ТФ / RSI ≥ — таймфрейм и порог RSI для разворота\n"
            f"  ⚙️ Настройки разворота — все параметры детально\n"
            f"  🔄 Полный перезапуск — перезапуск процесса без потери данных\n\n"
            f"Полный список команд: /menu",
            chat_id,
        )
        return

    if text in ("🔄 Полный перезапуск", "/restart"):
        last = _restart_cooldown.get(chat_id, 0)
        if time.time() - last < RESTART_COOLDOWN_SEC:
            wait_left = int(RESTART_COOLDOWN_SEC - (time.time() - last))
            await send_message(f"⏳ Подождите {wait_left}с перед повторным перезапуском", chat_id)
            return
        _restart_cooldown[chat_id] = time.time()
        await send_message(
            "🔄 <b>Перезапуск мониторинга...</b>\n"
            "ℹ️ История цен, БД и настройки сохраняются — перезапускается только процесс анализа.",
            chat_id,
        )
        asyncio.create_task(_cmd_full_restart(chat_id))
        return

    if text in ("📊 Статус", "/status"):
        uptime = int(time.time() - start_time)
        d, r   = divmod(uptime, 86400)
        h, r   = divmod(r, 3600)
        m      = r // 60
        await send_message(
            f"📊 <b>СТАТУС</b>\n\n"
            f"🟢 Аптайм: {d}д {h}ч {m}м\n"
            f"🪙 Монет в истории: {len(price_history)}\n"
            f"🔔 Сигналов: {signals_count}\n"
            f"🔄 Разворотов: {reversal_count}\n"
            f"🔄 Порог разворота: {REVERSAL_MIN_SCORE}/20\n"
            f"📉 Рост для разворота: {REVERSAL_GROWTH_MIN_PCT}% за {_fmt_dur(REVERSAL_WINDOW_SEC)}\n"
            f"🔔 Уведомление движения: {NOTIFY_BIG_MOVE_PCT}%\n"
            f"📊 RSI сигналов: ТФ {RSI_TF_LABELS.get(RSI_SIGNAL_TF, RSI_SIGNAL_TF)}, порог ≥{RSI_SIGNAL_LEVEL}\n"
            f"🔄📊 RSI разворота: ТФ {RSI_TF_LABELS.get(RSI_REVERSAL_TF, RSI_REVERSAL_TF)}, порог >{REVERSAL_RSI_OB}\n"
            f"🔄 Кулдаун разворота: {REVERSAL_COOLDOWN_SEC // 60} мин\n"
            f"📈 Порог роста: {current_percent}%\n"
            f"⏱ Период: {current_window // 60} мин\n"
            f"⚡ Интервал: {config.INTERVAL} сек\n"
            f"👥 Подписчиков: {len(db_get_subscribers())}\n"
            f"{'⏸ Пауза' if monitor_paused else '▶️ Активен'}",
            chat_id,
        )
        return

    if text in ("📋 История", "/history"):
        asyncio.create_task(_cmd_history(chat_id))
        return

    if text in ("🏆 Топ-5", "/top5"):
        asyncio.create_task(_cmd_top5(chat_id))
        return

    if text in ("📤 Экспорт", "/export"):
        asyncio.create_task(_cmd_export(chat_id))
        return

    if text in ("🗑 Кулдауны", "/clear_cooldowns"):
        _cache_clear_all()
        _reversal_cooldown.clear()
        _reversal_last.clear()
        _notify_cooldown.clear()
        await send_message("🗑 Кулдауны, уровни и история разворотов сброшены", chat_id)
        return

    if text in ("🔄 Развороты", "/reversals"):
        asyncio.create_task(_cmd_reversals(chat_id))
        return

    if text in ("⚙️ Настройки разворота", "⚙️ Разворот", "/reversal_settings"):
        await _cmd_reversal_settings(chat_id)
        return

    if text in ("🗄 БД Статистика", "/db_stats"):
        s = db_stats()
        await send_message(
            f"🗄 <b>База данных</b>\n\n"
            f"📋 Алертов: {s['alerts']}\n"
            f"📅 Старейший: {s['oldest_alert']}\n"
            f"🪙 Уровней: {s['levels']}\n"
            f"🔄 Разворотов: {s['reversals']}\n"
            f"💾 Размер: {s['size_mb']:.2f} МБ",
            chat_id,
        )
        return

    if text in ("🧹 БД Очистка", "/db_cleanup"):
        deleted = db_cleanup()
        total   = sum(deleted.values())
        await send_message(
            f"🧹 <b>Очистка завершена</b>\n\n"
            f"📋 Алертов: -{deleted.get('alerts', 0)}\n"
            f"📍 Уровней: -{deleted.get('alert_levels', 0)}\n"
            f"📊 price_stats: -{deleted.get('price_stats', 0)}\n"
            f"🔄 Разворотов: -{deleted.get('reversal_signals', 0)}\n"
            f"────────────────\n"
            f"🗑 Итого: {total} строк\n"
            f"💾 Размер: {db_size_mb():.2f} МБ",
            chat_id,
        )
        return

    # Кнопки % роста для разворотных сигналов в шорт
    _rev_growth_map = {
        "📉 Разворот 1%":  1.0,
        "📉 Разворот 5%":  5.0,
        "📉 Разворот 10%": 10.0,
        "📉 Разворот 15%": 15.0,
        "📉 Разворот 25%": 25.0,
    }
    if text in _rev_growth_map:
        REVERSAL_GROWTH_MIN_PCT = _rev_growth_map[text]
        await send_message(
            f"✅ Порог роста для разворота в шорт: <b>{REVERSAL_GROWTH_MIN_PCT}%</b>\n"
            f"ℹ️ Разворотный детектор сработает если рост ≥ {REVERSAL_GROWTH_MIN_PCT}%",
            chat_id,
        )
        return

    if text.startswith("/notify_pct"):
        try:
            val = float(text.split()[1])
            assert 1.0 <= val <= 100.0
            NOTIFY_BIG_MOVE_PCT = val
            _notify_cooldown.clear()
            await send_message(f"✅ Уведомление о движении: <b>≥ {val}%</b>", chat_id)
        except Exception:
            await send_message(
                f"❌ /notify_pct 15   (текущее: {NOTIFY_BIG_MOVE_PCT}%)\n"
                f"Диапазон: 1–100", chat_id
            )
        return

    if text.startswith("/rev_growth"):
        try:
            val = float(text.split()[1])
            assert 0.1 <= val <= 100.0
            REVERSAL_GROWTH_MIN_PCT = val
            await send_message(
                f"✅ Порог роста для разворота: <b>{val}%</b>", chat_id
            )
        except Exception:
            await send_message(
                f"❌ /rev_growth 5   (текущее: {REVERSAL_GROWTH_MIN_PCT}%)\n"
                f"Диапазон: 0.1–100", chat_id
            )
        return

    # Кнопки периода окна для расчёта роста разворота
    _rev_window_map = {
        "🕐 Разворот 5м":  (300,   "5 минут"),
        "🕐 Разворот 30м": (1800,  "30 минут"),
        "🕐 Разворот 1ч":  (3600,  "1 час"),
        "🕐 Разворот 4ч":  (14400, "4 часа"),
        "🕐 Разворот 1д":  (86400, "1 день"),
    }
    if text in _rev_window_map:
        REVERSAL_WINDOW_SEC, label = _rev_window_map[text]
        await send_message(
            f"✅ Окно роста для разворота: <b>{label}</b>\n"
            f"ℹ️ Разворотный детектор смотрит рост монеты за последние <b>{label}</b>.\n"
            f"Порог: <b>{REVERSAL_GROWTH_MIN_PCT}%</b> за этот период.",
            chat_id,
        )
        return

    if text.startswith("/rev_window"):
        try:
            val = int(text.split()[1])
            assert 1 <= val <= 10080
            REVERSAL_WINDOW_SEC = val * 60
            await send_message(
                f"✅ Окно разворота: <b>{val} мин</b> ({_fmt_dur(REVERSAL_WINDOW_SEC)})", chat_id
            )
        except Exception:
            cur = _fmt_dur(REVERSAL_WINDOW_SEC)
            await send_message(
                f"❌ /rev_window 60   (в минутах, 1–10080)\nТекущее: {cur}", chat_id
            )
        return

    if text.startswith("/rev_price_pct"):
        try:
            val = float(text.split()[1])
            assert 0.1 <= val <= 20.0
            REVERSAL_REPEAT_PRICE_PCT = val
            await send_message(
                f"✅ Мин. смена цены между разворотами: <b>{val}%</b>\n"
                f"ℹ️ Повторный сигнал по монете только если цена изменилась на ≥{val}%",
                chat_id,
            )
        except Exception:
            await send_message(
                f"❌ /rev_price_pct 3   (текущее: {REVERSAL_REPEAT_PRICE_PCT}%)\nДиапазон: 0.1–20",
                chat_id,
            )
        return

    if text.startswith("/rev_score_delta"):
        try:
            val = int(text.split()[1])
            assert 0 <= val <= 20
            REVERSAL_REPEAT_SCORE_DELTA = val
            await send_message(
                f"✅ Мин. прирост скора между разворотами: <b>+{val}</b>\n"
                f"ℹ️ Повторный сигнал только если скор вырос на ≥{val} факторов",
                chat_id,
            )
        except Exception:
            await send_message(
                f"❌ /rev_score_delta 2   (текущее: {REVERSAL_REPEAT_SCORE_DELTA})\nДиапазон: 0–20",
                chat_id,
            )
        return

    # Быстрая настройка порога разворота через кнопки (из 20 факторов)
    _rev_score_map = {
        "🎚 Порог 3/20":  3,
        "🎚 Порог 5/20":  5,
        "🎚 Порог 7/20":  7,
        "🎚 Порог 9/20":  9,
        "🎚 Порог 12/20": 12,
        "🎚 Порог 15/20": 15,
    }
    if text in _rev_score_map:
        REVERSAL_MIN_SCORE = _rev_score_map[text]  # global объявлен выше — OK
        await send_message(
            f"✅ Порог разворота: <b>{REVERSAL_MIN_SCORE}/20 факторов</b>\n"
            f"ℹ️ Агрессивный: 3, Мягкий: 5, Стандарт: 7, Строгий: 9, "
            f"Очень строгий: 12, Экстремальный: 15",
            chat_id,
        )
        return

    # ── RSI-фильтр обычных сигналов роста/падения: таймфрейм ──────────────────
    _rsi_signal_tf_map = {
        "📊 RSI ТФ 5м":  "Min5",
        "📊 RSI ТФ 15м": "Min15",
        "📊 RSI ТФ 1ч":  "Min60",
        "📊 RSI ТФ 4ч":  "Hour4",
        "📊 RSI ТФ 12ч": "Hour12",
        "📊 RSI ТФ 1д":  "Day1",
    }
    if text in _rsi_signal_tf_map:
        RSI_SIGNAL_TF = _rsi_signal_tf_map[text]
        lbl = RSI_TF_LABELS.get(RSI_SIGNAL_TF, RSI_SIGNAL_TF)
        await send_message(
            f"✅ RSI-фильтр сигналов роста/падения: ТФ <b>{lbl}</b>\n"
            f"ℹ️ Данные берутся напрямую с MEXC (точный RSI по выбранному ТФ).\n"
            f"Текущий порог: ≥{RSI_SIGNAL_LEVEL}",
            chat_id,
        )
        return

    # ── RSI-фильтр обычных сигналов роста/падения: порог "не меньше" ──────────
    _rsi_signal_lvl_map = {
        "📊 RSI ≥50": 50.0, "📊 RSI ≥60": 60.0, "📊 RSI ≥70": 70.0,
        "📊 RSI ≥80": 80.0, "📊 RSI ≥90": 90.0, "📊 RSI ≥95": 95.0,
    }
    if text in _rsi_signal_lvl_map:
        RSI_SIGNAL_LEVEL = _rsi_signal_lvl_map[text]
        await send_message(
            f"✅ RSI-порог сигналов: <b>≥{RSI_SIGNAL_LEVEL}</b>\n"
            f"ℹ️ Сигнал РОСТА проходит только если RSI ≥ {RSI_SIGNAL_LEVEL}.\n"
            f"Сигнал ПАДЕНИЯ проходит только если RSI ≤ {100 - RSI_SIGNAL_LEVEL}.\n"
            f"Работает вместе с фильтром роста/падения на текущем ТФ "
            f"({RSI_TF_LABELS.get(RSI_SIGNAL_TF, RSI_SIGNAL_TF)}).",
            chat_id,
        )
        return

    # ── RSI-фильтр РАЗВОРОТА: таймфрейм (отдельно от обычных сигналов) ────────
    _rsi_rev_tf_map = {
        "🔄📊 RSI ТФ 5м":  "Min5",
        "🔄📊 RSI ТФ 15м": "Min15",
        "🔄📊 RSI ТФ 1ч":  "Min60",
        "🔄📊 RSI ТФ 4ч":  "Hour4",
        "🔄📊 RSI ТФ 12ч": "Hour12",
        "🔄📊 RSI ТФ 1д":  "Day1",
    }
    if text in _rsi_rev_tf_map:
        RSI_REVERSAL_TF = _rsi_rev_tf_map[text]
        lbl = RSI_TF_LABELS.get(RSI_REVERSAL_TF, RSI_REVERSAL_TF)
        await send_message(
            f"✅ RSI для разворотного сигнала: ТФ <b>{lbl}</b>\n"
            f"ℹ️ Используется в факторе 15/20 («старший ТФ перекуплен»).\n"
            f"Текущий порог: >{REVERSAL_RSI_OB}",
            chat_id,
        )
        return

    # ── RSI-фильтр РАЗВОРОТА: порог "не меньше" ────────────────────────────────
    _rsi_rev_lvl_map = {
        "🔄📊 RSI ≥50": 50.0, "🔄📊 RSI ≥60": 60.0, "🔄📊 RSI ≥70": 70.0,
        "🔄📊 RSI ≥80": 80.0, "🔄📊 RSI ≥90": 90.0, "🔄📊 RSI ≥95": 95.0,
    }
    if text in _rsi_rev_lvl_map:
        REVERSAL_RSI_OB = _rsi_rev_lvl_map[text]  # global объявлен выше — OK
        await send_message(
            f"✅ RSI-порог разворота: <b>&gt;{REVERSAL_RSI_OB}</b>\n"
            f"ℹ️ Применяется к фактору 1 (Min1) и фактору 15 (ТФ "
            f"{RSI_TF_LABELS.get(RSI_REVERSAL_TF, RSI_REVERSAL_TF)}).",
            chat_id,
        )
        return

    # ── Команды настройки разворота ───────────────────────────────────────────
    _rev_cmds = {
        "/rev_score":    ("REVERSAL_MIN_SCORE",   int,   1,    20,    "Порог факторов",           "/20"),
        "/rev_rsi":      ("REVERSAL_RSI_OB",      float, 50,   95,    "RSI перекупленность",      ""),
        "/rev_stoch":    ("REVERSAL_STOCH_OB",    float, 0.5,  1.0,   "StochRSI порог",           ""),
        "/rev_bb":       ("REVERSAL_BB_OB",       float, 0.8,  1.5,   "Боллинджер %B порог",      ""),
        "/rev_accel":    ("REVERSAL_ACCEL",       float, 0.1,  1.0,   "Порог замедления",         ""),
        "/rev_momentum": ("REVERSAL_MOMENTUM",    float, -5.0, 0,     "Порог моментума",          "%"),
        "/rev_cooldown": ("REVERSAL_COOLDOWN_SEC",int,   1,    1440,  "Кулдаун разворота (мин)",  " мин"),
        "/rev_atr":      ("REVERSAL_ATR_MULT",    float, 1.0,  10.0,  "ATR-перегрев множитель",   "x"),
        "/rev_vol":      ("REVERSAL_VOL_RATIO",   float, 0.1,  1.0,   "Порог слабого объёма",     ""),
    }
    for cmd, (var, typ, vmin, vmax, label, sfx) in _rev_cmds.items():
        if text.startswith(cmd):
            try:
                raw = text.split()[1]
                val = typ(raw)
                assert vmin <= val <= vmax
                # Для кулдауна храним в секундах, но вводим/показываем в минутах
                stored = val * 60 if var == "REVERSAL_COOLDOWN_SEC" else val
                globals()[var] = stored
                await send_message(f"✅ {label}: <b>{val}{sfx}</b>", chat_id)
            except Exception:
                current = globals()[var]
                if var == "REVERSAL_COOLDOWN_SEC":
                    current = current // 60  # показываем в минутах, как вводится
                await send_message(
                    f"❌ {cmd} {vmin}…{vmax}  (текущее: {current})", chat_id
                )
            return

    if text.startswith("/rev_high"):
        try:
            val = float(text.split()[1])
            assert 0.0 <= val <= 5.0
            REVERSAL_HIGH_MARGIN = 1.0 - val / 100  # global объявлен выше — OK
            await send_message(f"✅ Зона отбоя от хая: <b>{val}%</b> (margin={REVERSAL_HIGH_MARGIN:.4f})", chat_id)
        except Exception:
            cur_pct = round((1.0 - REVERSAL_HIGH_MARGIN) * 100, 2)
            await send_message(f"❌ /rev_high 0.2  (отступ от хая в %, 0–5)\nТекущее: {cur_pct}%", chat_id)
        return

    if text.startswith("/set_percent"):
        try:
            val = float(text.split()[1])
            assert 0.01 <= val <= 100
            current_percent = val
            await send_message(f"✅ Новый порог: <b>{val}%</b>", chat_id)
        except Exception:
            await send_message("❌ /set_percent 2.5", chat_id)
        return

    if text.startswith("/set_window"):
        try:
            val = int(text.split()[1])
            assert 1 <= val <= 10080
            current_window = val * 60
            await send_message(f"✅ Новый период: <b>{val} мин</b>", chat_id)
        except Exception:
            await send_message("❌ /set_window 60  (в минутах)", chat_id)
        return

    if text == "/subscribe":
        db_add_subscriber(chat_id)
        await send_message("✅ Вы в списке получателей", chat_id)
        return

    await send_message(
        "❓ Неизвестная команда.\n/menu — открыть панель управления",
        chat_id,
        reply_markup=reply_keyboard(),
    )


async def _cmd_history(chat_id):
    rows = db_recent_alerts(10)
    if not rows:
        await send_message("📋 История пуста", chat_id)
        return
    lines = ["📋 <b>Последние 10 сигналов:</b>\n"]
    for r in rows:
        ts    = time.strftime("%d.%m %H:%M", time.localtime(r["ts"]))
        sign  = "🚀" if r["growth"] > 0 else "📉"
        rsi_s = f"RSI {r['rsi']:.1f}" if r["rsi"] is not None else "RSI —"
        lines.append(f"{sign} <b>{r['symbol']}</b> {r['growth']:+.2f}% | {rsi_s} | {ts}")
    await send_message("\n".join(lines), chat_id)


async def _cmd_top5(chat_id):
    rows = db_top_signals(5)
    if not rows:
        await send_message("🏆 Нет данных за 24ч", chat_id)
        return
    lines = ["🏆 <b>Топ-5 сигналов за 24ч:</b>\n"]
    for i, r in enumerate(rows, 1):
        ts   = time.strftime("%H:%M", time.localtime(r["ts"]))
        sign = "🚀" if r["growth"] > 0 else "📉"
        lines.append(f"{i}. {sign} <b>{r['symbol']}</b> {r['growth']:+.2f}% [{r['source']}] {ts}")
    await send_message("\n".join(lines), chat_id)


async def _cmd_reversals(chat_id):
    rows = db_recent_reversals(8)
    if not rows:
        await send_message("🔄 Разворотных сигналов пока нет", chat_id)
        return
    lines = ["🔄 <b>Последние 8 разворотов:</b>\n"]
    for r in rows:
        ts      = time.strftime("%d.%m %H:%M", time.localtime(r["ts"]))
        score   = r["score"]
        lvl     = "🔴" if score >= 12 else "🟠" if score >= 7 else "🟡"
        factors = r["factors"]
        # Экранируем HTML-символы в тексте фактора
        f_brief = factors[0].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") if factors else "—"
        t1_s    = f" → цель {r['target1']:.4g}" if r.get("target1") else ""
        lines.append(
            f"{lvl} <b>{r['symbol']}</b> [{score}/20] {ts}{t1_s}\n"
            f"   <i>{f_brief}</i>"
        )
    await send_message("\n".join(lines), chat_id)


async def _cmd_reversal_settings(chat_id):
    high_pct = round((1.0 - REVERSAL_HIGH_MARGIN) * 100, 2)
    await send_message(
        f"⚙️ <b>Настройки детектора разворота (20 факторов)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>Скоринг:</b>\n"
        f"  Порог:         <code>{REVERSAL_MIN_SCORE}/20</code>  → /rev_score 5\n\n"
        f"<b>Пороги факторов:</b>\n"
        f"  RSI OB (1m):   <code>&gt; {REVERSAL_RSI_OB}</code>     → /rev_rsi 70 (или кнопки 🔄📊 RSI ≥)\n"
        f"  RSI OB (старший ТФ): <code>&gt; {REVERSAL_RSI_OB}</code> на ТФ "
        f"<code>{RSI_TF_LABELS.get(RSI_REVERSAL_TF, RSI_REVERSAL_TF)}</code>  → кнопки 🔄📊 RSI ТФ\n"
        f"  StochRSI:      <code>&gt; {REVERSAL_STOCH_OB}</code>   → /rev_stoch 0.80\n"
        f"  Боллинджер:    <code>&gt; {REVERSAL_BB_OB}</code>      → /rev_bb 1.0\n"
        f"  Замедление:    <code>&lt; {REVERSAL_ACCEL}</code>      → /rev_accel 0.5\n"
        f"  Моментум:      <code>&lt; {REVERSAL_MOMENTUM}%</code>  → /rev_momentum -0.5\n"
        f"  ATR-перегрев:  <code>&gt; {REVERSAL_ATR_MULT}×</code> → /rev_atr 3.0\n"
        f"  Объём слабый:  <code>&lt; {REVERSAL_VOL_RATIO:.0%}</code>  → /rev_vol 0.7\n"
        f"  Зона хая 24h:  <code>{high_pct}%</code>        → /rev_high 0.2\n"
        f"  Wick Rejection:<code>&gt; 0.55</code>         (фиксировано)\n\n"
        f"<b>Факторы 13-16:</b>\n"
        f"  13. OBV-дивергенция (Min15, цена↑ OBV↓)\n"
        f"  14. Wick Rejection Ratio &gt;0.55 (Min1)\n"
        f"  15. RSI {RSI_TF_LABELS.get(RSI_REVERSAL_TF, RSI_REVERSAL_TF)} &gt;{REVERSAL_RSI_OB} (настраиваемый старший таймфрейм)\n"
        f"  16. Lower Highs (Min5, 3+ убывающих хая)\n\n"
        f"<b>Факторы 17-20:</b>\n"
        f"  17. MACD гистограмма падает (Min1)\n"
        f"  18. Цена растянута &gt;1.5% над EMA21 (Min5)\n"
        f"  19. Серия из 3+ красных свечей (Min1)\n"
        f"  20. StochRSI разворот вниз из экстремума (Min1)\n\n"
        f"<b>Кулдаун и фильтр дублей:</b>\n"
        f"  Кулдаун:         <code>{REVERSAL_COOLDOWN_SEC // 60} мин</code>  → /rev_cooldown 30\n"
        f"  Мин. смена цены: <code>{REVERSAL_REPEAT_PRICE_PCT}%</code>  → /rev_price_pct 3\n"
        f"  Мин. рост скора: <code>+{REVERSAL_REPEAT_SCORE_DELTA}</code>      → /rev_score_delta 2\n\n"
        f"<b>Пресеты:</b>\n"
        f"  Агрессивный: /rev_score 3  /rev_cooldown 10  /rev_price_pct 1\n"
        f"  Стандарт:    /rev_score 7  /rev_cooldown 30  /rev_price_pct 3\n"
        f"  Строгий:     /rev_score 12  /rev_cooldown 60  /rev_price_pct 5",
        chat_id,
    )


async def _cmd_full_restart(chat_id):
    """Перезапускает asyncio-задачу monitor() без потери price_history/БД/настроек."""
    try:
        await restart_monitor(reason=f"кнопка перезапуска (chat_id={chat_id})")
        await send_message(
            "✅ <b>Мониторинг перезапущен</b>\n"
            f"🪙 Монет в истории: {len(price_history)} (сохранены)\n"
            f"🔔 Сигналов всего: {signals_count} | 🔄 Разворотов: {reversal_count}",
            chat_id,
        )
    except Exception as e:
        log.exception("_cmd_full_restart: %s", e)
        await send_message(f"❌ Ошибка перезапуска: {_esc(str(e))}", chat_id)


async def _cmd_export(chat_id):
    await send_message("📤 Генерирую CSV...", chat_id)
    csv_data = db_export_csv(500)
    fname    = f"alerts_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    await send_document(chat_id, fname, csv_data, caption="📤 Последние 500 алертов")


# ================================================================
#  TELEGRAM LOOP
# ================================================================

async def get_updates() -> tuple[list, bool]:
    """Возвращает (updates, ok). ok=False при ошибке (включая 409 Conflict)."""
    global offset
    try:
        async with _session.get(
            f"{TG}/getUpdates",
            params={"timeout": 30, "offset": offset},
            timeout=aiohttp.ClientTimeout(total=35),
        ) as resp:
            data = await resp.json()
        if data.get("ok"):
            results = data["result"]
            if results:
                log.info("get_updates: получено %d обновлений, offset=%s", len(results), offset)
            return results, True
        else:
            log.warning("get_updates not ok: %s", data)
            return [], False
    except Exception as e:
        log.error("get_updates: %s", e)
        return [], False


async def telegram_loop():
    global offset
    # При ошибках (включая 409 Conflict) делаем экспоненциальный backoff,
    # вместо мгновенного повтора каждые 0.2с. Без этого бот при конфликте
    # двух инстансов бомбардирует Telegram запросами ~5 раз в секунду,
    # что не даёт конфликту разрешиться и забивает логи.
    backoff_sec      = 1.0
    BACKOFF_MAX      = 30.0
    consecutive_409  = 0
    CONFLICT_WARN_EVERY = 10  # предупреждать в чат не на каждой ошибке, а раз в N подряд

    while True:
        try:
            updates, ok = await get_updates()
            if ok:
                if backoff_sec != 1.0 or consecutive_409 > 0:
                    log.info("get_updates восстановлен после %d ошибок подряд", consecutive_409)
                backoff_sec     = 1.0
                consecutive_409 = 0
                for update in updates:
                    offset = update["update_id"] + 1
                    log.info("UPDATE: %s", update)
                    if "message" in update:
                        asyncio.create_task(handle_message(update["message"]))
                    elif "callback_query" in update:
                        asyncio.create_task(handle_message(update["callback_query"]["message"]))
                await asyncio.sleep(0.2)
            else:
                consecutive_409 += 1
                if consecutive_409 == CONFLICT_WARN_EVERY:
                    # Шлём владельцу предупреждение один раз, не на каждую ошибку,
                    # чтобы не заспамить чат при длительном конфликте инстансов.
                    try:
                        await send_message(
                            "⚠️ <b>Постоянный 409 Conflict от Telegram</b>\n"
                            f"Уже {consecutive_409} ошибок подряд.\n"
                            "Похоже, где-то ещё запущен второй процесс этого бота "
                            "с тем же токеном (другой сервер/Railway-сервис/локальный запуск). "
                            "Найдите и остановите дублирующий инстанс.",
                            int(config.CHAT_ID),
                        )
                    except Exception:
                        pass  # если и эта отправка не проходит — не критично, увидим в логах
                await asyncio.sleep(backoff_sec)
                backoff_sec = min(BACKOFF_MAX, backoff_sec * 2)
        except Exception as e:
            log.exception("telegram_loop: %s", e)
            await asyncio.sleep(backoff_sec)
            backoff_sec = min(BACKOFF_MAX, backoff_sec * 2)


# ================================================================
#  BACKGROUND TASKS
# ================================================================

async def heartbeat():
    while True:
        log.info("♥ alive | signals=%d | reversals=%d | coins=%d",
                 signals_count, reversal_count, len(price_history))
        await asyncio.sleep(300)


async def save_state_loop():
    while True:
        try:
            save_state()
        except Exception:
            pass
        await asyncio.sleep(30)


async def db_cleanup_loop():
    last_vacuum = time.time()
    while True:
        await asyncio.sleep(6 * 3600)
        try:
            deleted = db_cleanup()
            total   = sum(deleted.values())
            stats   = db_stats()
            log.info("DB cleanup: удалено %d | %.2f МБ | алертов %d",
                     total, stats["size_mb"], stats["alerts"])
            now = time.time()
            if total > 0 and now - last_vacuum >= DB_VACUUM_INTERVAL_H * 3600:
                log.info("DB VACUUM...")
                await asyncio.get_event_loop().run_in_executor(None, db_vacuum)
                last_vacuum = now
                log.info("VACUUM done | %.2f МБ", db_size_mb())
        except Exception as e:
            log.exception("db_cleanup_loop: %s", e)


# ================================================================
#  WATCHDOG
# ================================================================

WATCHDOG_TIMEOUT = 90


async def restart_monitor(reason: str = "ручной перезапуск") -> None:
    """
    Безопасно перезапускает задачу monitor() БЕЗ сброса данных:
    price_history, БД, настройки (current_percent, REVERSAL_*, кулдауны)
    остаются нетронутыми — только пересоздаётся asyncio.Task.
    """
    global monitor_task, last_check_time
    log.warning("Перезапуск monitor(): %s", reason)
    if monitor_task and not monitor_task.done():
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.exception("restart_monitor: ошибка при отмене старой задачи: %s", e)
    monitor_task    = asyncio.create_task(monitor())
    last_check_time = time.time()


async def watchdog():
    global monitor_task, last_check_time
    await asyncio.sleep(60)
    while True:
        try:
            if not monitor_paused:
                stall = (time.time() - last_check_time) if last_check_time else 0
                if stall > WATCHDOG_TIMEOUT:
                    log.warning("Watchdog: завис %.0f с — перезапуск", stall)
                    await broadcast(f"⚠️ Watchdog: завис {stall:.0f}с. Перезапуск...")
                    await restart_monitor(reason=f"watchdog: завис {stall:.0f}с")
                    await broadcast("✅ Monitor перезапущен")
        except Exception as e:
            log.exception("watchdog: %s", e)
        await asyncio.sleep(30)


# ================================================================
#  SHUTDOWN
# ================================================================

_shutdown_event: asyncio.Event | None = None


def _handle_signal(sig):
    log.info("Signal %s — завершение", sig)
    save_state()
    if _shutdown_event:
        _shutdown_event.set()


def handle_async_exception(loop, context):
    exc = context.get("exception")
    if exc:
        log.exception("Unhandled async exception", exc_info=exc)
    else:
        log.error("Async error: %s", context["message"])


# ================================================================
#  MAIN
# ================================================================

async def main():
    global monitor_task, _session, _shutdown_event

    _shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_async_exception)

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal, sig)

    if PROM_AVAILABLE:
        prom_port = getattr(config, "PROM_PORT", 8000)
        try:
            start_http_server(prom_port)
            log.info("Prometheus on :%d", prom_port)
        except Exception as e:
            log.warning("Prometheus failed: %s", e)

    connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        _session     = session
        monitor_task = asyncio.create_task(monitor())

        # Удаляем вебхук если активен (иначе getUpdates не работает)
        try:
            async with session.get(f"{TG}/deleteWebhook", params={"drop_pending_updates": "false"}) as resp:
                result = await resp.json()
                if result.get("ok"):
                    log.info("Вебхук удалён успешно")
                else:
                    log.warning("deleteWebhook: %s", result)
        except Exception as e:
            log.warning("deleteWebhook error: %s", e)

        # Отправляем клавиатуру владельцу при каждом запуске бота
        try:
            await send_message(
                "🤖 <b>Бот запущен</b> — клавиатура активирована",
                int(config.CHAT_ID),
                reply_markup=reply_keyboard(),
            )
        except Exception as e:
            log.warning("Не удалось отправить стартовое сообщение: %s", e)

        tasks = [
            monitor_task,
            asyncio.create_task(telegram_loop()),
            asyncio.create_task(heartbeat()),
            asyncio.create_task(save_state_loop()),
            asyncio.create_task(db_cleanup_loop()),
            asyncio.create_task(watchdog()),
            asyncio.create_task(_shutdown_event.wait()),
        ]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        log.info("Завершение...")
        for t in pending:
            if not t.done():
                t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    log.info("Бот остановлен")


# ── Захватываем single-instance lock ОДИН раз на весь процесс, ДО входа
#    в retry-цикл. Если лок брать внутри main(), при каждом внутреннем
#    перезапуске (после необработанного исключения) возникало бы окно
#    release→acquire, в которое мог проскочить второй конкурентный процесс.
acquire_single_instance_lock()
try:
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            break
        except Exception as e:
            log.exception("Критическая ошибка: %s", e)
            time.sleep(10)
finally:
    release_single_instance_lock()
