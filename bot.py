import sys
import re
import os
import json
import time
import math
import datetime
import threading
import urllib.request
import urllib.parse
import urllib.error
from concurrent.futures import ThreadPoolExecutor

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from groq import Groq
from pydantic import BaseModel
from typing import Optional, List

try:
    import pallet_handler
except ImportError:
    pallet_handler = None
    print("[INIT] pallet_handler not available", flush=True)

# ─── Конфиг ───────────────────────────────────────────────────────────────

TOKEN = os.environ.get("MAX_BOT_TOKEN", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

DATA_DIR = "/data" if os.path.exists("/data") else "."
os.makedirs(DATA_DIR, exist_ok=True)

MANAGER_ID_FILE = os.path.join(DATA_DIR, "manager_id.txt")
OWNER_ID_FILE = os.path.join(DATA_DIR, "owner_id.txt")

def _load_id(filepath):
    try:
        with open(filepath) as f:
            return int(f.read().strip())
    except Exception:
        return None

_manager_from_env = os.environ.get("MANAGER_CHAT_ID")
MANAGER_CHAT_ID = int(_manager_from_env) if _manager_from_env else _load_id(MANAGER_ID_FILE)

_owner_from_env = os.environ.get("OWNER_CHAT_ID")
OWNER_CHAT_ID = int(_owner_from_env) if _owner_from_env else _load_id(OWNER_ID_FILE)

YANDEX_ROUTING_KEY = os.environ.get("YANDEX_ROUTING_KEY", "")
BASE_COORDS = (44.992753, 39.838747)
BASE_NAME = "Архиповский карьер (с. Архиповское, Белореченский р-н)"
RATE_PER_TON_KM = 5
WORK_HOURS = "пн–сб 8:00–18:00"

def _today_msk() -> str:
    """Возвращает сегодняшнюю дату по МСК (UTC+3) в формате ДД.ММ.ГГГГ."""
    now_msk = datetime.datetime.utcnow() + datetime.timedelta(hours=3)
    return now_msk.strftime("%d.%m.%Y")

def _alert_admin(error_context: str, error: Exception):
    """Отправляет алерт администратору при критических ошибках Sheets."""
    if OWNER_CHAT_ID:
        try:
            msg = f"⚠️ Ошибка учётчика\n{error_context}\n{type(error).__name__}: {str(error)[:200]}"
            send_msg(OWNER_CHAT_ID, msg)
        except Exception:
            pass
    print(f"[ALERT] {error_context}: {error}", flush=True)

# ─── Группа производства "Архиповский блок" ───────────────────────────────
BLOK_GROUP_ID = int(os.environ.get("BLOK_GROUP_ID", "-68840834804304"))  # Производство Тихорецкая
GOOGLE_SA_B64 = os.environ.get("GOOGLE_SA_B64", "")  # base64(service_account.json)
SHEETS_ID = "1FwpvHhDHiNuFOdXlTcrVuTWKUqh2NmWVn810ylM0MkQ"

# Справочники для валидации (должны совпадать с Рейсы!J4:J и K4:K)
VALID_DRIVERS = {"Кораблев М.Н.", "Адлейба А.Ю.", "Кислицин А.С.", "Камышанов А.А."}
VALID_TRUCKS = {"у135рх 193", "р319ок 123", "р638хн 123"}

# Сокращения номеров машин (как пишут в чате → полный номер)
TRUCK_SHORTCUTS = {
    "135": "у135рх 193",
    "319": "р319ок 123",
    "638": "р638хн 123",
}

# Жёсткая привязка машины → штатному водителю.
# Если в сообщении упомянута только машина (без водителя) — бот подставит водителя сам.
TRUCK_TO_DRIVER = {
    "у135рх 193": "Кораблев М.Н.",
    "р319ок 123": "Адлейба А.Ю.",
    "р638хн 123": "Кислицин А.С.",
}

def _driver_for_truck(truck: str) -> str:
    """Возвращает штатного водителя для машины (по полному номеру или сокращению)."""
    if not truck:
        return ""
    full = TRUCK_SHORTCUTS.get(truck.strip(), truck.strip())
    return TRUCK_TO_DRIVER.get(full, "")

# Дедупликация: персистентный кеш на диске (выживает перезапуск бота)
_DEDUP_FILE = os.path.join(DATA_DIR, "dedup_cache.json")
_DEDUP_TTL = 86400  # 24 часа в секундах

def _load_dedup_cache() -> dict:
    try:
        with open(_DEDUP_FILE, 'r') as f:
            return json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_dedup_cache(cache: dict):
    try:
        with open(_DEDUP_FILE, 'w') as f:
            f.write(json.dumps(cache))
    except Exception as e:
        print(f"[DEDUP] Ошибка сохранения кеша: {e}", flush=True)

_dedup_cache = _load_dedup_cache()
_dedup_lock = threading.Lock()

# ── Очистка dedup-кеша по env-флагу (для синхронизации после ручной правки Sheets)
if os.environ.get("CLEAR_DEDUP_ON_START") == "1":
    try:
        _dedup_cache.clear()
        if os.path.exists(_DEDUP_FILE):
            os.remove(_DEDUP_FILE)
        print(f"[STARTUP] CLEAR_DEDUP_ON_START=1: dedup_cache очищен", flush=True)
    except Exception as e:
        print(f"[STARTUP] Ошибка очистки dedup_cache: {e}", flush=True)

CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY", "")
_PALLET_PRICE = int(os.environ.get("PALLET_PRICE", "550"))

PRODUCTS = {
    "Отсев 0-5":             614,
    "Щебень 5-20":           345,
    "Щебень 20-40":          None,
    "Щебень 40-70":          None,
    "Песок мелкозернистый":  233,
    "Песок крупнозернистый": 566,
    "Гравий":                240,
    "ГПС эконом":            75,
    "ГПС премиум":           160,
}

DENSITY = {
    "Отсев 0-5":             1.27,
    "Щебень 5-20":           1.45,
    "Щебень 20-40":          1.42,
    "Щебень 40-70":          1.44,
    "Песок мелкозернистый":  1.50,
    "Песок крупнозернистый": 1.50,
    "Гравий":                1.45,
    "ГПС эконом":            1.77,
    "ГПС премиум":           1.77,
}
DEFAULT_DENSITY = 1.5

# Склонение названий товаров (родительный падеж)
PRODUCT_GENITIVE = {
    "Отсев 0-5": "отсева 0-5",
    "Щебень 5-20": "щебня 5-20",
    "Щебень 20-40": "щебня 20-40",
    "Щебень 40-70": "щебня 40-70",
    "Песок мелкозернистый": "песка мелкозернистого",
    "Песок крупнозернистый": "песка крупнозернистого",
    "Гравий": "гравия",
    "ГПС эконом": "ГПС эконом",
    "ГПС премиум": "ГПС премиум",
}

def product_genitive(name):
    """Возвращает название товара в родительном падеже."""
    return PRODUCT_GENITIVE.get(name, name)

# Исправления ошибок распознавания Whisper для местных топонимов
WHISPER_FIXES = {
    "лобинск": "Лабинск",
    "лобинске": "Лабинске",
    "лобинска": "Лабинска",
    "белоречинск": "Белореченск",
    "белоречинске": "Белореченске",
    "кропоткин": "Кропоткин",
    "майком": "Майкоп",
    "майкоб": "Майкоп",
    "армовир": "Армавир",
    "армовире": "Армавире",
    "курганинск": "Курганинск",
    "тихарецк": "Тихорецк",
    "тихарецке": "Тихорецке",
    "гулькевичи": "Гулькевичи",
    "усть-лобинск": "Усть-Лабинск",
    "усть-лобинске": "Усть-Лабинске",
    "апшеронск": "Апшеронск",
    "мостовский": "Мостовской",
    "мостовском": "Мостовском",
    "веселовская": "Весёловская",
    "веселовской": "Весёловской",
    "веселовка": "Весёловка",
    "веселовке": "Весёловке",
    "веселовку": "Весёловку",
    "эйск": "Ейск",
    "эйске": "Ейске",
    "эйска": "Ейска",
}
# Русские числительные → цифры (для парсинга "две машины", "три тонны" и т.п.)
WORD_NUMBERS = {
    "одн": 1, "одна": 1, "одну": 1, "одного": 1, "один": 1,
    "два": 2, "две": 2, "двух": 2,
    "три": 3, "трёх": 3, "трех": 3,
    "четыре": 4, "четырёх": 4, "четырех": 4,
    "пять": 5, "пяти": 5,
    "шесть": 6, "шести": 6,
    "семь": 7, "семи": 7,
    "восемь": 8, "восьми": 8,
    "девять": 9, "девяти": 9,
    "десять": 10, "десяти": 10,
    "пол": 0.5, "полторы": 1.5, "полтора": 1.5,
}

def words_to_numbers(text: str) -> str:
    """Заменяет русские числительные перед единицами на цифры."""
    import re as _re
    pattern = r'\b(' + '|'.join(WORD_NUMBERS.keys()) + r')\s+(тонн\w*|тн\b|т\b|куб\w*|м[³3]|машин\w*|рейс\w*)'
    def _repl(m):
        word = m.group(1).lower()
        return str(WORD_NUMBERS.get(word, word)) + " " + m.group(2)
    result = _re.sub(pattern, _repl, text, flags=_re.IGNORECASE)
    if result != text:
        print(f"[WORD2NUM] '{text}' -> '{result}'", flush=True)
    return result

def fix_whisper_typos(text: str) -> str:
    """Исправляет типичные ошибки Whisper в названиях и числах."""
    # Словесные фракции щебня (Whisper часто выдаёт числа словами)
    text = re.sub(r'пять\s*(?:[-—]\s*)?двадцать', '5-20', text, flags=re.IGNORECASE)
    text = re.sub(r'двадцать\s*(?:[-—]\s*)?сорок', '20-40', text, flags=re.IGNORECASE)
    text = re.sub(r'сорок\s*(?:[-—]\s*)?семьдесят', '40-70', text, flags=re.IGNORECASE)
    # "5.20" или "5,20" → "5-20"
    text = re.sub(r'\b5[.,]20\b', '5-20', text)
    text = re.sub(r'\b20[.,]40\b', '20-40', text)
    text = re.sub(r'\b40[.,]70\b', '40-70', text)
    # Фракции щебня: Whisper часто склеивает "5-20" в "520", "20-40" в "2040" и т.д.
    # "520 тысяч кубов" → "5-20, 1000 кубов" (щебень пять-двадцать, тысяча кубов)
    # "520 тысячу кубов" → "5-20, 1000 кубов"
    # Но "520 тонн" → оставляем как есть (520 тонн — реальный объём)
    text = re.sub(r'\b520\s+(тысяч[аиу]?|тыщ[аиу]?)\s+(куб\w*|м[³3])', r'5-20, 1000 \2', text, flags=re.IGNORECASE)
    text = re.sub(r'\b2040\s+(тысяч[аиу]?|тыщ[аиу]?)\s+(куб\w*|м[³3])', r'20-40, 1000 \2', text, flags=re.IGNORECASE)
    text = re.sub(r'\b4070\s+(тысяч[аиу]?|тыщ[аиу]?)\s+(куб\w*|м[³3])', r'40-70, 1000 \2', text, flags=re.IGNORECASE)
    # "520 кубов" без "тысяч" — тоже скорее всего фракция 5-20 + объём слипся
    # Но тут нужно быть осторожнее: "520 кубов" может быть реальным объёмом
    # Оставляем только вариант с "тысяч"

    # "щебень 520" → "щебень 5-20" (без объёма рядом)
    text = re.sub(r'(щебень\w*)\s+520\b(?!\s*(?:тонн|т\b|куб|м[³3]|тысяч|руб))', r'\1 5-20', text, flags=re.IGNORECASE)
    text = re.sub(r'(щебень\w*)\s+2040\b(?!\s*(?:тонн|т\b|куб|м[³3]|тысяч|руб))', r'\1 20-40', text, flags=re.IGNORECASE)
    text = re.sub(r'(щебень\w*)\s+4070\b(?!\s*(?:тонн|т\b|куб|м[³3]|тысяч|руб))', r'\1 40-70', text, flags=re.IGNORECASE)

    words = text.split()
    fixed = []
    for w in words:
        low = w.lower().strip(".,!?;:")
        if low in WHISPER_FIXES:
            prefix = ""
            suffix = ""
            for ch in w:
                if ch.isalpha() or ch == "-":
                    break
                prefix += ch
            for ch in reversed(w):
                if ch.isalpha() or ch == "-":
                    break
                suffix = ch + suffix
            fixed.append(prefix + WHISPER_FIXES[low] + suffix)
        else:
            fixed.append(w)
    result = " ".join(fixed)
    if result != text:
        print(f"[WHISPER_FIX] \'{text}\' -> \'{result}\'", flush=True)
    result = words_to_numbers(result)
    return result

PRODUCT, VOLUME, DELIVERY, ADDRESS, CONTACTS, PHONE_ONLY, CONFIRM = range(7)

# State machine (вместо ConversationHandler из PTB)
user_state: dict = {}
saved_contacts: dict = {}  # chat_id -> {"contact_name": ..., "phone": ..., "address": ...}   # chat_id -> int (состояние)
user_data: dict = {}    # chat_id -> dict (данные заявки)
pending_replies: dict = {}  # manager_id -> {"client_id": int, "expires": float, "summary": str}
order_summaries: dict = {}  # client_id -> краткий саммари заявки для менеджера
pending_voice: dict = {}    # chat_id -> (text, user_name, user_id)
processed_callbacks: set = set()  # дедупликация нажатий кнопок
user_chat_map: dict = {}   # user_id -> chat_id (Max: callback не содержит chat_id)
poll_wizard_data: dict = {}  # chat_id -> {"question": str, "options": list}

# Poll wizard states
POLL_WIZ_QUESTION = "poll_wiz_question"
POLL_WIZ_OPTIONS = "poll_wiz_options"
POLL_WIZ_TARGET = "poll_wiz_target"

# ─── Опросы (poll) ────────────────────────────────────────────────────────
# poll_data[poll_id] = {
#   "question": str,
#   "options": ["opt1", "opt2", ...],
#   "votes": {option_index: set(user_id, ...), ...},
#   "chat_id": int,
#   "message_id": str,
# }
POLLS_FILE = os.path.join(DATA_DIR, "polls.json")
poll_data: dict = {}
_poll_counter = 0
_poll_lock = threading.Lock()

def _save_polls():
    """Сохранить poll_data в файл (votes: set → list для JSON)."""
    try:
        serializable = {}
        for pid, p in poll_data.items():
            serializable[pid] = {
                "question": p["question"],
                "options": p["options"],
                "votes": {str(k): list(v) for k, v in p["votes"].items()},
                "chat_id": p.get("chat_id"),
                "message_id": p.get("message_id", ""),
            }
        with open(POLLS_FILE, "w") as f:
            json.dump(serializable, f, ensure_ascii=False)
    except Exception as e:
        print(f"[POLL] Ошибка сохранения: {e}", flush=True)

def _load_polls():
    """Загрузить poll_data из файла."""
    global poll_data, _poll_counter
    try:
        with open(POLLS_FILE) as f:
            raw = json.load(f)
        for pid, p in raw.items():
            poll_data[pid] = {
                "question": p["question"],
                "options": p["options"],
                "votes": {int(k): set(v) for k, v in p["votes"].items()},
                "chat_id": p.get("chat_id"),
                "message_id": p.get("message_id", ""),
            }
        _poll_counter = len(poll_data)
        print(f"[POLL] Загружено {len(poll_data)} опросов из {POLLS_FILE}", flush=True)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[POLL] Ошибка загрузки: {e}", flush=True)

REPLY_TIMEOUT = 30 * 60  # 30 минут

# ─── Блокировки для многопоточности ────────────────────────────────────────
_save_lock = threading.Lock()          # защита записи bot_state.json
_voice_lock = threading.Lock()         # защита pending_voice
_user_locks: dict = {}                 # chat_id -> Lock (один поток на пользователя)
_user_locks_guard = threading.Lock()   # защита самого словаря _user_locks
def get_user_lock(chat_id: int) -> threading.Lock:
    with _user_locks_guard:
        if chat_id not in _user_locks:
            _user_locks[chat_id] = threading.Lock()
        return _user_locks[chat_id]
STATE_FILE = os.path.join(DATA_DIR, "bot_state.json")
ANALYTICS_FILE = os.path.join(DATA_DIR, "analytics.json")
ORDERS_FILE = os.path.join(DATA_DIR, "orders.json")
def save_order(order: dict):
    """Дописывает заявку в orders.json (append-only)."""
    try:
        with open(ORDERS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(order, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[ORDERS] Ошибка записи: {e}", flush=True)
def load_orders(limit: int = 10) -> list:
    """Загружает последние N заявок."""
    orders = []
    try:
        with open(ORDERS_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    orders.append(json.loads(line))
                except Exception:
                    pass
    except FileNotFoundError:
        pass
    return orders[-limit:]
def track_event(event: str, **kwargs):
    """Дописывает событие в analytics.json (append-only)."""
    record = {"ts": time.time(), "event": event, **kwargs}
    try:
        with open(ANALYTICS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[ANALYTICS] Ошибка записи: {e}", flush=True)
def load_analytics(days: int = 7) -> list:
    """Загружает события за последние N дней."""
    since = time.time() - days * 86400
    events = []
    try:
        with open(ANALYTICS_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    if rec.get("ts", 0) >= since:
                        events.append(rec)
                except Exception:
                    pass
    except FileNotFoundError:
        pass
    return events
def save_state():
    """Атомарно сохраняет состояние диалогов на диск (потокобезопасно)."""
    with _save_lock:
        data = {
            "user_state": {str(k): v for k, v in user_state.items()},
            "user_data": {str(k): v for k, v in user_data.items()},
            "pending_replies": {str(k): v for k, v in pending_replies.items()},
            "order_summaries": {str(k): v for k, v in order_summaries.items()},
            "user_chat_map": {str(k): v for k, v in user_chat_map.items()},
        }
        tmp = STATE_FILE + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            os.replace(tmp, STATE_FILE)
        except Exception as e:
            print(f"[STATE] Ошибка сохранения: {e}", flush=True)
def load_state():
    """Загружает состояние диалогов при старте."""
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        user_state.update({int(k): v for k, v in data.get("user_state", {}).items()})
        user_data.update({int(k): v for k, v in data.get("user_data", {}).items()})
        # pending_replies: поддержка старого формата (int) и нового (dict)
        for k, v in data.get("pending_replies", {}).items():
            pending_replies[int(k)] = v if isinstance(v, dict) else {"client_id": int(v), "expires": 0, "summary": ""}
        order_summaries.update({int(k): v for k, v in data.get("order_summaries", {}).items()})
        user_chat_map.update({int(k): v for k, v in data.get("user_chat_map", {}).items()})
        print(f"[STATE] Загружено: {len(user_state)} диалогов, {len(pending_replies)} ожидающих ответов", flush=True)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[STATE] Ошибка загрузки: {e}", flush=True)

# ─── Проверка критических env vars ─────────────────────────────────────────
_REQUIRED_VARS = {"MAX_BOT_TOKEN": TOKEN, "GOOGLE_SA_B64": GOOGLE_SA_B64}
_OPTIONAL_VARS = {"CLAUDE_API_KEY": CLAUDE_API_KEY, "GROQ_API_KEY": GROQ_API_KEY}
_missing_required = [vn for vn, vv in _REQUIRED_VARS.items() if not vv]
if _missing_required:
    for _vn in _missing_required:
        print(f"[INIT] КРИТИЧНО: {_vn} не задан! Бот не сможет работать.", flush=True)
    print(f"[INIT] Завершаю работу — задайте {', '.join(_missing_required)} в окружении.", flush=True)
    sys.exit(1)
for _vn, _vv in _OPTIONAL_VARS.items():
    if not _vv:
        print(f"[INIT] ПРЕДУПРЕЖДЕНИЕ: {_vn} не задан — часть функций отключена.", flush=True)

# ─── Max Bot API ───────────────────────────────────────────────────────────

BASE_URL = "https://botapi.max.ru"
def _api(method: str, endpoint: str, params: dict = None, body: dict = None) -> dict:
    p = dict(params or {})
    # MAX API: query-параметр access_token deprecated с мая 2026, теперь header Authorization (без Bearer)
    url = f"{BASE_URL}/{endpoint}"
    if p:
        url += "?" + urllib.parse.urlencode(p)
    data = json.dumps(body).encode("utf-8") if body is not None else None
    for _retry in range(2):
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", TOKEN)
        if data:
            req.add_header("Content-Type", "application/json")
        req.add_header("User-Agent", "quarry-max-bot/1.0")
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            code_val = e.code
            if code_val in (429, 500, 502, 503) and _retry == 0:
                print(f"[API] {method} /{endpoint} HTTP {code_val}, retry in 2s...", flush=True)
                time.sleep(2)
                continue
            print(f"[API] {method} /{endpoint} HTTP {code_val}: {e.read()[:300]}", flush=True)
            return {}
        except Exception as e:
            if _retry == 0:
                print(f"[API] {method} /{endpoint} error: {e}, retry...", flush=True)
                time.sleep(1)
                continue
            print(f"[API] {method} /{endpoint} error: {e}", flush=True)
            return {}
    return {}
def send_msg(chat_id: int, text: str, buttons=None) -> dict:
    """Отправить сообщение. buttons = [[{text, payload}, ...], ...] или None."""
    # MAX API limit ~4000 chars
    if len(text) > 3900:
        text = text[:3900] + "\n... (обрезано)"
    body = {"text": text}
    if buttons:
        body["attachments"] = [{
            "type": "inline_keyboard",
            "payload": {"buttons": buttons}
        }]
    return _api("POST", "messages", params={"chat_id": chat_id}, body=body)
def send_action(chat_id: int, action: str = "typing_on") -> dict:
    """Отправить индикатор действия (typing_on) в чат."""
    return _api("POST", f"chats/{chat_id}/actions", body={"action": action})
def send_photo_msg(chat_id: int, photo_url: str, caption: str = "") -> dict:
    """Отправить изображение по URL."""
    body = {
        "text": caption,
        "attachments": [{"type": "image", "payload": {"url": photo_url}}]
    }
    return _api("POST", "messages", params={"chat_id": chat_id}, body=body)
def answer_cb(callback_id: str, notification: str = "") -> dict:
    if not callback_id:
        return {}
    params = {"callback_id": callback_id}
    body = {"notification": notification}
    result = _api("POST", "answers", params=params, body=body)
    print(f"[ANSWER_CB] notification={notification!r} result={result}", flush=True)
    return result
def get_updates(marker=None, timeout: int = 30) -> dict:
    p = {"timeout": timeout}
    if marker is not None:
        p["marker"] = marker
    return _api("GET", "updates", params=p)
def make_buttons(items: list) -> list:
    """Список строк → список рядов кнопок (одна кнопка в ряд)."""
    return [[{"type": "callback", "text": s, "payload": s}] for s in items]
def edit_msg(message_id: str, text: str, buttons=None) -> dict:
    """Редактировать сообщение по message_id."""
    body = {"text": text}
    if buttons:
        body["attachments"] = [{
            "type": "inline_keyboard",
            "payload": {"buttons": buttons}
        }]
    result = _api("PUT", "messages", params={"message_id": message_id}, body=body)
    print(f"[EDIT_MSG] mid={message_id[:30]} result_ok={bool(result)}", flush=True)
    return result
def _format_poll_text(question: str, options: list, votes: dict) -> str:
    """Форматирует текст опроса с текущими результатами."""
    total = sum(len(v) for v in votes.values())
    lines = [question, ""]
    for i, opt in enumerate(options):
        count = len(votes.get(i, set()))
        pct = round(count / total * 100) if total else 0
        bar_len = round(pct / 5) if total else 0
        bar = "▓" * bar_len + "░" * (20 - bar_len)
        lines.append(f"{opt}  —  {count} ({pct}%)")
        lines.append(f"{bar}")
        lines.append("")
    lines.append(f"Всего голосов: {total}")
    return "\n".join(lines)
def send_poll(chat_id: int, question: str, options: list) -> str:
    """Отправляет опрос с inline-кнопками. Возвращает poll_id.

    Args:
        chat_id: ID чата для отправки
        question: текст вопроса
        options: список вариантов ответа (строки)

    Returns:
        poll_id (str) для отслеживания, или "" при ошибке
    """
    global _poll_counter
    with _poll_lock:
        _poll_counter += 1
        poll_id = f"poll_{int(time.time())}_{_poll_counter}"

    votes = {i: set() for i in range(len(options))}
    text = _format_poll_text(question, options, votes)

    buttons = []
    for i, opt in enumerate(options):
        buttons.append([{"type": "callback", "text": opt, "payload": f"pollvote_{poll_id}_{i}"}])

    result = send_msg(chat_id, text, buttons)
    message_id = result.get("message", {}).get("body", {}).get("mid", "")

    if not message_id:
        print(f"[POLL] Не удалось получить message_id: {result}", flush=True)
        return ""

    poll_data[poll_id] = {
        "question": question,
        "options": options,
        "votes": votes,
        "chat_id": chat_id,
        "message_id": message_id,
    }
    print(f"[POLL] Создан {poll_id}: {question} ({len(options)} вариантов)", flush=True)
    _save_polls()
    return poll_id
def handle_poll_vote(user_id: int, callback_id: str, payload: str, orig_msg: dict = None):
    """Обработка голоса в опросе. Multiple choice: toggle vote."""
    print(f"[POLL_VOTE] START user={user_id} payload={payload} cb={callback_id[:20]}...", flush=True)
    print(f"[POLL_VOTE] poll_data keys: {list(poll_data.keys())}", flush=True)
    parts = payload.split("_")
    # payload = pollvote_poll_<ts>_<counter>_<option_index>
    if len(parts) < 5:
        print(f"[POLL_VOTE] ERROR: parts < 5: {parts}", flush=True)
        answer_cb(callback_id, "Ошибка опроса")
        return
    option_idx = int(parts[-1])
    poll_id = "_".join(parts[1:-1])  # poll_<ts>_<counter>
    print(f"[POLL_VOTE] poll_id={poll_id} option_idx={option_idx}", flush=True)

    poll = poll_data.get(poll_id)
    # Восстановление опроса из callback message (если бот перезапустился)
    if not poll and orig_msg:
        try:
            mid = orig_msg.get("body", {}).get("mid", "")
            # Извлекаем варианты из кнопок
            atts = orig_msg.get("body", {}).get("attachments", [])
            options = []
            for att in atts:
                if att.get("type") == "inline_keyboard":
                    for row in att.get("payload", {}).get("buttons", []):
                        for btn in row:
                            if btn.get("payload", "").startswith("pollvote_"):
                                options.append(btn["text"])
            if options:
                # Извлекаем вопрос из первой строки текста
                text_lines = orig_msg.get("body", {}).get("text", "").split("\n")
                question = text_lines[0].replace("📊 ", "") if text_lines else "Опрос"
                votes = {i: set() for i in range(len(options))}
                poll_data[poll_id] = {
                    "question": question,
                    "options": options,
                    "votes": votes,
                    "message_id": mid,
                }
                poll = poll_data[poll_id]
                print(f"[POLL] Восстановлен {poll_id} из callback: {len(options)} вариантов", flush=True)
        except Exception as e:
            print(f"[POLL] Ошибка восстановления: {e}", flush=True)

    if not poll:
        answer_cb(callback_id, "Опрос завершён")
        return

    votes = poll["votes"]
    if option_idx not in votes:
        answer_cb(callback_id, "Ошибка")
        return

    opt_name = poll["options"][option_idx]

    # Toggle: если уже голосовал за этот вариант — снимаем голос
    if user_id in votes[option_idx]:
        votes[option_idx].discard(user_id)
        answer_cb(callback_id, f'Голос за "{opt_name}" снят')
    else:
        votes[option_idx].add(user_id)
        answer_cb(callback_id, f'Вы проголосовали за "{opt_name}"')

    # Обновляем сообщение с результатами
    text = _format_poll_text(poll["question"], poll["options"], votes)
    buttons = []
    for i, opt in enumerate(poll["options"]):
        buttons.append([{"type": "callback", "text": opt, "payload": f"pollvote_{poll_id}_{i}"}])

    edit_msg(poll["message_id"], text, buttons)
    _save_polls()
# ─── Pydantic модели ───────────────────────────────────────────────────────

class OrderItem(BaseModel):
    product: Optional[str] = None
    tons: Optional[float] = None
    raw_value: Optional[float] = None
    unit: Optional[str] = None  # 'тонн' или 'куб'

class OrderParsed(BaseModel):
    items: Optional[List[OrderItem]] = None
    product: Optional[str] = None
    tons: Optional[float] = None
    unit: Optional[str] = None  # 'тонн' или 'куб'
    delivery: Optional[str] = None
    address: Optional[str] = None

class ContactsParsed(BaseModel):
    name: Optional[str] = None
    company: Optional[str] = None
    phone: Optional[str] = None
# ─── Парсеры (идентично tg-bot) ───────────────────────────────────────────

def parse_order_regex(text: str) -> OrderParsed:
    t = text.lower()
    t_norm = re.sub(r'(\d)[./](\d)', r'\1-\2', t)
    result = OrderParsed()
    for product in PRODUCTS:
        if product.lower() in t_norm:
            result.product = product
            break
    if not result.product:
        patterns = [
            (r'щебень.*?5-20|5-20.*?щебень|\b5-20\b', "Щебень 5-20"),
            (r'щебень.*?20-40|20-40.*?щебень|\b20-40\b', "Щебень 20-40"),
            (r'щебень.*?40-70|40-70.*?щебень|\b40-70\b', "Щебень 40-70"),
            (r'\bотсев\b', "Отсев 0-5"),
            (r'гравий', "Гравий"),
            (r'гпс.*?плох|плох.*?гпс', "ГПС эконом"),
            (r'гпс.*?хор|хор.*?гпс', "ГПС премиум"),
            (r'\bгпс\b', "ГПС премиум"),
            (r'песок.*?мелк|мелк.*?песок', "Песок мелкозернистый"),
            (r'песок.*?круп|круп.*?песок', "Песок крупнозернистый"),
            (r'\bпесок\b', "Песок мелкозернистый"),
            (r'\bщебень\b', None)  # не подставляем фракцию — спросим у клиента,
        ]
        for pat, name in patterns:
            if re.search(pat, t_norm):
                result.product = name
                break
    m_frac = re.search(r'(\d+)[- ](\d+)[- ](\d+)', t_norm)
    if m_frac and not result.product:
        frac = f"{m_frac.group(2)}-{m_frac.group(3)}"
        for pat, name in [("5-20", "Щебень 5-20"), ("20-40", "Щебень 20-40"), ("40-70", "Щебень 40-70")]:
            if frac == pat:
                result.product = name
                if not result.tons:
                    result.tons = float(m_frac.group(1))
                break
    m = re.search(r'(\d+[.,]?\d*)\s*(тонн\w*|тн\b|т\b|куб\w*|м[³3]|машин\w*)', t)
    if m:
        val = float(m.group(1).replace(",", "."))
        unit_str = m.group(2)
        if re.match(r'куб|м[³3]', unit_str):
            result.unit = 'куб'
            density = DENSITY.get(result.product, DEFAULT_DENSITY)
            result.tons = round(val * density, 1)
        elif re.match(r'машин', unit_str):
            result.unit = 'тонн'
            result.tons = round(val * 30)  # 1 машина ≈ 30 тонн
        else:
            result.unit = 'тонн'
            result.tons = val
    if any(w in t for w in ["доставк", "привез", "привоз", "доставьте", "привезти", "доставить"]):
        result.delivery = "Доставка"
    elif any(w in t for w in ["самовывоз", "заберу", "сам заберу"]):
        result.delivery = "Самовывоз"
    if result.delivery == "Доставка":
        m = re.search(r'(?:по адресу|доставить в|привезти в|доставку в|в\s+г[.\s]|в\s+город|в\s+)(.{5,60}?)(?:\s*\d+\s*тонн|\s*,\s*\d|\s*$)', t)
        if m:
            result.address = m.group(1).strip()
    return result
def parse_order_groq(text: str) -> OrderParsed:
    products_list = ", ".join(PRODUCTS.keys())
    client = Groq(api_key=GROQ_API_KEY)
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": "Ты помощник для парсинга заявок клиентов карьера. Отвечай ТОЛЬКО валидным JSON без пояснений и markdown."},
            {"role": "user", "content": (
                f"Доступные товары: {products_list}\n\n"
                f"Сообщение клиента: «{text}»\n\n"
                "ВАЖНО: фракция щебня — два числа через дефис (5-20, 20-40, 40-70). "
                "Если клиент пишет '7 5-20' — это 7 тонн щебня 5-20. "
                "КРИТИЧНО: 520, 2040, 4070 — это НЕ тоннаж, это фракции слитно.\n"
                "НЕ конвертируй кубы в тонны! Верни число КАК ЕСТЬ и укажи единицу.\n\n"
                "Верни JSON:\n"
                "- items: [{\"product\": точное название или null, \"value\": число или null, \"unit\": \"тонн\" или \"куб\"}]\n"
                "- delivery: «Доставка» или «Самовывоз» или null\n"
                "- address: адрес или null\n"
                "Пример: {\"items\": [{\"product\": \"Щебень 5-20\", \"value\": 7, \"unit\": \"тонн\"}], \"delivery\": \"Доставка\", \"address\": \"Краснодар\"}"
            )},
        ],
        temperature=0,
        max_tokens=200,
    )
    raw = response.choices[0].message.content.strip()
    raw = re.sub(r"```[a-z]*\n?", "", raw).strip("` \n")
    data = json.loads(raw)
    FRACTION_ARTIFACTS = {520, 2040, 4070, 520.0, 2040.0, 4070.0}
    # Определяем единицу из исходного текста (fallback если LLM не вернула unit)
    text_lower = text.lower()
    text_has_cubes = bool(re.search(r'куб|м[³3]|кубометр|кубов', text_lower))
    text_has_volume = bool(re.search(r'тысяч|тонн|куб|м[³3]', text_lower))
    print(f"[GROQ] raw JSON: {raw}", flush=True)
    items = []
    for it in (data.get("items") or []):
        raw_val = float(it["value"]) if it.get("value") else (float(it["tons"]) if it.get("tons") else None)
        unit = it.get("unit")
        # Fallback: если LLM не вернула unit — определяем из текста
        if not unit:
            unit = "куб" if text_has_cubes else "тонн"
        # 520/2040/4070 — артефакты ТОЛЬКО если в тексте нет явного объёма (тысяч, тонн, кубов)
        if raw_val and raw_val in FRACTION_ARTIFACTS and not text_has_volume:
            raw_val = None
        # Конвертация кубов в тонны — в Python, не в LLM
        product = it.get("product")
        if raw_val and unit and re.match(r"куб", unit):
            density = DENSITY.get(product, DEFAULT_DENSITY)
            tons = round(raw_val * density, 1)
            print(f"[PARSE] {raw_val} куб × {density} = {tons} т ({product})", flush=True)
        else:
            tons = raw_val
        items.append(OrderItem(product=product, tons=tons, raw_value=raw_val, unit=unit))
    first = items[0] if items else OrderItem()
    return OrderParsed(
        items=items if items else None,
        product=first.product,
        tons=first.tons,
        unit=first.unit,
        delivery=data.get("delivery"),
        address=data.get("address"),
    )
def parse_order(text: str) -> OrderParsed:
    text = fix_whisper_typos(text)
    # Конвертируем "машины/рейсы" в тонны до отправки в LLM
    m_machines = re.search(r'(\d+[.,]?\d*)\s*(машин\w*|рейс\w*)', text, re.IGNORECASE)
    if m_machines:
        machine_val = float(m_machines.group(1).replace(",", "."))
        tons_val = round(machine_val * 30)
        text = text[:m_machines.start()] + f"{tons_val} тонн" + text[m_machines.end():]
        print(f"[MACHINES] {m_machines.group(0)} -> {tons_val} тонн", flush=True)
    if GROQ_API_KEY:
        try:
            return parse_order_groq(text)
        except Exception as e:
            print(f"[GROQ] parse failed: {e}, using regex")
    return parse_order_regex(text)
def parse_contacts_groq(text: str) -> ContactsParsed:
    client = Groq(api_key=GROQ_API_KEY)
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": "Ты помощник для извлечения контактных данных. Отвечай ТОЛЬКО валидным JSON без пояснений."},
            {"role": "user", "content": (
                f"Сообщение: «{text}»\n\n"
                "Верни JSON: {\"name\": str|null, \"company\": str|null, \"phone\": str|null}"
            )},
        ],
        temperature=0,
        max_tokens=100,
    )
    raw = response.choices[0].message.content.strip()
    raw = re.sub(r"```[a-z]*\n?", "", raw).strip("` \n")
    data = json.loads(raw)
    return ContactsParsed(name=data.get("name"), company=data.get("company"), phone=data.get("phone"))
# ─── Геокодирование и маршрутизация ───────────────────────────────────────

# Зона обслуживания: bounding box регионов (lat_min, lat_max, lon_min, lon_max)
# Координаты местных населённых пунктов (2700+ из GeoNames — КК + Адыгея)
def _load_local_coords():
    try:
        with open(os.path.join(os.path.dirname(__file__) or ".", "local_coords.json"), encoding="utf-8") as f:
            raw = json.load(f)
        coords = {k: tuple(v) for k, v in raw.items()}
        # Гарантируем наличие ключевых точек
        coords.setdefault("архиповское", (44.9928, 39.8387))
        print(f"[GEOCODE] Загружено {len(coords)} населённых пунктов из local_coords.json", flush=True)
        return coords
    except Exception as e:
        print(f"[GEOCODE] Ошибка загрузки local_coords.json: {e}, используем минимальный словарь", flush=True)
        return {
            "архиповское": (44.9928, 39.8387),
            "белореченск": (44.7667, 39.8833),
            "краснодар": (45.0453, 38.9818),
            "сочи": (43.597, 39.7248),
        }

LOCAL_COORDS = _load_local_coords()

def _lookup_local(address: str):
    """Ищет адрес в словаре известных координат."""
    addr_lower = address.lower()
    for name, coords in LOCAL_COORDS.items():
        if name in addr_lower:
            print(f"[GEOCODE] LOCAL: '{name}' найдено в '{address}' -> {coords}", flush=True)
            return coords
    return None

SERVICE_REGIONS = [
    ("Краснодарский край",   43.40, 46.30, 36.60, 41.75),
    ("Республика Адыгея",    43.75, 45.25, 38.80, 40.50),
    ("Ростовская область",   45.85, 50.25, 38.10, 44.30),
    ("Ставропольский край",  43.65, 46.25, 40.80, 45.35),
]

def _in_service_area(lat, lon):
    """Проверяет, попадают ли координаты в зону обслуживания."""
    for name, lat_min, lat_max, lon_min, lon_max in SERVICE_REGIONS:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return name
    return None

def get_coords(address: str):
    """Геокодирование с приоритетом: локальный словарь → КК → Адыгея → Ростов → Ставрополье."""
    address = fix_whisper_typos(address)
    # Сначала ищем в локальном словаре (быстро и точно)
    local = _lookup_local(address)
    if local:
        return local
    try:
        from geopy.geocoders import Nominatim
        geolocator = Nominatim(user_agent="quarry_delivery_bot_krd", timeout=5)
        parts = [p.strip() for p in address.split(",") if p.strip()]
        city_candidate = parts[0] if parts else address

        # Нормализация: "станица X" -> доп. вариант "ст. X" и просто "X"
        addr_variants = [address]
        city_variants = [city_candidate]
        for prefix in ["станица ", "станицa ", "ст. ", "ст ", "хутор ", "посёлок ", "поселок ", "село ", "пос. ", "пос "]:
            for src in [address.lower(), city_candidate.lower()]:
                if src.startswith(prefix):
                    short = src[len(prefix):].strip().capitalize()
                    if short not in addr_variants:
                        addr_variants.append(short)
                    if short not in city_variants:
                        city_variants.append(short)

        # Группы запросов по приоритету: КК → Адыгея → Ростов → Ставрополье
        # Внутри КК: сначала с районами (точнее), потом без
        kk_queries = []
        for av in addr_variants:
            kk_queries.append(f"{av}, Краснодарский край, Россия")
        for cv in city_variants:
            q = f"{cv}, Краснодарский край, Россия"
            if q not in kk_queries:
                kk_queries.append(q)

        other_queries = []
        for av in addr_variants:
            other_queries.append(f"{av}, Республика Адыгея, Россия")
        for av in addr_variants:
            other_queries.append(f"{av}, Ростовская область, Россия")
        for av in addr_variants:
            other_queries.append(f"{av}, Ставропольский край, Россия")

        # Сначала ищем ТОЛЬКО в КК
        for query in kk_queries:
            try:
                loc = geolocator.geocode(query)
                if loc:
                    region = _in_service_area(loc.latitude, loc.longitude)
                    if region:
                        print(f"[GEOCODE] OK (КК): {query!r} -> ({loc.latitude:.4f}, {loc.longitude:.4f}) [{region}]", flush=True)
                        return (loc.latitude, loc.longitude)
                    else:
                        print(f"[GEOCODE] вне зоны: {query!r} -> ({loc.latitude:.4f}, {loc.longitude:.4f})", flush=True)
            except Exception as e:
                print(f"[GEOCODE] ошибка: {e}", flush=True)

        # Если в КК не нашли — ищем в соседних регионах
        for query in other_queries:
            try:
                loc = geolocator.geocode(query)
                if loc:
                    region = _in_service_area(loc.latitude, loc.longitude)
                    if region:
                        print(f"[GEOCODE] OK (сосед): {query!r} -> ({loc.latitude:.4f}, {loc.longitude:.4f}) [{region}]", flush=True)
                        return (loc.latitude, loc.longitude)
                    else:
                        print(f"[GEOCODE] вне зоны: {query!r} -> ({loc.latitude:.4f}, {loc.longitude:.4f})", flush=True)
            except Exception as e:
                print(f"[GEOCODE] ошибка: {e}", flush=True)
        print(f"[GEOCODE] не найдено в зоне обслуживания: {address!r}", flush=True)
    except Exception as e:
        print(f"[GEOCODE] критическая ошибка: {e}", flush=True)
    return None
def get_road_distance(origin, destination):
    if YANDEX_ROUTING_KEY:
        params = urllib.parse.urlencode({
            "apikey": YANDEX_ROUTING_KEY,
            "waypoints": f"{origin[0]},{origin[1]}|{destination[0]},{destination[1]}",
            "vehicle_type": "truck",
            "route_type": "shortest",
        })
        try:
            req = urllib.request.Request(
                f"https://api.routing.yandex.net/v2/route?{params}",
                headers={"User-Agent": "quarry-bot/1.0"}
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
            total_m = sum(leg.get("distance", 0) for leg in data["route"]["legs"])
            if total_m:
                return round(total_m / 1000, 1)
        except Exception as e:
            print(f"[ROUTING] Яндекс ошибка: {e}")
    try:
        url = (
            f"http://router.project-osrm.org/route/v1/driving/"
            f"{origin[1]},{origin[0]};{destination[1]},{destination[0]}?overview=false"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "quarry-bot/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        if data.get("code") == "Ok":
            return round(data["routes"][0]["distance"] / 1000, 1)
    except Exception as e:
        print(f"[ROUTING] OSRM ошибка: {e}")
    return None
def parse_tons(text: str, product: str = None):
    m = re.search(r"(\d+[.,]?\d*)\s*(тонн\w*|тн\b|т\b|куб\w*|м[³3]|машин\w*|рейс\w*)", text)
    if m:
        val = float(m.group(1).replace(",", "."))
        unit_str = m.group(2)
        if re.match(r'куб|м[³3]', unit_str):
            density = DENSITY.get(product, DEFAULT_DENSITY)
            return round(val * density, 1)
        if re.match(r'машин|рейс', unit_str):
            return round(val * 30)  # 1 машина/рейс ≈ 30 тонн
        return val
    m = re.search(r"(\d+[.,]?\d*)", text)
    return float(m.group(1).replace(",", ".")) if m else None
# ─── Голосовые сообщения ──────────────────────────────────────────────────

def transcribe_voice_url(audio_url: str):
    try:
        req = urllib.request.Request(audio_url, headers={"User-Agent": "quarry-max-bot/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            audio_data = r.read()
            content_type = r.headers.get("Content-Type", "")
        # Определяем расширение из URL, затем из Content-Type
        parsed_path = urllib.parse.urlparse(audio_url).path
        ext = os.path.splitext(parsed_path)[1].lower()
        if not ext:
            if "ogg" in content_type or "opus" in content_type:
                ext = ".ogg"
            elif "mp3" in content_type or "mpeg" in content_type:
                ext = ".mp3"
            elif "mp4" in content_type or "m4a" in content_type:
                ext = ".mp4"
            elif "wav" in content_type:
                ext = ".wav"
            else:
                ext = ".ogg"
        print(f"[VOICE] Загружено {len(audio_data)} байт, ext={ext}, content_type={content_type}", flush=True)
        result = Groq(api_key=GROQ_API_KEY).audio.transcriptions.create(
            file=(f"voice{ext}", audio_data),
            model="whisper-large-v3",
            language="ru",
        )
        text = result.text.strip()
        print(f"[VOICE] Распознано: «{text}»", flush=True)
        return text
    except Exception as e:
        print(f"[VOICE] ошибка расшифровки: {e}", flush=True)
        return None
# ─── Логика диалога ────────────────────────────────────────────────────────

def try_parse_freeform(text: str, chat_id: int) -> bool:
    parsed = parse_order(text)
    found = False
    d = user_data[chat_id]

    if parsed.items and not d.get("items"):
        valid_items = [
            {"product": it.product, "tons": it.tons, "price_per_ton": PRODUCTS.get(it.product)}
            for it in parsed.items if it.product and it.tons and it.tons > 0
        ]
        if len(valid_items) > 1:
            d["items"] = valid_items
            d["product"] = ", ".join(i["product"] for i in valid_items)
            d["tons"] = sum(i["tons"] for i in valid_items)
            d["volume_text"] = " + ".join(f"{i['tons']}т {i['product']}" for i in valid_items)
            found = True
        elif len(valid_items) == 1:
            it = valid_items[0]
            if not d.get("product"):
                d["product"] = it["product"]
                d["price_per_ton"] = it["price_per_ton"]
                found = True
            if not d.get("tons"):
                d["tons"] = it["tons"]
                raw_item = parsed.items[0] if parsed.items else None
                if raw_item and raw_item.unit and re.match(r"куб", raw_item.unit) and raw_item.raw_value:
                    d["volume_text"] = f"{raw_item.raw_value:.0f} м³ = {it['tons']} т"
                else:
                    d["volume_text"] = f"{it['tons']} т"
                found = True
    elif parsed.product and parsed.product in PRODUCTS and not d.get("product"):
        d["product"] = parsed.product
        d["price_per_ton"] = PRODUCTS[parsed.product]
        found = True
    if parsed.tons and parsed.tons > 0 and not d.get("tons"):
        d["tons"] = parsed.tons
        if parsed.unit and re.match(r"куб", parsed.unit) and parsed.items and parsed.items[0].raw_value:
            d["volume_text"] = f"{parsed.items[0].raw_value:.0f} м³ = {parsed.tons} т"
        else:
            d["volume_text"] = f"{parsed.tons} т"
        found = True
    if parsed.delivery and not d.get("delivery"):
        d["delivery"] = parsed.delivery
        found = True
    if parsed.address and not d.get("address"):
        d["address"] = parsed.address
        found = True
    if GROQ_API_KEY and not (d.get("contact_name") and d.get("phone")):
        try:
            contacts = parse_contacts_groq(text)
            if contacts.name and not d.get("contact_name"):
                d["contact_name"] = contacts.name
                found = True
            if contacts.company and not d.get("company"):
                d["company"] = contacts.company
                found = True
            if contacts.phone and not d.get("phone"):
                d["phone"] = contacts.phone
                found = True
        except Exception:
            pass
    return found
def build_confirm_summary(d: dict) -> str:
    """Краткое резюме заявки для шага подтверждения."""
    lines = []
    items = d.get("items")
    if items:
        for i in items:
            lines.append(f"  {i['product']} — {i['tons']} т")
    else:
        lines.append(f"  Товар: {d.get('product', '—')}")
        lines.append(f"  Объём: {d.get('volume_text', d.get('tons', '—'))}")
    lines.append(f"  Получение: {d.get('delivery', '—')}")
    if d.get("delivery") == "Доставка" and d.get("address"):
        lines.append(f"  Адрес: {d['address']}")
    lines.append(f"  Имя: {d.get('contact_name', '—')}")
    if d.get("company"):
        lines.append(f"  Компания: {d['company']}")
    lines.append(f"  Телефон: {d.get('phone', '—')}")
    return "\n".join(lines)
FUNNEL_NAMES = {PRODUCT: "product", VOLUME: "volume", DELIVERY: "delivery", ADDRESS: "address", CONTACTS: "contacts", PHONE_ONLY: "phone", CONFIRM: "confirm"}

def advance(chat_id: int) -> int:
    """Определяет следующий шаг диалога. Возвращает состояние или -1 (конец)."""
    d = user_data.get(chat_id, {})

    if not d.get("product"):
        btns = make_buttons(list(PRODUCTS.keys()))
        send_msg(chat_id, "С чем поможем? Выберите продукцию или напишите своё название:", btns)
        return PRODUCT

    if not d.get("tons"):
        send_msg(chat_id, f"Сколько тонн {product_genitive(d['product'])} вам нужно?\n\nНапример: 30 тонн")
        return VOLUME

    if not d.get("delivery"):
        btns = [[
            {"type": "callback", "text": "Самовывоз", "payload": "Самовывоз"},
            {"type": "callback", "text": "Доставка", "payload": "Доставка"}
        ]]
        send_msg(chat_id,
            "Как удобнее получить заказ?\n\nМинимальный объём для доставки — 30 тонн (20 кубов).",
            btns)
        return DELIVERY

    if d.get("delivery") == "Доставка":
        tons = d.get("tons", 0)
        if tons < 30 and not d.get("delivery_warning_shown"):
            d["delivery_warning_shown"] = True
            btns = [
                [{"type": "callback", "text": "Самовывоз", "payload": "Самовывоз"}],
                [{"type": "callback", "text": "Всё равно доставка", "payload": "force_delivery"}],
            ]
            send_msg(chat_id,
                f"Обычно доставка от 30 тонн (загрузка машины).\n\n"
                f"Вы указали {tons} т — доставка возможна, но стоимость за тонну будет выше.\n\n"
                f"Продолжить с доставкой или заберёте самовывозом?",
                btns)
            return DELIVERY
        if not d.get("address"):
            send_msg(chat_id, "Куда доставить? Укажите адрес (город, улица, дом) — рассчитаем стоимость.")
            return ADDRESS

    if not d.get("phone"):
        ca = d.get("contacts_asked")
        if not ca:
            d["contacts_asked"] = True
            send_msg(chat_id,
                "Почти готово! Осталось оставить контакты:\n\n"
                "Напишите имя, организацию (если есть) и номер телефона.")
            return CONTACTS
        else:
            d["contacts_asked"] = "phone_only"
            send_msg(chat_id, "Последний шаг — подскажите номер телефона, и заявка готова.")
            return PHONE_ONLY

    summary = build_confirm_summary(user_data.get(chat_id, {}))
    btns = [[
        {"type": "callback", "text": "✅ Отправить заявку", "payload": "confirm_yes"},
        {"type": "callback", "text": "✏️ Изменить", "payload": "confirm_edit"},
        {"type": "callback", "text": "❌ Начать заново", "payload": "confirm_no"},
    ]]
    send_msg(chat_id, f"Проверьте заявку:\n\n{summary}\n\nВсё верно?", btns)
    track_event("funnel_step", chat_id=chat_id, step="confirm")
    return CONFIRM
def finalize(chat_id: int):
    global MANAGER_CHAT_ID, OWNER_CHAT_ID
    d = user_data.get(chat_id, {})
    product       = d.get("product")
    volume_text   = d.get("volume_text")
    tons          = d.get("tons")
    delivery      = d.get("delivery")
    address       = d.get("address", "—")
    price_per_ton = d.get("price_per_ton")
    contact_name  = d.get("contact_name", "—")
    company       = d.get("company", "—")
    phone         = d.get("phone")
    items         = d.get("items")

    MAX_TRUCK = 30
    trucks = math.ceil(tons / MAX_TRUCK) if tons and tons > 0 else 1
    material_cost = round(tons * price_per_ton) if price_per_ton and not items else None
    if items:
        material_cost = sum(
            round(i["tons"] * i["price_per_ton"]) for i in items if i.get("price_per_ton")
        ) or None

    distance_km = None
    delivery_cost = None
    geocode_failed = False
    map_url = None

    if delivery == "Доставка":
        try:
            coords = get_coords(address)
            if coords:
                distance_km = get_road_distance(BASE_COORDS, coords)
                if distance_km is None:
                    from geopy.distance import geodesic
                    distance_km = round(geodesic(BASE_COORDS, coords).km * 1.3, 1)
                if distance_km is not None and tons:
                    delivery_cost = round(distance_km * tons * RATE_PER_TON_KM)
                map_url = (
                    f"https://static-maps.yandex.ru/1.x/?l=map&lang=ru_RU&size=600,400"
                    f"&pt={BASE_COORDS[1]},{BASE_COORDS[0]},pm2rdm"
                    f"~{coords[1]},{coords[0]},pm2blm"
                )
            else:
                geocode_failed = True
        except Exception as e:
            print(f"[GEOCODE] ошибка в finalize: {e}", flush=True)
            geocode_failed = True

    # ── Клиенту ────────────────────────────────────────────────────────────
    track_event("order_completed",
        chat_id=chat_id,
        product=product,
        tons=tons,
        delivery=delivery,
        material_cost=material_cost,
    )
    save_order({
        "ts": time.time(),
        "client_id": chat_id,
        "name": contact_name,
        "company": company,
        "phone": phone,
        "product": product,
        "tons": tons,
        "delivery": delivery,
        "address": d.get("address", ""),
        "material_cost": material_cost,
        "delivery_cost": delivery_cost,
    })

    lines = ["Заявка принята! Передаём менеджеру.\n"]
    if items:
        for i in items:
            lines.append(f"Товар: {i['product']} — {i['tons']} т")
        lines.append(f"Итого: {tons} т")
    else:
        lines += [f"Товар: {product}", f"Объём: {volume_text}"]
    if trucks > 1:
        lines.append(f"Количество рейсов: {trucks} (по {MAX_TRUCK} т)")
    lines.append(f"Способ получения: {delivery}")
    if delivery == "Доставка":
        lines.append(f"Адрес: {address}")
    lines.append("")
    if material_cost is not None:
        lines.append(f"Стоимость материала: ~{material_cost:,} руб.".replace(",", " "))
    else:
        lines.append("Стоимость материала: уточнит менеджер")
    if delivery == "Доставка":
        if distance_km is not None and delivery_cost is not None:
            lines.append(f"Стоимость доставки: ~{delivery_cost:,} руб. (~{distance_km} км)".replace(",", " "))
            if material_cost is not None:
                lines.append(f"Итого: ~{material_cost + delivery_cost:,} руб.".replace(",", " "))
        else:
            lines.append("Стоимость доставки: уточнит менеджер")
    lines += [
        "",
        "Расчёт предварительный — точную стоимость подтвердит менеджер при звонке.",
        "",
        f"Ожидайте звонка на номер {phone}",
        f"Рабочие часы: {WORK_HOURS}",
        "",
        "Благодарим, что выбрали Архиповский карьер!",
        "Для новой заявки нажмите кнопку ниже",

    ]
    new_order_btn = [[{"type": "callback", "text": "📋 Новая заявка", "payload": "/start"}]]
    send_msg(chat_id, "\n".join(lines), new_order_btn)
    if map_url:
        try:
            send_photo_msg(chat_id, map_url, f"Маршрут: {BASE_NAME} -> {address} (~{distance_km} км)")
        except Exception as e:
            print(f"[MAP] Ошибка карты клиенту: {e}")

    # ── Менеджеру ──────────────────────────────────────────────────────────
    if not MANAGER_CHAT_ID:
        print("[WARN] MANAGER_CHAT_ID не задан. Установите через /myid")
        return

    mgr = [
        "НОВАЯ ЗАЯВКА\n",
        f"Клиент: {contact_name}",
        f"Компания: {company}",
        f"Телефон: {phone}",
        "",
    ]
    if items:
        for i in items:
            mgr.append(f"{i['product']}: {i['tons']} т")
        mgr.append(f"Итого: {tons} т")
    else:
        mgr += [f"Товар: {product}", f"Объём: {volume_text}"]
    if trucks > 1:
        mgr.append(f"Рейсов: {trucks} (по {MAX_TRUCK} т)")
    mgr.append(f"Получение: {delivery}")
    if delivery == "Доставка":
        mgr.append(f"Адрес: {address}")
        if distance_km is not None and delivery_cost is not None:
            mgr.append(f"~{distance_km} км -> доставка ~{delivery_cost:,} руб.".replace(",", " "))
        elif geocode_failed:
            mgr.append("Расстояние: уточнить вручную")
    if material_cost is not None:
        mgr.append(f"Материал (предв.): ~{material_cost:,} руб. ({price_per_ton} руб/т)".replace(",", " "))
    mgr.append(f"\nMax ID клиента: {chat_id}")

    # Сохраняем краткий саммари для контекста при ответе менеджера
    product_str = d.get("volume_text") or f"{tons} т"
    if items:
        product_str = " + ".join(f"{i['product']} {i['tons']}т" for i in items)
    order_summaries[chat_id] = f"{contact_name} | {product_str} | тел: {phone}"

    reply_btn = [[{"type": "callback", "text": "Ответить клиенту", "payload": f"reply_{chat_id}"}]]
    mgr_result = send_msg(MANAGER_CHAT_ID, "\n".join(mgr), reply_btn)
    if mgr_result.get("message"):
        print(f"[FINALIZE] Менеджеру {MANAGER_CHAT_ID} отправлено ✓", flush=True)
    else:
        print(f"[FINALIZE] ОШИБКА отправки менеджеру {MANAGER_CHAT_ID}: {mgr_result}", flush=True)
    if map_url:
        try:
            send_photo_msg(MANAGER_CHAT_ID, map_url, f"Маршрут: {BASE_NAME} -> {address} (~{distance_km} км)")
        except Exception as e:
            print(f"[MAP] Ошибка карты менеджеру: {e}")

    # ── Владельцу (копия без кнопки ответа) ────────────────────────────────
    if OWNER_CHAT_ID and OWNER_CHAT_ID != MANAGER_CHAT_ID:
        owner_msg = "[Копия] " + "\n".join(mgr)
        owner_result = send_msg(OWNER_CHAT_ID, owner_msg)
        if not owner_result.get("message"):
            print(f"[FINALIZE] ОШИБКА отправки владельцу {OWNER_CHAT_ID}: {owner_result}", flush=True)
        if map_url:
            try:
                send_photo_msg(OWNER_CHAT_ID, map_url, f"Маршрут: {BASE_NAME} -> {address} (~{distance_km} км)")
            except Exception as e:
                print(f"[MAP] Ошибка карты владельцу: {e}")
# ─── Обработка сообщений ──────────────────────────────────────────────────

def handle_message(chat_id: int, text: str, user_name: str = "", user_id: int = None):
    global MANAGER_CHAT_ID, OWNER_CHAT_ID
    if user_id is None:
        user_id = chat_id

    # Отмена режима ответа
    if text.strip() == "/cancel_reply":
        if user_id in pending_replies:
            pending_replies.pop(user_id)
            save_state()
            send_msg(chat_id, "Режим ответа отменён.")
        else:
            send_msg(chat_id, "Нет активного режима ответа.")
        return

    # Ответ менеджера клиенту (приоритет над всем) — проверяем по user_id
    if user_id in pending_replies:
        entry = pending_replies.pop(user_id)
        client_id = entry["client_id"] if isinstance(entry, dict) else entry
        expires = entry.get("expires", 0) if isinstance(entry, dict) else 0
        summary = entry.get("summary", "") if isinstance(entry, dict) else ""
        if expires and time.time() > expires:
            # Таймаут, но всё равно отправляем — не теряем сообщение менеджера
            try:
                send_msg(client_id, f"Ответ менеджера:\n\n{text}")
                send_msg(chat_id, f"✅ Ответ отправлен клиенту (сессия истекла, но сообщение доставлено).")
                track_event("manager_replied", manager_id=user_id, client_id=client_id, response_mins=None)
            except Exception as e:
                send_msg(chat_id, f"Не удалось отправить: {e}")
            save_state()
            return
        try:
            send_msg(client_id, f"Ответ менеджера:\n\n{text}")
            send_msg(chat_id, f"✅ Ответ отправлен клиенту.")
            # Считаем время ответа менеджера
            response_mins = None
            try:
                events = load_analytics(days=1)
                order_events = [e for e in events if e.get("event") == "order_completed" and e.get("chat_id") == client_id]
                if order_events:
                    order_ts = max(e["ts"] for e in order_events)
                    response_mins = round((time.time() - order_ts) / 60, 1)
            except Exception:
                pass
            track_event("manager_replied", manager_id=user_id, client_id=client_id, response_mins=response_mins)
        except Exception as e:
            send_msg(chat_id, f"Не удалось отправить: {e}")
        save_state()
        return

    # Команды
    if text.strip() == "/stats":
        if OWNER_CHAT_ID and chat_id != OWNER_CHAT_ID and user_id != OWNER_CHAT_ID:
            send_msg(chat_id, "Команда доступна только владельцу.")
            return
        now = time.time()
        today_start = now - (now % 86400)  # начало суток UTC
        events_7d = load_analytics(days=7)
        events_today = [e for e in events_7d if e.get("ts", 0) >= today_start]

        def count(evs, etype): return sum(1 for e in evs if e.get("event") == etype)

        started_today = count(events_today, "conversation_started")
        started_7d    = count(events_7d,    "conversation_started")
        completed_today = count(events_today, "order_completed")
        completed_7d    = count(events_7d,    "order_completed")
        replied_today = count(events_today, "manager_replied")
        replied_7d    = count(events_7d,    "manager_replied")

        conv_today = f"{round(completed_today/started_today*100)}%" if started_today else "—"
        conv_7d    = f"{round(completed_7d/started_7d*100)}%" if started_7d else "—"

        # Топ товаров за 7 дней
        from collections import Counter
        products_7d = [e.get("product") for e in events_7d if e.get("event") == "order_completed" and e.get("product")]
        top_products = Counter(products_7d).most_common(3)
        top_str = "\n".join(f"  {p}: {n} заявок" for p, n in top_products) or "  нет данных"

        # Доставка vs самовывоз
        deliveries = [e.get("delivery") for e in events_7d if e.get("event") == "order_completed"]
        delivery_count  = deliveries.count("Доставка")
        pickup_count    = deliveries.count("Самовывоз")

        # Среднее время ответа менеджера
        reply_events = [e for e in events_7d if e.get("event") == "manager_replied" and e.get("response_mins")]
        if reply_events:
            avg_resp = round(sum(e["response_mins"] for e in reply_events) / len(reply_events), 1)
            max_resp = round(max(e["response_mins"] for e in reply_events), 1)
            resp_str = f"  Среднее время ответа: {avg_resp} мин\n  Макс. время ответа: {max_resp} мин"
        else:
            resp_str = "  Время ответа: нет данных"

        # Воронка за 7 дней
        funnel_events = [e for e in events_7d if e.get("event") == "funnel_step"]
        funnel_steps = {"product": 0, "volume": 0, "delivery": 0, "address": 0, "contacts": 0, "phone": 0, "confirm": 0}
        seen_chats = {step: set() for step in funnel_steps}
        for e in funnel_events:
            step = e.get("step")
            cid = e.get("chat_id")
            if step in funnel_steps and cid:
                seen_chats[step].add(cid)
        for step in funnel_steps:
            funnel_steps[step] = len(seen_chats[step])
        funnel_str = (
            f"  Товар: {funnel_steps['product']}\n"
            f"  Объём: {funnel_steps['volume']}\n"
            f"  Доставка: {funnel_steps['delivery']}\n"
            f"  Адрес: {funnel_steps['address']}\n"
            f"  Контакты: {funnel_steps['contacts'] + funnel_steps['phone']}\n"
            f"  Подтверждение: {funnel_steps['confirm']}\n"
            f"  Оформлено: {completed_7d}"
        )

        msg = (
            f"Статистика бота (Max)\n\n"
            f"Сегодня:\n"
            f"  Начали диалог: {started_today}\n"
            f"  Оформили заявку: {completed_today}\n"
            f"  Конверсия: {conv_today}\n"
            f"  Ответов менеджера: {replied_today}\n\n"
            f"За 7 дней:\n"
            f"  Начали диалог: {started_7d}\n"
            f"  Оформили заявку: {completed_7d}\n"
            f"  Конверсия: {conv_7d}\n"
            f"  Ответов менеджера: {replied_7d}\n"
            f"  Доставка: {delivery_count} | Самовывоз: {pickup_count}\n\n"
            f"Менеджер (7 дней):\n{resp_str}\n\n"
            f"Воронка (7 дней):\n{funnel_str}\n\n"
            f"Топ товаров (7 дней):\n{top_str}"
        )
        send_msg(chat_id, msg)
        return

    if text.strip() in ("/заявки", "/orders"):
        is_owner = OWNER_CHAT_ID and (chat_id == OWNER_CHAT_ID or user_id == OWNER_CHAT_ID)
        is_manager = MANAGER_CHAT_ID and (chat_id == MANAGER_CHAT_ID or user_id == MANAGER_CHAT_ID)
        if not is_owner and not is_manager:
            send_msg(chat_id, "Команда доступна только менеджеру или владельцу.")
            return
        orders = load_orders(limit=10)
        if not orders:
            send_msg(chat_id, "Заявок пока нет.")
            return
        import datetime
        for o in reversed(orders):
            dt = datetime.datetime.fromtimestamp(o["ts"]).strftime("%d.%m %H:%M")
            lines = [f"{dt} — {o.get('name', '—')}"]
            lines.append(f"  {o.get('product', '—')}, {o.get('tons', '?')} т, {o.get('delivery', '—')}")
            if o.get("address"):
                lines.append(f"  Адрес: {o['address']}")
            lines.append(f"  Тел: {o.get('phone', '—')}")
            if o.get("material_cost"):
                total = o["material_cost"] + (o.get("delivery_cost") or 0)
                lines.append(f"  ~{total:,} руб.".replace(",", " "))
            btn = [[{"type": "callback", "text": "Ответить", "payload": f"reply_{o['client_id']}"}]]
            send_msg(chat_id, "\n".join(lines), btn)
        return

    if text.strip() == "/myid":
        with open(MANAGER_ID_FILE, "w") as f:
            f.write(str(chat_id))
        MANAGER_CHAT_ID = chat_id
        send_msg(chat_id, f"Ваш Max ID: {chat_id}\nВы сохранены как менеджер — заявки будут приходить вам.")
        print(f"[MYID] Менеджер сохранён: {user_name} -> {chat_id}")
        return

    if text.strip() == "/ownerid":
        with open(OWNER_ID_FILE, "w") as f:
            f.write(str(chat_id))
        OWNER_CHAT_ID = chat_id
        send_msg(chat_id, f"Ваш Max ID: {chat_id}\nВы сохранены как владелец — будете получать копии всех заявок.")
        print(f"[OWNERID] Владелец сохранён: {user_name} -> {chat_id}")
        return

    # /newpoll — визард создания опроса (только владелец)
    if text.strip() in ("/newpoll", "/опрос"):
        is_owner = OWNER_CHAT_ID and (chat_id == OWNER_CHAT_ID or user_id == OWNER_CHAT_ID)
        if not is_owner:
            send_msg(chat_id, "Команда доступна только владельцу.")
            return
        poll_wizard_data[chat_id] = {"question": None, "options": []}
        user_state[chat_id] = POLL_WIZ_QUESTION
        send_msg(chat_id, "Напиши вопрос для опроса:")
        return

    # Poll wizard: обработка шагов
    wiz_state = user_state.get(chat_id)
    if wiz_state in (POLL_WIZ_QUESTION, POLL_WIZ_OPTIONS, POLL_WIZ_TARGET):
        is_owner = OWNER_CHAT_ID and (chat_id == OWNER_CHAT_ID or user_id == OWNER_CHAT_ID)
        if not is_owner:
            user_state.pop(chat_id, None)
            poll_wizard_data.pop(chat_id, None)
        else:
            if wiz_state == POLL_WIZ_QUESTION:
                poll_wizard_data[chat_id]["question"] = text.strip()
                user_state[chat_id] = POLL_WIZ_OPTIONS
                send_msg(chat_id, "Варианты ответа — каждый с новой строки.\nНапример:\nДа\nНет\nХочу посмотреть")
                return
            elif wiz_state == POLL_WIZ_OPTIONS:
                lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
                if len(lines) < 2:
                    # Может быть через | 
                    lines = [l.strip() for l in text.strip().split("|") if l.strip()]
                if len(lines) < 2:
                    send_msg(chat_id, "Нужно минимум 2 варианта. Каждый с новой строки или через |")
                    return
                poll_wizard_data[chat_id]["options"] = lines
                q = poll_wizard_data[chat_id]["question"]
                opts_text = "\n".join(f"  {i+1}. {o}" for i, o in enumerate(lines))
                preview = f"Опрос:\n\n{q}\n\n{opts_text}\n\nОтправить в канал?"
                btns = [
                    [{"type": "callback", "text": "Отправить в канал", "payload": "/pollwiz_channel"}],
                    [{"type": "callback", "text": "Отправить мне (тест)", "payload": "/pollwiz_me"}],
                    [{"type": "callback", "text": "Отмена", "payload": "/pollwiz_cancel"}],
                ]
                send_msg(chat_id, preview, btns)
                user_state[chat_id] = POLL_WIZ_TARGET
                return
            elif wiz_state == POLL_WIZ_TARGET:
                # Текстовый ответ вместо кнопки — отменяем
                user_state.pop(chat_id, None)
                poll_wizard_data.pop(chat_id, None)
                send_msg(chat_id, "Визард опроса отменён. Нажми кнопку или /newpoll заново.")
                return

    # /poll <chat_id> <question> | opt1 | opt2 | ... — создать опрос (только владелец)
    if text.strip().startswith("/poll "):
        is_owner = OWNER_CHAT_ID and (chat_id == OWNER_CHAT_ID or user_id == OWNER_CHAT_ID)
        if not is_owner:
            send_msg(chat_id, "Команда доступна только владельцу.")
            return
        try:
            raw = text.strip()[6:]
            first_space = raw.index(" ")
            target_chat = int(raw[:first_space])
            rest = raw[first_space + 1:]
            parts = [p.strip() for p in rest.split("|")]
            question = parts[0]
            options = parts[1:]
            if len(options) < 2:
                send_msg(chat_id, "Нужно минимум 2 варианта.\nФормат: /poll <chat_id> Вопрос? | вариант1 | вариант2")
                return
            pid = send_poll(target_chat, question, options)
            if pid:
                send_msg(chat_id, f"Опрос создан: {pid}\nВариантов: {len(options)}")
            else:
                send_msg(chat_id, "Ошибка создания опроса.")
        except Exception as e:
            send_msg(chat_id, f"Ошибка: {e}\nФормат: /poll <chat_id> Вопрос? | вариант1 | вариант2")
        return

    if text.strip() in ("/menu", "/помощь", "/help", "меню"):
        is_owner = OWNER_CHAT_ID and (chat_id == OWNER_CHAT_ID or user_id == OWNER_CHAT_ID)
        is_manager = MANAGER_CHAT_ID and (chat_id == MANAGER_CHAT_ID or user_id == MANAGER_CHAT_ID)
        if not is_owner and not is_manager:
            send_msg(chat_id, "Напишите что вам нужно или /start для новой заявки.")
            return
        btns = [
            [{"type": "callback", "text": "Новая заявка", "payload": "/start"}],
            [{"type": "callback", "text": "Отменить заявку", "payload": "/cancel"}],
        ]
        menu_text = (
            "Меню:\n\n"
            "/price — прайс-лист\n"
            "/start — новая заявка\n"
            "/cancel — отменить текущую\n"
        )
        if is_manager or is_owner:
            btns.append([{"type": "callback", "text": "Список заявок", "payload": "/заявки"}])
            menu_text += "/заявки — последние заявки\n"
        if is_owner:
            btns.append([{"type": "callback", "text": "Статистика", "payload": "/stats"}])
            menu_text += "/stats — статистика и воронка\n"
        menu_text += "/menu — это меню"
        send_msg(chat_id, menu_text, btns)
        return

    if text.strip() in ("/price", "/прайс", "/цены", "прайс", "цены"):
        send_msg(chat_id, _format_price_list())
        return

    if text.strip() in ("/cancel", "/отмена"):
        user_state.pop(chat_id, None)
        user_data.pop(chat_id, None)
        send_msg(chat_id, "Хорошо, отменили. Если надумаете — пишите /start")
        return

    # Начало / рестарт диалога
    if text.strip() in ("/start", "начать", "start") or chat_id not in user_state:
        if chat_id not in user_state:
            track_event("conversation_started", chat_id=chat_id)
        user_data[chat_id] = {}
        skip = text.strip() in ("/start", "начать", "start", "")
        greeting_words = ["привет", "здравствуй", "добрый", "хай", "hello", "hi"]
        if not skip and len(text.split()) <= 2 and any(w in text.lower() for w in greeting_words):
            skip = True

        if not skip:
            parsed = parse_order(text)
            found = []
            d = user_data[chat_id]
            if parsed.items and len(parsed.items) > 1:
                valid_items = [
                    {"product": it.product, "tons": it.tons, "price_per_ton": PRODUCTS.get(it.product)}
                    for it in parsed.items if it.product and it.tons and it.tons > 0
                ]
                if len(valid_items) > 1:
                    d["items"] = valid_items
                    d["product"] = ", ".join(i["product"] for i in valid_items)
                    d["tons"] = sum(i["tons"] for i in valid_items)
                    d["volume_text"] = " + ".join(f"{i['tons']}т {i['product']}" for i in valid_items)
                    for i in valid_items:
                        found.append(f"{i['product']}: {i['tons']} т")
            if not d.get("product") and parsed.product and parsed.product in PRODUCTS:
                d["product"] = parsed.product
                d["price_per_ton"] = PRODUCTS[parsed.product]
                found.append(f"Товар: {parsed.product}")
            if not d.get("tons") and parsed.tons and parsed.tons > 0:
                d["tons"] = parsed.tons
                d["volume_text"] = f"{parsed.tons} т"
                found.append(f"Объём: {parsed.tons} т")
            if parsed.delivery:
                d["delivery"] = parsed.delivery
                found.append(f"Получение: {parsed.delivery}")
            if parsed.address:
                d["address"] = parsed.address
                found.append(f"Адрес: {parsed.address}")
            if GROQ_API_KEY:
                try:
                    contacts = parse_contacts_groq(text)
                    if contacts.name:
                        d["contact_name"] = contacts.name
                        found.append(f"Имя: {contacts.name}")
                    if contacts.company:
                        d["company"] = contacts.company
                        found.append(f"Компания: {contacts.company}")
                    if contacts.phone:
                        d["phone"] = contacts.phone
                        found.append(f"Телефон: {contacts.phone}")
                except Exception as e:
                    print(f"[START] contacts error: {e}")
            if not d.get("phone"):
                m = re.search(r"[\+\d][\d\s\-\(\)]{9,}", text)
                if m and len(re.sub(r'\D', '', m.group(0))) >= 10:
                    d["phone"] = m.group(0).strip()
                    found.append(f"Телефон: {d['phone']}")
            if found:
                send_msg(chat_id, "Вот что нашёл в вашем сообщении:\n" + "\n".join(found))
        else:
            send_msg(chat_id,
                "Здравствуйте! Рады вас видеть!\n\n"
                "Вы обратились в Архиповский карьер — поставляем щебень, отсев, гравий, песок и ГПС "
                "по Краснодарскому краю.\n\n"
                "Помогу оформить заявку прямо сейчас.\n\n"
                "Напишите что вам нужно или выберите из меню. Для отмены — /cancel"
            )
        new_state = advance(chat_id)
        if new_state >= 0:
            user_state[chat_id] = new_state
            track_event("funnel_step", chat_id=chat_id, step=FUNNEL_NAMES.get(new_state, str(new_state)))
        return

    # Обработка текущего состояния
    state = user_state.get(chat_id, PRODUCT)
    d = user_data.setdefault(chat_id, {})

    if state == PRODUCT:
        if text in PRODUCTS:
            d["product"] = text
            d["price_per_ton"] = PRODUCTS[text]
        else:
            try_parse_freeform(text, chat_id)
            if not d.get("product"):
                d["product"] = text[:60]
                d["price_per_ton"] = None
                d["tons"] = d.get("tons") or 1
                d["volume_text"] = "уточнить"
                send_msg(chat_id, f"По продукции «{text[:60]}» менеджер подберёт условия и свяжется с вами.")

    elif state == VOLUME:
        tons = parse_tons(text, d.get("product"))
        if tons and tons > 0:
            d["tons"] = tons
            # Если ввод в кубах — показываем конвертацию
            m_vol = re.search(r"(\d+[.,]?\d*)\s*(куб\w*|м[³3]|кубометр\w*)", text.lower())
            if m_vol:
                raw_cubes = float(m_vol.group(1).replace(",", "."))
                density = DENSITY.get(d.get("product", ""), DEFAULT_DENSITY)
                d["volume_text"] = f"{raw_cubes:.0f} м³ = {tons} т"
                send_msg(chat_id, f"Принял: {raw_cubes:.0f} м³ × {density} т/м³ = {tons} тонн")
            else:
                d["volume_text"] = f"{tons} т"
        else:
            try_parse_freeform(text, chat_id)
            if not d.get("tons"):
                send_msg(chat_id, "Пожалуйста, укажите объём числом, например: 30 тонн или 20 кубов")
                return

    elif state == DELIVERY:
        t = text.lower()
        if any(w in t for w in ["самовывоз", "сам заберу", "заберу сам", "заберем", "заберём"]):
            d["delivery"] = "Самовывоз"
        elif any(w in t for w in ["доставк", "привезите", "привезти", "доставьте"]):
            d["delivery"] = "Доставка"
        elif text in ("Самовывоз", "Доставка"):
            d["delivery"] = text
        else:
            # Попробуем распознать новый объём (если пользователь корректирует)
            vol_match = re.search(r"(\d+[.,]?\d*)\s*(тонн\w*|тн\b|т\b|куб\w*|м[³3]|машин\w*)", t)
            if vol_match:
                val = float(vol_match.group(1).replace(",", "."))
                unit_str = vol_match.group(2)
                if re.match(r"машин", unit_str):
                    val = val * 30  # 1 машина ≈ 30 тонн
                elif re.match(r"куб|м[³3]", unit_str):
                    val = round(val * 1.4)  # приблизительная конверсия
                d["tons"] = val
                d["volume_text"] = f"{val:.0f} т"
                send_msg(chat_id, f"Объём обновлён: {val:.0f} т")
                # Не устанавливаем delivery — advance() спросит снова
            else:
                try_parse_freeform(text, chat_id)
            if not d.get("delivery"):
                send_msg(chat_id, "Уточните: самовывоз или доставка?")
                return

    elif state == ADDRESS:
        d["address"] = text.strip()
        if GROQ_API_KEY and not d.get("phone"):
            try:
                contacts = parse_contacts_groq(text)
                if contacts.name and not d.get("contact_name"):
                    d["contact_name"] = contacts.name
                if contacts.company and not d.get("company"):
                    d["company"] = contacts.company
                if contacts.phone:
                    d["phone"] = contacts.phone
            except Exception:
                pass

    elif state == CONTACTS:
        try:
            parsed = parse_contacts_groq(text)
            if parsed.name:
                d["contact_name"] = parsed.name
            if parsed.company:
                d["company"] = parsed.company
            if parsed.phone:
                d["phone"] = parsed.phone
        except Exception as e:
            print(f"[CONTACTS] parse failed: {e}")
        if not d.get("phone"):
            m = re.search(r"[\+\d][\d\s\-\(\)]{9,}", text)
            if m and len(re.sub(r'\D', '', m.group(0))) >= 10:
                d["phone"] = m.group(0).strip()
        if not d.get("contact_name"):
            # Убираем телефон из текста, чтобы имя не стало "+79991234567"
            name_candidate = re.sub(r'[\+\d][\d\s\-\(\)]{9,}', '', text).strip(' ,')
            d["contact_name"] = name_candidate if name_candidate else text

    elif state == PHONE_ONLY:
        d["phone"] = text.strip()

    elif state == CONFIRM:
        t = text.lower().strip()
        if any(w in t for w in ["да", "верно", "отправить", "подтвержд", "ок", "ok", "yes", "✅"]):
            send_msg(chat_id, "⏳ Принимаю заявку, рассчитываю маршрут...")
            try:
                finalize(chat_id)
            except Exception as e:
                print(f"[FINALIZE] Ошибка: {e}", flush=True)
                send_msg(chat_id, "Произошла ошибка при оформлении. Напишите /start и попробуйте снова.")
            user_state.pop(chat_id, None)
            user_data.pop(chat_id, None)
            save_state()
            return
        elif any(w in t for w in ["нет", "заново", "начать", "отмена", "cancel", "❌"]):
            user_state.pop(chat_id, None)
            user_data.pop(chat_id, None)
            save_state()
            send_msg(chat_id, "Хорошо, начнём заново. Напишите что вам нужно или /start")
            return
        else:
            send_msg(chat_id, "Нажмите ✅ Отправить заявку или ❌ Начать заново")
            return

    new_state = advance(chat_id)
    if new_state >= 0:
        user_state[chat_id] = new_state
        track_event("funnel_step", chat_id=chat_id, step=FUNNEL_NAMES.get(new_state, str(new_state)))
def handle_callback(user_id: int, chat_id: int, callback_id: str, payload: str, **kwargs):
    global processed_callbacks
    # Игнорируем повторные нажатия одной и той же кнопки
    with _dedup_lock:
        if callback_id in processed_callbacks:
            answer_cb(callback_id)
            print(f"[CB] Дубль проигнорирован: {callback_id[:20]}", flush=True)
            return
        processed_callbacks.add(callback_id)
        if len(processed_callbacks) > 2000:
            processed_callbacks.clear()  # reset: set has no order, can't keep "recent"

    if payload == "voice_ok":
        print(f"[VOICE_CB] voice_ok: user_id={user_id}, chat_id={chat_id}, pending_keys={list(pending_voice.keys())}", flush=True)
        with _voice_lock:
            entry = pending_voice.pop(chat_id, None) or pending_voice.pop(user_id, None)
        if entry:
            transcribed, uname, uid, orig_chat_id = entry
            print(f"[VOICE_CB] found entry, orig_chat_id={orig_chat_id}, transcribed={transcribed!r}", flush=True)
            answer_cb(callback_id)
            send_msg(orig_chat_id, "✅ Принято, обрабатываю...")
            handle_message(orig_chat_id, transcribed, uname, user_id=uid)
        else:
            print(f"[VOICE_CB] entry not found in pending_voice", flush=True)
            answer_cb(callback_id)
        return

    # Кнопки меню
    if payload in ("/start", "/cancel", "/заявки", "/stats", "/menu", "/newpoll"):
        answer_cb(callback_id)
        handle_message(chat_id, payload, "", user_id=user_id)
        return

    # Poll wizard callbacks
    if payload.startswith("/pollwiz_"):
        answer_cb(callback_id)
        wiz = poll_wizard_data.get(chat_id) or poll_wizard_data.get(user_id)
        src_id = chat_id or user_id
        if not wiz:
            send_msg(src_id, "Визард опроса истёк. /newpoll — начать заново.")
            return
        if payload == "/pollwiz_cancel":
            user_state.pop(src_id, None)
            poll_wizard_data.pop(src_id, None)
            send_msg(src_id, "Опрос отменён.")
            return
        target_chat = src_id if payload == "/pollwiz_me" else -72678007708240
        q = wiz["question"]
        opts = wiz["options"]
        pid = send_poll(target_chat, q, opts)
        user_state.pop(src_id, None)
        poll_wizard_data.pop(src_id, None)
        if pid:
            where = "тебе (тест)" if payload == "/pollwiz_me" else "в канал"
            send_msg(src_id, f"Опрос отправлен {where}! ID: {pid}")
        else:
            send_msg(src_id, "Ошибка при создании опроса. Попробуй /newpoll заново.")
        return

    if payload == "voice_retry":
        with _voice_lock:
            entry = pending_voice.pop(chat_id, None) or pending_voice.pop(user_id, None)
        orig_chat_id = entry[3] if (entry and len(entry) > 3) else (chat_id or user_id)
        answer_cb(callback_id)
        send_msg(orig_chat_id, "Хорошо, отправьте голосовое ещё раз.")
        return

    if payload.startswith("reply_"):
        try:
            client_id = int(payload.split("_")[1])
            summary = order_summaries.get(client_id, "")
            pending_replies[user_id] = {
                "client_id": client_id,
                "expires": time.time() + REPLY_TIMEOUT,
                "summary": summary,
            }
            print(f"[REPLY] Менеджер {user_id} → клиент {client_id}: {summary}")
            context_line = f"\nЗаявка: {summary}" if summary else ""
            prompt = f"Напишите ответ — я перешлю клиенту:{context_line}\n\n/cancel_reply — отменить"
            result = send_msg(chat_id, prompt)
            if not result.get("message"):
                answer_cb(callback_id, "Напишите ответ — он будет переслан клиенту")
            else:
                answer_cb(callback_id)
        except Exception as e:
            print(f"[REPLY] Ошибка: {e}")
            answer_cb(callback_id, "Напишите ответ — он будет переслан клиенту")
        return

    if payload == "force_delivery":
        answer_cb(callback_id)
        d = user_data.get(chat_id, {})
        d["delivery"] = "Доставка"
        d["delivery_warning_shown"] = True
        user_data[chat_id] = d
        new_state = advance(chat_id)
        if new_state >= 0:
            user_state[chat_id] = new_state
        return

    if payload == "confirm_yes":
        answer_cb(callback_id)
        if chat_id in user_data:
            send_msg(chat_id, "⏳ Принимаю заявку, рассчитываю маршрут...")
            try:
                finalize(chat_id)
            except Exception as e:
                print(f"[FINALIZE] Ошибка: {e}", flush=True)
                send_msg(chat_id, "Произошла ошибка при оформлении. Напишите /start и попробуйте снова.")
            user_state.pop(chat_id, None)
            user_data.pop(chat_id, None)
            save_state()
        return

    if payload == "use_saved_contacts":
        answer_cb(callback_id)
        d = user_data.get(chat_id, {})
        saved = saved_contacts.get(chat_id, {})
        d["contact_name"] = saved.get("contact_name", "")
        d["phone"] = saved.get("phone", "")
        if saved.get("address") and not d.get("address"):
            d["address"] = saved["address"]
        user_data[chat_id] = d
        new_state = advance(chat_id)
        if new_state >= 0:
            user_state[chat_id] = new_state
        return

    if payload == "new_contacts":
        answer_cb(callback_id)
        d = user_data.get(chat_id, {})
        d.pop("contact_name", None)
        d.pop("phone", None)
        d.pop("contacts_asked", None)
        user_data[chat_id] = d
        user_state[chat_id] = CONTACTS
        send_msg(
            chat_id,
            "Напишите имя, организацию (если есть) и номер телефона."
        )
        return

    if payload == "confirm_edit":
        answer_cb(callback_id)
        d = user_data.get(chat_id, {})
        edit_btns = []
        if d.get("product"):
            edit_btns.append([{"type": "callback", "text": "Товар", "payload": "edit_product"}])
        if d.get("tons"):
            edit_btns.append([{"type": "callback", "text": "Объём", "payload": "edit_volume"}])
        if d.get("delivery"):
            edit_btns.append([{"type": "callback", "text": "Доставка/Самовывоз", "payload": "edit_delivery"}])
        if d.get("address"):
            edit_btns.append([{"type": "callback", "text": "Адрес", "payload": "edit_address"}])
        if d.get("phone"):
            edit_btns.append([{"type": "callback", "text": "Телефон", "payload": "edit_phone"}])
        send_msg(chat_id, "Что изменить?", edit_btns)
        return

    # Обработка конкретных edit_*
    if payload.startswith("edit_"):
        answer_cb(callback_id)
        d = user_data.get(chat_id, {})
        field = payload.replace("edit_", "")
        if field == "product":
            d.pop("product", None)
            d.pop("price_per_ton", None)
            d.pop("items", None)
        elif field == "volume":
            d.pop("tons", None)
            d.pop("volume_text", None)
            d.pop("items", None)
        elif field == "delivery":
            d.pop("delivery", None)
        elif field == "address":
            d.pop("address", None)
        elif field == "phone":
            d.pop("phone", None)
            d.pop("contact_name", None)
            d.pop("contacts_asked", None)
        user_data[chat_id] = d
        new_state = advance(chat_id)
        if new_state >= 0:
            user_state[chat_id] = new_state
        return

    if payload == "confirm_no":
        answer_cb(callback_id)
        user_state.pop(chat_id, None)
        user_data.pop(chat_id, None)
        save_state()
        send_msg(chat_id, "Хорошо, начнём заново. Напишите что вам нужно или /start")
        return

    # Голоса в опросах
    if payload.startswith("pollvote_"):
        handle_poll_vote(user_id, callback_id, payload, orig_msg=kwargs.get("orig_msg"))
        return

    answer_cb(callback_id)
    # Кнопки-варианты (продукт, доставка) — обрабатываем как текст
    handle_message(chat_id, payload, user_id=user_id)
def process_update_safe(update: dict):
    """Обёртка: определяет chat_id и выполняет update под per-user lock."""
    utype = update.get("update_type")
    # Определяем chat_id для блокировки
    if utype == "message_created":
        msg = update.get("message", {})
        chat_id = msg.get("recipient", {}).get("chat_id") or msg.get("sender", {}).get("user_id") or 0
    elif utype == "message_callback":
        cb = update.get("callback", {})
        orig_msg = cb.get("message", {})
        uid = cb.get("user", {}).get("user_id") or 0
        chat_id = orig_msg.get("recipient", {}).get("chat_id") or user_chat_map.get(uid) or uid or 0
    else:
        chat_id = 0

    lock = get_user_lock(chat_id) if chat_id else None
    try:
        if lock:
            lock.acquire()
        process_update(update)
        save_state()
    except Exception as e:
        import traceback
        err_text = f"[ERROR] {e}\n{traceback.format_exc()[:300]}"
        print(err_text, flush=True)
        # Отправляем ошибку owner'у чтобы не терять диагностику
        if OWNER_CHAT_ID:
            try:
                send_msg(OWNER_CHAT_ID, f"⚠️ Ошибка обработки:\n{err_text[:500]}")
            except:
                pass
    finally:
        if lock:
            lock.release()
# ─── Мониторинг группы "Архиповский блок" ─────────────────────────────────

def _debug_log_chat(chat_id: int, sender: str, text_preview: str):
    """Логируем все групповые chat_id для поиска нужной группы."""
    log_path = os.path.join(DATA_DIR, "group_chat_ids.log")
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | chat_id={chat_id} | sender={sender} | {text_preview}\n"
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass

def _normalize_location_for_dedup(loc: str) -> str:
    """Нормализует from_location для дедупликации.

    Цель: «Тихорецкая», «КРД», «Карьер» — это всё НАШИ объекты, должны давать
    одинаковый ключ (Claude может вернуть любой из этих вариантов для одного рейса).
    А вот «Великовечное», «Белореченск», «ИП Евсеев» — это РАЗНЫЕ поставщики,
    они должны давать разные ключи (иначе две закупки в один день потеряются).

    Также «Великовечное» и «Великовечное (ИП Евсеев)» — это ОДНО место,
    просто менеджер уточнил поставщика в скобках. Скобки и всё в них режем.
    """
    import re as _re
    if not loc:
        return ''
    s = loc.lower().strip()
    # Убираем уточнения в скобках: "Великовечное (ИП Евсеев)" → "Великовечное"
    s = _re.sub(r'\s*\([^)]*\)\s*', '', s).strip()
    # Убираем повторные пробелы
    s = _re.sub(r'\s+', ' ', s)
    # Наши собственные объекты — унифицируем
    our_keywords = ['тихорец', 'крд', 'краснодар', 'карьер', 'архип', 'наш', 'свой склад']
    if any(kw in s for kw in our_keywords):
        return '_ours_'
    # Generic «закупка» без имени поставщика — отдельная корзина
    if s in ('закуп', 'закупка', 'закупили'):
        return '_purchase_generic_'
    # Иначе — это имя поставщика, оставляем как есть
    return s


# Derived из основного TRUCK_TO_DRIVER (90): {короткий_номер: (полный_номер, водитель)}
# Регенерируется при импорте — добавишь машину в TRUCK_TO_DRIVER и она автоматом появится в fallback.
def _build_truck_shortcuts():
    import re as _re
    result = {}
    for full, driver in TRUCK_TO_DRIVER.items():
        m = _re.search(r'(\d{3})', full)
        if m:
            result[m.group(1)] = (full, driver)
    return result

_TRUCK_TO_DRIVER = _build_truck_shortcuts()


def _parse_pallet_transfer_fallback(text: str, msg_date: str = "") -> list:
    """Гибкий regex-фолбэк для перемещений поддонов между нашими складами.
    Триггер: 'поддон/подд/пд' + (упоминание 'перемещ/перемест' ИЛИ паттерн 'с X на Y' с двумя нашими локациями)."""
    t = text.strip()
    t_lo = t.lower()
    if not re.search(r'(?:поддон|подд|пд)', t_lo):
        return []
    has_move_word = bool(re.search(r'\b(перемещ|перемест)', t_lo))
    LOC_PAT = r'(?:тихорецк[а-яё]*|карьер[а-яё]*|крд|краснодар[а-яё]*|архип[а-яё]*)'
    m_from_to = re.search(rf'с\s+({LOC_PAT})\s+(?:на|в)\s+({LOC_PAT})', t_lo)
    if not has_move_word and not m_from_to:
        return []
    m_pall = re.search(r'(\d{1,4})\s*(?:поддон[а-я]*|подд|пд)\b', t_lo)
    if not m_pall:
        m_pall = re.search(r'поддон[а-я]*\s+(\d{1,4})', t_lo)
    if not m_pall:
        return []
    pallets = int(m_pall.group(1))
    LOC_MAP = {
        'тихорецк': 'Тихорецкая', 'тихорецкой': 'Тихорецкая', 'тихорецкая': 'Тихорецкая', 'тихорецкую': 'Тихорецкая',
        'карьер': 'Карьер', 'карьера': 'Карьер', 'карьеру': 'Карьер', 'карьере': 'Карьер',
        'крд': 'КРД', 'краснодар': 'КРД', 'краснодара': 'КРД', 'краснодаре': 'КРД', 'краснодару': 'КРД', 'краснодарскую': 'КРД',
        'архип': 'Архип', 'архипа': 'Архип',
    }
    def norm_loc(raw):
        key = raw.strip().lower()
        if key in LOC_MAP:
            return LOC_MAP[key]
        for prefix, name in LOC_MAP.items():
            if key.startswith(prefix):
                return name
        return raw.capitalize()
    from_loc = None
    to_loc = None
    if m_from_to:
        from_loc = norm_loc(m_from_to.group(1))
        to_loc = norm_loc(m_from_to.group(2))
    else:
        m_arrow = re.search(rf'({LOC_PAT})\s*(?:→|->|—|-|на)\s*({LOC_PAT})', t_lo)
        if m_arrow:
            from_loc = norm_loc(m_arrow.group(1))
            to_loc = norm_loc(m_arrow.group(2))
    truck = None
    driver = None
    m_truck = re.search(r'(?:для|машин[а-я]*|на)\s+(\d{3})\b', t_lo)
    if not m_truck:
        for cand in re.findall(r'\b(\d{3})\b', t_lo):
            if cand in _TRUCK_TO_DRIVER:
                truck, driver = _TRUCK_TO_DRIVER[cand]
                break
    elif m_truck.group(1) in _TRUCK_TO_DRIVER:
        truck, driver = _TRUCK_TO_DRIVER[m_truck.group(1)]
    date = msg_date or datetime.datetime.now().strftime("%d.%m.%Y")
    m_date = re.search(r'(?:от\s+)?(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?', t_lo)
    if m_date:
        dd = m_date.group(1).zfill(2)
        mm = m_date.group(2).zfill(2)
        yy_raw = m_date.group(3)
        yyyy = (yy_raw if (yy_raw and len(yy_raw) == 4)
                else (f"20{yy_raw}" if yy_raw else datetime.datetime.now().strftime("%Y")))
        date = f"{dd}.{mm}.{yyyy}"
    event = {
        "type": "pallet_transfer",
        "date": date,
        "from_location": from_loc,
        "to_location": to_loc,
        "pallets": pallets,
    }
    if truck:
        event["truck"] = truck
    if driver:
        event["driver"] = driver
    print(f"[FALLBACK_PARSE] pallet_transfer regex: {event}", flush=True)
    return [event]

def _parse_pallet_return_fallback(text: str, msg_date: str = "") -> list:
    """Гибкий regex-фолбэк для возвратов поддонов от клиента.
    Триггер: 'поддон/подд/пд' + (слова возврат/вернул*) + наличие клиента вида 'от ИП X' / 'от ООО X' / 'от Фамилии'."""
    t = text.strip()
    t_lo = t.lower()
    if not re.search(r'(?:поддон|подд|пд)', t_lo):
        return []
    if not re.search(r'\b(возврат|вернул[аи]?|вернувш)', t_lo):
        return []
    m_pall = re.search(r'(\d{1,4})\s*(?:поддон[а-я]*|подд|пд)\b', t_lo)
    if not m_pall:
        return []
    pallets = int(m_pall.group(1))
    client = None
    # 1) от ИП/ООО/АО/ЗАО/ПАО ИмяСобственное
    m_org = re.search(
        r'от\s+(ИП|ООО|АО|ЗАО|ПАО)\s+([А-ЯЁA-Z][А-ЯЁа-яёA-Za-z\.\-]+(?:\s+[А-ЯЁA-Z]\.?)*)',
        text
    )
    if m_org:
        client = f"{m_org.group(1)} {m_org.group(2)}".strip().rstrip(',.;')
    if not client:
        # 2) Просто «от Фамилия» (заглавная буква, не дата)
        for m in re.finditer(r'от\s+([А-ЯЁ][а-яё]{2,}(?:[а-яё]|ых|их|ой|ого|а|у)?)', text):
            cand = m.group(1).strip()
            if not re.match(r'^\d', cand):
                client = cand
                break
    if not client:
        # 3) Без «от» — например «N поддонов вернули от X» — но это уже покрыто; ещё «X вернул N поддонов»
        m_org2 = re.search(r'(ИП|ООО|АО|ЗАО|ПАО)\s+([А-ЯЁA-Z][А-ЯЁа-яёA-Za-z\.\-]+)', text)
        if m_org2:
            client = f"{m_org2.group(1)} {m_org2.group(2)}".strip()
    if not client:
        return []
    date = msg_date or datetime.datetime.now().strftime("%d.%m.%Y")
    m_date = re.search(r'(?:от\s+)?(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?', t_lo)
    if m_date:
        dd = m_date.group(1).zfill(2)
        mm = m_date.group(2).zfill(2)
        yy_raw = m_date.group(3)
        yyyy = (yy_raw if (yy_raw and len(yy_raw) == 4)
                else (f"20{yy_raw}" if yy_raw else datetime.datetime.now().strftime("%Y")))
        date = f"{dd}.{mm}.{yyyy}"
    obj = None
    m_obj = re.search(r'объект[ы]?\s+([а-яё]+)', t_lo)
    if not m_obj:
        m_obj = re.search(r'\bна\s+(склад|карьер|тихорецк[а-яё]*|крд|краснодар[а-яё]*)\b', t_lo)
    if m_obj:
        obj = m_obj.group(1).capitalize()
    truck = None
    driver = None
    m_truck = re.search(r'(?:на|машин[а-я]*|для)\s+(\d{3})\b', t_lo)
    if m_truck and m_truck.group(1) in _TRUCK_TO_DRIVER:
        truck, driver = _TRUCK_TO_DRIVER[m_truck.group(1)]
    if not truck:
        for cand in reversed(re.findall(r'\b(\d{3})\b', t_lo)):
            if cand in _TRUCK_TO_DRIVER:
                truck, driver = _TRUCK_TO_DRIVER[cand]
                break
    # Тип поддона: «1*» = «Поддон 1*»
    p_type = "Поддон 1*" if re.search(r'\b1\s*\*', t_lo) else ""
    event = {
        "type": "return",
        "date": date,
        "client": client,
        "pallets": pallets,
        "return_pallets": pallets,
    }
    if obj:
        event["to_location"] = obj
    if truck:
        event["truck"] = truck
    if driver:
        event["driver"] = driver
    if p_type:
        event["pallet_type"] = p_type
    print(f"[FALLBACK_PARSE] pallet_return regex: {event}", flush=True)
    return [event]

def _merge_combined_trip_events(events: list) -> list:
    """Страховка от Claude: если он вернул 2+ отдельных trip-события с одинаковыми
    (date, truck, driver, from_location, source) но разными клиентами — мерджим
    в один сборный рейс с deliveries[]. Работает только для type=trip без существующего
    deliveries[]. Все остальные события (correction, return, price, ...) проходят как есть."""
    from collections import defaultdict
    groups = defaultdict(list)
    others = []
    for e in events:
        if e.get('type') != 'trip' or e.get('deliveries'):
            others.append(e)
            continue
        key = (
            e.get('date') or '',
            e.get('truck') or '',
            e.get('driver') or '',
            (e.get('from_location') or '').lower(),
            (e.get('source') or '').lower(),
            (e.get('supplier') or '').lower(),
        )
        groups[key].append(e)
    result = list(others)
    for key, evts in groups.items():
        if len(evts) <= 1:
            result.extend(evts)
            continue
        # 2+ trip с одинаковым ключом — мерджим
        base = dict(evts[0])
        deliveries = []
        for e in evts:
            deliveries.append({
                'client': e.get('client'),
                'delivery_in': e.get('delivery_in'),
                'products': e.get('products_detail') or [],
            })
        base['deliveries'] = deliveries
        for k in ('client', 'products_detail', 'delivery_in', 'price_buy', 'price_sell', 'product', 'pallets'):
            base.pop(k, None)
        result.append(base)
        print(f"[MERGE] Сборный рейс: {len(evts)} событий слиты в одно, клиенты={[d.get('client') for d in deliveries]}", flush=True)
    return result


def _dedup_check(event: dict) -> bool:
    """Проверяет, не было ли это событие уже обработано за последние 24ч.
    Возвращает True если ДУБЛИКАТ (уже было), False если новое."""
    import hashlib
    now = time.time()
    with _dedup_lock:
        # Чистим устаревшие записи
        expired = [k for k, v in _dedup_cache.items() if now - v > _DEDUP_TTL]
        for k in expired:
            del _dedup_cache[k]
        # Хеш по ключевым полям события
        # from_location — нормализуется через _normalize_location_for_dedup:
        # наши склады (Тихорецкая/КРД/Карьер) сводятся к одному ключу,
        # а реальные поставщики (Великовечное и т.д.) различаются между собой.
        key_parts = [
            event.get('type') or '',
            event.get('date') or '',
            event.get('client') or '',
            event.get('product') or event.get('price_product') or '',
            str(event.get('price') or event.get('price_new') or ''),
            event.get('driver') or '',
            event.get('truck') or '',
            _normalize_location_for_dedup(event.get('from_location') or ''),
            event.get('to_location') or '',
            event.get('price_contact') or '',
            event.get('price_date_from') or '',
            event.get('object_name') or '',
            str(event.get('payment_amount') or ''),
            event.get('payment_type') or '',
            event.get('payment_method') or '',
            str(event.get('pallets') or ''),
            str(event.get('return_pallets') or ''),
        ]
        h = hashlib.md5('|'.join(key_parts).lower().encode()).hexdigest()
        if h in _dedup_cache:
            print(f"[DEDUP] Дубликат отклонён: {key_parts}", flush=True)
            return True
        _dedup_cache[h] = now
        _save_dedup_cache(_dedup_cache)
        return False
def _normalize_truck(truck: str) -> str:
    """Раскрывает сокращения номеров машин: 135 → у135рх 193"""
    if not truck:
        return truck
    truck_stripped = truck.strip()
    # Если это просто число (сокращение)
    if truck_stripped in TRUCK_SHORTCUTS:
        return TRUCK_SHORTCUTS[truck_stripped]
    # Если число внутри текста: "135го", "135-му"
    for short, full in TRUCK_SHORTCUTS.items():
        if short in truck_stripped and len(truck_stripped) < 10:
            return full
    return truck
def _log_blok_message(sender_name: str, text: str, raw: dict):
    """Пишем сырое сообщение из группы в лог-файл для анализа формата."""
    log_path = os.path.join(DATA_DIR, "blok_group_log.jsonl")
    entry = {
        "ts": datetime.datetime.now().isoformat(),
        "sender": sender_name,
        "text": text,
        "raw": raw,
    }
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass
    print(f"[BLOK_GROUP] {sender_name}: {text[:80]}", flush=True)
# Промпт парсера вынесен в отдельный модуль parser_prompt.py чтобы
# tests/runner.py гонял ТОЧНО ТАКОЙ ЖЕ промпт что и прод. См. parser_prompt.build_parser_prompt.
from parser_prompt import build_parser_prompt as _build_parser_prompt


def _parse_blok_plan_claude(text: str, msg_date: str = "") -> list:
    """Парсит план менеджера через Claude API. Возвращает список рейсов."""
    if not CLAUDE_API_KEY:
        return []
    prompt = _build_parser_prompt(text, msg_date)

    body = json.dumps({
        "model": "claude-sonnet-4-6",
        "max_tokens": 4096,  # увеличено для длинных правок с details и множественных событий
        "system": "Ты JSON-only парсер. Отвечай ИСКЛЮЧИТЕЛЬНО валидным JSON-массивом. Без markdown, без объяснений, без префиксов, без code-блоков. Если событий нет — верни []. Никакого текста до '[' или после ']'.",
        "messages": [{"role": "user", "content": prompt}]
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "x-api-key": CLAUDE_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
    )
    for _attempt in range(2):
      try:
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.loads(r.read())
        raw_text = resp["content"][0]["text"].strip()
        # Убираем markdown-блок если есть
        raw_text = re.sub(r'^```[a-z]*\n?', '', raw_text)
        raw_text = re.sub(r'\n?```$', '', raw_text)
        try:
            events = json.loads(raw_text)
        except json.JSONDecodeError:
            # Fallback: попробовать вытащить JSON-массив regex'ом (если Claude всё-таки добавил прозу вокруг)
            m = re.search(r'\[\s*\{[\s\S]*\}\s*\]', raw_text)
            events = None
            if m:
                try:
                    events = json.loads(m.group(0))
                except json.JSONDecodeError:
                    events = None
            if events is None:
                if _attempt == 0:
                    print(f"[BLOK_PARSE] Невалидный JSON от Claude (попытка 1), повтор...", flush=True)
                    continue
                else:
                    print(f"[BLOK_PARSE] Невалидный JSON от Claude (попытка 2): {raw_text[:300]}", flush=True)
                    return []
        # Нормализация полей (Claude иногда возвращает альтернативные имена)
        for evt in events:
            if 'new_price' in evt and 'price_new' not in evt:
                evt['price_new'] = evt.pop('new_price')
            if 'old_price' in evt and 'price_old' not in evt:
                evt['price_old'] = evt.pop('old_price')
            if 'object' in evt and 'price_product' not in evt and evt.get('type') == 'price':
                evt['price_product'] = evt.pop('object')
            if 'product' in evt and 'price_product' not in evt and evt.get('type') == 'price':
                evt['price_product'] = evt.pop('product')
            if 'effective_date' in evt and 'price_date_from' not in evt:
                evt['price_date_from'] = evt.pop('effective_date')
            if 'date_from' in evt and 'price_date_from' not in evt:
                evt['price_date_from'] = evt.pop('date_from')
            if 'name' in evt and 'object_name' not in evt and evt.get('type') == 'object_add':
                evt['object_name'] = evt.pop('name')
            if 'amount' in evt and 'payment_amount' not in evt:
                evt['payment_amount'] = evt.pop('amount')
            if 'method' in evt and 'payment_method' not in evt:
                evt['payment_method'] = evt.pop('method')
            if 'sum' in evt and 'payment_amount' not in evt:
                evt['payment_amount'] = evt.pop('sum')
            if 'contact' in evt and 'price_contact' not in evt and evt.get('type') == 'price':
                evt['price_contact'] = evt.pop('contact')
            if 'contact_name' in evt and 'price_contact' not in evt and evt.get('type') == 'price':
                evt['price_contact'] = evt.pop('contact_name')
            if 'qty_per_pallet' in evt and 'price_qty_per_pallet' not in evt:
                evt['price_qty_per_pallet'] = evt.pop('qty_per_pallet')
            # payment_date → date (нормализация для payment_info)
            if 'payment_date' in evt and 'date' not in evt:
                evt['date'] = evt.pop('payment_date')
        # Нормализация дат: Claude может вернуть "2026-05-15" вместо "15.05.2026"
        import re as _re_date
        for evt in events:
            for dk in ('date', 'price_date_from'):
                dv = evt.get(dk)
                if dv and isinstance(dv, str):
                    m = _re_date.match(r'(\d{4})-(\d{1,2})-(\d{1,2})', dv)
                    if m:
                        evt[dk] = f"{int(m.group(3)):02d}.{int(m.group(2)):02d}.{m.group(1)}"
        return events
      except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode('utf-8', errors='replace')[:500]
        except Exception:
            err_body = ""
        if _attempt == 0:
            print(f"[BLOK_PARSE] HTTPError (попытка 1): {e} | body={err_body}, повтор...", flush=True)
            time.sleep(1)
            continue
        print(f"[BLOK_PARSE] HTTPError (попытка 2): {e} | body={err_body}", flush=True)
        return []
      except Exception as e:
        if _attempt == 0:
            print(f"[BLOK_PARSE] Ошибка (попытка 1): {e}, повтор...", flush=True)
            time.sleep(1)
            continue
        print(f"[BLOK_PARSE] Ошибка (попытка 2): {e}", flush=True)
        return []
    return []  # fallback

# ─── Складской учёт: маппинг продукции → мини-таблицы на КРД(склад) / Карьер(склад) ───

# Каждая мини-таблица = 11 колонок. Offset +4 от начала = "Отгрузка (Подд, шт.)"
# Строка 5 = 01.05.2026 (1-е число месяца), строка 6 = 02.05 и т.д.
# Бот пишет ТОЛЬКО кол-во поддонов в "Отгрузка (Подд, шт.)", остальное — формулы.

_WAREHOUSE_MAP_KRD = {
    # Блок 20, 3-пустотный, отсев
    "20(3-0)":        0,    # 90 шт/подд — дефолт для КРД
    "20(3-0)/90":     0,
    "20(3-0)/72":     11,
    "20(3-0)/75":     22,
    # Блок 20, 3-пуст, керамзит
    "20(3-0)керамзит":     33,   # 10-11 кг (лёгкий) — дефолт керамзит
    "20(3-0)керамзит/10":  33,
    "20(3-0)керамзит/16":  44,   # 16-17 кг (тяжёлый)
    # Блок 20, 4-пустотный
    "20(4-0)":        55,   # 90 шт/подд — дефолт
    "20(4-0)/90":     55,
    "20(4-0)/72":     66,
    # Блок 12
    "12(2-0)":        88,   # 120 шт/подд — дефолт
    "12(2-0)/150":    77,
    "12(2-0)/120":    88,
    # Блок 9, отсев
    "9(2-0)":         99,   # 180 шт/подд — дефолт
    "9(2-0)/180":     99,
    "9(2-0)/144":     110,
    "9(2-0)/150":     121,
    "9(2-0)/120":     132,
    # Блок 9, керамзит
    "9(2-0)керамзит":      143,  # 144 шт/подд — дефолт
    "9(2-0)керамзит/144":  143,
    "9(2-0)керамзит/180":  154,
}

_WAREHOUSE_MAP_KARYER = {
    "20(3-0)":        0,    # 75 шт/подд — дефолт для Карьера
    "20(3-0)/75":     0,
    "20(3-0)/72":     11,
    "20(4-0)":        22,   # 75 шт/подд
    "20(4-0)/75":     22,
    "12(2-0)":        33,   # 120 шт/подд — дефолт
    "12(2-0)/120":    33,
    "12(2-0)/150":    44,
    "9(2-0)":         55,   # 150 шт/подд — дефолт
    "9(2-0)/150":     55,
    "9(2-0)/144":     66,
    "20(3-0)керамзит": 77,  # 75 шт/подд
}

def _apply_warehouse_shipment(from_location: str, product: str, pallets: int, trip_date: str = "", products_detail: list = None):
    """Записывает кол-во отгруженных поддонов в КРД(склад) или Карьер(склад).

    Args:
        from_location: откуда отгрузка ("КРД", "Карьер")
        product: код продукции ("20(3-0)", "20(3-0)+9(2-0)", и т.д.)
        pallets: кол-во поддонов (общее, используется если нет products_detail)
        trip_date: дата рейса ДД.ММ.ГГГГ (если пусто — сегодня МСК)
        products_detail: список [{"code":"20(3-0)","pallets":10,"qty_per_pallet":72}, ...]

    Returns:
        (ok: bool, message: str)
    """
    if not GOOGLE_SA_B64:
        return False, "Нет GOOGLE_SA_B64"

    loc = (from_location or "").strip().upper()
    if "КРД" in loc or "КРАСНОДАР" in loc or "ТИХОРЕЦК" in loc or "ТИХОРЕ" in loc:
        sheet_name = "КРД(склад)"
        wh_map = _WAREHOUSE_MAP_KRD
    elif "КАРЬЕР" in loc or "АРХИП" in loc:
        sheet_name = "Карьер(склад)"
        wh_map = _WAREHOUSE_MAP_KARYER
    else:
        return False, f"Неизвестный склад: {from_location}"

    date_str = trip_date or _today_msk()
    try:
        day = int(date_str.split(".")[0])
        gs_row = 4 + day
    except (ValueError, IndexError):
        return False, f"Не могу определить день из даты: {date_str}"

    if gs_row < 5 or gs_row > 35:
        return False, f"День {day} вне диапазона (1-31)"

    # Строим список продуктов с поддонами
    items = []
    if products_detail and isinstance(products_detail, list):
        for pd in products_detail:
            code = pd.get("code", "").strip()
            pd_pallets = pd.get("pallets", 0)
            qpp = pd.get("qty_per_pallet")
            if code and pd_pallets and int(pd_pallets) > 0:
                key = f"{code}/{qpp}" if qpp else code
                items.append((key, int(pd_pallets)))
    
    if not items:
        # Фоллбек: старая логика — один продукт, общее кол-во поддонов
        if not pallets or pallets <= 0:
            return False, "Не указано кол-во поддонов"
        products_list = [p.strip() for p in product.split("+") if p.strip()]
        if not products_list:
            return False, f"Пустой product: {product}"
        if len(products_list) == 1:
            items = [(products_list[0], pallets)]
        else:
            # Несколько продуктов через "+" — делим поддоны поровну
            per_product = max(1, pallets // len(products_list))
            remainder = pallets - per_product * len(products_list)
            items = [(p, per_product + (1 if i == 0 and remainder > 0 else 0)) for i, p in enumerate(products_list)]
            print(f"[WAREHOUSE] split {pallets} pallets across {products_list}: {items}", flush=True)

    try:
        import base64
        import gspread
        from google.oauth2.service_account import Credentials

        sa_json = base64.b64decode(GOOGLE_SA_B64)
        sa_info = json.loads(sa_json)
        creds = Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEETS_ID)
        ws = sh.worksheet(sheet_name)

        messages = []
        for prod_key, prod_pallets in items:
            start_col = wh_map.get(prod_key)
            if start_col is None:
                base_key = prod_key.split("/")[0]
                start_col = wh_map.get(base_key)
            if start_col is None:
                messages.append(f"⚠️ '{prod_key}' не найден в маппинге")
                continue

            gs_col = start_col + 4 + 1
            current = ws.cell(gs_row, gs_col).value
            current_val = 0
            if current:
                try:
                    current_val = int(str(current).replace("\xa0", "").replace(" ", "").strip())
                except ValueError:
                    current_val = 0

            new_val = current_val + prod_pallets
            ws.update_cell(gs_row, gs_col, new_val)

            product_name = ws.cell(1, start_col + 1).value or prod_key
            msg = f"{product_name[:40]}: {current_val}→{new_val} подд."
            messages.append(msg)
            print(f"[WAREHOUSE] {sheet_name} {msg} ({date_str})", flush=True)

        full_msg = f"{sheet_name} {date_str}: " + " | ".join(messages)
        return True, full_msg
    except Exception as e:
        print(f"[WAREHOUSE] Ошибка записи в {sheet_name}: {e}", flush=True)
        _alert_admin(f"Склад: ошибка записи в {sheet_name}", e)
        return False, f"Ошибка записи в {sheet_name}: {e}"

def _apply_correction(event: dict):
    """Применяет правку к существующей записи в Sheets.

    event = {
        type: "correction",
        target_table: "Закупки" | "Рейсы" | "Склад",
        target_num: int (опционально),
        target_date: "ДД.ММ.ГГГГ" (опционально),
        target_client: str (опционально),
        updates: {"поле": значение, ...}
    }

    Возвращает (success: bool, message: str).
    """
    if not GOOGLE_SA_B64:
        return False, "Нет GOOGLE_SA_B64"

    table = (event.get("target_table") or "").strip()
    target_num = event.get("target_num")
    target_date = (event.get("target_date") or "").strip()
    target_client = (event.get("target_client") or "").strip()
    updates = event.get("updates") or {}

    if not table:
        return False, "Не указана таблица для правки"
    if not updates:
        return False, "Нет полей для обновления"

    # Маппинг таблиц → имя листа + правила поиска строки + маппинг полей → колонки
    SHEET_CONFIG = {
        "Закупки": {
            "sheet": "Закупки",
            "num_col": 1,        # A — № записи
            "date_col": 2,       # B — дата
            "client_col": 11,    # K — клиент
            "data_start_row": 4, # с какой строки начинаются данные
            "fields": {
                "поставщик": 3,                   # C
                "откуда": 4,                       # D
                "продукция": 5,                    # E
                "шт": 6,                           # F
                "кол-во_шт": 6,
                "подд": 7,                         # G
                "кол-во_подд": 7,
                "поддоны": 7,
                "цена_закупки": 8,                 # H
                "доставка_от_поставщика": 10,     # J
                "доставка_поставщик": 10,
                "клиент": 11,                      # K
                "дата_продажи": 12,                # L
                "цена_продажи": 13,                # M
                "доставка_клиенту": 15,           # O
                "доставка_клиент": 15,
            },
        },
        "Рейсы": {
            "sheet": "Рейсы",
            "num_col": 1,        # A — № п/п
            "date_col": 2,       # B — Дата
            "client_col": 7,     # G — Организация
            "data_start_row": 3, # шапка в 2-й строке
            "fields": {
                "дата": 2,                         # B
                "машина": 3,                       # C
                "номер_машины": 3,
                "водитель": 4,                     # D
                "откуда": 5,                       # E
                "куда": 6,                         # F
                "организация": 7,                  # G
                "клиент": 7,
                "продукция": 8,                    # H
                "источник": 9,                     # I
            },
        },
    }

    config = SHEET_CONFIG.get(table)
    if not config:
        return False, f"Неизвестная таблица: {table}"

    try:
        import base64
        import gspread
        from google.oauth2.service_account import Credentials
        sa_info = json.loads(base64.b64decode(GOOGLE_SA_B64))
        creds = Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEETS_ID)
        ws = sh.worksheet(config["sheet"])

        # Поиск строки: сначала по target_num, потом по date+client
        target_row = None
        if target_num is not None:
            col_a = ws.col_values(config["num_col"])
            num_str = str(target_num).strip()
            for i, v in enumerate(col_a, start=1):
                if v.strip() == num_str and i >= config["data_start_row"]:
                    target_row = i
                    break

        if target_row is None and target_date:
            # Нормализуем target_date (поддержка «18.05.26 г.» → «18.05.2026»)
            target_date_norm = _normalize_date(target_date)
            col_date = ws.col_values(config["date_col"])
            col_client = ws.col_values(config["client_col"]) if target_client else []
            for i, v in enumerate(col_date, start=1):
                if i < config["data_start_row"]:
                    continue
                row_date_norm = _normalize_date(v.strip())
                if target_date_norm and row_date_norm == target_date_norm:
                    if target_client:
                        client_in_row = col_client[i-1] if i-1 < len(col_client) else ""
                        if target_client.lower() in client_in_row.lower():
                            target_row = i
                            break
                    else:
                        target_row = i
                        break

        if target_row is None:
            return False, f"Не нашёл запись в {table} (num={target_num}, date={target_date}, client={target_client})"

        # Применяем обновления по полям (общие поля родителя)
        applied = []
        unknown = []
        for field, value in updates.items():
            field_norm = field.lower().strip().replace(" ", "_")
            col_idx = config["fields"].get(field_norm)
            if col_idx is None:
                unknown.append(field)
                continue
            col_letter = chr(64 + col_idx) if col_idx <= 26 else "A" + chr(64 + col_idx - 26)
            ws.update(values=[[value]], range_name=f'{col_letter}{target_row}',
                      value_input_option='USER_ENTERED')
            applied.append(f"{field}={value}")

        # === МНОГОПРОДУКТОВАЯ ПРАВКА (details) — только для Закупок ===
        # Если в правке details=[{...}, {...}] — это закупка с 2+ блоками.
        # Превращаем целевую строку в РОДИТЕЛЯ + создаём ДОЧЕРНИЕ строки 2.1, 2.2, ...
        details = event.get("details") or []
        if details and table == "Закупки":
            parent_num = updates.get("номер") or target_num
            if not parent_num:
                # Берём текущее значение А
                a_val = ws.cell(target_row, 1).value
                try:
                    parent_num = int(float(str(a_val).replace(",", ".")))
                except (ValueError, TypeError):
                    parent_num = None

            if parent_num is None:
                return False, f"Не определил № родителя для details (строка {target_row})"

            # ШАГ 1: удаляем старые дочерние строки родителя (где А="N.X")
            col_a_all = ws.col_values(1)
            children_to_delete = []
            for i, v in enumerate(col_a_all, start=1):
                if v.strip().startswith(f"{parent_num}.") and i != target_row:
                    children_to_delete.append(i)
            # Удаляем сверху вниз (в обратном порядке)
            for r_del in reversed(children_to_delete):
                ws.delete_rows(r_del)

            # ШАГ 2: пересчитываем target_row (мог сдвинуться если удаляли строки выше)
            col_a_after_del = ws.col_values(1)
            for i, v in enumerate(col_a_after_del, start=1):
                if v.strip() == str(parent_num) and i >= config["data_start_row"]:
                    target_row = i
                    break

            # ШАГ 3: вставляем N дочерних строк ПОСЛЕ родителя
            n_children = len(details)
            blank_rows = [[''] * 18 for _ in range(n_children)]
            ws.insert_rows(blank_rows, row=target_row + 1, value_input_option='USER_ENTERED')

            # ШАГ 4: заполняем дочерние строки и обновляем родителя
            for i, det in enumerate(details):
                child_row = target_row + 1 + i
                child_id = f"{parent_num}.{i+1}"
                # A — текстовый ID, B-D пустые, E=продукция, F=шт, G=подд, H=цена закупки
                ws.update(values=[[child_id]], range_name=f'A{child_row}',
                          value_input_option='RAW')
                d_product = det.get("продукция") or det.get("продукт") or ""
                d_pieces = det.get("шт") or det.get("штук") or ""
                d_pallets = det.get("подд") or det.get("поддоны") or ""
                d_price_buy = det.get("цена_закупки") or det.get("цена") or ""
                d_price_sell = det.get("цена_продажи") or ""
                ws.update(values=[['', '', '', d_product, d_pieces, d_pallets, d_price_buy]],
                          range_name=f'B{child_row}:H{child_row}', value_input_option='USER_ENTERED')
                ws.update(values=[[f'=IF(OR(F{child_row}="";H{child_row}="");"";F{child_row}*H{child_row})']],
                          range_name=f'I{child_row}', value_input_option='USER_ENTERED')
                ws.update(values=[['', '', '', d_price_sell]],
                          range_name=f'J{child_row}:M{child_row}', value_input_option='USER_ENTERED')
                ws.update(values=[[f'=IF(OR(F{child_row}="";M{child_row}="");"";F{child_row}*M{child_row})']],
                          range_name=f'N{child_row}', value_input_option='USER_ENTERED')
                # P, Q, R у дочки пусто (только у родителя)
                ws.update(values=[['', '', '', '']],
                          range_name=f'O{child_row}:R{child_row}', value_input_option='USER_ENTERED')

            # ШАГ 5: обновляем родителя — F, G, I, N становятся SUMIF дочерних
            r = target_row
            ws.update(values=[[f'=SUMIF($A$4:$A$30;$A{r}&".*";$F$4:$F$30)']],
                      range_name=f'F{r}', value_input_option='USER_ENTERED')
            ws.update(values=[[f'=SUMIF($A$4:$A$30;$A{r}&".*";$G$4:$G$30)']],
                      range_name=f'G{r}', value_input_option='USER_ENTERED')
            ws.update(values=[['']], range_name=f'H{r}', value_input_option='USER_ENTERED')  # H пусто (разные цены)
            ws.update(values=[[f'=SUMIF($A$4:$A$30;$A{r}&".*";$I$4:$I$30)']],
                      range_name=f'I{r}', value_input_option='USER_ENTERED')
            ws.update(values=[['']], range_name=f'M{r}', value_input_option='USER_ENTERED')  # M пусто
            ws.update(values=[[f'=SUMIF($A$4:$A$30;$A{r}&".*";$N$4:$N$30)']],
                      range_name=f'N{r}', value_input_option='USER_ENTERED')

            # Продукция родителя = "20(3-0)+12(2-0)" из деталей
            products_agg = "+".join(d.get("продукция") or d.get("продукт") or "" for d in details if d.get("продукция") or d.get("продукт"))
            if products_agg:
                ws.update(values=[[products_agg]], range_name=f'E{r}', value_input_option='USER_ENTERED')

            applied.append(f"details={len(details)} блока: {products_agg}")

        msg = f"✏️ Правка {table} (строка {target_row}): " + ", ".join(applied)
        if unknown:
            msg += f"\n⚠️ Неизвестные поля: {', '.join(unknown)}"
        print(f"[CORRECTION] {msg}", flush=True)
        return True, msg

    except Exception as e:
        import traceback
        print(f"[CORRECTION] Ошибка: {e}\n{traceback.format_exc()[:500]}", flush=True)
        _alert_admin(f"Ошибка правки в {table}", e)
        return False, str(e)

def _write_purchase_to_sheets(trip: dict):
    """Записывает закупку блока в лист 'Закупки'.

    Структура листа:
    - Строка 1: заголовок-merge
    - Строка 3: шапка таблицы (A-R)
    - Строки 4..N: записи закупок (заполняются СВЕРХУ ВНИЗ)
    - Колонки I (Сумма закупки), N (Сумма продажи), P (Итого расходы),
      Q (Маржа руб), R (Маржа %) — ФОРМУЛЫ, не трогать
    - Строка с ИТОГО (обычно 32) — SUM(F4:F30) и т.п., тоже не трогать

    Заполняем только: A=№, B=дата, C=поставщик, D=откуда, E=продукт,
    F=шт, G=подд, H=цена закупки, J=доставка от поставщ., K=клиент,
    L=дата продажи, M=цена продажи, O=доставка клиенту.

    Алгоритм:
    1. Найти строку ИТОГО (граница)
    2. Найти ПЕРВУЮ пустую строку с 4 до ИТОГО-1
    3. Если есть свободная — пишем туда через update (раздельно A:H, J:M, O —
       НЕ затрагивая I, N, P, Q, R с формулами)
    4. Если все заполнены — insert_row перед ИТОГО + восстанавливаем формулы
    """
    if not GOOGLE_SA_B64:
        return False, "Нет GOOGLE_SA_B64"
    try:
        import base64
        import gspread
        from google.oauth2.service_account import Credentials
        sa_info = json.loads(base64.b64decode(GOOGLE_SA_B64))
        creds = Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEETS_ID)
        ws = sh.worksheet("Закупки")

        # 1. Находим ИТОГО
        col_a = ws.col_values(1)
        itogo_row = None
        for i, v in enumerate(col_a, start=1):
            if v.strip().upper() == "ИТОГО":
                itogo_row = i
                break

        # 2. Находим последний номер и первую ПОЛНОСТЬЮ ПУСТУЮ строку.
        # ВАЖНО: дочки иерархии имеют ТЕКСТОВЫЙ ID ("2.2", "5.1") — они НЕ цифры,
        # но строка занята. Раньше алгоритм считал такие строки пустыми и
        # затирал дочки записывая родителя сверху.
        last_num = 0
        target_row = None
        max_search = itogo_row - 1 if itogo_row else 50
        for i in range(4, max_search + 1):
            cell_val = (col_a[i - 1] if i - 1 < len(col_a) else '').strip()
            if cell_val.isdigit():
                last_num = max(last_num, int(cell_val))
            elif cell_val == '' and target_row is None:
                target_row = i
            # cell_val это нечисловой текст ("2.2", "5.1") — пропускаем, строка занята
        new_num = last_num + 1

        # 3. Если все строки до ИТОГО заполнены — вставляем перед ИТОГО
        need_restore_formulas = False
        if target_row is None:
            if itogo_row:
                target_row = itogo_row
                ws.insert_row([], index=target_row, value_input_option="USER_ENTERED")
                need_restore_formulas = True
            else:
                target_row = max_search + 1

        # 4. Собираем данные
        from_loc = trip.get("from_location") or ""
        supplier = trip.get("supplier") or from_loc
        product = trip.get("product") or ""
        pallets = trip.get("pallets") or ""
        pieces = trip.get("pieces") or ""
        if not pieces and trip.get("products_detail"):
            try:
                # products_detail формат: [{code, pallets, qty_per_pallet, price_buy?, price_sell?}]
                pieces = sum(
                    int((p.get("pallets") or 0)) * int((p.get("qty_per_pallet") or 0))
                    for p in trip["products_detail"]
                )
                if pieces == 0:
                    # fallback на старое поле pieces
                    pieces = sum(int(p.get("pieces", 0) or 0) for p in trip["products_detail"])
                if pieces == 0:
                    pieces = ""
            except Exception:
                pieces = ""

        price_buy = trip.get("price_buy") or ""
        price_sell = trip.get("price_sell") or ""
        delivery_in = trip.get("delivery_in") or ""
        delivery_out = trip.get("delivery_out") or ""
        sell_date = trip.get("sell_date") or ""

        # ─── МНОГОПРОДУКТОВАЯ ЗАКУПКА: несколько блоков с разными ценами ───
        # Если в products_detail минимум 2 элемента с разными ценами — создаём
        # родителя (target_row) + дочерние строки 2.1, 2.2 ...
        pd = trip.get("products_detail") or []
        has_multi_prices = (
            len(pd) >= 2 and
            len(set(p.get("price_buy") for p in pd if p.get("price_buy"))) >= 2
        )

        # 5. Пишем данные РАЗДЕЛЬНО, чтобы не затереть формулы I, N, P, Q, R
        if has_multi_prices:
            # Родитель: общие поля. F, G, I, N, H, M — формулы или пусто.
            # Дедуплицируем коды (две позиции 12(2-0) → один "12(2-0)", не "12(2-0)+12(2-0)")
            _codes_seen = []
            for p in pd:
                c = p.get("code")
                if c and c not in _codes_seen:
                    _codes_seen.append(c)
            products_agg = "+".join(_codes_seen)
            ws.update(values=[[new_num, trip.get("date") or _today_msk(), supplier, from_loc, products_agg,
                               f'=SUMIF($A$4:$A$30;$A{target_row}&".*";$F$4:$F$30)',
                               f'=SUMIF($A$4:$A$30;$A{target_row}&".*";$G$4:$G$30)',
                               '']],
                      range_name=f'A{target_row}:H{target_row}', value_input_option='USER_ENTERED')
            ws.update(values=[[f'=SUMIF($A$4:$A$30;$A{target_row}&".*";$I$4:$I$30)']],
                      range_name=f'I{target_row}', value_input_option='USER_ENTERED')
            ws.update(values=[[delivery_in, trip.get("client") or trip.get("to_location") or "", sell_date, '']],
                      range_name=f'J{target_row}:M{target_row}', value_input_option='USER_ENTERED')
            ws.update(values=[[f'=SUMIF($A$4:$A$30;$A{target_row}&".*";$N$4:$N$30)']],
                      range_name=f'N{target_row}', value_input_option='USER_ENTERED')
            if delivery_out:
                ws.update(values=[[delivery_out]], range_name=f'O{target_row}', value_input_option='USER_ENTERED')

            # Вставляем дочерние строки сразу под родителем
            blank_rows = [[''] * 18 for _ in pd]
            ws.insert_rows(blank_rows, row=target_row + 1, value_input_option='USER_ENTERED')

            for ci, p_det in enumerate(pd):
                cr = target_row + 1 + ci
                d_code = p_det.get("code") or ""
                d_pallets = p_det.get("pallets") or ""
                d_qty = p_det.get("qty_per_pallet") or 0
                d_pieces = int(d_pallets) * int(d_qty) if d_pallets and d_qty else (p_det.get("pieces") or "")
                d_pbuy = p_det.get("price_buy") or ""
                d_psell = p_det.get("price_sell") or ""
                ws.update(values=[[f'{new_num}.{ci+1}']], range_name=f'A{cr}', value_input_option='RAW')
                ws.update(values=[['', '', '', d_code, d_pieces, d_pallets, d_pbuy]],
                          range_name=f'B{cr}:H{cr}', value_input_option='USER_ENTERED')
                ws.update(values=[[f'=IF(OR(F{cr}="";H{cr}="");"";F{cr}*H{cr})']],
                          range_name=f'I{cr}', value_input_option='USER_ENTERED')
                ws.update(values=[['', '', '', d_psell]],
                          range_name=f'J{cr}:M{cr}', value_input_option='USER_ENTERED')
                ws.update(values=[[f'=IF(OR(F{cr}="";M{cr}="");"";F{cr}*M{cr})']],
                          range_name=f'N{cr}', value_input_option='USER_ENTERED')
                ws.update(values=[['', '', '', '']],
                          range_name=f'O{cr}:R{cr}', value_input_option='USER_ENTERED')
        else:
            # Простая одноблочная закупка
            ah_row = [
                new_num, trip.get("date") or _today_msk(), supplier, from_loc, product,
                pieces, pallets, price_buy,
            ]
            ws.update(values=[ah_row], range_name=f'A{target_row}:H{target_row}', value_input_option='USER_ENTERED')

            jm_row = [delivery_in, trip.get("client") or trip.get("to_location") or "", sell_date, price_sell]
            ws.update(values=[jm_row], range_name=f'J{target_row}:M{target_row}', value_input_option='USER_ENTERED')

            if delivery_out:
                ws.update(values=[[delivery_out]], range_name=f'O{target_row}', value_input_option='USER_ENTERED')

        # 6. Если делали insert_row — формулы не унаследовались, восстанавливаем
        if need_restore_formulas:
            r = target_row
            ws.update(values=[[f'=IF(OR(F{r}="";H{r}="");"";F{r}*H{r})']],
                      range_name=f'I{r}', value_input_option='USER_ENTERED')
            ws.update(values=[[f'=IF(OR(F{r}="";M{r}="");"";F{r}*M{r})']],
                      range_name=f'N{r}', value_input_option='USER_ENTERED')
            ws.update(values=[[f'=IF(I{r}="";"";I{r}+IF(J{r}="";0;J{r})+IF(O{r}="";0;O{r}))']],
                      range_name=f'P{r}', value_input_option='USER_ENTERED')
            ws.update(values=[[f'=IF(OR(N{r}="";P{r}="");"";N{r}-P{r})']],
                      range_name=f'Q{r}', value_input_option='USER_ENTERED')
            ws.update(values=[[f'=IF(OR(N{r}="";N{r}=0;Q{r}="");"";ROUND(Q{r}/N{r}*100;1))']],
                      range_name=f'R{r}', value_input_option='USER_ENTERED')

        msg = f"📥 Закупка #{new_num}: {product} {pallets}пд от {supplier} → {trip.get('client') or trip.get('to_location') or 'склад'} [строка {target_row}]"
        if has_multi_prices:
            print(f"[PURCHASE] row#{new_num} (multi) → A{target_row}, products={[(p.get('code'), p.get('pallets'), p.get('price_buy')) for p in pd]}", flush=True)
        else:
            print(f"[PURCHASE] row#{new_num} → A{target_row}, data={ah_row}", flush=True)
        return True, msg
    except Exception as e:
        import traceback
        print(f"[PURCHASE] Ошибка: {e}\n{traceback.format_exc()[:500]}", flush=True)
        _alert_admin("Ошибка записи в лист Закупки", e)
        return False, str(e)

def _normalize_date(date_str: str) -> str:
    """Нормализует дату в формат ДД.ММ.ГГГГ.
    Поддерживает: «18.05.26», «18.05.2026», «18/05/2026», «18.05.26 г.», «2026-05-18».
    """
    import re as _re
    if not date_str:
        return ""
    s = str(date_str).strip()
    # Убираем хвост «г.» и пробелы
    s = _re.sub(r'\s*г\.?\s*$', '', s).strip()
    # Заменяем разделители на точки
    s = s.replace('/', '.').replace('-', '.')

    # YYYY.MM.DD → DD.MM.YYYY
    m = _re.match(r'^(\d{4})\.(\d{1,2})\.(\d{1,2})$', s)
    if m:
        y, mo, d = m.groups()
        return f"{int(d):02d}.{int(mo):02d}.{y}"

    # DD.MM.YYYY или DD.MM.YY
    m = _re.match(r'^(\d{1,2})\.(\d{1,2})\.(\d{2,4})$', s)
    if m:
        d, mo, y = m.groups()
        if len(y) == 2:
            y = "20" + y  # 26 → 2026
        return f"{int(d):02d}.{int(mo):02d}.{y}"
    return s


def _trip_date_to_sortkey(date_str: str):
    """Конвертирует строку даты в сортируемый кортеж (Y,M,D).
    Поддерживает «ДД.ММ.ГГГГ», «ДД.ММ.ГГ», «ДД.ММ.ГГ г.» (нормализуем).
    Если не парсится — возвращает None.
    """
    if not date_str:
        return None
    try:
        normalized = _normalize_date(date_str)
        parts = normalized.strip().split('.')
        if len(parts) == 3:
            d, m, y = parts
            return (int(y), int(m), int(d))
    except (ValueError, AttributeError):
        pass
    return None


def _insert_trip_calendar(ws, row_data: list, date_str: str, data_start_row: int = 3) -> int:
    """Вставляет строку рейса в правильное место по календарю + перенумеровывает.

    Returns: финальный номер вставленной записи (после перенумерации).
    """
    target_key = _trip_date_to_sortkey(date_str)
    if target_key is None:
        # Без даты — в конец
        ws.append_row(row_data, value_input_option='USER_ENTERED')
        all_a = ws.col_values(1)
        return sum(1 for v in all_a if v.strip().isdigit())

    # Находим позицию вставки — первая строка с датой строго больше нашей
    all_rows = ws.get_all_values()
    insert_at = None
    for i, r in enumerate(all_rows, start=1):
        if i < data_start_row + 1:
            continue
        if not r or not r[0].strip().isdigit():
            continue  # не рейс (заголовок/пусто)
        row_date = r[1] if len(r) > 1 else ''
        row_key = _trip_date_to_sortkey(row_date)
        if row_key is None:
            continue
        if row_key > target_key:
            insert_at = i
            break

    if insert_at is None:
        # Не нашли строку с большей датой — добавляем после последнего рейса
        last_trip_row = 0
        for i, r in enumerate(all_rows, start=1):
            if r and r[0].strip().isdigit():
                last_trip_row = i
        insert_at = last_trip_row + 1 if last_trip_row else (data_start_row + 1)

    ws.insert_row(row_data, index=insert_at, value_input_option='USER_ENTERED')

    # Перенумеровываем все рейсы по порядку
    all_rows = ws.get_all_values()
    n = 0
    updates_batch = []
    for i, r in enumerate(all_rows, start=1):
        if not r:
            continue
        # Считаем «рейсом» строку где есть дата в формате ДД.ММ.ГГГГ
        if len(r) > 1 and _trip_date_to_sortkey(r[1]) is not None:
            n += 1
            if r[0].strip() != str(n):
                updates_batch.append({'range': f'A{i}', 'values': [[str(n)]]})

    if updates_batch:
        ws.batch_update(updates_batch, value_input_option='USER_ENTERED')

    # Возвращаем итоговый № вставленной строки (строка теперь на insert_at)
    final_num = ws.cell(insert_at, 1).value
    try:
        return int(final_num)
    except (ValueError, TypeError):
        return n


def _write_trips_to_sheets(trips: list):
    """Записывает рейсы в лист 'Рейсы' Google Sheets через gspread."""
    if not trips or not GOOGLE_SA_B64:
        return
    try:
        import base64, tempfile
        import gspread
        from google.oauth2.service_account import Credentials
        sa_json = base64.b64decode(GOOGLE_SA_B64)
        sa_info = json.loads(sa_json)
        creds = Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEETS_ID)
        ws = sh.worksheet("Рейсы")
        # Получаем последний номер записи (для fallback и confirm-сообщений до перенумерации)
        all_vals = ws.col_values(1)
        last_num = 0
        for v in all_vals:
            if v.isdigit():
                last_num = max(last_num, int(v))

        for trip in trips:
            # Нормализуем номер машины (сокращения → полный)
            if trip.get('truck'):
                trip['truck'] = _normalize_truck(trip['truck'])
            # Автоподстановка водителя по машине — если в сообщении только машина (135 → Кораблев)
            if trip.get('truck') and not trip.get('driver'):
                auto_driver = _driver_for_truck(trip['truck'])
                if auto_driver:
                    trip['driver'] = auto_driver
            evt_type = trip.get("type", "trip")
            last_num += 1

            if evt_type == "price":
                # Цены НЕ пишем в лист Рейсы — они идут в секцию «Контактное лицо» через _apply_price_change
                last_num -= 1  # откатываем счётчик
                continue
            elif evt_type == "pallet_transfer":
                # Перемещение поддонов между складами → пишем в Рейсы, НЕ трогаем склад
                product_desc = f"Поддоны {trip.get('pallets', '')} шт"
                trip_date = trip.get("date") or _today_msk()
                row = [
                    '',  # № проставится при перенумерации
                    trip_date,
                    trip.get("truck") or "",
                    trip.get("driver") or "",
                    trip.get("from_location") or "",
                    trip.get("to_location") or "",
                    trip.get("client") or trip.get("to_location") or "",
                    product_desc,
                    'перемещение',  # I: Источник
                    trip.get("driver") or "",  # J: Водители
                ]
                final_num = _insert_trip_calendar(ws, row, trip_date)
                last_num = max(last_num, final_num)
                confirm = f"✅ Перемещение #{final_num}: {trip.get('driver','')} ({trip.get('truck','')})"
                confirm += f"\n{trip.get('from_location','')} → {trip.get('to_location','')}"
                confirm += f"\nПоддоны: {trip.get('pallets', '?')} шт | дата: {trip_date}"
                send_msg(BLOK_GROUP_ID, confirm)
                print(f"[BLOK_SHEETS] Перемещение поддонов вставлено по календарю: row={row}, num={final_num}", flush=True)
                continue  # НЕ списываем со склада
            elif evt_type == "return":
                # Возврат поддонов → пишем в секцию ВОЗВРАТ на листе клиента, НЕ в Рейсы
                last_num -= 1  # откатываем счётчик — возврат не считается рейсом
                client = trip.get("client") or ""
                ret_qty = trip.get("return_pallets") or trip.get("pallets") or 0
                ret_date = trip.get("date") or _today_msk()
                p_type = trip.get("pallet_type") or ""
                truck = trip.get("truck") or ""
                obj = trip.get("to_location") or trip.get("from_location") or ""
                contact = trip.get("price_contact") or trip.get("comment") or ""
                if client and ret_qty:
                    try:
                        ok, msg = _apply_pallet_return(client, int(ret_qty), p_type, ret_date, truck, obj, contact)
                        status = "✅" if ok else "⚠️"
                        send_msg(BLOK_GROUP_ID, f"{status} Sheets: {msg}")
                    except Exception as e:
                        send_msg(BLOK_GROUP_ID, f"⚠️ Ошибка записи возврата: {e}")
                        print(f"[BLOK_SHEETS] Ошибка возврата: {e}", flush=True)
                else:
                    send_msg(BLOK_GROUP_ID, f"⚠️ Возврат: не указан клиент или кол-во поддонов")
                continue  # НЕ пишем в Рейсы
            else:
                # Рейс — формат продукции: "20(3-0)" или "20(3-0)+9(2-0)"
                # Парсер возвращает product в правильном формате, pallets уже внутри
                # СБОРНЫЙ рейс (deliveries[]) — агрегируем клиентов и продукты для строки Рейсов
                deliveries = trip.get("deliveries") or []
                if deliveries:
                    clients_agg = ", ".join(d.get("client") or "" for d in deliveries if d.get("client"))
                    products_seen = []
                    for d in deliveries:
                        for p in (d.get("products") or []):
                            code = p.get("code")
                            if code and code not in products_seen:
                                products_seen.append(code)
                    product = "+".join(products_seen) if products_seen else (trip.get("product") or "")
                    trip['client'] = clients_agg  # обновляем в trip для последующих confirm-сообщений
                    trip['product'] = product
                else:
                    product = trip.get("product") or ""
                trip_date = trip.get("date") or _today_msk()
                row = [
                    '',  # № проставится при перенумерации
                    trip_date,
                    trip.get("truck") or "",
                    trip.get("driver") or "",
                    trip.get("from_location") or "",
                    trip.get("to_location") or "",
                    trip.get("client") or "",
                    product,
                ]
            # Валидация водителя и машины
            driver = trip.get("driver") or ""
            truck = trip.get("truck") or ""
            warnings = []
            if driver and driver not in VALID_DRIVERS:
                warnings.append(f"неизвестный водитель '{driver}'")
            if truck and truck not in VALID_TRUCKS:
                warnings.append(f"неизвестная машина '{truck}'")

            final_num = _insert_trip_calendar(ws, row, trip_date)
            last_num = max(last_num, final_num)
            # Подтверждение в группу
            confirm = f"✅ Рейс #{final_num}: {driver} ({truck})\n"
            confirm += f"{trip.get('from_location','')} → {trip.get('to_location','')} | {trip.get('client','')}\n"
            confirm += f"Продукция: {row[-1]} | дата: {trip_date}"
            if warnings:
                confirm += "\n⚠️ " + ", ".join(warnings)
            send_msg(BLOK_GROUP_ID, confirm)
            print(f"[BLOK_SHEETS] Рейс {evt_type} вставлен по календарю: row={row}, num={final_num}", flush=True)
            # Заменяем last_num на final_num чтобы остальной код работал с правильным номером
            last_num = final_num

            # ─── Складской учёт: списание со склада ───
            # Записываем в КРД(склад) или Карьер(склад) кол-во отгруженных поддонов
            # НЕ списываем если это закупной рейс (Источник = Закуп)
            trip_source = (trip.get("source") or "").lower()
            from_loc = (trip.get("from_location") or "").lower()
            is_purchase = trip_source == "закуп" or "закуп" in from_loc

            # Закупка → пишем в лист «Закупки» (отдельно от Рейсов)
            if is_purchase:
                deliveries = trip.get("deliveries") or []
                if deliveries:
                    # Сборный рейс — N закупок, по одной на каждую выгрузку
                    for d in deliveries:
                        pur_trip = {
                            "type": "trip",
                            "source": "закуп",
                            "date": trip.get("date"),
                            "truck": trip.get("truck"),
                            "driver": trip.get("driver"),
                            "from_location": trip.get("from_location"),
                            "to_location": trip.get("to_location"),
                            "supplier": trip.get("supplier"),
                            "client": d.get("client"),
                            "delivery_in": d.get("delivery_in"),
                            "products_detail": d.get("products") or [],
                            "sell_date": d.get("sell_date") or trip.get("sell_date") or trip.get("date"),
                        }
                        try:
                            pur_ok, pur_msg = _write_purchase_to_sheets(pur_trip)
                            if pur_ok:
                                send_msg(BLOK_GROUP_ID, pur_msg)
                        except Exception as e:
                            print(f"[PURCHASE] Ошибка delivery {d.get('client','?')}: {e}", flush=True)
                else:
                    try:
                        pur_ok, pur_msg = _write_purchase_to_sheets(trip)
                        if pur_ok:
                            send_msg(BLOK_GROUP_ID, pur_msg)
                    except Exception as e:
                        print(f"[PURCHASE] Ошибка: {e}", flush=True)

            if product and trip.get("from_location") and not is_purchase:
                trip_pallets = trip.get("pallets")
                p_detail = trip.get("products_detail")
                if trip_pallets or p_detail:
                    try:
                        wh_ok, wh_msg = _apply_warehouse_shipment(
                            trip.get("from_location"),
                            product,
                            int(trip_pallets) if trip_pallets else 0,
                            trip.get("date") or _today_msk(),
                            products_detail=p_detail,
                        )
                        if wh_ok:
                            send_msg(BLOK_GROUP_ID, f"📦 Склад: {wh_msg}")
                        else:
                            send_msg(BLOK_GROUP_ID, f"⚠️ Склад: {wh_msg}")
                    except Exception as e:
                        print(f"[WAREHOUSE] Ошибка: {e}", flush=True)
    except Exception as e:
        print(f"[BLOK_SHEETS] Ошибка записи: {e}", flush=True)
        _alert_admin("Ошибка записи рейсов в Sheets", e)
_ORG_PREFIXES = ("ИП ", "ООО ", "ОАО ", "ЗАО ", "АО ", "ПАО ", "НПО ", "ТД ", "КФХ ", "СПК ")

def _normalize_client_name(name: str) -> str:
    s = (name or "").upper().strip()
    for ch in ("«", "»", '"', "'", "`"):
        s = s.replace(ch, "")
    s = s.strip()
    changed = True
    while changed:
        changed = False
        for p in _ORG_PREFIXES:
            if s.startswith(p):
                s = s[len(p):].strip()
                changed = True
            if s.endswith(" " + p.strip()):
                s = s[:-(len(p.strip())+1)].strip()
                changed = True
    return s

def _find_client_worksheet(sh, client_name: str):
    """Ищет лист клиента: точное совпадение -> совпадение по началу.
    Нормализует обе стороны (срезает ИП/ООО/АО/ПАО/ЗАО/ОАО/КФХ/СПК и кавычки),
    чтобы лист 'Горячкина' находился по запросу 'ИП Горячкина'."""
    raw = client_name.upper().strip()
    norm = _normalize_client_name(client_name)
    if not norm or len(norm) < 3:
        return None
    sheets = sh.worksheets()
    # 1. Точное совпадение нормализованных имён
    for sheet in sheets:
        title_norm = _normalize_client_name(sheet.title)
        if title_norm == norm or sheet.title.upper().strip() == raw:
            return sheet
    # 2. Совпадение по началу (для коротких ключей типа МЕЛИК -> МЕЛИКСЕТЯНТ)
    for sheet in sheets:
        title_norm = _normalize_client_name(sheet.title)
        if not title_norm:
            continue
        if title_norm.startswith(norm) or norm.startswith(title_norm):
            common_len = min(len(title_norm), len(norm))
            if common_len >= 3:
                return sheet
    return None

def _apply_return_update(client_name: str, return_date: str, truck: str = "", obj: str = "", payment_type: str = ""):
    """Дописывает авто и объект в существующую запись возврата поддонов."""
    if not GOOGLE_SA_B64:
        return False, "Нет доступа к Google Sheets"
    try:
        import base64
        import gspread
        from google.oauth2.service_account import Credentials
        sa_json = base64.b64decode(GOOGLE_SA_B64)
        sa_info = json.loads(sa_json)
        creds = Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEETS_ID)
        ws = _find_client_worksheet(sh, client_name)
        if not ws:
            return False, f"Лист '{client_name}' не найден"

        # Ищем строку с нужной датой в секции возврата (X8:X100)
        ret_data = ws.get("X8:Z100")
        target_row = None
        for i, row in enumerate(ret_data, 8):
            if row and str(row[0]).strip() and _dates_match(str(row[0]).strip(), return_date):
                # Нашли строку с этой датой — проверяем пустые авто/объект
                cur_truck = row[1].strip() if len(row) > 1 and row[1] else ""
                cur_obj = row[2].strip() if len(row) > 2 and row[2] else ""
                if not cur_truck or not cur_obj:
                    target_row = i
                    break
        if not target_row:
            return False, f"Возврат от {return_date} в '{ws.title}' не найден или уже заполнен"

        updated = []
        if truck:
            ws.update_cell(target_row, 25, truck)  # Y
            updated.append(f"авто={truck}")
        if obj:
            ws.update_cell(target_row, 26, obj)  # Z
            updated.append(f"объект={obj}")
        # Обновляем тип оплаты в секции ОПЛАТА (O-V), если указан
        if payment_type:
            pay_type_lower = payment_type.lower()
            is_nal = 'нал' in pay_type_lower and 'безнал' not in pay_type_lower
            is_ip = 'ип' in pay_type_lower or 'безнал' in pay_type_lower
            is_ooo = 'ооо' in pay_type_lower
            # Ищем строку оплаты с этой датой и пометкой "возврат поддонов"
            try:
                pay_data = ws.get("O8:V150")
                pay_row = None
                for pi, pr in enumerate(pay_data, 8):
                    if pr and str(pr[0]).strip() == return_date:
                        note = pr[5].strip().lower() if len(pr) > 5 else ""
                        if "возврат" in note or "бартер" in (pr[7].strip().lower() if len(pr) > 7 else ""):
                            pay_row = pi
                            break
                        # Также проверяем колонку V (индекс 7 от O)
                        v_val = pr[7].strip().lower() if len(pr) > 7 else ""
                        if "бартер" in v_val:
                            pay_row = pi
                            break
                if not pay_row:
                    # Ищем просто по дате
                    for pi, pr in enumerate(pay_data, 8):
                        if pr and str(pr[0]).strip() == return_date:
                            pay_row = pi
                            break
                if pay_row:
                    ws.update_cell(pay_row, 16, is_ooo)   # P = ООО
                    ws.update_cell(pay_row, 17, is_ip)     # Q = ИП
                    ws.update_cell(pay_row, 18, is_nal)    # R = Нал
                    label = "Нал" if is_nal else ("ИП" if is_ip else "ООО")
                    updated.append(f"оплата={label} (строка оплаты {pay_row})")
                    print(f"[RETURN_UPD] Тип оплаты изменён на {label} в строке {pay_row}", flush=True)
                else:
                    updated.append(f"оплата={payment_type} (строка оплаты не найдена)")
            except Exception as pe:
                updated.append(f"оплата: ошибка {pe}")
                print(f"[RETURN_UPD] Ошибка обновления оплаты: {pe}", flush=True)

        if not updated:
            return False, "Нечего обновлять — авто, объект и оплата не указаны"
        msg = f"Возврат от {return_date} в '{ws.title}' строка {target_row}: дописано {', '.join(updated)}"
        print(f"[RETURN_UPD] {msg}", flush=True)
        return True, msg
    except Exception as e:
        print(f"[RETURN_UPD] Ошибка: {e}", flush=True)
        _alert_admin("Обновление возврата: ошибка Sheets", e)
        return False, str(e)

def _apply_price_change(client_name: str, product: str, new_price: float, date_from: str = "", contact_name: str = "", qty_per_pallet: str = ""):
    """Добавляет новую цену в секцию 'Контактное лицо' листа клиента.

    Секция расположена в колонках AF-AK:
      AF=дата, AG=клиент, AH=продукция, AI=шт/пдн, AJ=руб/шт, AK=примечания

    НЕ трогает колонку H (цены в продажах) — там исторические данные.
    Добавляет новую строку после последней заполненной в секции.
    Если qty_per_pallet не указан — ищет в предыдущих записях для этого продукта.
    """
    if not GOOGLE_SA_B64:
        print("[PRICE] GOOGLE_SA_B64 не задан", flush=True)
        return False, "Нет доступа к Google Sheets"

    try:
        import base64
        import gspread
        from google.oauth2.service_account import Credentials

        sa_json = base64.b64decode(GOOGLE_SA_B64)
        sa_info = json.loads(sa_json)
        creds = Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEETS_ID)

        # Ищем лист клиента
        ws = None
        client_upper = client_name.upper().strip()
        for sheet in sh.worksheets():
            if sheet.title.upper().strip() == client_upper:
                ws = sheet
                break

        if not ws:
            for sheet in sh.worksheets():
                if client_upper in sheet.title.upper() or sheet.title.upper() in client_upper:
                    ws = sheet
                    break

        if not ws:
            msg = f"Лист '{client_name}' не найден"
            print(f"[PRICE] {msg}", flush=True)
            return False, msg

        print(f"[PRICE] Нашёл лист: '{ws.title}'", flush=True)

        # Секция «Контактное лицо» в колонках AF-AK (1-based: 32-37)
        # AF=32(дата), AG=33(клиент), AH=34(продукция), AI=35(шт/пдн), AJ=36(руб/шт), AK=37(примечания)
        COL_DATE = 32    # AF
        COL_CLIENT = 33  # AG
        COL_PRODUCT = 34 # AH
        COL_QTY = 35     # AI
        COL_PRICE = 36   # AJ

        # Читаем секцию AF-AK для поиска последней заполненной строки и предыдущих данных
        price_data = ws.get("AF7:AK200")

        last_filled_row = 6  # строка 7 = первая строка данных
        found_qty = ""  # шт/пдн из предыдущих записей (строка, может быть "72/75")
        product_upper = product.upper().strip() if product else ""
        for i, row in enumerate(price_data, 7):
            if row and any(str(c).strip() for c in row):
                last_filled_row = i
                # Ищем шт/пдн для такого же продукта в предыдущих записях
                if not qty_per_pallet and len(row) >= 4:
                    row_product = str(row[2]).upper().strip() if len(row) > 2 else ""
                    row_qty = str(row[3]).strip() if len(row) > 3 else ""
                    if product_upper and product_upper in row_product and row_qty:
                        found_qty = row_qty

        price_date = date_from if date_from else _today_msk()

        # Определяем шт/пдн: из аргумента, или из предыдущих записей
        final_qty = qty_per_pallet if qty_per_pallet else found_qty

        # Проверяем: может последняя строка — тот же продукт+цена, но AG пустой?
        # Если да — дописываем в неё (не создаём дубль)
        update_existing = False
        if last_filled_row >= 7 and price_data:
            last_row_data = price_data[last_filled_row - 7] if (last_filled_row - 7) < len(price_data) else []
            if last_row_data:
                # AF-AK: [0]=дата, [1]=клиент, [2]=продукция, [3]=шт/пдн, [4]=цена
                last_product = str(last_row_data[2]).upper().strip() if len(last_row_data) > 2 else ""
                last_client = str(last_row_data[1]).strip() if len(last_row_data) > 1 else ""
                last_price_val = str(last_row_data[4]).strip() if len(last_row_data) > 4 else ""
                last_qty_val = str(last_row_data[3]).strip() if len(last_row_data) > 3 else ""
                try:
                    last_price_num = float(last_price_val.replace(",", ".")) if last_price_val else 0
                except (ValueError, TypeError):
                    last_price_num = 0
                # Совпадает продукт и цена, но клиент не заполнен
                if product_upper and product_upper in last_product and abs(last_price_num - new_price) < 0.01 and not last_client:
                    update_existing = True
                    print(f"[PRICE] Найдена существующая строка {last_filled_row} без клиента — дописываю", flush=True)

        if update_existing:
            target_row = last_filled_row
            if contact_name:
                ws.update_cell(target_row, COL_CLIENT, contact_name)
            if final_qty and not last_qty_val:
                ws.update_cell(target_row, COL_QTY, final_qty)
            qty_info = f", {final_qty} шт/пдн" if final_qty else ""
            contact_info = f" ({contact_name})" if contact_name else ""
            msg = f"Дописано в '{ws.title}' строка {target_row}: клиент{contact_info}{qty_info}"
            print(f"[PRICE] {msg}", flush=True)
            return True, msg

        # Новая строка
        new_row = last_filled_row + 1
        ws.update_cell(new_row, COL_DATE, price_date)
        if contact_name:
            ws.update_cell(new_row, COL_CLIENT, contact_name)
        ws.update_cell(new_row, COL_PRODUCT, product)
        if final_qty:
            ws.update_cell(new_row, COL_QTY, final_qty)
        ws.update_cell(new_row, COL_PRICE, new_price)

        qty_info = f", {final_qty} шт/пдн" if final_qty else ""
        contact_info = f" ({contact_name})" if contact_name else ""
        msg = f"Цена добавлена в '{ws.title}' строка {new_row}: {product} = {new_price} руб.{qty_info}{contact_info} с {price_date}"
        print(f"[PRICE] {msg}", flush=True)
        return True, msg

    except Exception as e:
        msg = f"Ошибка: {e}"
        print(f"[PRICE] {msg}", flush=True)
        _alert_admin(f"Цена для '{client_name}' / '{product}': ошибка Sheets", e)
        return False, msg

def _apply_payment(client_name: str, amount: float, pay_type: str = "", pay_method: str = "", payment_date: str = "", contact_name: str = ""):
    """Записывает оплату в лист клиента, секция ОПЛАТА (колонки O-V).
    
    O = дата, P = ООО (checkbox), Q = ИП (checkbox), R = Нал (checkbox),
    S = клиент/объект, T = примечание, U = сумма, V = "Деньги"/"Бартер"
    contact_name: подклиент (напр. "ИП Шубина" для листа ЧАСТНИКИ), пишется в S
    """
    if not GOOGLE_SA_B64:
        return False, "Нет доступа к Google Sheets"
    try:
        import base64
        import gspread
        from google.oauth2.service_account import Credentials
        sa_json = base64.b64decode(GOOGLE_SA_B64)
        sa_info = json.loads(sa_json)
        creds = Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEETS_ID)

        # Ищем лист клиента
        ws = _find_client_worksheet(sh, client_name)
        if not ws:
            return False, f"Лист \'{client_name}\' не найден"

        print(f"[PAYMENT] Нашёл лист: \'{ws.title}\'", flush=True)

        # Ищем первую пустую строку в секции ОПЛАТА (O8:O150)
        payment_dates = ws.get("O8:O150")
        empty_row = None
        for i, row in enumerate(payment_dates, 8):
            if not row or not str(row[0]).strip():
                empty_row = i
                break
        if empty_row is None:
            empty_row = len(payment_dates) + 8
        if empty_row > 148:
            return False, f"Секция ОПЛАТА переполнена в '{ws.title}' (строка {empty_row})"
        print(f"[PAYMENT] Пустая строка: {empty_row}", flush=True)

        # Колонки: O=15, P=16(ООО), Q=17(ИП), R=18(Нал), S=19, T=20, U=21, V=22
        COL_DATE = 15   # O
        COL_OOO = 16    # P — checkbox ООО
        COL_IP = 17     # Q — checkbox ИП
        COL_NAL = 18    # R — checkbox Нал
        COL_CLIENT = 19 # S
        COL_NOTE = 20   # T
        COL_SUM = 21    # U
        COL_TYPE = 22   # V

        today = payment_date if payment_date else _today_msk()

        # Определяем куда ставить галочку
        # pay_type: "нал"/"безнал", pay_method: "ИП"/"ООО"
        # Mutual exclusion: только ОДНА галочка
        pt = pay_type.lower()
        pm = pay_method.upper()
        if pm == "ООО" or "ооо" in pt:
            is_ooo, is_ip, is_nal = True, False, False
        elif pm == "ИП" or ("безнал" in pt and "ооо" not in pt) or "ип" in pt:
            is_ooo, is_ip, is_nal = False, True, False
        elif "нал" in pt and "безнал" not in pt:
            is_ooo, is_ip, is_nal = False, False, True
        elif "безнал" in pt:
            is_ooo, is_ip, is_nal = True, False, False  # безнал без уточнения → ООО
        else:
            is_ooo, is_ip, is_nal = False, False, True  # по умолчанию нал

        ws.update_cell(empty_row, COL_DATE, today)
        ws.update_cell(empty_row, COL_OOO, True if is_ooo else False)
        ws.update_cell(empty_row, COL_IP, True if is_ip else False)
        ws.update_cell(empty_row, COL_NAL, True if is_nal else False)
        ws.update_cell(empty_row, COL_SUM, amount)
        ws.update_cell(empty_row, COL_TYPE, "Деньги")
        # S (COL_CLIENT) — подклиент/объект. Для ЧАСТНИКИ = "ИП Шубина" и т.п.
        if contact_name:
            ws.update_cell(empty_row, COL_CLIENT, contact_name)

        checkbox = "ООО" if is_ooo else ("ИП" if is_ip else "Нал")
        msg = f"Оплата записана в \'{ws.title}\' строка {empty_row}: {amount} руб. ({checkbox})"
        print(f"[PAYMENT] {msg}", flush=True)
        return True, msg

    except Exception as e:
        msg = f"Ошибка: {e}"
        print(f"[PAYMENT] {msg}", flush=True)
        _alert_admin(f"Оплата для '{client_name}' ({amount} руб.): ошибка Sheets", e)
        return False, msg

def _apply_pallet_return(client_name: str, return_pallets: int, pallet_type: str = "",
                          return_date: str = "", truck: str = "", obj: str = "", contact_name: str = ""):
    """Записывает возврат поддонов в секцию ВОЗВРАТ ПОДДОНОВ (колонки X-AD) на листе клиента.
    
    X=дата, Y=№ а/м, Z=объект, AA=поддоны (тип), AB=кол-во, AC=цена, AD=сумма
    """
    if not GOOGLE_SA_B64:
        return False, "Нет доступа к Google Sheets"
    try:
        import base64
        import gspread
        from google.oauth2.service_account import Credentials
        sa_json = base64.b64decode(GOOGLE_SA_B64)
        sa_info = json.loads(sa_json)
        creds = Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEETS_ID)

        # Ищем лист клиента
        ws = _find_client_worksheet(sh, client_name)
        if not ws:
            return False, f"Лист '{client_name}' не найден"

        print(f"[RETURN] Нашёл лист: '{ws.title}'", flush=True)

        # Секция ВОЗВРАТ ПОДДОНОВ: X=24, Y=25, Z=26, AA=27, AB=28, AC=29, AD=30
        COL_DATE = 24    # X
        COL_TRUCK = 25   # Y
        COL_OBJ = 26     # Z
        COL_PTYPE = 27   # AA (тип поддона)
        COL_QTY = 28     # AB (кол-во)
        COL_PRICE = 29   # AC (цена)
        COL_SUM = 30     # AD (сумма = формула)

        # Ищем первую пустую строку в секции X8:X100
        ret_dates = ws.get("X8:X100")
        empty_row = None
        for i, row in enumerate(ret_dates, 8):
            if not row or not str(row[0]).strip():
                empty_row = i
                break
        if empty_row is None:
            empty_row = len(ret_dates) + 8
        if empty_row > 98:
            return False, f"Секция ВОЗВРАТ переполнена в '{ws.title}' (строка {empty_row})"
        print(f"[RETURN] Пустая строка: {empty_row}", flush=True)

        ret_date = return_date if return_date else _today_msk()
        # Определяем тип поддона
        p_type = "Поддон"
        if pallet_type:
            if "узк" in pallet_type.lower() or "0.8" in pallet_type or "1*" in pallet_type:
                p_type = "Поддон 1*"

        ws.update_cell(empty_row, COL_DATE, ret_date)
        if truck:
            ws.update_cell(empty_row, COL_TRUCK, truck)
        if obj:
            ws.update_cell(empty_row, COL_OBJ, obj)
        ws.update_cell(empty_row, COL_PTYPE, p_type)
        ws.update_cell(empty_row, COL_QTY, return_pallets)
        ws.update_cell(empty_row, COL_PRICE, _PALLET_PRICE)
        # Сумма — формула
        ws.update_acell(f"AD{empty_row}", f"=AB{empty_row}*AC{empty_row}")

        barter_sum = return_pallets * _PALLET_PRICE
        msg = f"Возврат поддонов записан в '{ws.title}' строка {empty_row}: {p_type} {return_pallets} шт от {ret_date}"
        print(f"[RETURN] {msg}", flush=True)

        # Возврат поддонов = оплата бартером → вносим в секцию ОПЛАТА
        # O=15(дата), P=16(ООО), Q=17(ИП), R=18(Нал), S=19, T=20(примечание), U=21(сумма), V=22(тип)
        # Галочка получателя: читаем из "Условия работы" (ячейка AG4)
        # "Поддоны выставляем за нал" → Нал, "от ИП" → ИП, "от ООО" → ООО
        try:
            pay_dates = ws.get("O8:O150")
            pay_row = None
            for pi, pr in enumerate(pay_dates, 8):
                if not pr or not str(pr[0]).strip():
                    pay_row = pi
                    break
            if pay_row is None:
                pay_row = len(pay_dates) + 8

            # Определяем получателя из условий работы клиента (AG4)
            is_ooo = False
            is_ip = False
            is_nal = False
            try:
                conditions = ws.acell("AG4").value or ""
                cond_lower = conditions.lower()
                # Ищем "поддоны выставляем за/от ..."
                if "поддон" in cond_lower:
                    if "за нал" in cond_lower or "наличн" in cond_lower:
                        is_nal = True
                    elif "от ип" in cond_lower or "за ип" in cond_lower:
                        is_ip = True
                    elif "от ооо" in cond_lower or "за ооо" in cond_lower:
                        is_ooo = True
                    else:
                        is_nal = True  # по умолчанию нал
                else:
                    is_nal = True  # нет условий — нал по умолчанию
                print(f"[RETURN] Условия: {conditions[:80]} → {'Нал' if is_nal else 'ИП' if is_ip else 'ООО'}", flush=True)
            except Exception:
                is_nal = True  # fallback

            ws.update_cell(pay_row, 15, ret_date)       # O = дата
            ws.update_cell(pay_row, 16, is_ooo)          # P = ООО
            ws.update_cell(pay_row, 17, is_ip)            # Q = ИП
            ws.update_cell(pay_row, 18, is_nal)           # R = Нал
            ws.update_cell(pay_row, 20, "возврат поддонов")  # T = примечание
            ws.update_cell(pay_row, 21, barter_sum)      # U = сумма
            # S = подклиент (для ЧАСТНИКИ — ИП Шубина и т.п.)
            if contact_name:
                ws.update_cell(pay_row, 19, contact_name)  # S = клиент
            ws.update_cell(pay_row, 22, "Бартер")        # V = тип
            checkbox = "ООО" if is_ooo else ("ИП" if is_ip else "Нал")
            msg += f"\n+ Оплата бартер: {barter_sum} руб. ({checkbox}, строка {pay_row})"
            print(f"[RETURN] Оплата бартер: {barter_sum} руб. ({checkbox}) в строку {pay_row}", flush=True)
        except Exception as pe:
            msg += f"\n❌ Возврат поддонов записан, но оплата-бартер УПАЛА: {pe}"
            print(f"[RETURN] Ошибка оплаты-бартер: {pe}", flush=True)
            _alert_admin(f"Возврат поддонов: бартер не записан (поддон уже записан в строку {pay_row})", pe)
            return False, msg

        return True, msg

    except Exception as e:
        msg = f"Ошибка: {e}"
        print(f"[RETURN] {msg}", flush=True)
        _alert_admin("Возврат поддонов: критическая ошибка", e)
        return False, msg
def _apply_object_add(client_name: str, object_name: str):
    """Добавляет объект в справочник клиента (колонка AM)."""
    if not GOOGLE_SA_B64:
        return False, "Нет доступа к Google Sheets"
    try:
        import base64
        import gspread
        from google.oauth2.service_account import Credentials
        sa_json = base64.b64decode(GOOGLE_SA_B64)
        sa_info = json.loads(sa_json)
        creds = Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEETS_ID)

        # Ищем лист клиента
        ws = _find_client_worksheet(sh, client_name)
        if not ws:
            return False, f"Лист '{client_name}' не найден"

        print(f"[OBJ_ADD] Нашёл лист: '{ws.title}'", flush=True)

        # Справочник объектов в колонке AM (39)
        COL_OBJ = 39  # AM
        obj_data = ws.get("AM8:AM50")
        # Проверяем дубликат
        for row in obj_data:
            if row and str(row[0]).strip().upper() == object_name.strip().upper():
                msg = f"Объект '{object_name}' уже есть в '{ws.title}'"
                print(f"[OBJ_ADD] {msg}", flush=True)
                return True, msg

        # Ищем первую пустую строку
        empty_row = 8
        for i, row in enumerate(obj_data, 8):
            if row and str(row[0]).strip():
                empty_row = i + 1
            else:
                empty_row = i
                break
        else:
            empty_row = len(obj_data) + 8

        ws.update_cell(empty_row, COL_OBJ, object_name)
        msg = f"Объект '{object_name}' добавлен в '{ws.title}' строка {empty_row}"
        print(f"[OBJ_ADD] {msg}", flush=True)
        return True, msg

    except Exception as e:
        msg = f"Ошибка: {e}"
        print(f"[OBJ_ADD] {msg}", flush=True)
        _alert_admin(f"Добавление объекта для '{client_name}': ошибка Sheets", e)
        return False, msg
def _dates_match(d1: str, d2: str) -> bool:
    """Сравнивает две даты в произвольном формате (ДД.ММ.ГГГГ, ДД.ММ.ГГ, ГГГГ-ММ-ДД)."""
    import re as _re
    def _norm(d):
        d = d.strip().rstrip('г.').rstrip('г').strip()
        # ДД.ММ.ГГГГ or ДД.ММ.ГГ
        m = _re.match(r'(\d{1,2})[./](\d{1,2})[./](\d{2,4})', d)
        if m:
            dd, mm, yy = m.group(1), m.group(2), m.group(3)
            if len(yy) == 2:
                yy = '20' + yy
            return f"{int(dd):02d}.{int(mm):02d}.{yy}"
        # ГГГГ-ММ-ДД
        m = _re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})', d)
        if m:
            return f"{int(m.group(3)):02d}.{int(m.group(2)):02d}.{m.group(1)}"
        return d
    return _norm(d1) == _norm(d2)

def _apply_payment_edit(client_name: str, contact_name: str, edit_date: str = ""):
    """Находит оплату по дате на листе клиента и вписывает клиента/контакт в колонку S.
    
    Используется для команд типа: "Внести изменения в ЧАСТНИКИ оплата от 20.04 клиент ИП Шубина"
    """
    if not GOOGLE_SA_B64:
        return False, "Нет доступа к Google Sheets"
    try:
        import base64
        import gspread
        from google.oauth2.service_account import Credentials
        sa_json = base64.b64decode(GOOGLE_SA_B64)
        sa_info = json.loads(sa_json)
        creds = Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEETS_ID)

        # Ищем лист клиента
        ws = _find_client_worksheet(sh, client_name)
        if not ws:
            return False, f"Лист '{client_name}' не найден"

        print(f"[PAY_EDIT] Нашёл лист: '{ws.title}'", flush=True)

        # Ищем оплату по дате в колонке O (15)
        COL_DATE = 15   # O
        COL_CLIENT = 19 # S

        # Читаем даты и клиентов
        pay_data = ws.get("O8:S150")
        found_rows = []
        for i, row in enumerate(pay_data, 8):
            if row and str(row[0]).strip():
                row_date = str(row[0]).strip()
                row_client = str(row[4]).strip() if len(row) > 4 else ""
                # Сравниваем даты — нормализуем для точного сравнения
                if edit_date and _dates_match(edit_date, row_date):
                    found_rows.append((i, row_date, row_client))

        if not found_rows:
            return False, f"Оплата от {edit_date} не найдена в '{ws.title}'"

        # Если несколько строк с этой датой — обновляем ту, где S пустой
        updated = 0
        for row_num, row_date, row_client in found_rows:
            if not row_client:  # S пустой — заполняем
                ws.update_cell(row_num, COL_CLIENT, contact_name)
                updated += 1
                print(f"[PAY_EDIT] Строка {row_num}: S = '{contact_name}'", flush=True)

        if updated == 0:
            # Все строки уже имеют клиента — обновляем первую найденную
            row_num = found_rows[0][0]
            ws.update_cell(row_num, COL_CLIENT, contact_name)
            updated = 1
            print(f"[PAY_EDIT] Строка {row_num}: S обновлён на '{contact_name}'", flush=True)

        msg = f"Клиент '{contact_name}' записан в '{ws.title}' ({updated} строк от {edit_date})"
        print(f"[PAY_EDIT] {msg}", flush=True)
        return True, msg

    except Exception as e:
        msg = f"Ошибка: {e}"
        print(f"[PAY_EDIT] {msg}", flush=True)
        _alert_admin(f"Правка оплаты для '{client_name}': ошибка Sheets", e)
        return False, msg
def _confirm_accounting_events(events: list, sender_name: str):
    """Отправляет подтверждение по бухгалтерским командам в группу."""
    lines = []
    for evt in events:
        etype = evt.get('type') or ''
        client = evt.get('client') or '—'
        if etype == 'price':
            product = evt.get('price_product') or '—'
            new_price = evt.get('price_new') or '—'
            date_from = evt.get('price_date_from') or ''
            contact = evt.get('price_contact') or ''
            date_str = f' с {date_from}' if date_from else ''
            contact_str = f' ({contact})' if contact else ''
            lines.append(f'💰 Цена: {client}{contact_str} → {product} = {new_price} руб.{date_str}')
        elif etype == 'object_add':
            obj = evt.get('object_name') or '—'
            lines.append(f'🏗 Объект: добавить «{obj}» для {client}')
        elif etype == 'client_add':
            pay = evt.get('payment_type') or ''
            pay_str = f' (оплата: {pay})' if pay else ''
            lines.append(f'👤 Новый клиент: {client}{pay_str}')
        elif etype == 'client_edit':
            comment = evt.get('comment') or '—'
            lines.append(f'✏️ Изменение: {client} — {comment}')
        elif etype == 'payment_info':
            pay = evt.get('payment_type') or '—'
            amount = evt.get('payment_amount') or ''
            method = evt.get('payment_method') or ''
            evt_date = evt.get('date') or ''
            amount_str = f' {amount} руб.' if amount else ''
            method_str = f' на {method}' if method else ''
            date_str = f' от {evt_date}' if evt_date else ''
            lines.append(f'💳 Оплата: {client} — {pay}{amount_str}{method_str}{date_str}')
        else:
            comment = evt.get('comment') or ''
            lines.append(f'📋 {etype}: {client} {comment}')

    if lines:
        header = f'✅ Принято от {sender_name}:'
        text = header + '\n' + '\n'.join(lines)
        send_msg(BLOK_GROUP_ID, text)
        print(f'[ACCOUNTING] Подтверждение: {text}', flush=True)

        # Применяем изменения в листах клиентов
        for evt in events:
            if evt.get('type') == 'price':
                client = evt.get('client') or ''
                product = evt.get('price_product') or ''
                price_new = evt.get('price_new')
                date_from = evt.get('price_date_from') or ''
                contact = evt.get('price_contact') or ''
                qty_pallet = str(evt.get('price_qty_per_pallet') or '').strip()
                if client and product and price_new is not None:
                    ok, msg = _apply_price_change(client, product, float(price_new), date_from, contact, qty_pallet)
                    status = '✅' if ok else '⚠️'
                    send_msg(BLOK_GROUP_ID, f'{status} Sheets: {msg}')
            elif evt.get('type') == 'payment_info':
                client = evt.get('client') or ''
                amount = evt.get('payment_amount')
                pay_type = evt.get('payment_type') or ''  # нал/безнал
                pay_method = evt.get('payment_method') or ''  # ИП/ООО
                evt_date = evt.get('date') or evt.get('payment_date') or ''
                contact = evt.get('price_contact') or ''
                if client and amount:
                    ok, msg = _apply_payment(client, float(amount), pay_type, pay_method, evt_date, contact)
                    status = '✅' if ok else '⚠️'
                    send_msg(BLOK_GROUP_ID, f'{status} Sheets: {msg}')
                elif client and not amount:
                    send_msg(BLOK_GROUP_ID, f'⚠️ Оплата {client}: не указана сумма. Напишите сумму оплаты.')

            elif evt.get('type') == 'client_edit':
                client = evt.get('client') or ''
                contact = evt.get('price_contact') or evt.get('comment') or ''
                edit_date = evt.get('date') or ''
                comment = evt.get('comment') or ''
                # Проверяем: это дополнение к возврату поддонов?
                comment_lower = comment.lower()
                if 'возврат' in comment_lower and ('поддон' in comment_lower or 'а/м' in comment_lower or 'объект' in comment_lower or 'наличк' in comment_lower or 'безнал' in comment_lower or 'нал' in comment_lower):
                    # Извлекаем авто и объект из комментария
                    import re as _re
                    truck = ''
                    obj = ''
                    pay_type = ''
                    truck_m = _re.search(r'а/м\s+([а-яА-Яa-zA-Z0-9]+\s*\d+)', comment)
                    truck = truck_m.group(1).strip() if truck_m else ''
                    # Нормализуем номер машины
                    if truck:
                        truck_upper = truck.upper()
                        for short, full in TRUCK_SHORTCUTS.items():
                            if short in truck_upper:
                                truck = full
                                break
                        else:
                            for valid in VALID_TRUCKS:
                                if truck_upper.replace(' ', '') in valid.upper().replace(' ', ''):
                                    truck = valid
                                    break
                    obj_m = _re.search(r'[Оо]бъект\s+(.+?)(?:$|,|\.|\\n)', comment)
                    obj = obj_m.group(1).strip() if obj_m else ''
                    # Извлекаем тип оплаты
                    pay_type = ''
                    if 'наличк' in comment_lower or 'нал ' in comment_lower or comment_lower.endswith('нал') or 'на нал' in comment_lower:
                        pay_type = 'нал'
                    elif 'безнал' in comment_lower:
                        pay_type = 'безнал'
                    elif ' ип' in comment_lower or 'от ип' in comment_lower:
                        pay_type = 'ип'
                    elif 'ооо' in comment_lower:
                        pay_type = 'ооо'
                    if client and edit_date and (truck or obj or pay_type):
                        ok, msg = _apply_return_update(client, edit_date, truck, obj, pay_type)
                        status = '✅' if ok else '⚠️'
                        send_msg(BLOK_GROUP_ID, f'{status} Sheets: {msg}')
                    else:
                        send_msg(BLOK_GROUP_ID, f'⚠️ Дополнение к возврату {client}: не удалось извлечь авто/объект из сообщения')
                elif client and (contact or edit_date):
                    ok, msg = _apply_payment_edit(client, contact, edit_date)
                    status = '✅' if ok else '⚠️'
                    send_msg(BLOK_GROUP_ID, f'{status} Sheets: {msg}')

            elif evt.get('type') == 'object_add':
                # Добавление объекта в справочник клиента (AM)
                client = evt.get('client') or ''
                obj_name = evt.get('object_name') or ''
                if client and obj_name:
                    ok, msg = _apply_object_add(client, obj_name)
                    status = '✅' if ok else '⚠️'
                    send_msg(BLOK_GROUP_ID, f'{status} Sheets: {msg}')

        # Записываем в Sheets (лист "Бухгалтерия") — только бухгалтерские, НЕ рейсы
        acct_only = [e for e in events if e.get('type') not in ('trip', 'return')]
        if acct_only:
            _write_accounting_to_sheets(acct_only)
def _write_accounting_to_sheets(events: list):
    """Записывает бухгалтерские команды в лист Бухгалтерия Google Sheets."""
    if not events or not GOOGLE_SA_B64:
        return
    try:
        import base64
        import gspread
        from google.oauth2.service_account import Credentials
        sa_json = base64.b64decode(GOOGLE_SA_B64)
        sa_info = json.loads(sa_json)
        creds = Credentials.from_service_account_info(
            sa_info,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEETS_ID)
        try:
            ws = sh.worksheet('Бухгалтерия')
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title='Бухгалтерия', rows=1000, cols=8)
            ws.append_row(['№', 'Дата', 'Тип', 'Клиент', 'Продукт', 'Цена', 'Объект', 'Комментарий'], value_input_option='USER_ENTERED')

        all_vals = ws.col_values(1)
        last_num = 0
        for v in all_vals:
            if v.isdigit():
                last_num = max(last_num, int(v))

        for evt in events:
            last_num += 1
            # Сумма: для цен — price_new, для оплат — payment_amount
            amount = evt.get('price_new') or evt.get('payment_amount') or ''
            product = evt.get('price_product') or ''
            # Для оплат: дополняем информацию
            comment = evt.get('comment') or ''
            evt_type = evt.get('type') or ''
            client_field = evt.get('client') or ''

            if evt_type == 'payment_info':
                pay_type = evt.get('payment_type') or ''
                pay_method = evt.get('payment_method') or ''
                parts = []
                if pay_type:
                    parts.append(pay_type)
                if pay_method:
                    parts.append(f'на {pay_method}')
                if parts:
                    comment = ', '.join(parts) + (f'; {comment}' if comment else '')

            # Для correction: формируем подробный комментарий из target и updates
            if evt_type == 'correction':
                tt = evt.get('target_table') or ''
                tn = evt.get('target_num')
                td = evt.get('target_date') or ''
                tc = evt.get('target_client') or ''
                updates = evt.get('updates') or {}
                details = evt.get('details') or []
                target_parts = []
                if tt:
                    target_parts.append(tt)
                if tn:
                    target_parts.append(f'#{tn}')
                if td:
                    target_parts.append(td)
                if tc:
                    target_parts.append(tc)
                target_str = ' '.join(target_parts)
                upd_str = ', '.join(f'{k}={v}' for k, v in updates.items())
                det_str = f' + {len(details)} блок-детал.' if details else ''
                comment = f'{target_str}: {upd_str}{det_str}'
                # client_field оставляем — это target_client
                if tc:
                    client_field = tc
                product = ''
                amount = ''

            row = [
                str(last_num),
                evt.get('date') or _today_msk(),
                evt_type,
                client_field,
                product,
                str(amount) if amount else '',
                evt.get('object_name') or '',
                comment,
            ]
            ws.append_row(row, value_input_option='USER_ENTERED')
            print(f'[ACCOUNTING_SHEETS] Добавлено: {row}', flush=True)
    except Exception as e:
        print(f'[ACCOUNTING_SHEETS] Ошибка: {e}', flush=True)
        _alert_admin("Бухгалтерия: ошибка записи событий в Sheets", e)
def handle_blok_group_message(sender_name: str, text: str, raw_msg: dict):
    """Основная точка входа для сообщений из группы производства."""
    _log_blok_message(sender_name, text, raw_msg)
    # Проверяем структурированные сообщения (#поддоны, #условия)
    if pallet_handler:
        pallet_result = pallet_handler.try_handle(text, sender_name)
        if pallet_result:
            print(f"[PALLET] {pallet_result}", flush=True)
            send_msg(BLOK_GROUP_ID, pallet_result)
            return
    # Фильтр ПЛАНОВ — бот реагирует ТОЛЬКО на факт (выполненные рейсы/события)
    # План = задание на будущее, бот НЕ должен записывать его в таблицу
    text_lower = text.lower()
    plan_markers = (
        "план на ", "план:", "план ", "завтра ", "планируем", "планирую",
        "нужно будет", "надо будет", "будет ехать", "поедет", "повезёт", "повезет",
        "запланирован", "на завтра", "на понедельник", "на вторник", "на среду",
        "на четверг", "на пятницу", "на субботу", "на следующ",
    )
    # Маркеры выполнения — если они есть, это ФАКТ, не план
    fact_markers = (
        "выполнен", "доставлен", "отгружен", "загружен", "разгрузил",
        "привёз", "привез", "отгрузил", "рейс выполн", "рейсы выполн",
    )
    has_fact = any(fm in text_lower for fm in fact_markers)
    if any(pm in text_lower for pm in plan_markers) and not has_fact:
        print(f"[BLOK_GROUP] ПЛАН — игнорируем: {text[:80]}", flush=True)
        return  # Это план, не факт — не обрабатываем

    # Ищем признаки выполненных работ или бухгалтерских команд
    keywords = (
        "блок", "поддон", "рейс", "везёт", "везет", "доставка",
        "возврат", "вернул", "забрал", "привёз", "привез", "отгруз", "загруз", "выгруз",
        "цен", "прайс", "стоимост", "подорож", "удешев",
        "двадцат", "девят", "двенаш", "полублок", "керамзит", "отсев",
        "20-", "9-", "12-", "20(", "9(", "12(",
        # Бухгалтерские команды (Светлана)
        "надо", "необходимо", "изменить", "добавить", "удалить", "клиент", "оплат",
        "объект", "литер", "390х", "188", "нал ", "безнал",
        "оплатил", "оплатила", "внёс", "внес", "перевёл", "перевел", "перечисл",
        # Подтверждения выполнения
        "выполнен", "доставлен", "отгружен", "загружен", "разгрузил", "приехал",
    )
    # Триггеры правок — для них дедупликация по тексту НЕ применяется
    # (менеджер может повторить «поправь #2 цена 40» если первая правка не сработала)
    correction_markers = ("поправ", "исправ", "правк", "корректировк", "уточнен", "дополни", "обнови")
    is_correction_msg = any(cm in text_lower for cm in correction_markers)

    # S4: keyword-фильтр переведён в ЛОГИРУЮЩИЙ режим — отсев в файл, но Claude всё равно вызывается.
    # Раньше тут был silent drop — менеджер ждал ответа, а бот не запускал парсер.
    # Теперь логируем «подозрительные» сообщения, но шанс на разбор даём.
    if not any(kw in text_lower for kw in keywords) and not is_correction_msg:
        try:
            silent_file = os.path.join(DATA_DIR, "silent_drops.jsonl")
            with open(silent_file, "a", encoding="utf-8") as _f:
                _f.write(json.dumps({
                    "ts": datetime.datetime.now().isoformat(),
                    "sender": sender_name,
                    "text": text[:500],
                    "reason": "no_keywords_match",
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        # Если сообщение слишком короткое (<10 символов) или это явный мусор (одно слово/смайлик) — реально игнорируем
        if len(text.strip()) < 10:
            return
        # Иначе — даём Claude шанс разобрать. +$0.01 за вызов, но не теряем команды.
        print(f"[BLOK_GROUP] No-keywords пропускаем в Claude: {text[:80]}", flush=True)

    # Дедупликация на уровне текста — если ТОЧНО тот же текст уже обработан, не парсим повторно
    # ИСКЛЮЧЕНИЕ: правки (correction) — пропускаем текстовый дедуп
    import hashlib as _hl
    _text_hash = _hl.md5(text.strip().lower().encode()).hexdigest()
    _text_dedup_key = f"txt_{_text_hash}"
    with _dedup_lock:
        _text_is_dup = _text_dedup_key in _dedup_cache and (time.time() - _dedup_cache[_text_dedup_key]) < _DEDUP_TTL
    if _text_is_dup and not is_correction_msg:
        print(f"[DEDUP] Текст уже обработан (повтор): {text[:60]}", flush=True)
        send_msg(BLOK_GROUP_ID, "ℹ️ Это сообщение уже было обработано ранее.")
        return

    # Показываем индикатор "печатает..." пока бот обрабатывает
    send_action(BLOK_GROUP_ID, "typing_on")
    # Извлекаем дату из timestamp сообщения (МСК) — точнее чем текущее время
    _msg_ts = raw_msg.get("timestamp") or raw_msg.get("body", {}).get("timestamp")
    if _msg_ts:
        try:
            _msg_dt = datetime.datetime.utcfromtimestamp(_msg_ts / 1000) + datetime.timedelta(hours=3)
            _msg_date = _msg_dt.strftime("%d.%m.%Y")
        except (ValueError, OSError, TypeError):
            _msg_date = _today_msk()
    else:
        _msg_date = _today_msk()
    trips = _parse_blok_plan_claude(text, msg_date=_msg_date)
    # Страховка: если Claude вернул 2+ trip с одинаковым ТС/датой/откуда — мерджим в сборный
    trips = _merge_combined_trip_events(trips)
    # Regex-фолбэк: если AI вернул [] на сообщении про перемещение поддонов — пробуем regex
    if not trips:
        fallback_trips = _parse_pallet_transfer_fallback(text, msg_date=_msg_date)
        if fallback_trips:
            print(f"[BLOK_GROUP] AI вернул [], regex-fallback нашёл pallet_transfer: {fallback_trips}", flush=True)
            trips = fallback_trips
    if not trips:
        fallback_trips = _parse_pallet_return_fallback(text, msg_date=_msg_date)
        if fallback_trips:
            print(f"[BLOK_GROUP] AI вернул [], regex-fallback нашёл pallet_return: {fallback_trips}", flush=True)
            trips = fallback_trips
    if not trips:
        print(f"[BLOK_GROUP] Парсер вернул пустой список для: {text[:60]}", flush=True)
        # S2: лог parse_failures.jsonl + алерт админу для разбора
        try:
            failures_file = os.path.join(DATA_DIR, "parse_failures.jsonl")
            with open(failures_file, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "ts": datetime.datetime.now().isoformat(),
                    "sender": sender_name,
                    "text": text,
                    "msg_date": _msg_date,
                    "fallback_attempted": True,
                    "reason": "parser_empty",
                }, ensure_ascii=False) + "\n")
        except Exception as _le:
            print(f"[PARSE_FAIL_LOG] {_le}", flush=True)
        # Эвристика: если в тексте несколько маркеров «Закупка <дата>» — это слитное сообщение
        purchase_markers = len(re.findall(r'(?i)\bзакупка\s+\d', text))
        if purchase_markers >= 2:
            send_msg(BLOK_GROUP_ID, "⚠️ Похоже здесь несколько закупок в одном сообщении. Напишите каждую закупку отдельным сообщением, пожалуйста.")
        else:
            # Подсказка с примерами рабочих формулировок (вместо абстрактного «переформулируйте»)
            send_msg(BLOK_GROUP_ID, (
                "⚠️ Не удалось разобрать сообщение. Примеры рабочих формулировок:\n"
                "• Перемещение поддонов: «Перемещение 200 поддонов с Тихорецкой на Карьер для 135 от 26.05»\n"
                "• Рейс: «Рейс 26.05 для 135: 15 подд 20(3-0) с КРД на ИП Шубина»\n"
                "• Возврат: «Возврат от ИП Шубина 20 поддонов от 26.05, машина 135»\n"
                "• Оплата: «ИП Шубина оплатил 50000 руб наличные»"
            ))
            # Алерт админу: повторяющиеся неудачи парсинга = знак для разбора промта
            try:
                _alert_admin(f"⚠️ Парсер не разобрал сообщение от {sender_name}:\n«{text[:200]}»\nЛог: {failures_file}", None)
            except Exception:
                pass
        return

    # Дедупликация — отсекаем повторные сообщения (Светлана повторяет 3-6 раз)
    unique_trips = [t for t in trips if not _dedup_check(t)]
    if not unique_trips:
        print(f"[DEDUP] Все {len(trips)} событий — дубликаты, пропускаем", flush=True)
        send_msg(BLOK_GROUP_ID, "ℹ️ Уже записано ранее, повторная запись не требуется.")
        return
    if len(unique_trips) < len(trips):
        print(f"[DEDUP] {len(trips) - len(unique_trips)} дубликатов отклонено из {len(trips)}", flush=True)
    trips = unique_trips

    print(f"[BLOK_GROUP] Распарсено {len(trips)} событий: {trips}", flush=True)

    # Запоминаем хеш текста как обработанный
    with _dedup_lock:
        _dedup_cache[_text_dedup_key] = time.time()
        _save_dedup_cache(_dedup_cache)

    # Правки (correction) — применяем отдельно, обходим _write_trips_to_sheets
    corrections = [t for t in trips if t.get('type') == 'correction']
    for corr in corrections:
        try:
            ok, msg = _apply_correction(corr)
            status = '✏️' if ok else '⚠️'
            send_msg(BLOK_GROUP_ID, f'{status} {msg}')
        except Exception as e:
            print(f'[CORRECTION] handler error: {e}', flush=True)
            send_msg(BLOK_GROUP_ID, f'⚠️ Правка не применена: {e}')
    # Логируем правки в Бухгалтерию (для аудита кто что когда правил)
    if corrections:
        try:
            _write_accounting_to_sheets(corrections)
        except Exception as e:
            print(f'[CORRECTION] log error: {e}', flush=True)

    # Рейсы/возвраты/цены/перемещения поддонов → в Sheets
    # pallet_transfer: пишется в «Рейсы» (БЕЗ списания со склада — это пустые поддоны)
    sheet_events = [t for t in trips if t.get('type') in ('trip', 'return', 'price', 'pallet_transfer')]
    if sheet_events:
        _write_trips_to_sheets(sheet_events)

    # Бухгалтерские события → подтверждение в чат
    # Исключаем: trip/return (уже подтверждаются в _write_trips_to_sheets) +
    # pallet_transfer (тоже подтверждается там же — иначе двойное уведомление) +
    # correction (обработан выше отдельно)
    non_trip_events = [t for t in trips if t.get('type') not in ('trip', 'return', 'pallet_transfer', 'correction')]
    if non_trip_events:
        _confirm_accounting_events(non_trip_events, sender_name)
def process_update(update: dict):
    utype = update.get("update_type")
    if utype == "message_created":
        msg = update.get("message", {})
        sender = msg.get("sender", {})
        if sender.get("is_bot"):
            return
        user_id = sender.get("user_id")
        if not user_id:
            return
        chat_id = msg.get("recipient", {}).get("chat_id") or user_id
        user_chat_map[user_id] = chat_id  # запоминаем правильный chat_id для коллбэков
        user_name = sender.get("name", "")
        body = msg.get("body", {})
        text = (body.get("text") or "").strip()
        # MAX: пересланные сообщения — текст в link.message.text
        if not text:
            link = msg.get("link", {})
            if link.get("type") == "forward":
                text = (link.get("message", {}).get("text") or "").strip()

        # Логируем ВСЕ входящие сообщения для диагностики
        _debug_log_chat(chat_id, user_name, text[:80] if text else f"[no text, update_type={utype}]")

        # Сообщение из группы производства — отдельная обработка
        if chat_id == BLOK_GROUP_ID:
            print(f"[BLOK_MSG] from={user_name} text={text[:80]!r} has_text={bool(text)}", flush=True)
            if text:
                try:
                    handle_blok_group_message(user_name, text, msg)
                except Exception as e:
                    import traceback
                    full_trace = traceback.format_exc()
                    err = f"[BLOK_ERROR] {e}\n{full_trace[:1200]}"
                    print(err, flush=True)
                    # В Telegram админу — полный трейс (до 3500 символов)
                    if OWNER_CHAT_ID:
                        try:
                            admin_msg = (
                                f"⚠️ Ошибка блок-обработки\n"
                                f"Тип: {type(e).__name__}\n"
                                f"Сообщение пользователя: {text[:150]}\n\n"
                                f"Trace:\n{full_trace[:2500]}"
                            )
                            send_msg(OWNER_CHAT_ID, admin_msg[:3800])
                        except Exception:
                            pass
            return  # не пускаем в основную логику бота

        # Голосовое / аудио
        VOICE_TYPES = ("audio", "voice", "audio_msg", "audio_message", "voice_message")
        attachments = body.get("attachments") or []
        for att in attachments:
            att_type = att.get("type", "")
            print(f"[ATT] Тип вложения: {att_type}", flush=True)
            if att_type in VOICE_TYPES:
                audio_url = att.get("payload", {}).get("url", "")
                if audio_url and GROQ_API_KEY:
                    transcribed = transcribe_voice_url(audio_url)
                    if transcribed:
                        with _voice_lock:
                            pending_voice[chat_id] = (transcribed, user_name, user_id, chat_id)
                            if user_id and user_id != chat_id:
                                pending_voice[user_id] = (transcribed, user_name, user_id, chat_id)
                        btns = [[
                            {"type": "callback", "text": "✅ Всё правильно", "payload": "voice_ok"},
                            {"type": "callback", "text": "🔄 Повторить", "payload": "voice_retry"},
                        ]]
                        send_msg(chat_id, f"Распознал: «{transcribed}»\n\nВсё верно?", btns)
                    else:
                        send_msg(chat_id, "Не смог распознать голосовое. Пожалуйста, напишите текстом.")
                else:
                    send_msg(chat_id, "Голосовые сообщения пока не поддерживаются. Напишите текстом.")
                return

        # Фото/стикер/файл без текста и без голоса — подсказка
        if attachments and not text:
            has_voice_att = any(a.get("type","") in VOICE_TYPES for a in attachments)
            if not has_voice_att:
                send_msg(chat_id, "Я понимаю текст и голосовые сообщения. Напишите или наговорите что вам нужно 🙂")
                return
        if text:
            handle_message(chat_id, text, user_name, user_id=user_id)

    elif utype == "message_callback":
        cb = update.get("callback", {})
        print(f"[CB_RAW] {json.dumps(cb, ensure_ascii=False)[:400]}", flush=True)
        callback_id = cb.get("callback_id", "")
        payload = cb.get("payload", "")
        user = cb.get("user", {})
        user_id = user.get("user_id")
        if not user_id:
            return
        # chat_id берём из маппинга (Max не передаёт chat_id в callback)
        orig_msg = cb.get("message", {})
        chat_id = orig_msg.get("recipient", {}).get("chat_id") or user_chat_map.get(user_id) or user_id
        print(f"[CB] user_id={user_id} chat_id={chat_id} payload={payload!r}", flush=True)
        handle_callback(user_id, chat_id, callback_id, payload, orig_msg=orig_msg)
# ─── Главный цикл ─────────────────────────────────────────────────────────

# ─── Еженедельный отчёт ────────────────────────────────────────────────────

def build_weekly_report() -> str:
    """Генерирует текст еженедельного отчёта."""
    from collections import Counter
    events = load_analytics(days=7)

    def count(etype): return sum(1 for e in events if e.get("event") == etype)

    started = count("conversation_started")
    completed = count("order_completed")
    replied = count("manager_replied")
    conv = f"{round(completed/started*100)}%" if started else "—"

    # Время ответа менеджера
    reply_events = [e for e in events if e.get("event") == "manager_replied" and e.get("response_mins")]
    if reply_events:
        avg_resp = round(sum(e["response_mins"] for e in reply_events) / len(reply_events), 1)
        max_resp = round(max(e["response_mins"] for e in reply_events), 1)
        resp_str = f"  Среднее: {avg_resp} мин | Макс: {max_resp} мин"
    else:
        resp_str = "  нет данных"

    # Воронка
    funnel_events = [e for e in events if e.get("event") == "funnel_step"]
    funnel_steps = {"product": set(), "volume": set(), "delivery": set(), "address": set(), "contacts": set(), "phone": set(), "confirm": set()}
    for e in funnel_events:
        step = e.get("step")
        cid = e.get("chat_id")
        if step in funnel_steps and cid:
            funnel_steps[step].add(cid)
    funnel_str = (
        f"  Товар: {len(funnel_steps['product'])}\n"
        f"  Объём: {len(funnel_steps['volume'])}\n"
        f"  Доставка: {len(funnel_steps['delivery'])}\n"
        f"  Адрес: {len(funnel_steps['address'])}\n"
        f"  Контакты: {len(funnel_steps['contacts']) + len(funnel_steps['phone'])}\n"
        f"  Подтверждение: {len(funnel_steps['confirm'])}\n"
        f"  Оформлено: {completed}"
    )

    # Топ товаров
    products = [e.get("product") for e in events if e.get("event") == "order_completed" and e.get("product")]
    top = Counter(products).most_common(3)
    top_str = "\n".join(f"  {p}: {n}" for p, n in top) or "  нет данных"

    # Доставка vs самовывоз
    deliveries = [e.get("delivery") for e in events if e.get("event") == "order_completed"]
    d_count = deliveries.count("Доставка")
    p_count = deliveries.count("Самовывоз")

    return (
        f"Еженедельный отчёт бота (Max)\n\n"
        f"Диалоги: {started}\n"
        f"Заявки оформлены: {completed}\n"
        f"Конверсия: {conv}\n"
        f"Ответов менеджера: {replied}\n"
        f"Доставка: {d_count} | Самовывоз: {p_count}\n\n"
        f"Время ответа менеджера:\n{resp_str}\n\n"
        f"Воронка:\n{funnel_str}\n\n"
        f"Топ товаров:\n{top_str}"
    )
def _reset_checkpoint_if_needed():
    """Сбрасывает чекпоинт если GROUP_ID изменился."""
    checkpoint_path = os.path.join(DATA_DIR, "blok_checkpoint.json")
    try:
        with open(checkpoint_path) as f:
            data = json.load(f)
        saved_group = data.get("group_id")
        if saved_group and saved_group != BLOK_GROUP_ID:
            print(f"[BLOK_SYNC] GROUP_ID изменился ({saved_group} → {BLOK_GROUP_ID}), сброс чекпоинта", flush=True)
            with open(checkpoint_path, "w") as f:
                json.dump({"last_ts_ms": 0, "last_dt": "reset", "group_id": BLOK_GROUP_ID}, f)
        elif not saved_group:
            # Дописываем group_id в существующий чекпоинт
            data["group_id"] = BLOK_GROUP_ID
            # Если ts из старой группы — тоже сбрасываем
            old_ts = data.get("last_ts_ms", 0)
            if old_ts > 0:
                print(f"[BLOK_SYNC] Сброс чекпоинта (старая группа без group_id)", flush=True)
                data["last_ts_ms"] = 0
                data["last_dt"] = "reset"
            with open(checkpoint_path, "w") as f:
                json.dump(data, f)
    except FileNotFoundError:
        pass
def _run_blok_import():
    """Запускает import_blok_history.py как подпроцесс."""
    import subprocess
    import sys as _sys
    script = os.path.join(os.path.dirname(os.path.abspath(_sys.argv[0])), "import_blok_history.py")
    print(f"[BLOK_SYNC] Запуск: {script}", flush=True)
    result = subprocess.run(
        [_sys.executable, script],
        capture_output=True, text=True, encoding="utf-8", timeout=180
    )
    print(f"[BLOK_SYNC] Завершён (rc={result.returncode})", flush=True)
    if result.stdout:
        print(result.stdout[-800:], flush=True)
    if result.stderr:
        print(f"[BLOK_SYNC_ERR] {result.stderr[-300:]}", flush=True)
def blok_sync_loop():
    """Фоновый тред: сразу при старте + каждые 4 часа (8, 12, 16, 20 МСК)."""
    last_run_key = None
    RUN_HOURS = {8, 12, 16, 20}

    # Первый запуск — через 2 минуты после старта бота (дать время инициализироваться)
    print("[BLOK_SYNC] Первый запуск через 2 минуты...", flush=True)
    time.sleep(120)
    try:
        _run_blok_import()
        last_run_key = ("startup",)
    except Exception as e:
        print(f"[BLOK_SYNC] Ошибка при старте: {e}", flush=True)

    while True:
        try:
            time.sleep(600)  # проверяем каждые 10 минут
            now_msk = datetime.datetime.utcnow() + datetime.timedelta(hours=3)
            hour = now_msk.hour
            run_key = (now_msk.date(), hour)
            if hour in RUN_HOURS and last_run_key != run_key:
                last_run_key = run_key
                print(f"[BLOK_SYNC] Плановый запуск {now_msk.strftime('%Y-%m-%d %H:%M')}", flush=True)
                _run_blok_import()
        except Exception as e:
            print(f"[BLOK_SYNC] Ошибка: {e}", flush=True)
def weekly_report_loop():
    """Фоновый тред: отправляет отчёт владельцу по воскресеньям в 20:00 МСК."""
    import datetime
    last_sent_week = None
    while True:
        try:
            # МСК = UTC+3
            now_msk = datetime.datetime.utcnow() + datetime.timedelta(hours=3)
            week_num = now_msk.isocalendar()[1]
            # Воскресенье = 6 (weekday()), час >= 20
            if now_msk.weekday() == 6 and now_msk.hour >= 20 and last_sent_week != week_num:
                if OWNER_CHAT_ID:
                    report = build_weekly_report()
                    send_msg(OWNER_CHAT_ID, report)
                    print(f"[WEEKLY] Отчёт отправлен владельцу {OWNER_CHAT_ID}", flush=True)
                    last_sent_week = week_num
        except Exception as e:
            print(f"[WEEKLY] Ошибка: {e}", flush=True)
        time.sleep(1800)  # проверяем каждые 30 минут
# ─── Прайс-лист ───────────────────────────────────────────────────────────

def _format_price_list():
    """Формирует прайс-лист для отправки."""
    lines = ["📋 <b>Прайс-лист Архиповского карьера</b>\n"]
    lines.append("<b>Материал | Цена за тонну</b>")
    lines.append("─" * 30)
    for product, price in PRODUCTS.items():
        if price is not None:
            lines.append(f"▸ {product} — <b>{price} ₽/т</b>")
        else:
            lines.append(f"▸ {product} — <b>по запросу</b>")
    lines.append("─" * 30)
    lines.append("\n🚚 Доставка от 30 тонн — рассчитывается по расстоянию")
    lines.append(f"📍 {BASE_NAME}")
    lines.append(f"🕐 {WORK_HOURS}")
    lines.append("\n💬 Для заказа напишите что вам нужно или /start")
    return "\n".join(lines)

def _run_self_test():
    """Selftest при старте: парсим тестовое сообщение через Claude, отправляем результат owner."""
    # ── Тест 1: простое изменение цены ──
    test_text = "Надо изменить цену у ТЕСТ-КЛИЕНТ на Блок 390х190х188 с 01.01.2026 на 99,99 руб"
    print(f"[SELFTEST] Тест 1 (цена): {test_text}", flush=True)

    events = _parse_blok_plan_claude(test_text)
    if not events:
        report = "❌ SELFTEST Тест 1 FAIL: Claude вернул пустой список"
        print(f"[SELFTEST] {report}", flush=True)
        # НЕ отправляем владельцу — только в лог
        return

    # ── Тест 2: сборный рейс с двумя выгрузками + одна продукция по 2 ценам ──
    test_text_2 = ("Выполненный рейс от 19.05 для 135-го. "
                   "Закупка 19.05 от ИП Евсеев, Великовечное, 12(2-0) - 8 подд по 120 шт/подд цена 32 руб + "
                   "12(2-0) - 1 подд по 120 шт/подд цена 30 руб, доставка 10200 руб на ИП Шубина цена продажи 48 руб. "
                   "Закупка 19.05 от ИП Евсеев, Великовечное, 20(3-0) - 2 подд по 75 шт/подд цена 39 руб + "
                   "12(2-0) - 4 подд по 120 шт/подд цена 30 руб, доставка 6800 руб на ИП Горячкина "
                   "цена продажи 20(3-0) - 61 руб, 12(2-0) - 48 руб")
    print(f"[SELFTEST] Тест 2 (сборный рейс): {test_text_2[:80]}...", flush=True)
    events_2 = _parse_blok_plan_claude(test_text_2, msg_date="19.05.2026")
    events_2 = _merge_combined_trip_events(events_2)
    if not events_2:
        print("[SELFTEST] ❌ Тест 2 FAIL: пустой список", flush=True)
    elif len(events_2) != 1:
        print(f"[SELFTEST] ❌ Тест 2 FAIL: ожидалось 1 событие, получено {len(events_2)}: {events_2}", flush=True)
    else:
        evt = events_2[0]
        if evt.get('type') != 'trip':
            print(f"[SELFTEST] ❌ Тест 2 FAIL: type={evt.get('type')}, ожидалось trip", flush=True)
        else:
            deliv = evt.get('deliveries') or []
            if len(deliv) != 2:
                print(f"[SELFTEST] ❌ Тест 2 FAIL: deliveries={len(deliv)}, ожидалось 2 | event={evt}", flush=True)
            else:
                clients = [d.get('client') for d in deliv]
                products_per_d = [len(d.get('products') or []) for d in deliv]
                if any(p < 2 for p in products_per_d):
                    print(f"[SELFTEST] ⚠️ Тест 2 ЧАСТИЧНО: deliveries есть но products={products_per_d}, клиенты={clients}", flush=True)
                else:
                    print(f"[SELFTEST] ✅ Тест 2 OK: 1 trip + 2 deliveries, клиенты={clients}, products/delivery={products_per_d}", flush=True)
    
    # Построим подтверждение как в реальной обработке
    lines = []
    for evt in events:
        etype = evt.get('type') or ''
        client = evt.get('client') or '—'
        if etype == 'price':
            product = evt.get('price_product') or '—'
            new_price = evt.get('price_new') or '—'
            date_from = evt.get('price_date_from') or ''
            contact = evt.get('price_contact') or ''
            date_str = f' с {date_from}' if date_from else ''
            contact_str = f' ({contact})' if contact else ''
            lines.append(f'💰 Цена: {client}{contact_str} → {product} = {new_price} руб.{date_str}')
        elif etype == 'object_add':
            obj = evt.get('object_name') or '—'
            lines.append(f'🏗 Объект: «{obj}» для {client}')
        elif etype == 'client_add':
            lines.append(f'👤 Клиент: {client}')
        else:
            lines.append(f'📋 {etype}: {client}')
    
    report = f"✅ SELFTEST OK: {len(events)} событий\n" + "\n".join(lines)
    print(f"[SELFTEST] {report}", flush=True)
    # НЕ отправляем владельцу — только в лог (по просьбе Алексея)
def main():
    print("[STARTUP] Запуск...", flush=True)
    time.sleep(3)  # minimal delay for clean restart
    if not TOKEN:
        print("[STARTUP] ОШИБКА: MAX_BOT_TOKEN не задан!", flush=True)
        return
    print(f"[STARTUP] Бот запущен!", flush=True)
    print(f"[STARTUP] MANAGER_CHAT_ID = {MANAGER_CHAT_ID or 'НЕ ЗАДАН — заявки некуда слать!'}", flush=True)
    print(f"[STARTUP] OWNER_CHAT_ID   = {OWNER_CHAT_ID or 'не задан'}", flush=True)
    load_state()
    _load_polls()
    # Clear stale wizard states (wizard data not persisted across restarts)
    stale_keys = [k for k, v in user_state.items() if isinstance(v, str) and v.startswith("poll_wiz")]
    for k in stale_keys:
        user_state.pop(k, None)
        print(f"[STARTUP] Cleared stale wizard state for {k}", flush=True)

    # Запускаем фоновый тред для еженедельного отчёта
    report_thread = threading.Thread(target=weekly_report_loop, daemon=True)
    report_thread.start()
    print("[STARTUP] Еженедельный отчёт: воскресенье 20:00 МСК", flush=True)

    # Сбрасываем чекпоинт если GROUP_ID изменился
    _reset_checkpoint_if_needed()

    # Запускаем фоновый тред синхронизации группы Архиповский блок
    if CLAUDE_API_KEY and GOOGLE_SA_B64:
        sync_thread = threading.Thread(target=blok_sync_loop, daemon=True)
        sync_thread.start()
        print("[STARTUP] Blok sync: каждые 4 часа (8, 12, 16, 20 МСК)", flush=True)
    else:
        print("[STARTUP] Blok sync ОТКЛЮЧЁН (нет CLAUDE_API_KEY или GOOGLE_SA_B64)", flush=True)

    # Диагностика: пишем info о боте и его чатах в файл
    try:
        me = _api("GET", "me")
        chats_resp = _api("GET", "chats", params={"count": 50})
        diag_path = os.path.join(DATA_DIR, "bot_diag.json")
        with open(diag_path, "w", encoding="utf-8") as f:
            json.dump({"me": me, "chats": chats_resp}, f, ensure_ascii=False, indent=2)
        print(f"[STARTUP] Bot name: {me.get('name')} @{me.get('username')}", flush=True)
        for c in chats_resp.get("chats", []):
            print(f"[STARTUP] Chat: id={c.get('chat_id')} type={c.get('type')} title={c.get('title')}", flush=True)
    except Exception as e:
        print(f"[STARTUP] Диагностика ошибка: {e}", flush=True)

    # Proof of life
    try:
        send_msg(BLOK_GROUP_ID, "Учётчик перезапущен и готов к работе")
        print("[STARTUP] Proof of life sent", flush=True)
    except Exception as e:
        print(f"[STARTUP] Proof of life error: {e}", flush=True)

    # Self-test: полная цепочка парсинга
    try:
        _run_self_test()
    except Exception as e:
        print(f"[SELFTEST] Критическая ошибка: {e}", flush=True)
        if OWNER_CHAT_ID:
            try:
                send_msg(OWNER_CHAT_ID, f"❌ SELFTEST CRASH: {e}")
            except:
                pass

    marker = None
    with ThreadPoolExecutor(max_workers=6) as pool:
        while True:
            try:
                resp = get_updates(marker=marker, timeout=30)
                updates = resp.get("updates", [])
                if "marker" in resp:
                    marker = resp["marker"]

                for upd in updates:
                    # Диагностика: логируем каждый update
                    _ut = upd.get("update_type", "?")
                    _msg = upd.get("message", {})
                    _sn = _msg.get("sender", {}).get("name", "?")
                    _cid = _msg.get("recipient", {}).get("chat_id", "?")
                    _txt = (_msg.get("body", {}).get("text") or "")[:50]
                    print(f"[POLL] {_ut} chat={_cid} from={_sn}: {_txt!r}", flush=True)
                    pool.submit(process_update_safe, upd)

            except KeyboardInterrupt:
                print("[SHUTDOWN] Остановлен.", flush=True)
                break
            except Exception as e:
                import traceback
                print(f"[ERROR] polling: {e}\n{traceback.format_exc()[:300]}", flush=True)
                time.sleep(5)
if __name__ == "__main__":
    main()
