import sys
import re
import os
import json
import time
import math
import threading
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from groq import Groq
from pydantic import BaseModel
from typing import Optional, List

# ─── Конфиг ───────────────────────────────────────────────────────────────

TOKEN = os.environ.get("MAX_BOT_TOKEN", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

MANAGER_ID_FILE = "manager_id.txt"
OWNER_ID_FILE = "owner_id.txt"

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

PRODUCTS = {
    "Отсев 0-5":             614,
    "Щебень 5-20":           345,
    "Щебень 20-40":          None,
    "Щебень 40-70":          None,
    "Песок мелкозернистый":  233,
    "Песок крупнозернистый": 566,
    "Гравий":                240,
    "ГПС плохой":            75,
    "ГПС хороший":           160,
}

DENSITY = {
    "Отсев 0-5":             1.27,
    "Щебень 5-20":           1.45,
    "Щебень 20-40":          1.42,
    "Щебень 40-70":          1.44,
    "Песок мелкозернистый":  1.50,
    "Песок крупнозернистый": 1.50,
    "Гравий":                1.45,
    "ГПС плохой":            1.77,
    "ГПС хороший":           1.77,
}
DEFAULT_DENSITY = 1.5

PRODUCT, VOLUME, DELIVERY, ADDRESS, CONTACTS, PHONE_ONLY, CONFIRM = range(7)

# State machine (вместо ConversationHandler из PTB)
user_state: dict = {}   # chat_id -> int (состояние)
user_data: dict = {}    # chat_id -> dict (данные заявки)
pending_replies: dict = {}  # manager_id -> {"client_id": int, "expires": float, "summary": str}
order_summaries: dict = {}  # client_id -> краткий саммари заявки для менеджера
pending_voice: dict = {}    # chat_id -> (text, user_name, user_id)
processed_callbacks: set = set()  # дедупликация нажатий кнопок
user_chat_map: dict = {}   # user_id -> chat_id (Max: callback не содержит chat_id)

REPLY_TIMEOUT = 30 * 60  # 30 минут

# ─── Блокировки для многопоточности ────────────────────────────────────────
_save_lock = threading.Lock()          # защита записи bot_state.json
_user_locks: dict = {}                 # chat_id -> Lock (один поток на пользователя)
_user_locks_guard = threading.Lock()   # защита самого словаря _user_locks


def get_user_lock(chat_id: int) -> threading.Lock:
    with _user_locks_guard:
        if chat_id not in _user_locks:
            _user_locks[chat_id] = threading.Lock()
        return _user_locks[chat_id]


STATE_FILE = "bot_state.json"
ANALYTICS_FILE = "analytics.json"
ORDERS_FILE = "orders.json"


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

# ─── Max Bot API ───────────────────────────────────────────────────────────

BASE_URL = "https://botapi.max.ru"


def _api(method: str, endpoint: str, params: dict = None, body: dict = None) -> dict:
    p = dict(params or {})
    p["access_token"] = TOKEN
    url = f"{BASE_URL}/{endpoint}?{urllib.parse.urlencode(p)}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
    req.add_header("User-Agent", "quarry-max-bot/1.0")
    try:
        with urllib.request.urlopen(req, timeout=35) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        print(f"[API] {method} /{endpoint} HTTP {e.code}: {e.read()[:300]}", flush=True)
        return {}
    except Exception as e:
        print(f"[API] {method} /{endpoint} error: {e}", flush=True)
        return {}


def send_msg(chat_id: int, text: str, buttons=None) -> dict:
    """Отправить сообщение. buttons = [[{text, payload}, ...], ...] или None."""
    body = {"text": text}
    if buttons:
        body["attachments"] = [{
            "type": "inline_keyboard",
            "payload": {"buttons": buttons}
        }]
    return _api("POST", "messages", params={"chat_id": chat_id}, body=body)


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
    # Max API требует callback_id как URL-параметр, а не в теле
    params = {"callback_id": callback_id}
    body = {"notification": notification}
    return _api("POST", "answers", params=params, body=body)


def get_updates(marker=None, timeout: int = 30) -> dict:
    p = {"timeout": timeout}
    if marker is not None:
        p["marker"] = marker
    return _api("GET", "updates", params=p)


def make_buttons(items: list) -> list:
    """Список строк → список рядов кнопок (одна кнопка в ряд)."""
    return [[{"type": "callback", "text": s, "payload": s}] for s in items]


# ─── Pydantic модели ───────────────────────────────────────────────────────

class OrderItem(BaseModel):
    product: Optional[str] = None
    tons: Optional[float] = None

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
            (r'гпс.*?плох|плох.*?гпс', "ГПС плохой"),
            (r'гпс.*?хор|хор.*?гпс', "ГПС хороший"),
            (r'\bгпс\b', "ГПС хороший"),
            (r'песок.*?мелк|мелк.*?песок', "Песок мелкозернистый"),
            (r'песок.*?круп|круп.*?песок', "Песок крупнозернистый"),
            (r'\bпесок\b', "Песок мелкозернистый"),
            (r'\bщебень\b', "Щебень 5-20"),
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
    m = re.search(r'(\d+[.,]?\d*)\s*(тонн\w*|тн\b|т\b|куб\w*|м[³3])', t)
    if m:
        val = float(m.group(1).replace(",", "."))
        unit_str = m.group(2)
        if re.match(r'куб|м[³3]', unit_str):
            result.unit = 'куб'
            density = DENSITY.get(result.product, DEFAULT_DENSITY)
            result.tons = round(val * density, 1)
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
                "КУБЫ → ТОННЫ: если клиент указал кубометры/кубов/куб/м³ — переведи в тонны по насыпной плотности: "
                "Отсев 0-5=1.27, Щебень 5-20=1.45, Щебень 20-40=1.42, Щебень 40-70=1.44, "
                "Песок мелкозернистый=1.50, Песок крупнозернистый=1.50, Гравий=1.45, ГПС=1.77. "
                "Если продукт неизвестен — умножай на 1.5. В tons всегда возвращай тонны.\n\n"
                "Верни JSON:\n"
                "- items: [{\"product\": точное название или null, \"tons\": число или null}]\n"
                "- delivery: «Доставка» или «Самовывоз» или null\n"
                "- address: адрес или null\n"
                "Пример: {\"items\": [{\"product\": \"Щебень 5-20\", \"tons\": 7}], \"delivery\": \"Доставка\", \"address\": \"Краснодар\"}"
            )},
        ],
        temperature=0,
        max_tokens=200,
    )
    raw = response.choices[0].message.content.strip()
    raw = re.sub(r"```[a-z]*\n?", "", raw).strip("` \n")
    data = json.loads(raw)
    FRACTION_ARTIFACTS = {520, 2040, 4070, 520.0, 2040.0, 4070.0}
    items = []
    for it in (data.get("items") or []):
        t = float(it["tons"]) if it.get("tons") else None
        if t in FRACTION_ARTIFACTS:
            t = None
        items.append(OrderItem(product=it.get("product"), tons=t))
    first = items[0] if items else OrderItem()
    return OrderParsed(
        items=items if items else None,
        product=first.product,
        tons=first.tons,
        delivery=data.get("delivery"),
        address=data.get("address"),
    )


def parse_order(text: str) -> OrderParsed:
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


# ─── Геокодирование и маршрутизация (идентично tg-bot) ───────────────────

def get_coords(address: str):
    try:
        from geopy.geocoders import Nominatim
        geolocator = Nominatim(user_agent="quarry_delivery_bot_krd", timeout=5)
        parts = [p.strip() for p in address.split(",") if p.strip()]
        city_candidate = parts[0] if parts else address
        city_only = city_candidate.split()[0] if city_candidate.split() else city_candidate

        def in_russia(loc):
            return 40.0 <= loc.latitude <= 80.0 and 25.0 <= loc.longitude <= 180.0

        queries = [
            f"{address}, Краснодарский край, Россия",
            f"{city_candidate}, Краснодарский край, Россия",
            f"{city_only}, Россия",
        ]
        for query in queries:
            try:
                loc = geolocator.geocode(query)
                if loc and in_russia(loc):
                    print(f"[GEOCODE] OK: {query!r} -> ({loc.latitude:.4f}, {loc.longitude:.4f})", flush=True)
                    return (loc.latitude, loc.longitude)
            except Exception as e:
                print(f"[GEOCODE] ошибка: {e}", flush=True)
        print(f"[GEOCODE] не найдено: {address!r}", flush=True)
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
    m = re.search(r"(\d+[.,]?\d*)\s*(тонн\w*|тн\b|т\b|куб\w*|м[³3])", text)
    if m:
        val = float(m.group(1).replace(",", "."))
        unit_str = m.group(2)
        if re.match(r'куб|м[³3]', unit_str):
            density = DENSITY.get(product, DEFAULT_DENSITY)
            return round(val * density, 1)
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
                d["volume_text"] = f"{it['tons']} т"
                found = True
    elif parsed.product and parsed.product in PRODUCTS and not d.get("product"):
        d["product"] = parsed.product
        d["price_per_ton"] = PRODUCTS[parsed.product]
        found = True
    if parsed.tons and parsed.tons > 0 and not d.get("tons"):
        d["tons"] = parsed.tons
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


def advance(chat_id: int) -> int:
    """Определяет следующий шаг диалога. Возвращает состояние или -1 (конец)."""
    d = user_data.get(chat_id, {})

    if not d.get("product"):
        btns = make_buttons(list(PRODUCTS.keys()))
        send_msg(chat_id, "С чем поможем? Выберите продукцию или напишите своё название:", btns)
        return PRODUCT

    if not d.get("tons"):
        send_msg(chat_id, f"Сколько тонн {d['product']} вам нужно?\n\nНапример: 30 тонн")
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
        if tons < 30:
            d.pop("delivery", None)
            btns = [[
                {"type": "callback", "text": "Самовывоз", "payload": "Самовывоз"},
                {"type": "callback", "text": "Доставка", "payload": "Доставка"}
            ]]
            send_msg(chat_id,
                f"Доставка возможна от 30 тонн — минимальная загрузка машины.\n\n"
                f"Вы указали {tons} т. Заберёте самовывозом или скорректируем объём?",
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
        {"type": "callback", "text": "❌ Начать заново", "payload": "confirm_no"},
    ]]
    send_msg(chat_id, f"Проверьте заявку:\n\n{summary}\n\nВсё верно?", btns)
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
        "Для новой заявки напишите /start",
    ]
    send_msg(chat_id, "\n".join(lines))
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
    send_msg(MANAGER_CHAT_ID, "\n".join(mgr), reply_btn)
    if map_url:
        try:
            send_photo_msg(MANAGER_CHAT_ID, map_url, f"Маршрут: {BASE_NAME} -> {address} (~{distance_km} км)")
        except Exception as e:
            print(f"[MAP] Ошибка карты менеджеру: {e}")

    # ── Владельцу (копия без кнопки ответа) ────────────────────────────────
    if OWNER_CHAT_ID and OWNER_CHAT_ID != MANAGER_CHAT_ID:
        owner_msg = "[Копия] " + "\n".join(mgr)
        send_msg(OWNER_CHAT_ID, owner_msg)
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
            save_state()
            send_msg(chat_id, f"⏱ Время ожидания истекло (30 мин). Нажмите кнопку «Ответить клиенту» снова.")
            return
        try:
            send_msg(client_id, f"Ответ менеджера:\n\n{text}")
            send_msg(chat_id, f"✅ Ответ отправлен клиенту.")
            track_event("manager_replied", manager_id=user_id, client_id=client_id)
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
            d["volume_text"] = text
        else:
            try_parse_freeform(text, chat_id)
            if not d.get("tons"):
                send_msg(chat_id, "Пожалуйста, укажите объём числом, например: 30 или 15.5")
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


def handle_callback(user_id: int, chat_id: int, callback_id: str, payload: str):
    global processed_callbacks
    # Игнорируем повторные нажатия одной и той же кнопки
    if callback_id in processed_callbacks:
        answer_cb(callback_id)
        print(f"[CB] Дубль проигнорирован: {callback_id[:20]}", flush=True)
        return
    processed_callbacks.add(callback_id)
    if len(processed_callbacks) > 2000:
        processed_callbacks = set(list(processed_callbacks)[-1000:])

    if payload == "voice_ok":
        print(f"[VOICE_CB] voice_ok: user_id={user_id}, chat_id={chat_id}, pending_keys={list(pending_voice.keys())}", flush=True)
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

    if payload == "voice_retry":
        entry = pending_voice.pop(chat_id, None) or pending_voice.pop(user_id, None)
        orig_chat_id = entry[3] if entry else (chat_id or user_id)
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

    if payload == "confirm_no":
        answer_cb(callback_id)
        user_state.pop(chat_id, None)
        user_data.pop(chat_id, None)
        save_state()
        send_msg(chat_id, "Хорошо, начнём заново. Напишите что вам нужно или /start")
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
        print(f"[ERROR] process_update: {e}\n{traceback.format_exc()[:400]}", flush=True)
    finally:
        if lock:
            lock.release()


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
        handle_callback(user_id, chat_id, callback_id, payload)


# ─── Главный цикл ─────────────────────────────────────────────────────────

def main():
    print("[STARTUP] Жду 45 сек перед запуском...", flush=True)
    time.sleep(45)
    if not TOKEN:
        print("[STARTUP] ОШИБКА: MAX_BOT_TOKEN не задан!", flush=True)
        return
    print(f"[STARTUP] Бот запущен!", flush=True)
    print(f"[STARTUP] MANAGER_CHAT_ID = {MANAGER_CHAT_ID or 'НЕ ЗАДАН — заявки некуда слать!'}", flush=True)
    print(f"[STARTUP] OWNER_CHAT_ID   = {OWNER_CHAT_ID or 'не задан'}", flush=True)
    load_state()

    marker = None
    with ThreadPoolExecutor(max_workers=8) as pool:
        while True:
            try:
                resp = get_updates(marker=marker, timeout=30)
                updates = resp.get("updates", [])
                if "marker" in resp:
                    marker = resp["marker"]

                for upd in updates:
                    pool.submit(process_update_safe, upd)

            except KeyboardInterrupt:
                print("[SHUTDOWN] Остановлен.", flush=True)
                break
            except Exception as e:
                print(f"[ERROR] polling: {e}", flush=True)
                time.sleep(5)


if __name__ == "__main__":
    main()
