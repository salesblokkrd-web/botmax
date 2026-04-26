"""
Инкрементальный импорт истории группы "Архиповский блок" → Google Sheets.

Запуск на Amvera (где есть MAX_BOT_TOKEN):
    python import_blok_history.py [--dry-run]

Принцип работы:
- При первом запуске читает с 2026-04-06
- Запоминает последний обработанный timestamp в blok_checkpoint.json
- При каждом следующем запуске читает только новые сообщения
- Можно запускать вручную или как cron-задачу

Флаги:
    --dry-run   Показать что нашли, НЕ записывать в Sheets
"""

import os, sys, json, re, base64, datetime, time
import urllib.request, urllib.parse, ssl

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DRY_RUN = "--dry-run" in sys.argv

# ─── Конфиг ────────────────────────────────────────────────────────────────
TOKEN = os.environ.get("MAX_BOT_TOKEN", "")
# Telegram для уведомлений хозяину о том что разнесено
TG_TOKEN = os.environ.get("SECRETARY_BOT_TOKEN", "")
OWNER_TG_ID = int(os.environ.get("SECRETARY_ADMIN_ID", "215294536"))
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY", "")
GOOGLE_SA_B64 = os.environ.get("GOOGLE_SA_B64", "")
SHEETS_ID = "1FwpvHhDHiNuFOdXlTcrVuTWKUqh2NmWVn810ylM0MkQ"
GROUP_ID = -72678007708240

DATA_DIR = "/app/data" if os.path.exists("/app") else "data"
os.makedirs(DATA_DIR, exist_ok=True)
CHECKPOINT_FILE = os.path.join(DATA_DIR, "blok_checkpoint.json")

# Стартовая дата если чекпоинт не найден
FIRST_RUN_DATE = datetime.datetime(2026, 4, 6, 0, 0, 0)

MAX_API = "https://botapi.max.ru"
ANTHROPIC_API = "https://api.anthropic.com/v1/messages"

ssl_ctx = ssl.create_default_context()


# ─── Чекпоинт ──────────────────────────────────────────────────────────────

def load_checkpoint() -> int:
    """Возвращает timestamp (мс) с которого читать. 0 = с самого начала (FIRST_RUN_DATE)."""
    try:
        with open(CHECKPOINT_FILE) as f:
            data = json.load(f)
            ts = data.get("last_ts_ms", 0)
            dt = datetime.datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")
            print(f"[Чекпоинт] Продолжаем с {dt} (ts={ts})")
            return ts
    except FileNotFoundError:
        ts = int(FIRST_RUN_DATE.timestamp() * 1000)
        print(f"[Чекпоинт] Первый запуск — читаем с {FIRST_RUN_DATE.date()}")
        return ts


def save_checkpoint(last_ts_ms: int):
    """Сохраняем позицию после успешной обработки."""
    dt = datetime.datetime.fromtimestamp(last_ts_ms / 1000).strftime("%Y-%m-%d %H:%M")
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_ts_ms": last_ts_ms, "last_dt": dt}, f)
    print(f"[Чекпоинт] Сохранён: {dt}")


# ─── MAX API ────────────────────────────────────────────────────────────────

def max_get(path: str, params: dict = None) -> dict:
    url = f"{MAX_API}{path}?access_token={TOKEN}"
    if params:
        url += "&" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as r:
        return json.loads(r.read())


def get_new_messages(from_ts_ms: int) -> list:
    """Читает все сообщения из группы начиная с from_ts_ms."""
    messages = []
    cursor = from_ts_ms + 1  # +1 чтобы не читать последнее уже обработанное

    while True:
        try:
            resp = max_get("/messages", params={
                "chat_id": GROUP_ID,
                "count": 100,
                "from": cursor,
            })
        except Exception as e:
            print(f"[MAX API ERROR] {e}")
            break

        batch = resp.get("messages", [])
        if not batch:
            break

        messages.extend(batch)
        print(f"  Получено {len(batch)} сообщений (всего {len(messages)})")

        if len(batch) < 100:
            break  # Конец страниц

        cursor = batch[-1].get("timestamp", 0) + 1
        time.sleep(0.3)

    return messages


# ─── Парсинг ─────────────────────────────────────────────────────────────

PLAN_KEYWORDS = ["план", "блок", "поддон", "рейс", "везёт", "везет",
                 "отгрузка", "доставка", "машина", "маз", "камаз", "газ",
                 "полублок", "девятка", "двадцатка", "двенашка",
                 "20-3", "20-4", "12-2", "9-2", "90-2"]

# ─── Каталог продукции ──────────────────────────────────────────────────────
# Используется в промпте и при сопоставлении строк таблицы
BLOCK_CATALOG = """
КАТАЛОГ ПРОДУКЦИИ (бетонные блоки):

Блок 20 (двадцатка, размер 188×190×390 мм):
  - 3-пустотный: называют "20-3,0", "20/3", "20 3.0", "двадцатка трёшка"
  - 4-пустотный: называют "20-4,0", "20/4", "20 4.0", "двадцатка четвёрка"
  Наполнитель: "отсев" или "керамзит"
  Примеры полных названий: "Блок 20 отсев 3,0", "Блок 20 керамзит 4,0"

Блок 12 (двенашка, 120, размер 120×190×390 мм):
  - 2-пустотный: называют "12-2,0", "120-2,0", "12/2", "двенашка"
  Наполнитель: "отсев" или "керамзит"
  Примеры: "Блок 12 отсев 2,0", "120 керамзит"

Блок 9 (девятка, полублок, 90, размер 90×190×390 мм):
  - 2-пустотный: называют "9-2,0", "90-2,0", "9/2", "девятка", "полублок"
  Наполнитель: "отсев" или "керамзит"
  Примеры: "Блок 9 отсев 2,0", "полублок керамзит"
"""


def is_plan_message(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in PLAN_KEYWORDS)


def claude_parse(text: str, msg_date: str) -> list:
    """Парсим одно сообщение менеджера через Claude Sonnet."""
    prompt = f"""Ты парсер производственных заданий завода бетонных блоков.
Дата сообщения: {msg_date}

{BLOCK_CATALOG}

Из текста извлеки список рейсов (отгрузок). Каждый рейс — JSON объект:
- date: дата рейса YYYY-MM-DD (из текста или дата сообщения)
- truck: номер/название машины или null
- block_type: НОРМАЛИЗОВАННОЕ название блока по каталогу выше.
  Формат: "Блок [размер] [наполнитель] [пустотность]"
  Примеры: "Блок 20 отсев 3,0", "Блок 9 керамзит 2,0", "Блок 12 отсев 2,0"
  Если наполнитель неизвестен — без него: "Блок 20 3,0"
  Если пустотность неизвестна — без неё: "Блок 20 отсев"
- pallets: количество поддонов (число) или null
- pallet_type: "большой" или "узкий" если указан, иначе null
- client: название клиента или организации или null
- address: адрес доставки или null
- time: время доставки строкой ("10:00") или null
- warehouse: "КРД" (Краснодар/КРД склад) или "Карьер" (карьерный склад) или null

Верни ТОЛЬКО JSON-массив. Если рейсов нет — [].

Текст сообщения:
{text}"""

    body = json.dumps({
        "model": "claude-sonnet-4-6",
        "max_tokens": 2048,
        "messages": [{"role": "user", "content": prompt}]
    }).encode()

    req = urllib.request.Request(ANTHROPIC_API, data=body, headers={
        "x-api-key": CLAUDE_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=30, context=ssl_ctx) as r:
            resp = json.loads(r.read())
        raw = resp["content"][0]["text"].strip()
        raw = re.sub(r'^```[a-z]*\n?', '', raw)
        raw = re.sub(r'\n?```$', '', raw)
        result = json.loads(raw)
        return result if isinstance(result, list) else []
    except Exception as e:
        print(f"  [CLAUDE ERROR] {e}")
        return []


# ─── Google Sheets ──────────────────────────────────────────────────────────

def get_gspread():
    import gspread
    from google.oauth2.service_account import Credentials
    sa_info = json.loads(base64.b64decode(GOOGLE_SA_B64))
    creds = Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds)


def write_trips(trips: list, gc):
    """Дописывает рейсы в лист 'Рейсы'."""
    sh = gc.open_by_key(SHEETS_ID)
    ws = sh.worksheet("Рейсы")
    for trip in trips:
        row = [
            trip.get("date") or "",
            trip.get("truck") or "",
            trip.get("block_type") or "",
            trip.get("pallets") or "",
            trip.get("pallet_type") or "",
            trip.get("client") or "",
            trip.get("address") or "",
            trip.get("time") or "",
            trip.get("warehouse") or "",
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")
        print(f"  ✓ Рейс: {row[0]} | {row[2]} {row[3]}пд | {row[5]} | склад={row[8]}")


def update_stock(trips: list, gc):
    """
    Уменьшает остатки на складе.
    Ищет строку с нужным типом блока и записывает отгрузку.
    """
    sh = gc.open_by_key(SHEETS_ID)

    # Суммируем поддоны по (склад, тип блока)
    totals = {}
    for trip in trips:
        wh = (trip.get("warehouse") or "").strip()
        bt = trip.get("block_type")
        pallets = trip.get("pallets")
        if not wh or not bt or not pallets:
            if not wh:
                print(f"  [СКЛАД] Склад не указан: {trip.get('block_type')} {trip.get('pallets')}пд — пропуск")
            continue
        key = (wh.upper(), bt)
        totals[key] = totals.get(key, 0) + int(pallets)

    for (wh, block_type), total_pallets in totals.items():
        sheet_name = "КРД(склад)" if wh == "КРД" else "Карьер(склад)"
        print(f"\n  [СКЛАД] {sheet_name} | {block_type} | -{total_pallets} поддонов")
        try:
            ws = sh.worksheet(sheet_name)
            data = ws.get_all_values()
            found = False
            for i, row in enumerate(data):
                if _row_matches_block(row, block_type):
                    found = True
                    print(f"    Строка {i+1}: {row[:6]}")
                    # Ищем столбец "Отгрузка" в заголовке
                    header = data[0] if data else []
                    col_idx = _find_col(header, ["отгрузка", "отгр"])
                    if col_idx is not None:
                        cell = ws.cell(i + 1, col_idx + 1)
                        old_val = int(cell.value or 0)
                        new_val = old_val + total_pallets
                        if not DRY_RUN:
                            ws.update_cell(i + 1, col_idx + 1, new_val)
                        print(f"    Отгрузка: {old_val} → {new_val} (столбец {col_idx+1})")
                    else:
                        print(f"    [!] Столбец 'Отгрузка' не найден в заголовке: {header[:8]}")
                    break
            if not found:
                print(f"    [!] Тип блока '{block_type}' не найден в {sheet_name}")
        except Exception as e:
            print(f"    [ОШИБКА] {e}")


def _normalize_block_type(bt: str) -> dict:
    """
    Разбирает название блока на компоненты: размер, наполнитель, пустотность.
    Возвращает dict с ключами 'size', 'filler', 'voids'.
    """
    bt = bt.lower().strip()
    result = {"size": None, "filler": None, "voids": None}

    # Размер
    if "20" in bt or "двадцатк" in bt:
        result["size"] = "20"
    elif "12" in bt or "120" in bt or "двенашк" in bt:
        result["size"] = "12"
    elif "9" in bt or "90" in bt or "девятк" in bt or "полублок" in bt:
        result["size"] = "9"

    # Наполнитель
    if "отсев" in bt:
        result["filler"] = "отсев"
    elif "керамзит" in bt:
        result["filler"] = "керамзит"

    # Пустотность
    if "3,0" in bt or "3.0" in bt or "/3" in bt or "-3" in bt:
        result["voids"] = "3,0"
    elif "4,0" in bt or "4.0" in bt or "/4" in bt or "-4" in bt:
        result["voids"] = "4,0"
    elif "2,0" in bt or "2.0" in bt or "/2" in bt or "-2" in bt:
        result["voids"] = "2,0"

    return result


def _row_matches_block(row: list, block_type: str) -> bool:
    """Проверяет, соответствует ли строка таблицы типу блока."""
    row_text = " ".join(row).lower()
    bt_norm = _normalize_block_type(block_type)

    # Нужен хотя бы размер
    if not bt_norm["size"]:
        return False

    size = bt_norm["size"]
    filler = bt_norm["filler"]
    voids = bt_norm["voids"]

    # Проверяем размер (с учётом "12" vs "120" vs "9" vs "90")
    size_map = {"20": ["20", "двадцатк"], "12": ["12", "120", "двенашк"], "9": ["9", "90", "девятк", "полублок"]}
    size_aliases = size_map.get(size, [size])
    if not any(a in row_text for a in size_aliases):
        return False

    # Проверяем наполнитель если есть
    if filler and filler not in row_text:
        return False

    # Проверяем пустотность если есть
    if voids and voids not in row_text and voids.replace(",", ".") not in row_text:
        return False

    return True


def _find_col(header: list, keywords: list) -> int | None:
    for i, h in enumerate(header):
        if any(kw in h.lower() for kw in keywords):
            return i
    return None


# ─── Telegram уведомление ──────────────────────────────────────────────────

def tg_send(text: str):
    """Отправляем уведомление хозяину в Telegram."""
    if not TG_TOKEN or not OWNER_TG_ID:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    body = json.dumps({
        "chat_id": OWNER_TG_ID,
        "text": text,
        "parse_mode": "HTML",
    }).encode()
    try:
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            pass
    except Exception as e:
        print(f"[TG ERROR] {e}")


def build_notify_text(trips: list, dry_run: bool) -> str:
    mode = "🔍 PREVIEW (не записано)" if dry_run else "✅ Разнесено в таблицу"
    lines = [f"<b>Архиповский блок — {mode}</b>", ""]
    for t in trips:
        wh = t.get("warehouse") or "?"
        icon = "🏭" if wh == "КРД" else "⛏" if wh == "Карьер" else "❓"
        lines.append(
            f"{icon} {t.get('date')} | {t.get('block_type')} {t.get('pallets')}пд"
            f" → {t.get('client') or '?'} | склад: {wh}"
        )
    if not trips:
        lines.append("Рейсов не найдено")
    return "\n".join(lines)


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    for name, val in [("MAX_BOT_TOKEN", TOKEN), ("CLAUDE_API_KEY", CLAUDE_API_KEY), ("GOOGLE_SA_B64", GOOGLE_SA_B64)]:
        if not val:
            print(f"ОШИБКА: {name} не задан!")
            sys.exit(1)

    if DRY_RUN:
        print("=== РЕЖИМ PREVIEW (dry-run) — в Sheets НЕ пишем ===\n")

    # 1. Загружаем чекпоинт
    from_ts = load_checkpoint()

    # 2. Читаем новые сообщения из MAX группы
    print(f"\nЧитаем сообщения из группы...")
    messages = get_new_messages(from_ts)
    print(f"Новых сообщений: {len(messages)}")

    if not messages:
        print("Нет новых сообщений. Выход.")
        return

    # 3. Фильтруем планы менеджера
    plan_msgs = []
    for msg in messages:
        text = (msg.get("body", {}).get("text") or "").strip()
        if not text or not is_plan_message(text):
            continue
        ts = msg.get("timestamp", 0)
        dt = datetime.datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")
        sender = msg.get("sender", {}).get("name", "?")
        plan_msgs.append({"dt": dt, "date": dt[:10], "sender": sender, "text": text, "ts": ts})

    print(f"Сообщений-планов: {len(plan_msgs)}\n")

    # 4. Парсим каждое сообщение
    all_trips = []
    for pm in plan_msgs:
        print(f"[{pm['dt']}] {pm['sender']}: {pm['text'][:70]}")
        trips = claude_parse(pm["text"], pm["date"])
        for t in trips:
            if not t.get("date"):
                t["date"] = pm["date"]
            print(f"  → {t}")
        all_trips.extend(trips)

    print(f"\n=== Итого рейсов: {len(all_trips)} ===")

    if not all_trips:
        # Чекпоинт всё равно обновляем чтобы не перечитывать
        last_ts = max(m.get("timestamp", 0) for m in messages)
        if not DRY_RUN:
            save_checkpoint(last_ts)
        print("Рейсов для записи нет.")
        return

    # 5. Записываем в Sheets
    if not DRY_RUN:
        gc = get_gspread()
        print("\n--- Запись рейсов в 'Рейсы' ---")
        write_trips(all_trips, gc)
        print("\n--- Обновление остатков ---")
        update_stock(all_trips, gc)
        # Сохраняем чекпоинт
        last_ts = max(m.get("timestamp", 0) for m in messages)
        save_checkpoint(last_ts)
    else:
        print("\n[dry-run] Рейсы НЕ записаны в Sheets")

    # Уведомляем хозяина в Telegram
    notify = build_notify_text(all_trips, DRY_RUN)
    tg_send(notify)
    print("\nУведомление отправлено в Telegram")
    print("\n✅ Готово!")


if __name__ == "__main__":
    main()
