"""
Однократный импорт истории группы "Архиповский блок" в Google Sheets.

Запуск на Amvera (где есть MAX_BOT_TOKEN):
    python import_blok_history.py

Читает сообщения с 2026-04-06, парсит через Claude Sonnet,
записывает рейсы в лист 'Рейсы' и корректирует остатки склада.
"""

import os, sys, json, re, base64, datetime, time
import urllib.request, urllib.parse, ssl

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ─── Конфиг ────────────────────────────────────────────────────────────────
TOKEN = os.environ.get("MAX_BOT_TOKEN", "")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY", "")
GOOGLE_SA_B64 = os.environ.get("GOOGLE_SA_B64", "")
SHEETS_ID = "1FwpvHhDHiNuFOdXlTcrVuTWKUqh2NmWVn810ylM0MkQ"
GROUP_ID = -72678007708240

# Дата с которой читаем (последняя запись была ~6 апреля)
FROM_DATE = datetime.date(2026, 4, 6)

MAX_API = "https://botapi.max.ru"
ANTHROPIC_API = "https://api.anthropic.com/v1/messages"

ssl_ctx = ssl.create_default_context()


def max_request(method: str, path: str, params: dict = None, body: dict = None) -> dict:
    url = f"{MAX_API}{path}?access_token={TOKEN}"
    if params:
        url += "&" + urllib.parse.urlencode(params)
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method,
        headers={"Content-Type": "application/json"} if data else {})
    with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as r:
        return json.loads(r.read())


def get_group_messages(from_ts_ms: int) -> list:
    """Читаем историю группы с нужной даты."""
    messages = []
    from_param = from_ts_ms

    while True:
        resp = max_request("GET", "/messages", params={
            "chat_id": GROUP_ID,
            "count": 100,
            "from": from_param,
        })
        batch = resp.get("messages", [])
        if not batch:
            break
        messages.extend(batch)
        # MAX API: если меньше 100 — конец
        if len(batch) < 100:
            break
        # Следующая страница — берём timestamp последнего + 1
        from_param = batch[-1].get("timestamp", 0) + 1
        time.sleep(0.3)

    print(f"Получено {len(messages)} сообщений из группы")
    return messages


def claude_parse(text: str) -> list:
    """Парсим план менеджера через Claude Sonnet."""
    today = datetime.date.today().isoformat()
    prompt = f"""Ты парсер производственных заданий для завода блоков.
Из текста извлеки список рейсов (отгрузок).

Каждый рейс — JSON объект:
- date: дата YYYY-MM-DD (если не указана — {today})
- truck: номер/название машины или null
- block_type: тип блока, например "Блок 20 отсев", "Блок 9 керамзит", "Блок 20 керамзит" или null
- pallets: количество поддонов (число) или null
- pallet_type: тип поддона "большой" или "узкий" (если упомянут) или null
- client: название клиента/организации или null
- address: адрес доставки или null
- time: время доставки строкой ("10:00") или null
- warehouse: склад откуда везут: "КРД" (Краснодар) или "Карьер" или null (если неизвестно)

Верни ТОЛЬКО JSON-массив, без пояснений. Если рейсов нет — пустой массив [].

Текст:
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


def get_gspread():
    """Возвращает авторизованный gspread клиент."""
    import gspread
    from google.oauth2.service_account import Credentials
    sa_info = json.loads(base64.b64decode(GOOGLE_SA_B64))
    creds = Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds)


def write_trips_to_sheets(trips: list, gc):
    """Записывает рейсы в лист 'Рейсы'."""
    sh = gc.open_by_key(SHEETS_ID)
    ws = sh.worksheet("Рейсы")
    for trip in trips:
        row = [
            trip.get("date") or str(datetime.date.today()),
            trip.get("truck") or "",
            trip.get("block_type") or "",
            str(trip.get("pallets") or ""),
            trip.get("pallet_type") or "",
            trip.get("client") or "",
            trip.get("address") or "",
            trip.get("time") or "",
            trip.get("warehouse") or "",
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")
        print(f"  → Рейс: {row[0]} | {row[2]} {row[3]}пд | {row[5]} | склад={row[8]}")


def update_stock(trips: list, gc):
    """
    Уменьшает остатки на складе.

    КРД(склад) gid=1319796632 — склад Краснодар
    Карьер(склад) gid=1105951724 — склад карьера
    """
    sh = gc.open_by_key(SHEETS_ID)

    # Группируем по складу
    by_warehouse = {}
    for trip in trips:
        wh = trip.get("warehouse") or ""
        if not wh:
            print(f"  [STOCK] Склад не определён для {trip} — пропускаем")
            continue
        bt = trip.get("block_type")
        pallets = trip.get("pallets")
        if not bt or not pallets:
            continue
        key = (wh, bt)
        by_warehouse[key] = by_warehouse.get(key, 0) + int(pallets)

    if not by_warehouse:
        print("  [STOCK] Нет данных для обновления остатков")
        return

    for (wh, block_type), total_pallets in by_warehouse.items():
        sheet_name = "КРД(склад)" if wh.upper() == "КРД" else "Карьер(склад)"
        try:
            ws = sh.worksheet(sheet_name)
            data = ws.get_all_values()
            # Ищем строку с нужным типом блока
            for i, row in enumerate(data):
                row_text = " ".join(row).lower()
                bt_lower = block_type.lower()
                if bt_lower in row_text or _block_match(block_type, row_text):
                    # Смотрим столбец "Отгрузка" (обычно 3-4 столбец)
                    print(f"  [STOCK] {sheet_name}: нашли строку {i+1} для '{block_type}'")
                    print(f"    Строка: {row[:8]}")
                    print(f"    → Нужно вычесть {total_pallets} поддонов")
                    # TODO: автоматически найти нужную ячейку и обновить
                    # Пока просто логируем — ручная проверка перед автозаписью
                    break
            else:
                print(f"  [STOCK] '{block_type}' НЕ найден в {sheet_name}")
        except Exception as e:
            print(f"  [STOCK ERROR] {e}")


def _block_match(block_type: str, row_text: str) -> bool:
    """Нечёткое совпадение типа блока."""
    mappings = {
        "блок 20 отсев": ["20", "отсев"],
        "блок 20 керамзит": ["20", "керамзит"],
        "блок 9 отсев": ["9", "отсев"],
        "блок 9 керамзит": ["9", "керамзит"],
        "блок 7": ["7"],
        "блок 4": ["4"],
    }
    bt = block_type.lower()
    for key, parts in mappings.items():
        if key in bt or bt in key:
            return all(p in row_text for p in parts)
    return False


def is_manager_message(text: str) -> bool:
    """Проверяем что это план менеджера, а не чат."""
    keywords = ["план", "блок", "поддон", "рейс", "везёт", "везет",
                "отгрузка", "доставка", "машина", "мaz", "маз", "камаз"]
    t = text.lower()
    return any(kw in t for kw in keywords)


def main():
    if not TOKEN:
        print("ОШИБКА: MAX_BOT_TOKEN не задан!")
        sys.exit(1)
    if not CLAUDE_API_KEY:
        print("ОШИБКА: CLAUDE_API_KEY не задан!")
        sys.exit(1)
    if not GOOGLE_SA_B64:
        print("ОШИБКА: GOOGLE_SA_B64 не задан!")
        sys.exit(1)

    from_ts = int(datetime.datetime(2026, 4, 6, 0, 0, 0).timestamp() * 1000)
    print(f"\n=== Импорт истории группы Архиповский блок ===")
    print(f"С даты: {FROM_DATE}\n")

    # 1. Получаем сообщения
    messages = get_group_messages(from_ts)

    # 2. Фильтруем — только сообщения менеджера с планами
    plan_messages = []
    for msg in messages:
        text = msg.get("body", {}).get("text", "").strip()
        if not text:
            continue
        sender = msg.get("sender", {}).get("name", "?")
        ts = msg.get("timestamp", 0)
        dt = datetime.datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")
        if is_manager_message(text):
            plan_messages.append({"dt": dt, "sender": sender, "text": text})
            print(f"[{dt}] {sender}: {text[:80]}")

    print(f"\nНайдено {len(plan_messages)} сообщений-планов\n")

    if not plan_messages:
        print("Нет сообщений для обработки.")
        return

    # 3. Парсим каждое сообщение
    all_trips = []
    for pm in plan_messages:
        print(f"\n--- Парсим: [{pm['dt']}] {pm['sender']} ---")
        print(f"  Текст: {pm['text'][:100]}")
        trips = claude_parse(pm["text"])
        print(f"  Рейсов найдено: {len(trips)}")
        for t in trips:
            print(f"    {t}")
        all_trips.extend(trips)

    print(f"\n=== Итого рейсов: {len(all_trips)} ===")

    if not all_trips:
        print("Нет рейсов для записи.")
        return

    # 4. Показываем что будем записывать — PREVIEW
    print("\n=== PREVIEW (что запишем в Sheets) ===")
    for t in all_trips:
        print(f"  {t.get('date')} | {t.get('truck')} | {t.get('block_type')} {t.get('pallets')}пд | {t.get('client')} | склад={t.get('warehouse')}")

    # Пауза для проверки (можно убрать в production)
    confirm = input("\nЗаписать в Google Sheets? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("Отменено.")
        return

    # 5. Пишем в Sheets
    gc = get_gspread()
    print("\n=== Запись рейсов ===")
    write_trips_to_sheets(all_trips, gc)

    print("\n=== Обновление остатков ===")
    update_stock(all_trips, gc)

    print("\n✅ Готово!")


if __name__ == "__main__":
    main()
