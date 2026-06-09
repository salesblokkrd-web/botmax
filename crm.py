# -*- coding: utf-8 -*-
"""
CRM-воронка заявок Архиповского карьера (инертные материалы).

Отдельная Google-таблица «CRM Заявки». Каждая заявка из бота → строка.
Илья двигает заявку по воронке кнопками в MAX (конечный автомат статусов).

Не ломает основной поток заявок: все функции безопасны (ошибка не пробрасывается
наружу — заявка всё равно уйдёт менеджеру). Переиспользует service-account из бота.

ENV:
  GOOGLE_SA_B64     — base64(service_account.json) (тот же, что у учётчика)
  CRM_SHEETS_ID     — (опц.) ID готовой таблицы. Если пусто — бот создаёт сам.
  CRM_SHARE_EMAILS  — (опц.) кому выдать доступ при автосоздании, через запятую.
"""

import os
import json
import base64
import datetime
import threading

# ─── Конфиг ────────────────────────────────────────────────────────────────
GOOGLE_SA_B64 = os.environ.get("GOOGLE_SA_B64", "")
# Отдельная таблица «CRM Заявки Карьер» (создана владельцем, доступ выдан боту).
# ID не секретен — это часть ссылки доступа. Можно переопределить через env CRM_SHEETS_ID.
CRM_SHEETS_ID_ENV = os.environ.get(
    "CRM_SHEETS_ID", "1Q1EHTytc6uej9EzLV4H0vRNfZUi28l9ZCabY34eDEbU"
).strip()
CRM_SHARE_EMAILS = [e.strip() for e in os.environ.get("CRM_SHARE_EMAILS", "").split(",") if e.strip()]

_DATA_DIR = "/data" if os.path.exists("/data") else "."
_CRM_ID_FILE = os.path.join(_DATA_DIR, "crm_sheet_id.txt")

WS_ORDERS = "Заявки"
WS_DASH = "Дашборд"

# ─── Воронка (конечный автомат) ─────────────────────────────────────────────
# key -> человекочитаемый ярлык статуса (пишется в колонку «Статус»)
STATUS_LABELS = {
    "new":        "🆕 Новая",
    "work":       "⚙️ В работе",
    "invoice":    "📄 Счёт выставлен",
    "prepaid":    "💰 Предоплата получена",
    "shipping":   "🚚 Отгрузка",
    "partial":    "📦 Частичная отгрузка",
    "won":        "✅ Закрыта-успех",
    "think":      "⏸ Думает / отложена",
    "lost_price": "❌ Отказ-цена",
    "lost_other": "🚫 Отказ-не клиент",
}
WON_KEY = "won"
WON_LABEL = STATUS_LABELS[WON_KEY]
TERMINAL_KEYS = {"won", "lost_other"}

# Текст кнопки (действие-глагол) для перехода в данный статус
BUTTON_TEXT = {
    "work":       "⚙️ Взял в работу",
    "invoice":    "📄 Счёт выставлен",
    "prepaid":    "💰 Предоплата",
    "shipping":   "🚚 Начать отгрузку",
    "partial":    "📦 Ещё рейс / частично",
    "won":        "✅ Закрыл (привезли)",
    "think":      "⏸ Думает",
    "lost_price": "❌ Отказ-цена",
    "lost_other": "🚫 Не клиент",
}

# Допустимые переходы из текущего статуса (что показываем Илье кнопками)
NEXT = {
    "new":        ["work", "lost_other"],
    "work":       ["invoice", "think", "lost_price", "lost_other"],
    "invoice":    ["prepaid", "think", "lost_price"],
    "prepaid":    ["shipping"],
    "shipping":   ["partial", "won"],
    "partial":    ["partial", "won"],
    "won":        [],
    "think":      ["work", "lost_price", "lost_other"],
    "lost_price": ["work"],
    "lost_other": [],
}

# ─── Колонки таблицы «Заявки» (порядок = порядок в строке) ───────────────────
HEADERS = [
    "№ заявки", "Дата/время", "Источник", "Имя", "Компания", "Телефон",
    "Товар", "Объём (т)", "Получение", "Адрес", "Предв. сумма, ₽",
    "Статус", "Точная цена материала, ₽", "Стоимость доставки, ₽", "Итог, ₽",
    "№ счёта", "Сумма счёта, ₽", "Предоплата, ₽", "Остаток оплаты, ₽",
    "Отгружено", "Осталось отгрузить", "Реакция менеджера, мин", "Тип клиента",
    "Дата след. касания", "Причина отказа", "Дата закрытия", "Комментарий",
    "История статусов",
]
# 1-based индексы часто используемых колонок
COL_ID = 1
COL_PHONE = 6
COL_STATUS = 12
COL_REACTION = 22
COL_CLIENT_TYPE = 23
COL_CLOSED = 26
COL_HISTORY = 28

_lock = threading.Lock()
_ws_cache = {}  # {"orders": worksheet, "sh": spreadsheet}


# ─── Утилиты времени ─────────────────────────────────────────────────────────
def _now_msk() -> datetime.datetime:
    return datetime.datetime.utcnow() + datetime.timedelta(hours=3)


def _now_str() -> str:
    return _now_msk().strftime("%d.%m.%Y %H:%M")


def _today_str() -> str:
    return _now_msk().strftime("%d.%m.%Y")


def _make_order_id(chat_id) -> str:
    """ID заявки = МСК-время YYMMDDHHMMSS + хвост chat_id. Без '_' (важно для payload)."""
    tail = str(abs(int(chat_id)))[-3:] if chat_id else "000"
    return f"{_now_msk().strftime('%y%m%d%H%M%S')}-{tail}"


def _reaction_mins(order_id: str):
    """Минуты от создания заявки (зашито в order_id) до сейчас."""
    try:
        created = datetime.datetime.strptime(order_id[:12], "%y%m%d%H%M%S")
        return round((_now_msk() - created).total_seconds() / 60, 1)
    except Exception:
        return ""


# ─── Доступ к таблице ────────────────────────────────────────────────────────
def is_available() -> bool:
    return bool(GOOGLE_SA_B64)


def _auth():
    import gspread
    from google.oauth2.service_account import Credentials
    sa_info = json.loads(base64.b64decode(GOOGLE_SA_B64))
    creds = Credentials.from_service_account_info(
        sa_info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def _read_saved_id() -> str:
    if CRM_SHEETS_ID_ENV:
        return CRM_SHEETS_ID_ENV
    try:
        with open(_CRM_ID_FILE, encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


def _save_id(sheet_id: str):
    try:
        with open(_CRM_ID_FILE, "w", encoding="utf-8") as f:
            f.write(sheet_id)
    except Exception as e:
        print(f"[CRM] Не удалось сохранить id таблицы: {e}", flush=True)


def _ensure_orders_ws(sh):
    """Гарантирует лист «Заявки» с шапкой."""
    try:
        ws = sh.worksheet(WS_ORDERS)
    except Exception:
        ws = sh.add_worksheet(title=WS_ORDERS, rows=1000, cols=len(HEADERS))
    first = ws.row_values(1)
    if first[:1] != [HEADERS[0]]:
        ws.update("A1", [HEADERS])
        try:
            ws.freeze(rows=1)
            ws.format("A1:AB1", {"textFormat": {"bold": True}})
        except Exception:
            pass
    return ws


def _build_dashboard(sh):
    """Лист «Дашборд» — KPI на формулах (без участия кода)."""
    try:
        try:
            ws = sh.worksheet(WS_DASH)
        except Exception:
            ws = sh.add_worksheet(title=WS_DASH, rows=40, cols=4)
        q = "'%s'" % WS_ORDERS
        # Разделитель аргументов «;» — таблица в русской локали (ru_RU).
        rows = [
            ["Показатель", "Значение"],
            ["Всего заявок", "=COUNTA(%s!A2:A)" % q],
            ["", ""],
            ["— По стадиям —", ""],
        ]
        for key in ["new", "work", "invoice", "prepaid", "shipping", "partial",
                    "won", "think", "lost_price", "lost_other"]:
            label = STATUS_LABELS[key]
            rows.append([label, '=COUNTIF(%s!L2:L;"%s")' % (q, label)])
        rows += [
            ["", ""],
            ["Выручка закрытых, ₽", '=SUMIF(%s!L2:L;"%s";%s!O2:O)' % (q, WON_LABEL, q)],
            ["Конверсия в успех",
             '=IFERROR(COUNTIF(%s!L2:L;"%s")/COUNTA(%s!A2:A);0)' % (q, WON_LABEL, q)],
            ["Средняя реакция менеджера, мин", "=IFERROR(AVERAGE(%s!V2:V);0)" % q],
            ["", ""],
            ["Постоянных клиентов", '=COUNTIF(%s!W2:W;"Постоянный")' % q],
        ]
        ws.update("A1", rows, value_input_option="USER_ENTERED")
        try:
            ws.format("A1:B1", {"textFormat": {"bold": True}})
        except Exception:
            pass
    except Exception as e:
        print(f"[CRM] Дашборд не создан (не критично): {e}", flush=True)


def _create_spreadsheet(gc):
    sh = gc.create("CRM Заявки — Архиповский карьер")
    for email in CRM_SHARE_EMAILS:
        try:
            sh.share(email, perm_type="user", role="writer", notify=False)
            print(f"[CRM] Доступ выдан: {email}", flush=True)
        except Exception as e:
            print(f"[CRM] Не удалось выдать доступ {email}: {e}", flush=True)
    _ensure_orders_ws(sh)
    _build_dashboard(sh)
    _save_id(sh.id)
    print(f"[CRM] Создана таблица: https://docs.google.com/spreadsheets/d/{sh.id}", flush=True)
    return sh


def _get_ws():
    """Возвращает worksheet «Заявки», создавая таблицу при первом обращении. Кэшируется."""
    if _ws_cache.get("orders") is not None:
        return _ws_cache["orders"]
    gc = _auth()
    sheet_id = _read_saved_id()
    if sheet_id:
        sh = gc.open_by_key(sheet_id)
        ws = _ensure_orders_ws(sh)
        _build_dashboard(sh)  # идемпотентно: создаст/обновит лист «Дашборд»
    else:
        sh = _create_spreadsheet(gc)
        ws = sh.worksheet(WS_ORDERS)
    _ws_cache["sh"] = sh
    _ws_cache["orders"] = ws
    return ws


def sheet_url() -> str:
    sid = _read_saved_id()
    return f"https://docs.google.com/spreadsheets/d/{sid}" if sid else ""


def bootstrap() -> str:
    """Гарантирует, что таблица/листы существуют (создаёт при первом деплое).
    Возвращает URL таблицы или "" при сбое. Безопасно для вызова при старте."""
    if not is_available():
        return ""
    with _lock:
        try:
            _get_ws()
            return sheet_url()
        except Exception as e:
            print(f"[CRM] bootstrap не удался: {e}", flush=True)
            _ws_cache.clear()
            return ""


# ─── Публичные операции ──────────────────────────────────────────────────────
def _client_type(ws, phone: str) -> str:
    """Сегмент клиента по числу прошлых успешных закрытий на этот телефон."""
    if not phone:
        return "Новый"
    try:
        values = ws.get_all_values()
        wins = 0
        for r in values[1:]:
            if len(r) >= COL_STATUS and r[COL_PHONE - 1].strip() == phone.strip() \
                    and r[COL_STATUS - 1].strip() == WON_LABEL:
                wins += 1
        if wins >= 4:
            return "Постоянный"
        if wins >= 1:
            return "Повторный"
        return "Новый"
    except Exception:
        return "Новый"


def append_order(order: dict) -> str:
    """
    Добавляет заявку в CRM. Возвращает order_id (или "" при сбое).
    order: name, company, phone, product, tons, delivery, address,
           material_cost, delivery_cost, chat_id
    """
    if not is_available():
        return ""
    with _lock:
        try:
            ws = _get_ws()
            chat_id = order.get("chat_id")
            order_id = _make_order_id(chat_id)
            client_type = _client_type(ws, order.get("phone", ""))

            material = order.get("material_cost")
            delivery_cost = order.get("delivery_cost")
            prelim = ""
            if material is not None:
                prelim = material + (delivery_cost or 0)

            row = [""] * len(HEADERS)
            row[0] = order_id
            row[1] = _now_str()
            row[2] = "MAX-бот"
            row[3] = order.get("name", "")
            row[4] = order.get("company", "")
            row[5] = order.get("phone", "")
            row[6] = order.get("product", "")
            row[7] = order.get("tons", "")
            row[8] = order.get("delivery", "")
            row[9] = order.get("address", "")
            row[10] = prelim
            row[COL_STATUS - 1] = STATUS_LABELS["new"]
            row[COL_CLIENT_TYPE - 1] = client_type
            row[COL_HISTORY - 1] = f"{_now_str()}: {STATUS_LABELS['new']}"
            ws.append_row(row, value_input_option="USER_ENTERED")
            print(f"[CRM] Заявка {order_id} добавлена ({client_type})", flush=True)
            return order_id
        except Exception as e:
            print(f"[CRM] Ошибка append_order: {e}", flush=True)
            _ws_cache.clear()  # сбросить кэш — переподключимся в след. раз
            return ""


def _find_row(ws, order_id: str) -> int:
    """Номер строки (1-based) заявки по order_id, или 0."""
    col = ws.col_values(COL_ID)
    for idx, val in enumerate(col, start=1):
        if val.strip() == order_id:
            return idx
    return 0


def update_status(order_id: str, status_key: str) -> dict:
    """
    Переводит заявку в новый статус. Возвращает:
      {"ok": bool, "label": str, "next": [keys], "terminal": bool}
    """
    label = STATUS_LABELS.get(status_key, status_key)
    result = {"ok": False, "label": label, "next": NEXT.get(status_key, []),
              "terminal": status_key in TERMINAL_KEYS}
    if not is_available() or not order_id:
        return result
    with _lock:
        try:
            ws = _get_ws()
            row = _find_row(ws, order_id)
            if not row:
                print(f"[CRM] Заявка {order_id} не найдена для смены статуса", flush=True)
                return result

            ws.update_cell(row, COL_STATUS, label)

            # Реакция менеджера: при первом переходе в «В работе»
            if status_key == "work":
                cur = ws.cell(row, COL_REACTION).value
                if not cur:
                    ws.update_cell(row, COL_REACTION, _reaction_mins(order_id))

            # Финал → дата закрытия
            if status_key in TERMINAL_KEYS:
                ws.update_cell(row, COL_CLOSED, _today_str())

            # История статусов (append)
            hist = ws.cell(row, COL_HISTORY).value or ""
            hist = (hist + f"\n{_now_str()}: {label}").strip()
            ws.update_cell(row, COL_HISTORY, hist)

            result["ok"] = True
            print(f"[CRM] {order_id} → {label}", flush=True)
            return result
        except Exception as e:
            print(f"[CRM] Ошибка update_status {order_id}: {e}", flush=True)
            _ws_cache.clear()
            return result


# ─── Кнопки для MAX ──────────────────────────────────────────────────────────
def status_buttons(order_id: str, status_key: str) -> list:
    """Ряды кнопок (формат MAX) с допустимыми переходами из текущего статуса."""
    rows = []
    for nxt in NEXT.get(status_key, []):
        rows.append([{
            "type": "callback",
            "text": BUTTON_TEXT.get(nxt, nxt),
            "payload": f"crm_{order_id}_{nxt}",
        }])
    return rows
