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
    "postpay":    "🤝 Оплата по факту",
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
    "postpay":    "🤝 Оплата по факту",
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
    "work":       ["invoice", "prepaid", "postpay", "think", "lost_price"],
    "invoice":    ["prepaid", "postpay", "think", "lost_price"],
    "prepaid":    ["shipping"],
    "postpay":    ["shipping"],
    "shipping":   ["partial", "won"],
    "partial":    ["partial", "won"],
    "won":        [],
    # «Думает» ведёт прямо к оплате (счёт уже мог быть выставлен) — без повторного счёта
    "think":      ["invoice", "prepaid", "postpay", "lost_price", "lost_other"],
    "lost_price": ["work"],
    "lost_other": [],
}

# ─── Колонки таблицы «Заявки» (порядок = порядок в строке) ───────────────────
HEADERS = [
    "№ заявки", "Дата/время", "Источник", "Имя", "Компания", "Телефон",
    "Товар", "Объём (т)", "Получение", "Адрес", "Предв. расчёт, ₽",
    "Статус", "Точная цена материала, ₽", "Стоимость доставки, ₽", "Точный расчёт, ₽",
    "№ счёта", "Сумма счёта, ₽", "Предоплата, ₽", "Остаток оплаты, ₽",
    "Отгружено", "Осталось отгрузить", "Реакция менеджера, мин", "Тип клиента",
    "Дата след. касания", "Причина отказа", "Дата закрытия", "Комментарий",
    "История статусов",
]
# 1-based индексы часто используемых колонок
COL_ID = 1
COL_PHONE = 6
COL_PRELIM = 11   # Предв. расчёт (авто, бот)
COL_STATUS = 12
COL_EXACT = 15    # Точный расчёт (из ответа Ильи клиенту) — считается выручкой
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
    if first != HEADERS:  # синхронизируем шапку (в т.ч. при переименовании колонок)
        ws.update("A1", [HEADERS])
        try:
            ws.freeze(rows=1)
            ws.format("A1:AB1", {"textFormat": {"bold": True}})
        except Exception:
            pass
    return ws


# Раскладка дашборда (0-based строки) — используется и при заполнении, и при оформлении
_DASH = {
    "title": 0, "kpi_lbl": 2, "kpi_val": 3,
    "ch_hdr_sec": 5, "ch_cols": 6, "ch_max": 7, "ch_tg": 8, "ch_total": 9,
    "stage_sec": 11, "stage_first": 12,
}

def _build_dashboard(sh):
    """Лист «Дашборд» — KPI-карточки, разбивка по каналам (MAX/TG), стадии."""
    try:
        try:
            ws = sh.worksheet(WS_DASH)
        except Exception:
            ws = sh.add_worksheet(title=WS_DASH, rows=40, cols=8)
        q = "'%s'" % WS_ORDERS
        W = WON_LABEL
        m = []
        m.append(["📊 CRM КАРЬЕР · Воронка заявок", "", "", "", "", ""])
        m.append(["", "", "", "", "", ""])
        m.append(["Всего заявок", "Закрыто", "Конверсия", "Выручка точная, ₽", "Выручка предв., ₽", "Ср. реакция, мин"])
        m.append([
            "=COUNTA(%s!A2:A)" % q,
            '=COUNTIF(%s!L2:L;"%s")' % (q, W),
            '=IFERROR(COUNTIF(%s!L2:L;"%s")/COUNTA(%s!A2:A);0)' % (q, W, q),
            '=SUMIF(%s!L2:L;"%s";%s!O2:O)' % (q, W, q),
            '=SUMIF(%s!L2:L;"%s";%s!K2:K)' % (q, W, q),
            "=IFERROR(AVERAGE(%s!V2:V);0)" % q,
        ])
        m.append(["", "", "", "", "", ""])
        m.append(["ПО КАНАЛАМ — какой работает лучше", "", "", "", "", ""])
        m.append(["Канал", "Заявок", "Закрыто", "Конверсия", "Выручка точная, ₽", ""])
        for ch in ("MAX-бот", "TG-бот"):
            m.append([
                ch,
                '=COUNTIF(%s!C2:C;"%s")' % (q, ch),
                '=COUNTIFS(%s!C2:C;"%s";%s!L2:L;"%s")' % (q, ch, q, W),
                '=IFERROR(COUNTIFS(%s!C2:C;"%s";%s!L2:L;"%s")/COUNTIF(%s!C2:C;"%s");0)' % (q, ch, q, W, q, ch),
                '=SUMIFS(%s!O2:O;%s!C2:C;"%s";%s!L2:L;"%s")' % (q, q, ch, q, W),
                "",
            ])
        m.append([
            "ИТОГО",
            "=COUNTA(%s!A2:A)" % q,
            '=COUNTIF(%s!L2:L;"%s")' % (q, W),
            '=IFERROR(COUNTIF(%s!L2:L;"%s")/COUNTA(%s!A2:A);0)' % (q, W, q),
            '=SUMIF(%s!L2:L;"%s";%s!O2:O)' % (q, W, q),
            "",
        ])
        m.append(["", "", "", "", "", ""])
        m.append(["ПО СТАДИЯМ ВОРОНКИ", "", "", "", "", ""])
        for key in ["new", "work", "invoice", "prepaid", "postpay", "shipping",
                    "partial", "won", "think", "lost_price", "lost_other"]:
            lbl = STATUS_LABELS[key]
            m.append([lbl, '=COUNTIF(%s!L2:L;"%s")' % (q, lbl), "", "", "", ""])
        ws.batch_clear(["A1:Z60"])
        ws.update("A1", m, value_input_option="USER_ENTERED")
    except Exception as e:
        print(f"[CRM] Дашборд не создан (не критично): {e}", flush=True)


def _rgb(r, g, b):
    return {"red": r, "green": g, "blue": b}

# Ширины колонок (px), индекс = позиция колонки (0..27)
_COL_WIDTHS = [
    140, 110, 78, 115, 130, 105, 165, 64, 96, 175, 100, 160, 115, 115, 110,
    82, 105, 100, 100, 90, 100, 95, 100, 110, 140, 105, 180, 340,
]
_MONEY_COLS = [10, 12, 13, 14, 16, 17, 18]  # предв, точн.цена, доставка, точный, сумма счёта, предоплата, остаток

def format_sheet(sh):
    """Оформляет лист «Заявки» (и «Дашборд») — шапка, рамки, чередование, ширины."""
    HEADER_BG = _rgb(0.12, 0.29, 0.49)   # тёмно-синяя шапка
    WHITE = _rgb(1, 1, 1)
    BAND = _rgb(0.92, 0.95, 0.99)        # светлый фон чётных строк
    GRID = _rgb(0.74, 0.78, 0.83)        # внутренняя сетка
    OUTER = _rgb(0.09, 0.20, 0.36)       # рамка по периметру
    ncols = len(HEADERS)
    body_rows = 200
    end_row = body_rows + 1
    try:
        ws = sh.worksheet(WS_ORDERS)
    except Exception:
        return
    sid = ws.id
    req = []
    # заморозка шапки + 1-й колонки
    req.append({"updateSheetProperties": {
        "properties": {"sheetId": sid, "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 1}},
        "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}})
    # высота шапки (под 2-3 строки)
    req.append({"updateDimensionProperties": {
        "range": {"sheetId": sid, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
        "properties": {"pixelSize": 54}, "fields": "pixelSize"}})
    # ширины колонок
    for i, w in enumerate(_COL_WIDTHS):
        req.append({"updateDimensionProperties": {
            "range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": w}, "fields": "pixelSize"}})
    # шапка: цвет, белый жирный текст, центр, перенос
    req.append({"repeatCell": {
        "range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": ncols},
        "cell": {"userEnteredFormat": {
            "backgroundColor": HEADER_BG, "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP",
            "textFormat": {"bold": True, "fontSize": 10, "foregroundColor": WHITE}}},
        "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,wrapStrategy,textFormat)"}})
    # тело: шрифт, вертикаль по центру, обрезка
    req.append({"repeatCell": {
        "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": ncols},
        "cell": {"userEnteredFormat": {
            "verticalAlignment": "MIDDLE", "wrapStrategy": "CLIP",
            "textFormat": {"fontSize": 10}}},
        "fields": "userEnteredFormat(verticalAlignment,wrapStrategy,textFormat)"}})
    # денежные колонки: вправо + формат «# ##0 ₽»
    for c in _MONEY_COLS:
        req.append({"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end_row, "startColumnIndex": c, "endColumnIndex": c + 1},
            "cell": {"userEnteredFormat": {
                "horizontalAlignment": "RIGHT",
                "numberFormat": {"type": "NUMBER", "pattern": '#,##0" ₽"'}}},
            "fields": "userEnteredFormat(horizontalAlignment,numberFormat)"}})
    # объём + реакция: по центру
    for c in (7, 21):
        req.append({"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end_row, "startColumnIndex": c, "endColumnIndex": c + 1},
            "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
            "fields": "userEnteredFormat.horizontalAlignment"}})
    # рамки: периметр толстый, внутри сетка
    req.append({"updateBorders": {
        "range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": ncols},
        "top": {"style": "SOLID_MEDIUM", "color": OUTER},
        "bottom": {"style": "SOLID_MEDIUM", "color": OUTER},
        "left": {"style": "SOLID_MEDIUM", "color": OUTER},
        "right": {"style": "SOLID_MEDIUM", "color": OUTER},
        "innerHorizontal": {"style": "SOLID", "color": GRID},
        "innerVertical": {"style": "SOLID", "color": GRID}}})
    try:
        sh.batch_update({"requests": req})
    except Exception as e:
        print(f"[CRM] format_sheet (основное) ошибка: {e}", flush=True)
        return
    # чередование строк (banding) — отдельно: при повторе кидает overlap, гасим
    try:
        sh.batch_update({"requests": [{"addBanding": {"bandedRange": {
            "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": end_row, "startColumnIndex": 0, "endColumnIndex": ncols},
            "rowProperties": {"firstBandColor": WHITE, "secondBandColor": BAND}}}}]})
    except Exception:
        pass
    _format_dashboard(sh)
    print("[CRM] Оформление таблицы применено", flush=True)


def _format_dashboard(sh):
    """Уникальное оформление дашборда: баннер, KPI-карточки, таблица каналов, тепловая шкала."""
    NAVY = _rgb(0.11, 0.24, 0.42)     # титул + секции
    ACCENT = _rgb(0.20, 0.40, 0.62)   # подписи KPI
    CARD = _rgb(0.95, 0.97, 1.0)      # фон карточек-значений
    LIGHT = _rgb(0.90, 0.93, 0.97)    # шапка таблицы каналов
    WHITE = _rgb(1, 1, 1)
    GRID = _rgb(0.74, 0.78, 0.83)
    OUTER = _rgb(0.09, 0.20, 0.36)
    MONEY = '#,##0" ₽"'
    try:
        ws = sh.worksheet(WS_DASH)
    except Exception:
        return
    sid = ws.id
    D = _DASH

    def cell(r0, r1, c0, c1, fmt, fields):
        return {"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": r0, "endRowIndex": r1,
                      "startColumnIndex": c0, "endColumnIndex": c1},
            "cell": {"userEnteredFormat": fmt},
            "fields": "userEnteredFormat(%s)" % fields}}

    def merge(r0, r1, c0, c1):
        return {"mergeCells": {"mergeType": "MERGE_ALL",
                "range": {"sheetId": sid, "startRowIndex": r0, "endRowIndex": r1,
                          "startColumnIndex": c0, "endColumnIndex": c1}}}

    def borders(r0, r1, c0, c1):
        return {"updateBorders": {
            "range": {"sheetId": sid, "startRowIndex": r0, "endRowIndex": r1, "startColumnIndex": c0, "endColumnIndex": c1},
            "top": {"style": "SOLID_MEDIUM", "color": OUTER}, "bottom": {"style": "SOLID_MEDIUM", "color": OUTER},
            "left": {"style": "SOLID_MEDIUM", "color": OUTER}, "right": {"style": "SOLID_MEDIUM", "color": OUTER},
            "innerHorizontal": {"style": "SOLID", "color": GRID}, "innerVertical": {"style": "SOLID", "color": GRID}}}

    req = []
    # ширины + заморозка + высоты
    req.append({"updateSheetProperties": {"properties": {"sheetId": sid, "gridProperties": {"frozenRowCount": 1}}, "fields": "gridProperties.frozenRowCount"}})
    for i, w in enumerate([210, 105, 105, 115, 150, 120]):
        req.append({"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1}, "properties": {"pixelSize": w}, "fields": "pixelSize"}})
    for r, h in [(D["title"], 46), (D["kpi_val"], 38)]:
        req.append({"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "ROWS", "startIndex": r, "endIndex": r + 1}, "properties": {"pixelSize": h}, "fields": "pixelSize"}})
    # титул-баннер
    req.append(merge(D["title"], D["title"] + 1, 0, 6))
    req.append(cell(D["title"], D["title"] + 1, 0, 6,
                    {"backgroundColor": NAVY, "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                     "textFormat": {"bold": True, "fontSize": 15, "foregroundColor": WHITE}},
                    "backgroundColor,horizontalAlignment,verticalAlignment,textFormat"))
    # KPI: подписи + значения
    req.append(cell(D["kpi_lbl"], D["kpi_lbl"] + 1, 0, 6,
                    {"backgroundColor": ACCENT, "horizontalAlignment": "CENTER", "wrapStrategy": "WRAP",
                     "textFormat": {"bold": True, "fontSize": 9, "foregroundColor": WHITE}},
                    "backgroundColor,horizontalAlignment,wrapStrategy,textFormat"))
    req.append(cell(D["kpi_val"], D["kpi_val"] + 1, 0, 6,
                    {"backgroundColor": CARD, "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                     "textFormat": {"bold": True, "fontSize": 14}},
                    "backgroundColor,horizontalAlignment,verticalAlignment,textFormat"))
    # форматы чисел в KPI: конверсия %, выручка ₽, реакция 0.0
    req.append(cell(D["kpi_val"], D["kpi_val"] + 1, 2, 3, {"numberFormat": {"type": "PERCENT", "pattern": "0%"}}, "numberFormat"))
    req.append(cell(D["kpi_val"], D["kpi_val"] + 1, 3, 5, {"numberFormat": {"type": "NUMBER", "pattern": MONEY}}, "numberFormat"))
    req.append(cell(D["kpi_val"], D["kpi_val"] + 1, 5, 6, {"numberFormat": {"type": "NUMBER", "pattern": "0.0"}}, "numberFormat"))
    req.append(borders(D["kpi_lbl"], D["kpi_val"] + 1, 0, 6))
    # секции
    for r in (D["ch_hdr_sec"], D["stage_sec"]):
        req.append(merge(r, r + 1, 0, 6))
        req.append(cell(r, r + 1, 0, 6,
                        {"backgroundColor": NAVY, "horizontalAlignment": "LEFT", "verticalAlignment": "MIDDLE",
                         "textFormat": {"bold": True, "fontSize": 11, "foregroundColor": WHITE}},
                        "backgroundColor,horizontalAlignment,verticalAlignment,textFormat"))
    # таблица каналов: шапка + данные + итог
    req.append(cell(D["ch_cols"], D["ch_cols"] + 1, 0, 5,
                    {"backgroundColor": LIGHT, "horizontalAlignment": "CENTER", "textFormat": {"bold": True, "fontSize": 10}},
                    "backgroundColor,horizontalAlignment,textFormat"))
    req.append(cell(D["ch_max"], D["ch_total"] + 1, 1, 5, {"horizontalAlignment": "CENTER"}, "horizontalAlignment"))
    req.append(cell(D["ch_max"], D["ch_total"] + 1, 3, 4, {"numberFormat": {"type": "PERCENT", "pattern": "0%"}}, "numberFormat"))
    req.append(cell(D["ch_max"], D["ch_total"] + 1, 4, 5, {"numberFormat": {"type": "NUMBER", "pattern": MONEY}}, "numberFormat"))
    req.append(cell(D["ch_total"], D["ch_total"] + 1, 0, 5, {"textFormat": {"bold": True, "fontSize": 10}}, "textFormat"))
    req.append(borders(D["ch_cols"], D["ch_total"] + 1, 0, 5))
    # таблица стадий
    stage_last = D["stage_first"] + 11
    req.append(cell(D["stage_first"], stage_last, 1, 2, {"horizontalAlignment": "CENTER"}, "horizontalAlignment"))
    req.append(borders(D["stage_first"], stage_last, 0, 2))
    try:
        sh.batch_update({"requests": req})
    except Exception as e:
        print(f"[CRM] _format_dashboard ошибка: {e}", flush=True)
        return
    # тепловая шкала по конверсии (KPI + каналы): красный→жёлтый→зелёный
    try:
        sh.batch_update({"requests": [{"addConditionalFormatRule": {"index": 0, "rule": {
            "ranges": [
                {"sheetId": sid, "startRowIndex": D["kpi_val"], "endRowIndex": D["kpi_val"] + 1, "startColumnIndex": 2, "endColumnIndex": 3},
                {"sheetId": sid, "startRowIndex": D["ch_max"], "endRowIndex": D["ch_tg"] + 1, "startColumnIndex": 3, "endColumnIndex": 4},
            ],
            "gradientRule": {
                "minpoint": {"color": _rgb(0.96, 0.60, 0.60), "type": "NUMBER", "value": "0"},
                "midpoint": {"color": _rgb(1.0, 0.90, 0.55), "type": "NUMBER", "value": "0.4"},
                "maxpoint": {"color": _rgb(0.58, 0.83, 0.58), "type": "NUMBER", "value": "0.8"}}}}}]})
    except Exception:
        pass


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
            try:
                format_sheet(_ws_cache["sh"])
            except Exception as e:
                print(f"[CRM] format_sheet при bootstrap: {e}", flush=True)
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


def set_exact_total(order_id: str, amount) -> bool:
    """Записывает точный расчёт (₽) в колонку «Точный расчёт» — считается выручкой."""
    if not is_available() or not order_id:
        return False
    with _lock:
        try:
            ws = _get_ws()
            row = _find_row(ws, order_id)
            if not row:
                return False
            ws.update_cell(row, COL_EXACT, amount)
            print(f"[CRM] {order_id}: точный расчёт = {amount}", flush=True)
            return True
        except Exception as e:
            print(f"[CRM] set_exact_total {order_id}: {e}", flush=True)
            _ws_cache.clear()
            return False


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

            # Дубль-нажатие (статус не изменился) — не плодим историю, просто ок
            current = (ws.cell(row, COL_STATUS).value or "").strip()
            if current == label:
                result["ok"] = True
                result["duplicate"] = True
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
