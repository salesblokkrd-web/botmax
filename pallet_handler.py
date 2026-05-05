"""
Обработка сообщений #поддоны и #условия из группы производства.
Автоматическая запись в Google Sheets (ВОЗВРАТ ПОДДОНОВ + ОПЛАТА).
"""
import os
import re
import json
import time
import datetime
import urllib.parse
from sheets_logger import _sheets_request, SPREADSHEET_ID

DATA_DIR = "/data" if os.path.exists("/data") else "."
CONDITIONS_FILE = os.path.join(DATA_DIR, "pallet_conditions.json")

# Водители → номера машин
DRIVERS = {
    "кораблев": "у135рх 193",
    "адлейба": "р319ок 123",
    "кислицин": "р638хн 123",
    "камышанов": "",
}


def _load_conditions():
    try:
        with open(CONDITIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_conditions(data):
    with open(CONDITIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _current_month_key():
    return datetime.date.today().strftime("%Y-%m")


def parse_conditions(text):
    """Парсит сообщение #условия. Возвращает {client, payment_type} или None."""
    lines = text.strip().split("\n")
    if not lines or "#условия" not in lines[0].lower():
        return None
    result = {}
    for line in lines[1:]:
        line = line.strip()
        if ":" not in line:
            continue
        key, val = line.split(":", 1)
        key = key.strip().lower()
        val = val.strip()
        if key in ("клиент", "client"):
            result["client"] = val
        elif key in ("оплата", "payment"):
            val_lower = val.lower().strip()
            if val_lower in ("ооо", "ooo"):
                result["payment_type"] = "ООО"
            elif val_lower == "ип":
                result["payment_type"] = "ИП"
            elif val_lower in ("нал", "наличка", "наличные", "cash"):
                result["payment_type"] = "нал"
            else:
                result["payment_type"] = val
    if "client" in result and "payment_type" in result:
        return result
    return None


def handle_conditions_message(text):
    """Обрабатывает #условия, сохраняет. Возвращает текст ответа."""
    parsed = parse_conditions(text)
    if not parsed:
        return "Не удалось распарсить. Формат:\n#условия\nКлиент: ИмяКлиента\nОплата: ООО/ИП/нал"
    conditions = _load_conditions()
    month = _current_month_key()
    client = parsed["client"]
    payment = parsed["payment_type"]
    conditions[client] = {
        "payment_type": payment,
        "month": month,
        "updated_at": datetime.datetime.now().isoformat(),
    }
    _save_conditions(conditions)
    return f"✓ Условия сохранены: {client} → {payment} (до конца {month})"


def parse_pallets(text):
    """Парсит сообщение #поддоны. Возвращает dict или None."""
    lines = text.strip().split("\n")
    if not lines or "#поддоны" not in lines[0].lower():
        return None
    result = {}
    for line in lines[1:]:
        line = line.strip()
        if ":" not in line:
            continue
        key, val = line.split(":", 1)
        key = key.strip().lower()
        val = val.strip()
        if key in ("клиент", "client"):
            result["client"] = val
        elif key in ("кол-во", "количество", "qty", "шт"):
            nums = re.findall(r"\d+", val)
            if nums:
                result["qty"] = int(nums[0])
        elif key in ("поддон", "тип", "pallet"):
            val_lower = val.lower()
            if "зелён" in val_lower or "зелен" in val_lower or "узк" in val_lower or "0.8" in val_lower:
                result["pallet_type"] = "Поддон 1*"
            else:
                result["pallet_type"] = "Поддон"
        elif key in ("дата", "date"):
            result["date"] = val
    if "client" in result and "qty" in result:
        if "pallet_type" not in result:
            result["pallet_type"] = "Поддон"
        if "date" not in result:
            result["date"] = datetime.date.today().strftime("%d.%m.%Y")
        return result
    return None


def _get_payment_type(client):
    """Приоритет: 1) #условия текущего месяца, 2) дефолт из ЦЕНЫ (AF4)."""
    conditions = _load_conditions()
    month = _current_month_key()
    if client in conditions:
        cond = conditions[client]
        if cond.get("month") == month:
            return cond["payment_type"]
    encoded = urllib.parse.quote(f"'{client}'!AF4:AH4")
    result = _sheets_request("GET", f"values/{encoded}")
    if not result:
        return None
    values = result.get("values", [])
    if not values or len(values[0]) < 2:
        return None
    conditions_text = values[0][1].lower()
    match = re.search(r"поддоны\s+выставляем\s+(от\s+|за\s+)(ооо|ип|нал)", conditions_text)
    if match:
        val = match.group(2)
        if val == "ооо":
            return "ООО"
        elif val == "ип":
            return "ИП"
        elif val == "нал":
            return "нал"
    return None


def _get_pallet_price(client, pallet_type="Поддон"):
    """Читает цену поддона из ЦЕНЫ клиента (AF8+)."""
    encoded = urllib.parse.quote(f"'{client}'!AF8:AK20")
    result = _sheets_request("GET", f"values/{encoded}")
    if not result:
        return 550.0
    for row in result.get("values", []):
        if len(row) >= 5 and "поддон" in (row[2] or "").lower():
            if pallet_type == "Поддон 1*" and "1*" not in row[2]:
                continue
            if pallet_type == "Поддон" and "1*" in row[2]:
                continue
            try:
                return float(row[4].replace(",", ".").replace("\xa0", ""))
            except (ValueError, IndexError):
                continue
    for row in result.get("values", []):
        if len(row) >= 5 and "поддон" in (row[2] or "").lower():
            try:
                return float(row[4].replace(",", ".").replace("\xa0", ""))
            except (ValueError, IndexError):
                continue
    return 550.0


def _find_next_empty_row(client, col, start_row=8):
    """Ищет первую пустую строку в указанном столбце клиента."""
    encoded = urllib.parse.quote(f"'{client}'!{col}{start_row}:{col}50")
    result = _sheets_request("GET", f"values/{encoded}")
    if not result:
        return start_row
    values = result.get("values", [])
    for i, row in enumerate(values):
        if not row or not row[0] or row[0].strip() == "":
            return start_row + i
    return start_row + len(values)


def _write_range(sheet, rng, values):
    """Пишет значения в ячейки."""
    encoded = urllib.parse.quote(f"'{sheet}'!{rng}")
    body = {"values": values}
    result = _sheets_request(
        "PUT",
        f"values/{encoded}?valueInputOption=USER_ENTERED",
        body
    )
    return result is not None


def handle_pallets_message(text, sender_name=""):
    """Обрабатывает #поддоны, записывает в таблицу. Возвращает текст ответа."""
    parsed = parse_pallets(text)
    if not parsed:
        return "Не удалось распарсить. Формат:\n#поддоны\nКлиент: ИмяКлиента\nКол-во: 31\nПоддон: красный"

    client = parsed["client"]
    qty = parsed["qty"]
    pallet_type = parsed["pallet_type"]
    date = parsed["date"]

    truck = ""
    sender_lower = sender_name.lower()
    for driver_key, truck_num in DRIVERS.items():
        if driver_key in sender_lower:
            truck = truck_num
            break

    price = _get_pallet_price(client, pallet_type)
    total = int(qty * price)

    payment_type = _get_payment_type(client)
    ooo = "TRUE" if payment_type == "ООО" else "FALSE"
    ip = "TRUE" if payment_type == "ИП" else "FALSE"
    nal = "TRUE" if payment_type == "нал" else "FALSE"

    return_row = _find_next_empty_row(client, "X")
    payment_row = _find_next_empty_row(client, "O")

    errors = []

    ok1 = _write_range(client, f"X{return_row}:AC{return_row}",
                       [[date, truck, "Склад", pallet_type, str(qty), str(price).replace(".", ",")]])
    if not ok1:
        errors.append("ВОЗВРАТ")

    ok2 = _write_range(client, f"O{payment_row}:V{payment_row}",
                       [[date, ooo, ip, nal, "Склад", "возврат поддонов", str(total), "Бартер"]])
    if not ok2:
        errors.append("ОПЛАТА")

    if errors:
        return f"Ошибка записи ({', '.join(errors)}) для {client}. Возможно, лист защищён."

    payment_label = payment_type or "не определён"
    return (f"✓ {client}: возврат {qty} шт ({pallet_type})\n"
            f"Сумма: {total:,}₽ (по {price:.0f}₽/шт)\n"
            f"Получатель: {payment_label}\n"
            f"Записано в ВОЗВРАТ (стр.{return_row}) и ОПЛАТА (стр.{payment_row})")


def try_handle(text, sender_name=""):
    """Проверяет, является ли сообщение #поддоны или #условия.
    Возвращает текст ответа или None (если не наше сообщение)."""
    text_lower = text.strip().lower()
    if text_lower.startswith("#поддоны"):
        return handle_pallets_message(text, sender_name)
    if text_lower.startswith("#условия"):
        return handle_conditions_message(text)
    return None
