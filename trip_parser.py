"""
Парсер сообщений о рейсах из группы "Производство и отгрузка".
Распознаёт: дату, машину, водителя, маршрут, продукцию (блоки).
"""
import re
from datetime import datetime, date

# Известные водители (фамилия → полное ФИО)
DRIVERS = {
    "кораблев": "Кораблев М.Н.",
    "кораблёв": "Кораблев М.Н.",
    "адлейба": "Адлейба А.Ю.",
    "кислицин": "Кислицин А.С.",
    "кислицын": "Кислицин А.С.",
    "камышанов": "Камышанов А.А.",
}

# Известные машины (часть номера → полный номер)
VEHICLES = {
    "135": "у135рх 193",
    "319": "р319ок 123",
    "638": "р638хн 123",
    "688": "688",
    "689": "689",
}

# Кодировка продукции
PRODUCT_CODES = {
    "20(3-0)": "Блок 20(3-0) 188x190x390 3-пуст.",
    "20(4-0)": "Блок 20(4-0) 188x190x390 4-пуст.",
    "9(2-0)": "Блок 9(2-0) 90x188x390 2-пуст.",
    "12(2-0)": "Блок 12(2-0) 120x188x390 2-пуст.",
}

# Паттерн для распознавания кодов блоков: 20(3-0), 9(2-0) и т.д.
BLOCK_PATTERN = re.compile(r'(\d{1,2})\s*\(\s*(\d)\s*[-–]\s*(\d)\s*\)')

# Паттерн для номера авто: буква+цифры+буквы или просто 3 цифры (как "135", "688")
VEHICLE_PATTERN = re.compile(r'[а-яА-Я]?\d{3}[а-яА-Я]{0,2}\s*\d{0,3}')

# Паттерн для даты: ДД.ММ или ДД.ММ.ГГ(ГГ)
DATE_PATTERN = re.compile(r'(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?')


def parse_trip_message(text: str) -> dict | None:
    """
    Пытается распарсить сообщение как информацию о рейсе.
    Возвращает dict с полями или None если не рейс.
    """
    if not text or len(text) < 5:
        return None

    text_lower = text.lower()
    result = {
        "date": None,
        "vehicle": None,
        "driver": None,
        "from_location": None,
        "to_location": None,
        "client": None,
        "product": None,
        "raw_text": text,
    }

    # Ищем коды блоков — если нет блоков, скорее всего не наше сообщение
    blocks_found = []
    for m in BLOCK_PATTERN.finditer(text):
        code = f"{m.group(1)}({m.group(2)}-{m.group(3)})"
        if code in PRODUCT_CODES:
            blocks_found.append(code)

    # Также ищем слова-маркеры рейса
    trip_markers = ["рейс", "отгруз", "загруз", "выехал", "доставк", "блок", "поддон",
                    "двадцат", "девят", "двенаш", "карьер", "склад", "крд"]
    has_marker = any(m in text_lower for m in trip_markers)

    if not blocks_found and not has_marker:
        return None

    # Дата
    date_match = DATE_PATTERN.search(text)
    if date_match:
        day = int(date_match.group(1))
        month = int(date_match.group(2))
        year_str = date_match.group(3)
        if year_str:
            year = int(year_str)
            if year < 100:
                year += 2000
        else:
            year = date.today().year
        try:
            result["date"] = f"{day:02d}.{month:02d}.{year}"
        except:
            result["date"] = date.today().strftime("%d.%m.%Y")
    else:
        result["date"] = date.today().strftime("%d.%m.%Y")

    # Водитель
    for key, full_name in DRIVERS.items():
        if key in text_lower:
            result["driver"] = full_name
            break

    # Машина — ищем номер или известный номер
    for key, full_num in VEHICLES.items():
        if key in text:
            result["vehicle"] = full_num
            break
    if not result["vehicle"]:
        vm = VEHICLE_PATTERN.search(text)
        if vm:
            result["vehicle"] = vm.group(0).strip()

    # Продукция
    if blocks_found:
        result["product"] = "+".join(blocks_found)
    else:
        # Ищем текстовые упоминания
        prods = []
        if "двадцат" in text_lower or "20-к" in text_lower:
            prods.append("20(3-0)")
        if "девят" in text_lower or "9-к" in text_lower:
            prods.append("9(2-0)")
        if "двенаш" in text_lower or "12-к" in text_lower:
            prods.append("12(2-0)")
        if "поддон" in text_lower:
            prods.append("Поддоны")
        if prods:
            result["product"] = "+".join(prods)

    # Маршрут: карьер → КРД или наоборот
    if "карьер" in text_lower and "крд" in text_lower:
        result["from_location"] = "Карьер"
        result["to_location"] = "КРД"
    elif "крд" in text_lower and "карьер" in text_lower:
        result["from_location"] = "КРД"
        result["to_location"] = "Карьер"
    elif "карьер" in text_lower:
        result["from_location"] = "Карьер"
    elif "крд" in text_lower:
        result["to_location"] = "КРД"

    # Если нашли хотя бы продукцию ИЛИ (водитель + машину) — считаем рейсом
    if result["product"] or (result["driver"] and result["vehicle"]):
        return result

    return None


def format_trip_row(trip: dict, number: int) -> list:
    """Форматирует рейс в строку для Google Sheets."""
    return [
        str(number),
        trip.get("date", ""),
        trip.get("vehicle", ""),
        trip.get("driver", ""),
        trip.get("from_location", ""),
        trip.get("to_location", ""),
        trip.get("client", ""),
        trip.get("product", ""),
    ]


def format_trip_confirmation(trip: dict) -> str:
    """Форматирует подтверждение для ответа в чат."""
    parts = ["📋 Записал рейс:"]
    if trip.get("date"):
        parts.append(f"  Дата: {trip['date']}")
    if trip.get("vehicle"):
        parts.append(f"  Машина: {trip['vehicle']}")
    if trip.get("driver"):
        parts.append(f"  Водитель: {trip['driver']}")
    if trip.get("from_location") or trip.get("to_location"):
        route = f"{trip.get('from_location', '?')} → {trip.get('to_location', '?')}"
        parts.append(f"  Маршрут: {route}")
    if trip.get("product"):
        parts.append(f"  Продукция: {trip['product']}")
    return "\n".join(parts)
