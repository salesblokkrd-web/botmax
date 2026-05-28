"""Прогон корпуса regression-тестов maxbot.

Запуск:
    cd maxbot && python tests/runner.py

По каждому кейсу:
- зовём parser_isolated.parse(input)
- сравниваем с expected (мягко — type обязателен, поля по правилам)
- печатаем PASS / FAIL / PARTIAL + причину

В конце — итог + token usage.
"""
from __future__ import annotations

import sys
from pathlib import Path

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

sys.path.insert(0, str(Path(__file__).parent))
import parser_isolated as parser

try:
    import yaml
except ImportError:
    print("pip install pyyaml")
    raise SystemExit(1)


def _norm(v):
    """Нормализация значений для сравнения: убираем лишние пробелы, регистр для строк."""
    if isinstance(v, str):
        return v.strip()
    return v


def _resolve(actual: dict, key: str):
    """Достаёт значение с учётом fallback в продукт-массивы и deliveries[0]."""
    val = actual.get(key)
    if val is not None:
        return val
    # fallback для product → products_detail[0].code
    if key == "product":
        details = actual.get("products_detail") or []
        if details and isinstance(details, list):
            return details[0].get("code")
    if key == "qty_per_pallet":
        details = actual.get("products_detail") or []
        if details:
            return details[0].get("qty_per_pallet")
    if key == "price_buy":
        details = actual.get("products_detail") or []
        if details:
            return details[0].get("price_buy")
    if key == "price_sell":
        details = actual.get("products_detail") or []
        if details:
            return details[0].get("price_sell")
    # fallback для client → deliveries[0].client
    if key == "client":
        deliveries = actual.get("deliveries") or []
        if deliveries:
            return deliveries[0].get("client")
    return None


def _check_field(actual: dict, key: str, expected_val) -> tuple[bool, str]:
    """Проверка одного поля. key может содержать суффиксы _contains, _has, _count."""
    if key.endswith("_contains"):
        real_key = key[:-len("_contains")]
        val = _resolve(actual, real_key)
        if val is None:
            return False, f"{real_key}: missing (ожидали contains {expected_val!r})"
        if expected_val.lower() not in str(val).lower():
            return False, f"{real_key}={val!r} не содержит {expected_val!r}"
        return True, ""

    if key.endswith("_has"):
        real_key = key[:-len("_has")]
        val = actual.get(real_key) or {}
        if not isinstance(val, dict):
            return False, f"{real_key} не dict"
        missing = [k for k in expected_val if k not in val]
        if missing:
            return False, f"{real_key}: нет ключей {missing}"
        return True, ""

    if key.endswith("_count"):
        real_key = key[:-len("_count")]
        val = actual.get(real_key)
        if val is None:
            return False, f"{real_key}: missing (ожидали count={expected_val})"
        if len(val) != expected_val:
            return False, f"{real_key}: count={len(val)}, ожидали {expected_val}"
        return True, ""

    # обычная проверка с fallback на products_detail/deliveries
    val = _resolve(actual, key)
    if val is None and expected_val is not None:
        return False, f"{key}: missing (ожидали {expected_val!r})"
    if _norm(val) != _norm(expected_val):
        return False, f"{key}={val!r}, ожидали {expected_val!r}"
    return True, ""


def check_event(actual: dict, expected: dict) -> tuple[bool, list[str]]:
    """Проверяет одно событие по словарю expected. Возвращает (passed, problems)."""
    problems = []
    for key, exp_val in expected.items():
        ok, msg = _check_field(actual, key, exp_val)
        if not ok:
            problems.append(msg)
    return (not problems), problems


def run_case(case: dict) -> dict:
    """Прогон одного кейса. Возвращает {status, problems, actual, name}."""
    name = case.get("name", "?")
    input_text = case["input"]
    expected = case.get("expected", [])

    actual = parser.parse(input_text)

    # парсер вернул ошибку
    if actual and isinstance(actual[0], dict) and "_parse_error" in actual[0]:
        return {
            "name": name, "status": "ERROR",
            "problems": [actual[0].get("_parse_error", "?")],
            "actual": actual, "expected_count": len(expected),
        }

    # ожидали пустой массив (план / шум)
    if expected == []:
        if actual == []:
            return {"name": name, "status": "PASS", "problems": [], "actual": [], "expected_count": 0}
        return {
            "name": name, "status": "FAIL",
            "problems": [f"ожидали [] (план/шум игнорируется), получили {len(actual)} событий"],
            "actual": actual, "expected_count": 0,
        }

    # количество событий
    if len(actual) != len(expected):
        return {
            "name": name, "status": "FAIL",
            "problems": [f"событий: actual={len(actual)}, expected={len(expected)}"],
            "actual": actual, "expected_count": len(expected),
        }

    # поэлементно — type первого + поля
    problems = []
    for i, (exp, act) in enumerate(zip(expected, actual)):
        ok, p = check_event(act, exp)
        if not ok:
            problems.extend([f"[{i}] {pr}" for pr in p])

    if not problems:
        return {"name": name, "status": "PASS", "problems": [], "actual": actual,
                "expected_count": len(expected)}
    if len(problems) <= 2:
        status = "PARTIAL"
    else:
        status = "FAIL"
    return {"name": name, "status": status, "problems": problems, "actual": actual,
            "expected_count": len(expected)}


def main():
    corpus_path = Path(__file__).parent / "corpus.yaml"
    with open(corpus_path, encoding="utf-8") as f:
        corpus = yaml.safe_load(f)

    print(f"Корпус: {len(corpus)} кейсов\n")
    parser.reset_usage()

    import time as _t
    results = []
    for i, case in enumerate(corpus, 1):
        result = run_case(case)
        results.append(result)
        _t.sleep(1.5)  # bypass anthropic rate-limit на длинных промптах
        icon = {"PASS": "✅", "PARTIAL": "🟡", "FAIL": "❌", "ERROR": "🔥"}[result["status"]]
        print(f"{icon} {i:02d}. {result['name'][:80]}")
        for p in result["problems"][:4]:
            print(f"      — {p}")
        if result["status"] in ("FAIL", "PARTIAL") and result.get("actual"):
            import json as _j
            preview = _j.dumps(result["actual"], ensure_ascii=False)[:300]
            print(f"      actual: {preview}")

    # итог
    cnt = {"PASS": 0, "PARTIAL": 0, "FAIL": 0, "ERROR": 0}
    for r in results:
        cnt[r["status"]] += 1

    print("\n" + "=" * 70)
    print(f"ИТОГ: ✅ {cnt['PASS']} PASS │ 🟡 {cnt['PARTIAL']} PARTIAL │ "
          f"❌ {cnt['FAIL']} FAIL │ 🔥 {cnt['ERROR']} ERROR  (из {len(corpus)})")
    print("=" * 70)

    u = parser.get_usage()
    cost_in = u["input_tokens"] / 1_000_000 * 3.0   # Sonnet 4.6 input $3/M
    cost_out = u["output_tokens"] / 1_000_000 * 15.0  # output $15/M
    total = cost_in + cost_out
    print(f"Claude: {u['calls']} вызовов, "
          f"in={u['input_tokens']:,} out={u['output_tokens']:,} токенов")
    print(f"Стоимость прогона: ${total:.4f} ≈ {total * 85:.1f} ₽")

    return 0 if cnt["FAIL"] == 0 and cnt["ERROR"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
