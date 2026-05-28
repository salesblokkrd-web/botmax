"""Изолированный парсер для тестов maxbot.

Гонит ТОЧНО ТАКОЙ ЖЕ промпт что и прод (через parser_prompt.build_parser_prompt),
без 4843-строчных side-effects от import bot.py.

Использование:
    from parser_isolated import parse, get_usage
    events = parse("Рейс 135 от 14.05 КРД → Шубина...")
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

# parser_prompt лежит на уровень выше — в maxbot/
_here = Path(__file__).resolve()
sys.path.insert(0, str(_here.parent.parent))
from parser_prompt import build_parser_prompt  # noqa: E402


def _load_api_key() -> str | None:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        return key
    # ищем в .env родительских проектов
    for parent in _here.parents:
        for envname in (".env", "../mechanic-bot/.env"):
            p = parent / envname
            if p.exists():
                for line in p.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if line.startswith("ANTHROPIC_API_KEY="):
                        return line.split("=", 1)[1].strip()
    return None


CLAUDE_API_KEY = _load_api_key()

_usage = {"input_tokens": 0, "output_tokens": 0, "calls": 0}


def get_usage() -> dict:
    return dict(_usage)


def reset_usage() -> None:
    _usage["input_tokens"] = 0
    _usage["output_tokens"] = 0
    _usage["calls"] = 0


def parse(text: str, msg_date: str = "") -> list:
    """Точный аналог _parse_blok_plan_claude из bot.py — гонит тот же промпт."""
    if not CLAUDE_API_KEY:
        raise RuntimeError(
            "ANTHROPIC_API_KEY не найден. Положи в .env или в mechanic-bot/.env"
        )

    prompt = build_parser_prompt(text, msg_date)

    body = json.dumps({
        "model": "claude-sonnet-4-6",
        "max_tokens": 4096,
        "system": "Ты JSON-only парсер. Отвечай ИСКЛЮЧИТЕЛЬНО валидным JSON-массивом. Без markdown, без объяснений, без префиксов, без code-блоков. Если событий нет — верни []. Никакого текста до '[' или после ']'.",
        "messages": [{"role": "user", "content": prompt}],
    }).encode()

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "x-api-key": CLAUDE_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
    )

    # До 4 попыток: rate-limit 429 ждём, остальные ошибки — короткая пауза.
    last_err = ""
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                resp = json.loads(r.read())
            _usage["calls"] += 1
            usage = resp.get("usage", {})
            _usage["input_tokens"] += usage.get("input_tokens", 0)
            _usage["output_tokens"] += usage.get("output_tokens", 0)

            raw = resp["content"][0]["text"].strip()
            raw = re.sub(r"^```[a-z]*\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw)
            try:
                events = json.loads(raw)
            except json.JSONDecodeError:
                m = re.search(r"\[\s*\{[\s\S]*\}\s*\]", raw)
                events = json.loads(m.group(0)) if m else None
                if events is None:
                    if attempt < 3:
                        time.sleep(1)
                        continue
                    return [{"_parse_error": "Невалидный JSON", "_raw": raw[:300]}]
            return events
        except urllib.error.HTTPError as e:
            try:
                body_err = e.read().decode("utf-8", errors="replace")[:300]
            except Exception:
                body_err = ""
            last_err = f"HTTP {e.code}: {body_err}"
            if e.code == 429 and attempt < 3:
                # rate-limit — ждём дольше (30 / 60 / 90 сек)
                time.sleep(30 * (attempt + 1))
                continue
            if attempt < 3:
                time.sleep(2)
                continue
            return [{"_parse_error": f"HTTP {e.code}", "_body": body_err}]
        except Exception as e:
            last_err = str(e)
            if attempt < 3:
                time.sleep(2)
                continue
            return [{"_parse_error": str(e)}]

    return [{"_parse_error": last_err or "no result after 4 attempts"}]
