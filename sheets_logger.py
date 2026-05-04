"""
Модуль записи рейсов в Google Sheets (таблица Отгр_блоки).
Использует REST API + JWT (без gspread).
"""
import os
import json
import time
import urllib.request
import urllib.parse

try:
    import jwt  # PyJWT
except ImportError:
    jwt = None
    print("[SHEETS] WARNING: PyJWT not installed, sheets logging disabled", flush=True)

SPREADSHEET_ID = "1FwpvHhDHiNuFOdXlTcrVuTWKUqh2NmWVn810ylM0MkQ"
SHEET_NAME = "Рейсы_бот"
SCOPES = "https://www.googleapis.com/auth/spreadsheets"
TOKEN_URL = "https://oauth2.googleapis.com/token"
SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"

_cached_token = None
_token_expires = 0


def _get_service_account():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _get_access_token():
    global _cached_token, _token_expires
    if _cached_token and time.time() < _token_expires - 60:
        return _cached_token
    if not jwt:
        return None
    sa = _get_service_account()
    if not sa:
        print("[SHEETS] No GOOGLE_SERVICE_ACCOUNT env var", flush=True)
        return None

    now = int(time.time())
    payload = {
        "iss": sa["client_email"],
        "scope": SCOPES,
        "aud": TOKEN_URL,
        "iat": now,
        "exp": now + 3600,
    }
    encoded = jwt.encode(payload, sa["private_key"], algorithm="RS256")
    data = urllib.parse.urlencode({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": encoded,
    }).encode()

    req = urllib.request.Request(TOKEN_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            _cached_token = result["access_token"]
            _token_expires = now + result.get("expires_in", 3600)
            return _cached_token
    except Exception as e:
        print(f"[SHEETS] Token error: {e}", flush=True)
        return None


def _sheets_request(method, path, body=None):
    token = _get_access_token()
    if not token:
        return None
    url = f"{SHEETS_API}/{SPREADSHEET_ID}/{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()[:300]
        print(f"[SHEETS] API error {e.code}: {err_body}", flush=True)
        return None
    except Exception as e:
        print(f"[SHEETS] Request error: {e}", flush=True)
        return None


def append_trip(row_data: list) -> bool:
    """row_data = [№, дата, № машины, водитель, откуда, куда, организация, продукция]"""
    body = {"values": [row_data]}
    range_str = urllib.parse.quote(f"{SHEET_NAME}!A:H")
    result = _sheets_request(
        "POST",
        f"values/{range_str}:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS",
        body
    )
    if result:
        print(f"[SHEETS] Записан рейс: {row_data}", flush=True)
        return True
    return False


def get_last_trip_number() -> int:
    range_str = urllib.parse.quote(f"{SHEET_NAME}!A:A")
    result = _sheets_request("GET", f"values/{range_str}")
    if not result:
        return 0
    values = result.get("values", [])
    last_num = 0
    for row in values:
        if row and row[0].isdigit():
            last_num = int(row[0])
    return last_num


def test_connection() -> bool:
    token = _get_access_token()
    if not token:
        return False
    result = _sheets_request("GET", f"values/{urllib.parse.quote(SHEET_NAME + '!A1:A1')}")
    return result is not None
