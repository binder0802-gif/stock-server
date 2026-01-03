from db import init_db, fetch_range, upsert_many
from fastapi import FastAPI, HTTPException
from datetime import date, datetime, timedelta
import urllib.request
import json
import time
import ssl

# =========================
# 重要：解決 Render 上的 SSL 驗證失敗
# =========================
ssl._create_default_https_context = ssl._create_unverified_context

app = FastAPI()
init_db()
# -------------------------
# 基本首頁：測試服務活著
# -------------------------
@app.get("/")
def root():
    return {"status": "ok"}


# -------------------------
# 小型快取（避免一直抓）
# - Free 方案重啟會清空（之後要永久不爬，需加 DB）
# -------------------------
CACHE_TTL_SEC = 300  # 5 分鐘
_cache = {}  # key -> {"t": epoch, "value": any}

def cache_get(key: str):
    item = _cache.get(key)
    if not item:
        return None
    if time.time() - item["t"] > CACHE_TTL_SEC:
        _cache.pop(key, None)
        return None
    return item["value"]

def cache_set(key: str, value):
    _cache[key] = {"t": time.time(), "value": value}


# -------------------------
# 共用工具
# -------------------------
def _parse_price(s):
    if s is None:
        return None
    s = str(s).replace(",", "").strip()
    if s in ["", "--"]:
        return None
    try:
        return float(s)
    except:
        return None

def _safe_int(s, default=0):
    try:
        s = str(s).replace(",", "").strip()
        if s in ["", "--"]:
            return default
        return int(float(s))
    except:
        return default

def _month_iter(start: date, end: date):
    y, m = start.year, start.month
    while (y < end.year) or (y == end.year and m <= end.month):
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1


# =========================================================
# 1) TWSE（上市）: 逐月抓
# =========================================================
def _parse_twse_roc_date(date_str: str) -> date:
    # TWSE 回來像 "114/01/02"（民國年）
    parts = date_str.strip().split("/")
    if len(parts) != 3:
        raise ValueError("bad date")
    y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
    y += 1911
    return date(y, m, d)

def _fetch_twse_month(stock_id: str, year: int, month: int):
    date_str = f"{year}{month:02d}01"
    url = (
        "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
        f"?response=json&date={date_str}&stockNo={stock_id}"
    )

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.twse.com.tw/",
            "Connection": "close",
        },
    )

    with urllib.request.urlopen(req, timeout=25) as resp:
        raw = resp.read().decode("utf-8")

    data = json.loads(raw)
    if data.get("stat") != "OK":
        raise ValueError(f"TWSE stat != OK: {data.get('stat')}")
    return data.get("data", [])

def get_history_twse(stock_id: str, start_dt: date, end_dt: date):
    all_rows = []
    for yy, mm in _month_iter(start_dt, end_dt):
        rows = _fetch_twse_month(stock_id, yy, mm)
        all_rows.extend(rows)

    out = []
    for row in all_rows:
        # 欄位：日期,成交股數,成交金額,開盤價,最高價,最低價,收盤價,漲跌價差,成交筆數
        try:
            dt = _parse_twse_roc_date(row[0])
        except:
            continue
        if not (start_dt <= dt <= end_dt):
            continue

        capacity = _safe_int(row[1], 0)
        txn = _safe_int(row[8], 0)
        avg_zhang = capacity / txn / 1000 if txn else 0.0

        out.append({
            "dt": dt,
            "open": _parse_price(row[3]),
            "high": _parse_price(row[4]),
            "low": _parse_price(row[5]),
            "close": _parse_price(row[6]),
            "capacity": capacity,
            "txn": txn,
            "avg_zhang": avg_zhang,
            "source": "twse",
        })

    out.sort(key=lambda x: x["dt"])
    return out


# =========================================================
# 2) TPEx（上櫃）: openapi 每日抓全市場，再挑出該股票
# =========================================================
def _fetch_tpex_daily_all(roc_date: str):
    url = (
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
        f"?l=zh-tw&d={roc_date}&s=0,asc,0"
    )

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.tpex.org.tw/",
            "Connection": "close",
        },
    )

    with urllib.request.urlopen(req, timeout=35) as resp:
        raw = resp.read().decode("utf-8")

    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError("TPEx 回傳格式非 list")
    return data

def get_history_tpex(stock_id: str, start_dt: date, end_dt: date):
    out = []
    cur = start_dt
    while cur <= end_dt:
        roc_y = cur.year - 1911
        roc_date = f"{roc_y}/{cur.month:02d}/{cur.day:02d}"

        try:
            rows = _fetch_tpex_daily_all(roc_date)
        except:
            cur += timedelta(days=1)
            continue

        row_for_stock = None
        for row in rows:
            code = str(row.get("SecuritiesCode", "")).strip()
            if code == stock_id:
                row_for_stock = row
                break

        if row_for_stock:
            open_p = _parse_price(row_for_stock.get("OpeningPrice"))
            high_p = _parse_price(row_for_stock.get("HighestPrice"))
            low_p  = _parse_price(row_for_stock.get("LowestPrice"))
            close_p= _parse_price(row_for_stock.get("ClosingPrice"))

            capacity = _safe_int(row_for_stock.get("TradeVolume"), 0)
            txn = _safe_int(row_for_stock.get("Transaction"), 0)
            avg_zhang = capacity / txn / 1000 if txn else 0.0

            out.append({
                "dt": cur,
                "open": open_p,
                "high": high_p,
                "low": low_p,
                "close": close_p,
                "capacity": capacity,
                "txn": txn,
                "avg_zhang": avg_zhang,
                "source": "tpex",
            })

        cur += timedelta(days=1)

    out.sort(key=lambda x: x["dt"])
    return out


# =========================================================
# 對外 API：/stock/history
# =========================================================
@app.get("/stock/history")
def stock_history(stock_id: str, start: str, end: str):
    # start/end：YYYY-MM-DD
    try:
        start_dt = datetime.strptime(start, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end, "%Y-%m-%d").date()
    except:
        raise HTTPException(400, "start/end 格式要 YYYY-MM-DD，例如 2025-01-01")

    if start_dt > end_dt:
        raise HTTPException(400, "start 不能大於 end")

    cache_key = f"hist:{stock_id}:{start_dt.isoformat()}:{end_dt.isoformat()}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    # 先試 TWSE（上市）
    twse_error = None
    try:
        data = get_history_twse(stock_id, start_dt, end_dt)
        if data:
            payload = {
                "stock_id": stock_id,
                "market": "twse",
                "start": start_dt.isoformat(),
                "end": end_dt.isoformat(),
                "count": len(data),
                "data": [{**d, "dt": d["dt"].isoformat()} for d in data],
            }
            cache_set(cache_key, payload)
            return payload
        twse_error = "TWSE 回空資料"
    except Exception as e:
        twse_error = str(e)

    # 再試 TPEx（上櫃）
    try:
        data2 = get_history_tpex(stock_id, start_dt, end_dt)
        if data2:
            payload = {
                "stock_id": stock_id,
                "market": "tpex",
                "start": start_dt.isoformat(),
                "end": end_dt.isoformat(),
                "count": len(data2),
                "data": [{**d, "dt": d["dt"].isoformat()} for d in data2],
            }
            cache_set(cache_key, payload)
            return payload
    except Exception as e2:
        raise HTTPException(502, f"TWSE 失敗：{twse_error}；TPEx 也失敗：{e2}")

    raise HTTPException(404, f"查不到資料：TWSE={twse_error}；TPEx=回空資料（可能區間無交易或代號不正確）")

