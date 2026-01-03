# -*- coding: utf-8 -*-
"""
Created on Sat Jan  3 21:15:03 2026

@author: User
"""

from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/hello")
def hello():
    return {"message": "server is running"}

from fastapi import FastAPI, HTTPException
from datetime import date, datetime
import urllib.request, json

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok"}

# ---------- 工具 ----------
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
    with urllib.request.urlopen(url) as resp:
        raw = resp.read().decode("utf-8")
    data = json.loads(raw)
    if data.get("stat") != "OK":
        raise ValueError(data.get("stat", "TWSE query failed"))
    return data.get("data", [])

def _month_iter(start: date, end: date):
    y, m = start.year, start.month
    while (y < end.year) or (y == end.year and m <= end.month):
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1

def get_history_twse(stock_id: str, start_dt: date, end_dt: date):
    all_rows = []
    for yy, mm in _month_iter(start_dt, end_dt):
        try:
            all_rows.extend(_fetch_twse_month(stock_id, yy, mm))
        except:
            # 某些月份可能抓不到，先跳過
            continue

    out = []
    for row in all_rows:
        # TWSE欄位：日期,成交股數,成交金額,開盤價,最高價,最低價,收盤價,漲跌價差,成交筆數
        try:
            dt = _parse_twse_roc_date(row[0])
        except:
            continue
        if not (start_dt <= dt <= end_dt):
            continue

        capacity = int(str(row[1]).replace(",", "").strip() or "0")
        txn_str = str(row[8]).replace(",", "").strip()
        txn = int(txn_str) if txn_str.isdigit() else 0
        avg_zhang = capacity / txn / 1000 if txn else 0.0

        out.append({
            "dt": dt.isoformat(),
            "open": _parse_price(row[3]),
            "high": _parse_price(row[4]),
            "low": _parse_price(row[5]),
            "close": _parse_price(row[6]),
            "capacity": capacity,
            "txn": txn,
            "avg_zhang": avg_zhang
        })

    out.sort(key=lambda x: x["dt"])
    return out

# ---------- API ----------
@app.get("/stock/history")
def stock_history(stock_id: str, start: str, end: str):
    # start/end 用 YYYY-MM-DD
    try:
        start_dt = datetime.strptime(start, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end, "%Y-%m-%d").date()
    except:
        raise HTTPException(400, "start/end 格式要 YYYY-MM-DD，例如 2025-01-01")

    if start_dt > end_dt:
        raise HTTPException(400, "start 不能大於 end")

    data = get_history_twse(stock_id, start_dt, end_dt)

    if not data:
        raise HTTPException(404, "查不到資料（可能不是上市、或日期區間無交易日）")

    return {
        "stock_id": stock_id,
        "start": start_dt.isoformat(),
        "end": end_dt.isoformat(),
        "count": len(data),
        "data": data
    }
