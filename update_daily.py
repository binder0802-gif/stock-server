# -*- coding: utf-8 -*-
"""
Created on Sat Jan  3 23:21:19 2026

@author: User
"""

from datetime import date, timedelta
from db import init_db, fetch_range, upsert_many

# 這裡匯入你 main.py 裡用來抓資料的函式
from main import get_history_twse, get_history_tpex

def update_one(stock_id: str, days_back: int = 10):
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=days_back)

    # 先看 DB 有沒有（可不做也行）
    _ = fetch_range(stock_id, start_dt, end_dt)

    # 抓資料：先 TWSE，沒有再 TPEx
    market = None
    items = []
    try:
        items = get_history_twse(stock_id, start_dt, end_dt)
        if items:
            market = "twse"
    except:
        items = []

    if not items:
        items = get_history_tpex(stock_id, start_dt, end_dt)
        if items:
            market = "tpex"

    if not items:
        print(f"{stock_id}: no data")
        return

    # 寫 DB
    n = upsert_many(stock_id, market, items)
    print(f"{stock_id}: upsert {n}")

def main():
    init_db()

    # 方案A：你先用固定清單（最簡單）
    watchlist = ["2330", "2317", "0050"]  # 你可改成自己的

    for sid in watchlist:
        update_one(sid, days_back=15)

if __name__ == "__main__":
    main()
