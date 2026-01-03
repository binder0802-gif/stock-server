# -*- coding: utf-8 -*-
"""
Created on Sat Jan  3 23:16:11 2026

@author: User
"""

import os
import psycopg2
from psycopg2.extras import execute_values

def get_conn():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    # Render 的 DATABASE_URL 通常可直接用
    return psycopg2.connect(dsn)

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_prices (
                stock_id TEXT NOT NULL,
                dt DATE NOT NULL,
                market TEXT NOT NULL,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                capacity BIGINT NOT NULL DEFAULT 0,
                txn INTEGER NOT NULL DEFAULT 0,
                avg_zhang DOUBLE PRECISION NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (stock_id, dt)
            );
            """)
        conn.commit()

def fetch_range(stock_id: str, start_dt, end_dt):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT stock_id, dt, market, open, high, low, close, capacity, txn, avg_zhang
                FROM daily_prices
                WHERE stock_id = %s AND dt BETWEEN %s AND %s
                ORDER BY dt ASC;
            """, (stock_id, start_dt, end_dt))
            rows = cur.fetchall()

    out = []
    for r in rows:
        out.append({
            "stock_id": r[0],
            "dt": r[1].isoformat(),
            "market": r[2],
            "open": r[3],
            "high": r[4],
            "low": r[5],
            "close": r[6],
            "capacity": int(r[7]),
            "txn": int(r[8]),
            "avg_zhang": float(r[9]),
        })
    return out

def upsert_many(stock_id: str, market: str, items: list[dict]):
    if not items:
        return 0
    values = []
    for it in items:
        values.append((
            stock_id,
            it["dt"],  # date object
            market,
            it.get("open"),
            it.get("high"),
            it.get("low"),
            it.get("close"),
            int(it.get("capacity", 0)),
            int(it.get("txn", 0)),
            float(it.get("avg_zhang", 0.0)),
        ))

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO daily_prices
                (stock_id, dt, market, open, high, low, close, capacity, txn, avg_zhang)
                VALUES %s
                ON CONFLICT (stock_id, dt) DO UPDATE SET
                    market = EXCLUDED.market,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    capacity = EXCLUDED.capacity,
                    txn = EXCLUDED.txn,
                    avg_zhang = EXCLUDED.avg_zhang,
                    updated_at = NOW();
            """, values)
        conn.commit()
    return len(items)
