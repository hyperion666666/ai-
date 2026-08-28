#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股民工作台 V3.9 —— 单文件整合旗舰版
=====================================
新增：
  - MACD / KDJ 技术指标
  - CSV 导出 API + 前端按钮
  - 表格排序 / 搜索筛选 / 暗亮主题切换
  - 异步数据库写入（线程池）
  - 数据库自动重连 + schema 版本管理
  - 分页 / 删除自选股 / 单只手动刷新
  - 前端骨架屏加载动画

作者：Stock Workstation Team
版本：3.9.0
日期：2026-08-28
"""
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import io
import json
import math
import multiprocessing
import os
import re
import socket
import sys
import threading
import time
import traceback
import urllib.request
import webbrowser
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple, Callable

# ============================================================
# 第一部分：启动前预处理
# ============================================================
APP_NAME = "股民工作台"
APP_VERSION = "3.9.0"
SCHEMA_VERSION = 2  # V3.9 数据库 schema 版本
BASE_DIR = Path(__file__).resolve().parent

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

def _is_running_from_temp() -> bool:
    if not getattr(sys, "frozen", False):
        return False
    import tempfile
    exe_path = os.path.abspath(sys.executable).lower()
    temp_dir = os.path.abspath(tempfile.gettempdir()).lower()
    return exe_path.startswith(temp_dir)

def _prep_environment():
    global BASE_DIR
    if getattr(sys, "frozen", False):
        exe_dir = os.path.dirname(os.path.abspath(sys.executable))
        BASE_DIR = Path(exe_dir)
        if _is_running_from_temp():
            print("=" * 62)
            print("  [重要] 你正在压缩包里直接运行本程序！")
            print("  此模式下数据不会保存，正确做法：")
            print("    1) 右键压缩包 → 「全部解压缩」")
            print("    2) 进入解压出的文件夹，再双击运行")
            print("=" * 62)
            try:
                input("按回车仅临时体验(数据不保存)，或直接关闭窗口...")
            except Exception:
                pass
        os.chdir(exe_dir)
    else:
        BASE_DIR = Path(__file__).resolve().parent
        os.chdir(BASE_DIR)

_prep_environment()

# ============================================================
# 第二部分：依赖导入与自检
# ============================================================
def _try_import(module_name: str) -> bool:
    try:
        __import__(module_name)
        return True
    except Exception:
        return False

def _selfcheck_dependencies():
    if getattr(sys, "frozen", False):
        return
    required = ["fastapi", "uvicorn", "duckdb", "pandas", "numpy",
                "apscheduler", "loguru", "pydantic", "yfinance"]
    missing = [m for m in required if not _try_import(m)]
    if missing:
        print(f"\n[错误] 缺少依赖: {', '.join(missing)}")
        print(f"[修复] pip install {' '.join(missing)}")
        try:
            input("\n按回车键退出...")
        except Exception:
            pass
        sys.exit(1)

_selfcheck_dependencies()

import duckdb
import numpy as np
import pandas as pd
import uvicorn
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from loguru import logger
from pydantic import BaseModel

# ============================================================
# 第三部分：配置
# ============================================================
@dataclass
class Config:
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DATA_DIR: Path = field(default_factory=lambda: Path("./data"))
    DB_PATH: Path = field(default_factory=lambda: Path("./data/stock.duckdb"))
    LOG_PATH: Path = field(default_factory=lambda: Path("./data/stock.log"))
    CACHE_TTL: int = 300
    FUNDAMENTAL_CACHE_TTL: int = 1800
    NEWS_CACHE_TTL: int = 900
    UPDATE_INTERVAL_MIN: int = 10
    RETRY_TIMES: int = 3
    RETRY_BACKOFF: float = 1.5
    MAX_WORKERS: int = 8                    # 并发抓取线程数（提升）
    BACKTEST_LOOKBACK_DAYS: int = 365
    BACKTEST_INITIAL_CAPITAL: float = 100_000.0
    DB_WRITE_WORKERS: int = 2               # 数据库写线程数
    DEFAULT_TICKERS: List[str] = field(default_factory=lambda: [
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
        "TSLA", "META", "BRK-B", "JPM", "V",
        "600519.SS", "000858.SZ", "601318.SS", "000333.SZ", "600036.SS",
    ])
    POSITIVE_WORDS: List[str] = field(default_factory=lambda: [
        "beat", "upgrade", "growth", "profit", "record", "strong",
        "positive", "outperform", "bullish", "gain", "surge", "raise",
        "买入", "增持", "增长", "盈利", "超预期", "利好", "突破",
    ])
    NEGATIVE_WORDS: List[str] = field(default_factory=lambda: [
        "miss", "downgrade", "loss", "decline", "weak", "negative",
        "underperform", "bearish", "drop", "fall", "cut", "risk",
        "卖出", "减持", "亏损", "下滑", "低于预期", "利空", "跌破",
    ])

def load_config_from_file() -> dict:
    cfg_file = BASE_DIR / "config.json"
    if cfg_file.exists():
        try:
            with open(cfg_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"读取 config.json 失败: {e}")
    return {}

def apply_config_overrides(config: Config, overrides: dict):
    for key, value in overrides.items():
        if hasattr(config, key):
            setattr(config, key, value)
    if "DATA_DIR" in overrides:
        config.DATA_DIR = Path(overrides["DATA_DIR"])
        config.DB_PATH = config.DATA_DIR / "stock.duckdb"
        config.LOG_PATH = config.DATA_DIR / "stock.log"

def parse_args():
    parser = argparse.ArgumentParser(description=APP_NAME)
    parser.add_argument("--port", type=int, help="指定监听端口")
    parser.add_argument("--host", type=str, help="指定监听地址")
    return parser.parse_args()

config = Config()
apply_config_overrides(config, load_config_from_file())
args = parse_args()
if args.port:
    config.PORT = args.port
if args.host:
    config.HOST = args.host

config.DATA_DIR.mkdir(parents=True, exist_ok=True)

logger.remove()
logger.add(sys.stderr, level="INFO",
           format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>")
logger.add(str(config.LOG_PATH), rotation="10 MB", retention="7 days", level="DEBUG", encoding="utf-8")

# ============================================================
# 第四部分：数据库层（V3.9：自动重连 + schema 版本）
# ============================================================
class Database:
    _lock = threading.Lock()
    _conn: Optional[duckdb.DuckDBPyConnection] = None
    _write_pool: Optional[concurrent.futures.ThreadPoolExecutor] = None

    @classmethod
    def _get_write_pool(cls) -> concurrent.futures.ThreadPoolExecutor:
        if cls._write_pool is None:
            cls._write_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=config.DB_WRITE_WORKERS, thread_name_prefix="db_write"
            )
        return cls._write_pool

    @classmethod
    def get_conn(cls) -> duckdb.DuckDBPyConnection:
        """获取连接，自动重连"""
        with cls._lock:
            if cls._conn is None:
                cls._conn = duckdb.connect(str(config.DB_PATH))
                cls._init_tables(cls._conn)
                cls._migrate(cls._conn)
            else:
                # 检测连接是否有效
                try:
                    cls._conn.execute("SELECT 1")
                except Exception:
                    logger.warning("数据库连接失效，尝试重连...")
                    try:
                        cls._conn.close()
                    except Exception:
                        pass
                    cls._conn = duckdb.connect(str(config.DB_PATH))
                    cls._init_tables(cls._conn)
                    cls._migrate(cls._conn)
            return cls._conn

    @classmethod
    def _init_tables(cls, conn):
        conn.execute("""
            CREATE TABLE IF NOT EXISTS quotes (
                ticker      VARCHAR NOT NULL,
                dt          TIMESTAMP NOT NULL,
                open        DOUBLE,
                high        DOUBLE,
                low         DOUBLE,
                close       DOUBLE,
                volume      BIGINT,
                adj_close   DOUBLE,
                PRIMARY KEY (ticker, dt)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id          INTEGER PRIMARY KEY,
                ticker      VARCHAR NOT NULL,
                signal_type VARCHAR NOT NULL,
                strength    DOUBLE,
                dt          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("CREATE SEQUENCE IF NOT EXISTS signals_id_seq START 1")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backtests (
                id          INTEGER PRIMARY KEY,
                strategy    VARCHAR NOT NULL,
                params      VARCHAR,
                result      VARCHAR,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("CREATE SEQUENCE IF NOT EXISTS backtests_id_seq START 1")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fundamentals (
                ticker          VARCHAR PRIMARY KEY,
                pe_ratio        DOUBLE, pb_ratio DOUBLE, market_cap DOUBLE,
                dividend_yield  DOUBLE, beta DOUBLE, eps DOUBLE,
                revenue         DOUBLE, profit_margin DOUBLE, roe DOUBLE,
                debt_to_equity  DOUBLE, week52_high DOUBLE, week52_low DOUBLE,
                avg_volume      BIGINT, sector VARCHAR, industry VARCHAR,
                description     VARCHAR,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id          INTEGER PRIMARY KEY,
                ticker      VARCHAR NOT NULL,
                title       VARCHAR NOT NULL,
                publisher   VARCHAR,
                link        VARCHAR,
                sentiment   VARCHAR,
                sentiment_score DOUBLE,
                published_at TIMESTAMP,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("CREATE SEQUENCE IF NOT EXISTS news_id_seq START 1")
        # V3.9: meta 表记录 schema 版本
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key         VARCHAR PRIMARY KEY,
                value       VARCHAR
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_quotes_ticker_dt ON quotes(ticker, dt)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_dt ON signals(dt)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_news_ticker ON news(ticker)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_news_published ON news(published_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fundamentals_ticker ON fundamentals(ticker)")

    @classmethod
    def _migrate(cls, conn):
        """数据库 schema 迁移"""
        try:
            df = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").df()
            current_version = int(df.iloc[0]['value']) if not df.empty else 0
        except Exception:
            current_version = 0
        if current_version < SCHEMA_VERSION:
            logger.info(f"数据库 schema 迁移: v{current_version} → v{SCHEMA_VERSION}")
            # 迁移步骤（若有）
            if current_version < 1:
                # V3.8 之前的迁移
                pass
            if current_version < 2:
                # V3.9 新增索引
                conn.execute("CREATE INDEX IF NOT EXISTS idx_news_published ON news(published_at)")
            conn.execute("""
                INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?)
            """, [str(SCHEMA_VERSION)])

    @classmethod
    def execute(cls, sql: str, params: list = None):
        with cls._lock:
            conn = cls.get_conn()
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)

    @classmethod
    def execute_async(cls, sql: str, params: list = None):
        """异步执行数据库写入（不阻塞调用线程）"""
        pool = cls._get_write_pool()
        pool.submit(cls.execute, sql, params)

    @classmethod
    def query_df(cls, sql: str, params: list = None) -> pd.DataFrame:
        with cls._lock:
            conn = cls.get_conn()
            if params:
                return conn.execute(sql, params).df()
            return conn.execute(sql).df()

    @classmethod
    def insert_quotes_batch(cls, df: pd.DataFrame):
        if df.empty:
            return
        df = df.reset_index(drop=True)
        rename_map = {
            'Ticker': 'ticker', 'ticker': 'ticker',
            'Datetime': 'dt', 'datetime': 'dt', 'Date': 'dt',
            'Open': 'open', 'open': 'open',
            'High': 'high', 'high': 'high',
            'Low': 'low', 'low': 'low',
            'Close': 'close', 'close': 'close',
            'Volume': 'volume', 'volume': 'volume',
            'Adj Close': 'adj_close', 'adj_close': 'adj_close',
        }
        df = df.rename(columns=rename_map)
        for col in ['open', 'high', 'low', 'close', 'volume', 'adj_close']:
            if col not in df.columns:
                df[col] = np.nan
        df = df[['ticker', 'dt', 'open', 'high', 'low', 'close', 'volume', 'adj_close']]
        df['dt'] = pd.to_datetime(df['dt'], errors='coerce')
        df = df.dropna(subset=['ticker', 'dt'])
        if df.empty:
            return
        with cls._lock:
            conn = cls.get_conn()
            conn.register('temp_quotes', df)
            conn.execute("""
                INSERT OR REPLACE INTO quotes
                SELECT ticker, dt, open, high, low, close, volume, adj_close
                FROM temp_quotes
            """)
            conn.unregister('temp_quotes')

    @classmethod
    def insert_quotes_batch_async(cls, df: pd.DataFrame):
        """异步批量写入行情"""
        pool = cls._get_write_pool()
        pool.submit(cls.insert_quotes_batch, df)

    @classmethod
    def get_latest_quotes(cls) -> pd.DataFrame:
        return cls.query_df("""
            SELECT q.* FROM quotes q
            INNER JOIN (
                SELECT ticker, MAX(dt) as max_dt FROM quotes GROUP BY ticker
            ) latest ON q.ticker = latest.ticker AND q.dt = latest.max_dt
            ORDER BY q.ticker
        """)

    @classmethod
    def get_quote_history(cls, ticker: str, days: int = 30) -> pd.DataFrame:
        return cls.query_df("""
            SELECT * FROM quotes WHERE ticker = ? AND dt >= ? ORDER BY dt
        """, [ticker, datetime.now() - timedelta(days=days)])

    @classmethod
    def insert_signal(cls, ticker: str, signal_type: str, strength: float):
        cls.execute("""
            INSERT INTO signals (id, ticker, signal_type, strength)
            VALUES (nextval('signals_id_seq'), ?, ?, ?)
        """, [ticker, signal_type, strength])

    @classmethod
    def get_recent_signals(cls, limit: int = 50) -> pd.DataFrame:
        return cls.query_df("SELECT * FROM signals ORDER BY dt DESC LIMIT ?", [limit])

    @classmethod
    def insert_backtest(cls, strategy: str, params: dict, result: dict) -> int:
        params_json = json.dumps(params, ensure_ascii=False, default=str)
        result_json = json.dumps(result, ensure_ascii=False, default=str)
        cls.execute("""
            INSERT INTO backtests (id, strategy, params, result)
            VALUES (nextval('backtests_id_seq'), ?, ?, ?)
        """, [strategy, params_json, result_json])
        df = cls.query_df("SELECT currval('backtests_id_seq') as id")
        return int(df.iloc[0]['id'])

    @classmethod
    def get_backtests(cls, limit: int = 20) -> pd.DataFrame:
        return cls.query_df("SELECT * FROM backtests ORDER BY created_at DESC LIMIT ?", [limit])

    @classmethod
    def upsert_fundamental(cls, ticker: str, data: Dict[str, Any]):
        cls.execute("""
            INSERT OR REPLACE INTO fundamentals
            (ticker, pe_ratio, pb_ratio, market_cap, dividend_yield, beta, eps,
             revenue, profit_margin, roe, debt_to_equity, week52_high, week52_low,
             avg_volume, sector, industry, description, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [ticker,
            data.get('pe_ratio'), data.get('pb_ratio'), data.get('market_cap'),
            data.get('dividend_yield'), data.get('beta'), data.get('eps'),
            data.get('revenue'), data.get('profit_margin'), data.get('roe'),
            data.get('debt_to_equity'), data.get('week52_high'), data.get('week52_low'),
            data.get('avg_volume'), data.get('sector'), data.get('industry'),
            data.get('description')])

    @classmethod
    def get_fundamental(cls, ticker: str) -> Optional[Dict[str, Any]]:
        df = cls.query_df("SELECT * FROM fundamentals WHERE ticker = ?", [ticker])
        if df.empty:
            return None
        row = df.iloc[0]
        result = {}
        for col in df.columns:
            val = row[col]
            if pd.isna(val):
                result[col] = None
            elif hasattr(val, 'isoformat'):
                result[col] = val.isoformat()
            elif isinstance(val, (np.integer,)):
                result[col] = int(val)
            elif isinstance(val, (np.floating,)):
                result[col] = float(val)
            else:
                result[col] = val
        return result

    @classmethod
    def get_all_fundamentals(cls) -> pd.DataFrame:
        return cls.query_df("SELECT * FROM fundamentals ORDER BY ticker")

    @classmethod
    def insert_news(cls, ticker: str, title: str, publisher: str, link: str,
                    sentiment: str, sentiment_score: float, published_at: datetime = None):
        existing = cls.query_df(
            "SELECT COUNT(*) as cnt FROM news WHERE ticker = ? AND title = ?",
            [ticker, title]
        )
        if existing.iloc[0]['cnt'] > 0:
            return
        cls.execute("""
            INSERT INTO news (id, ticker, title, publisher, link, sentiment, sentiment_score, published_at)
            VALUES (nextval('news_id_seq'), ?, ?, ?, ?, ?, ?, ?)
        """, [ticker, title, publisher, link, sentiment, sentiment_score, published_at])

    @classmethod
    def get_news_by_ticker(cls, ticker: str, limit: int = 20) -> pd.DataFrame:
        return cls.query_df("""
            SELECT * FROM news WHERE ticker = ? ORDER BY published_at DESC NULLS LAST LIMIT ?
        """, [ticker, limit])

    @classmethod
    def get_all_news(cls, limit: int = 50, offset: int = 0) -> pd.DataFrame:
        return cls.query_df("""
            SELECT * FROM news ORDER BY published_at DESC NULLS LAST LIMIT ? OFFSET ?
        """, [limit, offset])

    @classmethod
    def delete_ticker_data(cls, ticker: str):
        """删除某只股票的所有数据"""
        cls.execute("DELETE FROM quotes WHERE ticker = ?", [ticker])
        cls.execute("DELETE FROM signals WHERE ticker = ?", [ticker])
        cls.execute("DELETE FROM fundamentals WHERE ticker = ?", [ticker])
        cls.execute("DELETE FROM news WHERE ticker = ?", [ticker])

    @classmethod
    def archive_old_quotes(cls, days: int = 365):
        cutoff = datetime.now() - timedelta(days=days)
        archive_path = config.DATA_DIR / "archive"
        archive_path.mkdir(parents=True, exist_ok=True)
        df = cls.query_df("SELECT * FROM quotes WHERE dt < ?", [cutoff])
        if df.empty:
            return 0
        fname = archive_path / f"quotes_archive_{cutoff.strftime('%Y%m%d')}.csv"
        df.to_csv(fname, index=False, encoding='utf-8-sig')
        cls.execute("DELETE FROM quotes WHERE dt < ?", [cutoff])
        logger.info(f"已归档 {len(df)} 条行情记录 → {fname}")
        return len(df)

    @classmethod
    def close(cls):
        with cls._lock:
            if cls._write_pool is not None:
                cls._write_pool.shutdown(wait=True)
                cls._write_pool = None
            if cls._conn is not None:
                cls._conn.close()
                cls._conn = None

# ============================================================
# 第五部分：行情抓取
# ============================================================
class MarketDataFetcher:
    _cache: Dict[str, Tuple[float, pd.DataFrame]] = {}
    _cache_lock = threading.Lock()

    @classmethod
    def _get_cached(cls, key: str) -> Optional[pd.DataFrame]:
        with cls._cache_lock:
            item = cls._cache.get(key)
            if item and (time.time() - item[0]) < config.CACHE_TTL:
                return item[1]
        return None

    @classmethod
    def _set_cache(cls, key: str, df: pd.DataFrame):
        with cls._cache_lock:
            cls._cache[key] = (time.time(), df)

    @classmethod
    def fetch_one(cls, ticker: str, period: str = "1d", interval: str = "1m") -> Optional[pd.DataFrame]:
        cache_key = f"{ticker}:{period}:{interval}"
        cached = cls._get_cached(cache_key)
        if cached is not None:
            return cached
        last_err = None
        for attempt in range(config.RETRY_TIMES):
            try:
                t = yf.Ticker(ticker)
                df = t.history(period=period, interval=interval, auto_adjust=False)
                if df is not None and not df.empty:
                    df['ticker'] = ticker
                    cls._set_cache(cache_key, df)
                    return df
            except Exception as e:
                last_err = e
                logger.warning(f"抓取 {ticker} 第 {attempt+1} 次失败: {e}")
                if attempt < config.RETRY_TIMES - 1:
                    time.sleep(config.RETRY_BACKOFF ** attempt)
        logger.error(f"抓取 {ticker} 最终失败: {last_err}")
        return None

    @classmethod
    def fetch_daily(cls, ticker: str, days: int = 365) -> Optional[pd.DataFrame]:
        cache_key = f"{ticker}:{days}d:1d"
        cached = cls._get_cached(cache_key)
        if cached is not None:
            return cached
        try:
            t = yf.Ticker(ticker)
            df = t.history(period=f"{days}d", interval="1d", auto_adjust=False)
            if df is not None and not df.empty:
                df['ticker'] = ticker
                cls._set_cache(cache_key, df)
                return df
        except Exception as e:
            logger.error(f"抓取 {ticker} 日线失败: {e}")
        return None

    @classmethod
    def fetch_batch_daily(cls, tickers: List[str], days: int = 365) -> pd.DataFrame:
        frames = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
            future_to_ticker = {executor.submit(cls.fetch_daily, tk, days): tk for tk in tickers}
            for future in concurrent.futures.as_completed(future_to_ticker):
                tk = future_to_ticker[future]
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        frames.append(df)
                except Exception as e:
                    logger.error(f"并发抓取日线 {tk} 异常: {e}")
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, axis=0).reset_index(drop=True)

    @classmethod
    def clear_cache(cls):
        with cls._cache_lock:
            cls._cache.clear()

# ============================================================
# 第六部分：基本面 / 资金流 / 新闻
# ============================================================
class FundamentalFetcher:
    _cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
    _cache_lock = threading.Lock()

    @classmethod
    def _get_cached(cls, ticker: str) -> Optional[Dict[str, Any]]:
        with cls._cache_lock:
            item = cls._cache.get(ticker)
            if item and (time.time() - item[0]) < config.FUNDAMENTAL_CACHE_TTL:
                return item[1]
        return None

    @classmethod
    def _set_cache(cls, ticker: str, data: Dict[str, Any]):
        with cls._cache_lock:
            cls._cache[ticker] = (time.time(), data)

    @classmethod
    def _safe_float(cls, value) -> Optional[float]:
        try:
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return None
            return float(value)
        except (ValueError, TypeError):
            return None

    @classmethod
    def fetch(cls, ticker: str) -> Optional[Dict[str, Any]]:
        cached = cls._get_cached(ticker)
        if cached is not None:
            return cached
        try:
            t = yf.Ticker(ticker)
            info = t.info
            if not info or len(info) < 5:
                return None
            data = {
                'ticker': ticker,
                'pe_ratio': cls._safe_float(info.get('trailingPE') or info.get('forwardPE')),
                'pb_ratio': cls._safe_float(info.get('priceToBook')),
                'market_cap': cls._safe_float(info.get('marketCap')),
                'dividend_yield': cls._safe_float(info.get('dividendYield')),
                'beta': cls._safe_float(info.get('beta')),
                'eps': cls._safe_float(info.get('trailingEps')),
                'revenue': cls._safe_float(info.get('totalRevenue')),
                'profit_margin': cls._safe_float(info.get('profitMargins')),
                'roe': cls._safe_float(info.get('returnOnEquity')),
                'debt_to_equity': cls._safe_float(info.get('debtToEquity')),
                'week52_high': cls._safe_float(info.get('fiftyTwoWeekHigh')),
                'week52_low': cls._safe_float(info.get('fiftyTwoWeekLow')),
                'avg_volume': cls._safe_float(info.get('averageVolume')),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'description': (info.get('longBusinessSummary', '') or '')[:300],
            }
            cls._set_cache(ticker, data)
            return data
        except Exception as e:
            logger.warning(f"获取 {ticker} 基本面失败: {e}")
            return None

    @classmethod
    def fetch_batch(cls, tickers: List[str]) -> Dict[str, Dict[str, Any]]:
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
            future_to_ticker = {executor.submit(cls.fetch, tk): tk for tk in tickers}
            for future in concurrent.futures.as_completed(future_to_ticker):
                tk = future_to_ticker[future]
                try:
                    data = future.result()
                    if data:
                        results[tk] = data
                except Exception:
                    pass
        return results

    @classmethod
    def clear_cache(cls):
        with cls._cache_lock:
            cls._cache.clear()

class FundFlowCalculator:
    @staticmethod
    def calculate_flow(df: pd.DataFrame, days: int = 20) -> Dict[str, Any]:
        if df.empty or 'close' not in df.columns or len(df) < 5:
            return {"error": "数据不足"}
        df = df.sort_values('dt').reset_index(drop=True)
        close = df['close'].astype(float)
        volume = df['volume'].astype(float).fillna(0)
        high = df['high'].astype(float) if 'high' in df.columns else close
        low = df['low'].astype(float) if 'low' in df.columns else close
        typical_price = (high + low + close) / 3
        raw_money_flow = typical_price * volume
        price_change = typical_price.diff()
        positive_flow = raw_money_flow.where(price_change > 0, 0)
        negative_flow = raw_money_flow.where(price_change < 0, 0)
        period = min(14, len(df) - 1)
        pos_sum = positive_flow.rolling(window=period).sum()
        neg_sum = negative_flow.rolling(window=period).sum()
        mfi = 100 - (100 / (1 + (pos_sum / (neg_sum + 1e-10))))
        avg_volume = volume.rolling(window=min(days, len(df))).mean()
        rel_volume = volume / (avg_volume + 1e-10)
        last_5 = df.tail(5)
        price_trend = last_5['close'].astype(float).pct_change().sum()
        volume_trend = last_5['volume'].astype(float).pct_change().sum()
        divergence_signal = "无"
        if price_trend > 0 and volume_trend < 0:
            divergence_signal = "顶背离（价涨量缩，资金流出）"
        elif price_trend < 0 and volume_trend > 0:
            divergence_signal = "底背离（价跌量增，资金流入）"
        mfi_score = float(mfi.iloc[-1] - 50) if not mfi.empty and not pd.isna(mfi.iloc[-1]) else 0
        rel_vol_score = (float(rel_volume.iloc[-1]) - 1) * 20 if not rel_volume.empty and not pd.isna(rel_volume.iloc[-1]) else 0
        divergence_score = 0
        if divergence_signal.startswith("底背离"):
            divergence_score = 15
        elif divergence_signal.startswith("顶背离"):
            divergence_score = -15
        flow_score = max(-100, min(100, mfi_score + rel_vol_score + divergence_score))
        flow_direction = "流入" if flow_score > 20 else ("流出" if flow_score < -20 else "中性")
        return {
            "mfi": round(float(mfi.iloc[-1]), 2) if not mfi.empty and not pd.isna(mfi.iloc[-1]) else None,
            "relative_volume": round(float(rel_volume.iloc[-1]), 2) if not rel_volume.empty and not pd.isna(rel_volume.iloc[-1]) else None,
            "divergence_signal": divergence_signal,
            "flow_score": round(flow_score, 2),
            "flow_direction": flow_direction,
            "avg_volume_20d": round(float(avg_volume.iloc[-1]), 0) if not avg_volume.empty and not pd.isna(avg_volume.iloc[-1]) else None,
            "current_volume": round(float(volume.iloc[-1]), 0) if len(volume) > 0 else None,
        }

class NewsFetcher:
    _cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
    _cache_lock = threading.Lock()

    @classmethod
    def _get_cached(cls, ticker: str) -> Optional[List[Dict[str, Any]]]:
        with cls._cache_lock:
            item = cls._cache.get(ticker)
            if item and (time.time() - item[0]) < config.NEWS_CACHE_TTL:
                return item[1]
        return None

    @classmethod
    def _set_cache(cls, ticker: str, news: List[Dict[str, Any]]):
        with cls._cache_lock:
            cls._cache[ticker] = (time.time(), news)

    @classmethod
    def _analyze_sentiment(cls, title: str, publisher: str = "") -> Tuple[str, float]:
        text = (title + " " + publisher).lower()
        positive_count = sum(1 for w in config.POSITIVE_WORDS if w.lower() in text)
        negative_count = sum(1 for w in config.NEGATIVE_WORDS if w.lower() in text)
        total = positive_count + negative_count
        if total == 0:
            return "中性", 0.0
        score = (positive_count - negative_count) / total
        if score > 0.2:
            return "正面", round(score, 2)
        elif score < -0.2:
            return "负面", round(score, 2)
        return "中性", round(score, 2)

    @classmethod
    def fetch(cls, ticker: str, limit: int = 10) -> List[Dict[str, Any]]:
        cached = cls._get_cached(ticker)
        if cached is not None:
            return cached[:limit]
        try:
            t = yf.Ticker(ticker)
            news = t.news
            results = []
            for item in news[:limit]:
                title = item.get('title', '') or ''
                publisher = item.get('publisher', '') or ''
                link = item.get('link', '') or ''
                published_at = None
                provider_publish_time = item.get('providerPublishTime', 0)
                if provider_publish_time:
                    try:
                        published_at = datetime.fromtimestamp(provider_publish_time)
                    except Exception:
                        pass
                sentiment, score = cls._analyze_sentiment(title, publisher)
                results.append({
                    'ticker': ticker, 'title': title, 'publisher': publisher,
                    'link': link, 'sentiment': sentiment,
                    'sentiment_score': score, 'published_at': published_at,
                })
            cls._set_cache(ticker, results)
            return results
        except Exception as e:
            logger.warning(f"获取 {ticker} 新闻失败: {e}")
            return []

    @classmethod
    def fetch_batch(cls, tickers: List[str], limit: int = 10) -> List[Dict[str, Any]]:
        all_news = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
            future_to_ticker = {executor.submit(cls.fetch, tk, limit): tk for tk in tickers}
            for future in concurrent.futures.as_completed(future_to_ticker):
                tk = future_to_ticker[future]
                try:
                    all_news.extend(future.result())
                except Exception:
                    pass
        return all_news

    @classmethod
    def clear_cache(cls):
        with cls._cache_lock:
            cls._cache.clear()

# ============================================================
# 第七部分：技术指标 + 信号 + 回测
# ============================================================
class TechnicalIndicators:
    """V3.9 新增：MACD / KDJ"""

    @staticmethod
    def macd(prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
        """返回 (MACD线, 信号线, 柱状图)"""
        ema_fast = prices.ewm(span=fast, adjust=False).mean()
        ema_slow = prices.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def kdj(df: pd.DataFrame, n: int = 9, k_period: int = 3, d_period: int = 3):
        """返回 (K, D, J)"""
        low_n = df['low'].rolling(window=n).min()
        high_n = df['high'].rolling(window=n).max()
        rsv = (df['close'] - low_n) / (high_n - low_n + 1e-10) * 100
        k = rsv.ewm(com=k_period - 1, adjust=False).mean()
        d = k.ewm(com=d_period - 1, adjust=False).mean()
        j = 3 * k - 2 * d
        return k, d, j

    @classmethod
    def get_all_indicators(cls, df: pd.DataFrame) -> Dict[str, Any]:
        """获取全部技术指标"""
        if df.empty or 'close' not in df.columns or len(df) < 30:
            return {}
        df = df.sort_values('dt').reset_index(drop=True)
        close = df['close'].astype(float)
        # MACD
        macd_line, signal_line, histogram = cls.macd(close)
        # KDJ
        k, d, j = cls.kdj(df)
        # RSI
        rsi = SignalCalculator.rsi(close)
        return {
            "macd": {
                "macd_line": round(float(macd_line.iloc[-1]), 4) if not pd.isna(macd_line.iloc[-1]) else None,
                "signal_line": round(float(signal_line.iloc[-1]), 4) if not pd.isna(signal_line.iloc[-1]) else None,
                "histogram": round(float(histogram.iloc[-1]), 4) if not pd.isna(histogram.iloc[-1]) else None,
                "trend": "金叉" if macd_line.iloc[-1] > signal_line.iloc[-1] else "死叉",
            },
            "kdj": {
                "k": round(float(k.iloc[-1]), 2) if not pd.isna(k.iloc[-1]) else None,
                "d": round(float(d.iloc[-1]), 2) if not pd.isna(d.iloc[-1]) else None,
                "j": round(float(j.iloc[-1]), 2) if not pd.isna(j.iloc[-1]) else None,
                "signal": "超买" if j.iloc[-1] > 100 else ("超卖" if j.iloc[-1] < 0 else "中性"),
            },
            "rsi": round(float(rsi.iloc[-1]), 2) if not rsi.empty and not pd.isna(rsi.iloc[-1]) else None,
        }

class SignalCalculator:
    @staticmethod
    def rsi(prices: pd.Series, period: int = 14) -> pd.Series:
        delta = prices.diff()
        gain = delta.clip(lower=0).rolling(window=period).mean()
        loss = (-delta.clip(upper=0)).rolling(window=period).mean()
        rs = gain / (loss + 1e-10)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def bollinger_bands(prices: pd.Series, window: int = 20, num_std: float = 2.0):
        sma = prices.rolling(window=window).mean()
        std = prices.rolling(window=window).std()
        return sma + num_std * std, sma, sma - num_std * std

    @classmethod
    def detect_extreme_signals(cls, df: pd.DataFrame) -> List[Dict[str, Any]]:
        signals = []
        if df.empty or 'close' not in df.columns:
            return signals
        for ticker in df['ticker'].unique():
            sub = df[df['ticker'] == ticker].sort_values('dt')
            if len(sub) < 30:
                continue
            close = sub['close'].astype(float)
            rsi = cls.rsi(close)
            if not rsi.empty and not pd.isna(rsi.iloc[-1]):
                last_rsi = rsi.iloc[-1]
                if last_rsi < 20:
                    signals.append({"ticker": ticker, "signal_type": "RSI_超卖",
                                    "strength": round(float(last_rsi), 2),
                                    "dt": sub['dt'].iloc[-1].isoformat()})
                elif last_rsi > 80:
                    signals.append({"ticker": ticker, "signal_type": "RSI_超买",
                                    "strength": round(float(last_rsi), 2),
                                    "dt": sub['dt'].iloc[-1].isoformat()})
            upper, mid, lower = cls.bollinger_bands(close)
            if not pd.isna(upper.iloc[-1]):
                last_close = close.iloc[-1]
                if last_close > upper.iloc[-1]:
                    signals.append({"ticker": ticker, "signal_type": "布林上轨突破",
                                    "strength": round(float((last_close - upper.iloc[-1]) / upper.iloc[-1] * 100), 2),
                                    "dt": sub['dt'].iloc[-1].isoformat()})
                elif last_close < lower.iloc[-1]:
                    signals.append({"ticker": ticker, "signal_type": "布林下轨突破",
                                    "strength": round(float((lower.iloc[-1] - last_close) / lower.iloc[-1] * 100), 2),
                                    "dt": sub['dt'].iloc[-1].isoformat()})
        return signals

class BacktestEngine:
    @staticmethod
    def run_ma_cross(df: pd.DataFrame, short_window: int = 5, long_window: int = 20,
                     initial_capital: float = 100_000.0) -> Dict[str, Any]:
        if df.empty or 'close' not in df.columns:
            return {"error": "数据为空或缺少 close 列"}
        df = df.sort_values('dt').reset_index(drop=True)
        close = df['close'].astype(float)
        df['short_ma'] = close.rolling(window=short_window).mean()
        df['long_ma'] = close.rolling(window=long_window).mean()
        df['signal'] = 0
        df.loc[df['short_ma'] > df['long_ma'], 'signal'] = 1
        df['position'] = df['signal'].shift(1).fillna(0)
        df['daily_return'] = close.pct_change() * df['position']
        df['daily_return'] = df['daily_return'].fillna(0)
        df['equity'] = initial_capital * (1 + df['daily_return']).cumprod()
        total_return = float(df['equity'].iloc[-1] / initial_capital - 1)
        n_days = max(len(df), 1)
        annual_return = float((1 + total_return) ** (365 / n_days) - 1) if total_return > -1 else -1.0
        cummax = df['equity'].cummax()
        drawdown = (df['equity'] - cummax) / cummax
        max_drawdown = float(drawdown.min())
        daily_std = df['daily_return'].std()
        sharpe = float((df['daily_return'].mean() / (daily_std + 1e-10)) * math.sqrt(252))
        trades = int((df['position'].diff() != 0).sum())
        return {
            "strategy": "MA_CROSS",
            "params": {"short_window": short_window, "long_window": long_window,
                       "initial_capital": initial_capital},
            "start_date": df['dt'].iloc[0].isoformat() if len(df) > 0 else None,
            "end_date": df['dt'].iloc[-1].isoformat() if len(df) > 0 else None,
            "total_return_pct": round(total_return * 100, 2),
            "annual_return_pct": round(annual_return * 100, 2),
            "max_drawdown_pct": round(max_drawdown * 100, 2),
            "sharpe_ratio": round(sharpe, 3),
            "total_trades": trades,
            "final_equity": round(float(df['equity'].iloc[-1]), 2),
            "equity_curve": [
                {"dt": row['dt'].isoformat(), "equity": round(float(row['equity']), 2)}
                for _, row in df.iterrows()
            ],
        }

# ============================================================
# 第八部分：内嵌前端（V3.9：主题切换/排序/搜索/导出）
# ============================================================
FRONTEND_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>股民工作台 V3.9.0</title>
<style>
  :root {
    --bg: #0f1117; --bg-card: #1a1d29; --bg-hover: #252938;
    --text: #e4e6ed; --text-dim: #8b8fa3;
    --accent: #4f8cff; --green: #26a65b; --red: #e74c3c;
    --border: #2c3040; --yellow: #f0ad4e;
  }
  [data-theme="light"] {
    --bg: #f5f6fa; --bg-card: #ffffff; --bg-hover: #f0f2f7;
    --text: #1a1d29; --text-dim: #6b6f80;
    --border: #dde1eb;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Segoe UI', 'PingFang SC', 'Microsoft YaHei', sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; transition: background 0.2s; }
  .app-header { display: flex; align-items: center; justify-content: space-between; padding: 16px 24px; background: var(--bg-card); border-bottom: 1px solid var(--border); flex-wrap: wrap; gap: 12px; }
  .app-header h1 { font-size: 20px; font-weight: 600; }
  .app-header h1 span { color: var(--accent); }
  .header-actions { display: flex; gap: 8px; flex-wrap: wrap; }
  .btn { padding: 8px 16px; border-radius: 6px; border: 1px solid var(--border); background: var(--bg-hover); color: var(--text); cursor: pointer; font-size: 13px; transition: all 0.15s; white-space: nowrap; }
  .btn:hover { background: var(--accent); border-color: var(--accent); color: #fff; }
  .btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .btn.primary { background: var(--accent); border-color: var(--accent); color: #fff; }
  .status-bar { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; padding: 10px 24px; font-size: 12px; color: var(--text-dim); background: var(--bg-card); border-bottom: 1px solid var(--border); }
  .status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); animation: pulse 2s infinite; }
  .status-dot.offline { background: var(--red); animation: none; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
  .container { padding: 20px 24px; max-width: 1600px; margin: 0 auto; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 16px; }
  .card { background: var(--bg-card); border: 1px solid var(--border); border-radius: 10px; padding: 18px; overflow: hidden; transition: background 0.2s; }
  .card h2 { font-size: 15px; font-weight: 600; margin-bottom: 14px; display: flex; align-items: center; justify-content: space-between; }
  .card h2 .badge { font-size: 11px; padding: 3px 8px; border-radius: 20px; background: var(--bg-hover); color: var(--text-dim); }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid var(--border); }
  th { color: var(--text-dim); font-weight: 500; font-size: 11px; text-transform: uppercase; cursor: pointer; user-select: none; white-space: nowrap; }
  th:hover { color: var(--accent); }
  th .sort-icon { font-size: 9px; margin-left: 4px; }
  tr:hover td { background: var(--bg-hover); cursor: pointer; }
  .up { color: var(--green); } .down { color: var(--red); } .flat { color: var(--text-dim); }
  .sentiment-positive { color: var(--green); font-weight: 600; }
  .sentiment-negative { color: var(--red); font-weight: 600; }
  .sentiment-neutral { color: var(--yellow); font-weight: 600; }
  .signal-list, .news-list { max-height: 350px; overflow-y: auto; }
  .signal-item, .news-item { padding: 8px 4px; border-bottom: 1px solid var(--border); }
  .signal-item:last-child, .news-item:last-child { border-bottom: none; }
  .signal-type { font-size: 12px; color: var(--text-dim); } .signal-ticker { font-weight: 600; } .signal-strength { font-weight: 600; }
  .news-title { font-size: 13px; margin-bottom: 4px; }
  .news-meta { font-size: 11px; color: var(--text-dim); display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
  .empty-state { text-align: center; padding: 30px; color: var(--text-dim); font-size: 13px; }
  .skeleton { background: linear-gradient(90deg, var(--bg-hover) 25%, var(--border) 50%, var(--bg-hover) 75%); background-size: 200% 100%; animation: shimmer 1.5s infinite; border-radius: 4px; height: 16px; margin: 4px 0; }
  @keyframes shimmer { 0% { background-position: -200% 0; } 100% { background-position: 200% 0; } }
  .modal-overlay { display: none; position: fixed; inset: 0; z-index: 100; background: rgba(0,0,0,0.7); align-items: center; justify-content: center; }
  .modal-overlay.active { display: flex; }
  .modal { background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; padding: 24px; width: 92%; max-width: 700px; max-height: 85vh; overflow-y: auto; }
  .modal h3 { margin-bottom: 16px; font-size: 17px; }
  .modal label { display: block; margin-bottom: 6px; font-size: 13px; color: var(--text-dim); }
  .modal input { width: 100%; padding: 8px 12px; border-radius: 6px; border: 1px solid var(--border); background: var(--bg); color: var(--text); font-size: 13px; margin-bottom: 14px; }
  .modal .btn-row { display: flex; justify-content: flex-end; gap: 8px; }
  .toast { position: fixed; bottom: 24px; right: 24px; z-index: 200; padding: 12px 20px; border-radius: 8px; font-size: 13px; background: var(--bg-card); border: 1px solid var(--border); display: none; animation: slideIn 0.3s ease; }
  @keyframes slideIn { from { transform: translateY(20px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }
  .ticker-add { display: flex; gap: 8px; margin-bottom: 12px; }
  .ticker-add input { flex: 1; padding: 8px 12px; border-radius: 6px; border: 1px solid var(--border); background: var(--bg); color: var(--text); }
  .search-box { width: 100%; padding: 8px 12px; border-radius: 6px; border: 1px solid var(--border); background: var(--bg); color: var(--text); font-size: 13px; margin-bottom: 12px; }
  .detail-section { margin-bottom: 16px; }
  .detail-section h4 { font-size: 14px; margin-bottom: 8px; color: var(--accent); }
  .detail-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 8px; }
  .detail-item { background: var(--bg-hover); padding: 8px 12px; border-radius: 6px; }
  .detail-item .label { font-size: 11px; color: var(--text-dim); }
  .detail-item .value { font-size: 15px; font-weight: 600; margin-top: 2px; }
  @media (max-width: 768px) { .container { padding: 12px; } .grid { grid-template-columns: 1fr; } .app-header { padding: 12px 16px; } }
</style>
</head>
<body data-theme="dark">
<header class="app-header">
  <h1>📊 股民<span>工作台</span> <small style="font-size:12px;color:var(--text-dim)">V3.9</small></h1>
  <div class="header-actions">
    <button class="btn primary" onclick="refreshAll()">🔄 刷新</button>
    <button class="btn" onclick="advancedRefresh()">⚡ 高级刷新</button>
    <button class="btn" onclick="exportCSV()">📥 导出CSV</button>
    <button class="btn" onclick="openBacktestModal()">▶ 回测</button>
    <button class="btn" onclick="toggleTheme()">🌓 主题</button>
    <button class="btn" onclick="archiveData()">🗄 归档</button>
  </div>
</header>
<div class="status-bar">
  <div class="status-dot" id="statusDot"></div>
  <span id="statusText">连接中...</span>
  <span style="margin-left:auto" id="lastUpdate"></span>
</div>
<div class="container">
  <div class="grid">
    <div class="card">
      <h2>📈 行情总览 <span class="badge" id="quoteCount">0 只</span></h2>
      <div class="ticker-add">
        <input type="text" id="newTicker" placeholder="添加股票代码，如 TSLA 或 0700.HK" />
        <button class="btn" onclick="addTicker()">添加</button>
      </div>
      <input type="text" class="search-box" id="searchBox" placeholder="🔍 搜索股票代码..." oninput="filterQuotes(this.value)" />
      <div style="overflow-x:auto">
        <table id="quotesTable">
          <thead><tr>
            <th onclick="sortQuotes('ticker')">股票 <span class="sort-icon">⇅</span></th>
            <th onclick="sortQuotes('close')">最新价 <span class="sort-icon">⇅</span></th>
            <th onclick="sortQuotes('change_pct')">涨跌幅 <span class="sort-icon">⇅</span></th>
            <th onclick="sortQuotes('high')">最高 <span class="sort-icon">⇅</span></th>
            <th onclick="sortQuotes('low')">最低 <span class="sort-icon">⇅</span></th>
            <th onclick="sortQuotes('volume')">成交量 <span class="sort-icon">⇅</span></th>
          </tr></thead>
          <tbody id="quotesBody"></tbody>
        </table>
      </div>
      <div class="empty-state" id="quotesEmpty">暂无行情数据</div>
    </div>
    <div class="card">
      <h2>⚠️ 极值信号 <span class="badge" id="signalCount">0 条</span></h2>
      <div class="signal-list" id="signalList"></div>
      <div class="empty-state" id="signalEmpty">暂无极值信号</div>
    </div>
    <div class="card">
      <h2>💰 资金流向 <span class="badge">量价分析</span></h2>
      <div id="flowList" style="max-height:350px;overflow-y:auto;"></div>
      <div class="empty-state" id="flowEmpty">暂无资金流向数据</div>
    </div>
    <div class="card">
      <h2>📰 消息面 <span class="badge" id="newsCount">0 条</span></h2>
      <div class="news-list" id="newsList"></div>
      <div class="empty-state" id="newsEmpty">暂无新闻数据</div>
    </div>
    <div class="card">
      <h2>📊 最近回测</h2>
      <div id="backtestResult"></div>
      <div class="empty-state" id="backtestEmpty">点击「回测」开始</div>
    </div>
  </div>
</div>
<div class="modal-overlay" id="detailModal">
  <div class="modal">
    <h3 id="detailTitle">股票详情</h3>
    <div id="detailContent"></div>
    <div class="btn-row"><button class="btn" onclick="closeDetailModal()">关闭</button></div>
  </div>
</div>
<div class="modal-overlay" id="backtestModal">
  <div class="modal">
    <h3>移动均线交叉回测</h3>
    <label>股票代码</label><input type="text" id="btTicker" value="AAPL">
    <label>短均线窗口</label><input type="number" id="btShort" value="5" min="2" max="50">
    <label>长均线窗口</label><input type="number" id="btLong" value="20" min="5" max="200">
    <label>初始资金</label><input type="number" id="btCapital" value="100000" min="10000" step="10000">
    <div class="btn-row">
      <button class="btn" onclick="closeBacktestModal()">取消</button>
      <button class="btn primary" id="btRunBtn" onclick="runBacktest()">运行</button>
    </div>
  </div>
</div>
<div class="toast" id="toast"></div>
<script>
let allQuotesData = [];
let sortField = 'ticker';
let sortAsc = true;

function showToast(msg, isError = false) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.borderColor = isError ? '#e74c3c' : '#4f8cff';
  t.style.display = 'block';
  setTimeout(() => { t.style.display = 'none'; }, 3000);
}
async function apiGet(path) {
  const resp = await fetch(path);
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.json();
}
function toggleTheme() {
  const body = document.body;
  const current = body.getAttribute('data-theme');
  body.setAttribute('data-theme', current === 'dark' ? 'light' : 'dark');
}
async function checkHealth() {
  try {
    const health = await apiGet('/api/health');
    document.getElementById('statusDot').className = 'status-dot';
    document.getElementById('statusText').textContent = `运行中 · 端口 ${health.port || 8000}`;
    document.getElementById('lastUpdate').textContent = `最近更新: ${health.last_update || '—'}`;
  } catch (e) {
    document.getElementById('statusDot').className = 'status-dot offline';
    document.getElementById('statusText').textContent = '连接中断';
  }
}
function fmtNum(v, digits = 2) { if (v === null || v === undefined || isNaN(v)) return '—'; return Number(v).toFixed(digits); }
function fmtBig(v) { if (v === null || v === undefined || isNaN(v)) return '—'; const n = Number(v); if (Math.abs(n) >= 1e12) return (n / 1e12).toFixed(2) + 'T'; if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(2) + 'B'; if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + 'M'; if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(2) + 'K'; return n.toFixed(0); }

function sortQuotes(field) {
  if (sortField === field) sortAsc = !sortAsc;
  else { sortField = field; sortAsc = true; }
  renderQuotesTable();
}
function filterQuotes(search) {
  renderQuotesTable(search.toLowerCase());
}
function renderQuotesTable(filter = '') {
  const tbody = document.getElementById('quotesBody');
  const empty = document.getElementById('quotesEmpty');
  const count = document.getElementById('quoteCount');
  let data = [...allQuotesData];
  if (filter) data = data.filter(q => q.ticker.toLowerCase().includes(filter));
  data.sort((a, b) => {
    const va = a[sortField], vb = b[sortField];
    if (va === null || va === undefined) return 1;
    if (vb === null || vb === undefined) return -1;
    if (typeof va === 'string') return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
    return sortAsc ? va - vb : vb - va;
  });
  count.textContent = data.length + ' 只';
  if (data.length === 0) { tbody.innerHTML = ''; empty.style.display = 'block'; return; }
  empty.style.display = 'none';
  tbody.innerHTML = data.map(q => {
    const change = q.change_pct || 0;
    const cls = change > 0.01 ? 'up' : (change < -0.01 ? 'down' : 'flat');
    const sign = change > 0 ? '+' : '';
    return `<tr onclick="showDetail('${q.ticker}')" title="点击查看详情">
      <td><strong>${q.ticker}</strong></td>
      <td>${fmtNum(q.close)}</td>
      <td class="${cls}">${sign}${fmtNum(change)}%</td>
      <td>${fmtNum(q.high)}</td>
      <td>${fmtNum(q.low)}</td>
      <td>${fmtBig(q.volume)}</td>
    </tr>`;
  }).join('');
}
function renderQuotes(data) { allQuotesData = data || []; renderQuotesTable(); }
function renderSignals(data) {
  const list = document.getElementById('signalList');
  const empty = document.getElementById('signalEmpty');
  const count = document.getElementById('signalCount');
  if (!data || data.length === 0) { list.innerHTML=''; empty.style.display='block'; count.textContent='0 条'; return; }
  empty.style.display='none'; count.textContent = data.length + ' 条';
  list.innerHTML = data.map(s => `
    <div class="signal-item" onclick="showDetail('${s.ticker}')">
      <div><span class="signal-ticker">${s.ticker}</span><span class="signal-type"> · ${s.signal_type}</span></div>
      <span class="signal-strength">${s.strength}</span>
    </div>`).join('');
}
function renderFlow(data) {
  const container = document.getElementById('flowList');
  const empty = document.getElementById('flowEmpty');
  if (!data || data.length === 0) { container.innerHTML=''; empty.style.display='block'; return; }
  empty.style.display='none';
  container.innerHTML = data.map(f => {
    const color = f.flow_direction === '流入' ? 'var(--green)' : f.flow_direction === '流出' ? 'var(--red)' : 'var(--yellow)';
    return `<div class="signal-item" onclick="showDetail('${f.ticker}')">
      <div><span class="signal-ticker">${f.ticker}</span><span class="signal-type"> · ${f.divergence_signal || ''}</span></div>
      <span style="color:${color};font-weight:600">${f.flow_direction} ${fmtNum(f.flow_score,0)}</span>
    </div>`;
  }).join('');
}
function renderNews(data) {
  const list = document.getElementById('newsList');
  const empty = document.getElementById('newsEmpty');
  const count = document.getElementById('newsCount');
  if (!data || data.length === 0) { list.innerHTML=''; empty.style.display='block'; count.textContent='0 条'; return; }
  empty.style.display='none'; count.textContent = data.length + ' 条';
  list.innerHTML = data.map(n => {
    const sentCls = n.sentiment === '正面' ? 'sentiment-positive' : n.sentiment === '负面' ? 'sentiment-negative' : 'sentiment-neutral';
    return `<div class="news-item" onclick="window.open('${n.link || '#'}', '_blank')">
      <div class="news-title">[${n.ticker}] ${n.title}</div>
      <div class="news-meta"><span>${n.publisher || '未知'}</span><span class="${sentCls}">${n.sentiment} (${n.sentiment_score})</span></div>
    </div>`;
  }).join('');
}
function renderBacktest(data) {
  const container = document.getElementById('backtestResult');
  const empty = document.getElementById('backtestEmpty');
  if (!data) { container.innerHTML=''; empty.style.display='block'; return; }
  empty.style.display='none';
  container.innerHTML = `
    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:14px;">
      <div><small>总收益</small><br><strong class="${data.total_return_pct >= 0 ? 'up' : 'down'}">${data.total_return_pct}%</strong></div>
      <div><small>年化</small><br><strong>${data.annual_return_pct}%</strong></div>
      <div><small>回撤</small><br><strong class="down">${data.max_drawdown_pct}%</strong></div>
      <div><small>夏普</small><br><strong>${data.sharpe_ratio}</strong></div>
      <div><small>交易</small><br><strong>${data.total_trades}</strong></div>
      <div><small>权益</small><br><strong>${fmtNum(data.final_equity, 0)}</strong></div>
    </div>`;
}
async function showDetail(ticker) {
  document.getElementById('detailTitle').textContent = `📋 ${ticker} 详情`;
  document.getElementById('detailContent').innerHTML = '<div class="skeleton"></div><div class="skeleton"></div><div class="skeleton"></div>';
  document.getElementById('detailModal').classList.add('active');
  try {
    const [fund, flow, news, tech] = await Promise.all([
      apiGet(`/api/fundamentals/${ticker}`), apiGet(`/api/fundflow/${ticker}`),
      apiGet(`/api/news/${ticker}`), apiGet(`/api/indicators/${ticker}`)
    ]);
    let html = '<div class="detail-section"><h4>📊 基本面</h4>';
    if (fund && fund.pe_ratio !== null) {
      html += `<div class="detail-grid">
        <div class="detail-item"><div class="label">PE</div><div class="value">${fmtNum(fund.pe_ratio)}</div></div>
        <div class="detail-item"><div class="label">PB</div><div class="value">${fmtNum(fund.pb_ratio)}</div></div>
        <div class="detail-item"><div class="label">市值</div><div class="value">${fmtBig(fund.market_cap)}</div></div>
        <div class="detail-item"><div class="label">股息率</div><div class="value">${fund.dividend_yield ? (fund.dividend_yield * 100).toFixed(2) + '%' : '—'}</div></div>
        <div class="detail-item"><div class="label">Beta</div><div class="value">${fmtNum(fund.beta)}</div></div>
        <div class="detail-item"><div class="label">EPS</div><div class="value">${fmtNum(fund.eps)}</div></div>
        <div class="detail-item"><div class="label">52周高</div><div class="value">${fmtNum(fund.week52_high)}</div></div>
        <div class="detail-item"><div class="label">52周低</div><div class="value">${fmtNum(fund.week52_low)}</div></div>
      </div>`;
    } else { html += '<div class="empty-state">暂无基本面数据</div>'; }
    html += '</div><div class="detail-section"><h4>📈 技术指标</h4>';
    if (tech && tech.macd) {
      const macdCls = tech.macd.trend === '金叉' ? 'up' : 'down';
      html += `<div class="detail-grid">
        <div class="detail-item"><div class="label">MACD</div><div class="value">${tech.macd.macd_line}</div></div>
        <div class="detail-item"><div class="label">信号线</div><div class="value">${tech.macd.signal_line}</div></div>
        <div class="detail-item"><div class="label">趋势</div><div class="value ${macdCls}">${tech.macd.trend}</div></div>
        <div class="detail-item"><div class="label">K</div><div class="value">${tech.kdj ? tech.kdj.k : '—'}</div></div>
        <div class="detail-item"><div class="label">D</div><div class="value">${tech.kdj ? tech.kdj.d : '—'}</div></div>
        <div class="detail-item"><div class="label">J</div><div class="value">${tech.kdj ? tech.kdj.j : '—'}</div></div>
        <div class="detail-item"><div class="label">RSI</div><div class="value">${tech.rsi || '—'}</div></div>
        <div class="detail-item"><div class="label">KDJ信号</div><div class="value">${tech.kdj ? tech.kdj.signal : '—'}</div></div>
      </div>`;
    } else { html += '<div class="empty-state">暂无技术指标数据</div>'; }
    html += '</div><div class="detail-section"><h4>💰 资金流向</h4>';
    if (flow && !flow.error) {
      const color = flow.flow_direction === '流入' ? 'var(--green)' : flow.flow_direction === '流出' ? 'var(--red)' : 'var(--yellow)';
      html += `<div class="detail-grid">
        <div class="detail-item"><div class="label">MFI</div><div class="value">${fmtNum(flow.mfi)}</div></div>
        <div class="detail-item"><div class="label">相对量</div><div class="value">${fmtNum(flow.relative_volume)}x</div></div>
        <div class="detail-item"><div class="label">评分</div><div class="value" style="color:${color}">${fmtNum(flow.flow_score,0)}</div></div>
        <div class="detail-item"><div class="label">方向</div><div class="value" style="color:${color}">${flow.flow_direction}</div></div>
        <div class="detail-item" style="grid-column:span 2"><div class="label">量价信号</div><div class="value" style="font-size:12px">${flow.divergence_signal || '无'}</div></div>
      </div>`;
    } else { html += '<div class="empty-state">暂无资金流向数据</div>'; }
    html += '</div><div class="detail-section"><h4>📰 最近新闻</h4>';
    if (news && news.length > 0) {
      html += news.slice(0, 10).map(n => {
        const sentCls = n.sentiment === '正面' ? 'sentiment-positive' : n.sentiment === '负面' ? 'sentiment-negative' : 'sentiment-neutral';
        return `<div class="news-item"><div class="news-title">${n.title}</div><div class="news-meta"><span>${n.publisher || ''}</span><span class="${sentCls}">${n.sentiment}</span></div></div>`;
      }).join('');
    } else { html += '<div class="empty-state">暂无新闻</div>'; }
    html += '</div>';
    document.getElementById('detailContent').innerHTML = html;
  } catch (e) {
    document.getElementById('detailContent').innerHTML = `<div class="empty-state">加载失败: ${e.message}</div>`;
  }
}
function closeDetailModal() { document.getElementById('detailModal').classList.remove('active'); }
async function exportCSV() {
  try {
    const resp = await fetch('/api/export/csv');
    if (!resp.ok) throw new Error('导出失败');
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = 'quotes.csv'; a.click();
    URL.revokeObjectURL(url);
    showToast('导出成功');
  } catch (e) { showToast('导出失败: ' + e.message, true); }
}
async function refreshAll() {
  try {
    showToast('刷新中...');
    const [quotes, signals, flow, news] = await Promise.all([
      apiGet('/api/quotes'), apiGet('/api/signals'),
      apiGet('/api/fundflow/all'), apiGet('/api/news?limit=30')
    ]);
    renderQuotes(quotes); renderSignals(signals); renderFlow(flow); renderNews(news);
    showToast('刷新完成');
  } catch (e) { showToast('刷新失败: ' + e.message, true); }
}
async function advancedRefresh() {
  try {
    showToast('高级刷新中...');
    await fetch('/api/quotes/refresh?force=true', { method: 'POST' });
    showToast('高级刷新完成');
    await refreshAll();
  } catch (e) { showToast('高级刷新失败: ' + e.message, true); }
}
async function addTicker() {
  const input = document.getElementById('newTicker');
  const ticker = input.value.trim().toUpperCase();
  if (!ticker) return;
  try {
    const resp = await fetch('/api/tickers', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ticker }) });
    const data = await resp.json();
    if (resp.ok) { showToast(`成功添加 ${ticker}`); input.value = ''; await refreshAll(); }
    else { showToast(data.detail || '添加失败', true); }
  } catch (e) { showToast('添加失败: ' + e.message, true); }
}
function openBacktestModal() { document.getElementById('backtestModal').classList.add('active'); }
function closeBacktestModal() { document.getElementById('backtestModal').classList.remove('active'); }
async function runBacktest() {
  const btn = document.getElementById('btRunBtn');
  btn.disabled = true; btn.textContent = '运行中...';
  try {
    const params = {
      ticker: document.getElementById('btTicker').value.trim(),
      short_window: parseInt(document.getElementById('btShort').value),
      long_window: parseInt(document.getElementById('btLong').value),
      initial_capital: parseFloat(document.getElementById('btCapital').value),
    };
    const resp = await fetch('/api/backtest/run', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(params) });
    const data = await resp.json();
    if (data.error) throw new Error(data.error);
    renderBacktest(data); closeBacktestModal(); showToast('回测完成');
  } catch (e) { showToast('回测失败: ' + e.message, true); }
  finally { btn.disabled = false; btn.textContent = '运行'; }
}
async function archiveData() {
  try {
    const resp = await fetch('/api/archive', { method: 'POST' });
    const data = await resp.json();
    showToast(`归档完成，共 ${data.archived} 条记录`);
  } catch (e) { showToast('归档失败: ' + e.message, true); }
}
// 初始化
checkHealth();
setInterval(checkHealth, 10000);
refreshAll();
setInterval(refreshAll, 5 * 60 * 1000);
</script>
</body>
</html>
"""

# ============================================================
# 第九部分：FastAPI 应用
# ============================================================
app = FastAPI(title=APP_NAME, version=APP_VERSION,
              description="股民工作台 - 行情/信号/基本面/资金流/消息面/技术指标/回测")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

class BacktestRequest(BaseModel):
    ticker: str
    short_window: int = 5
    long_window: int = 20
    initial_capital: float = 100_000.0

class TickerAddRequest(BaseModel):
    ticker: str

# ============================================================
# 第十部分：调度器
# ============================================================
scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

def scheduled_full_refresh():
    try:
        df = MarketDataFetcher.fetch_batch_daily(config.DEFAULT_TICKERS, days=365)
        if not df.empty:
            Database.insert_quotes_batch_async(df)
            signals = SignalCalculator.detect_extreme_signals(df)
            for s in signals:
                Database.insert_signal(s['ticker'], s['signal_type'], s['strength'])
            logger.info(f"定时刷新: {len(df)} 条行情, {len(signals)} 条信号")
        fund_data = FundamentalFetcher.fetch_batch(config.DEFAULT_TICKERS)
        for ticker, data in fund_data.items():
            Database.upsert_fundamental(ticker, data)
        all_news = NewsFetcher.fetch_batch(config.DEFAULT_TICKERS, limit=10)
        for n in all_news:
            Database.insert_news(n['ticker'], n['title'], n['publisher'], n['link'],
                                 n['sentiment'], n['sentiment_score'], n['published_at'])
        logger.info(f"定时刷新完成: {len(fund_data)} 条基本面, {len(all_news)} 条新闻")
    except Exception as e:
        logger.error(f"定时刷新失败: {e}")

def start_scheduler():
    if scheduler.running:
        return
    scheduler.add_job(scheduled_full_refresh, 'interval', minutes=config.UPDATE_INTERVAL_MIN,
                      id='scheduled_full_refresh', max_instances=1, coalesce=True, replace_existing=True)
    scheduler.start()
    logger.info(f"调度器已启动，每 {config.UPDATE_INTERVAL_MIN} 分钟自动刷新")

def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)

# ============================================================
# 第十一部分：API 路由
# ============================================================
@app.get("/", response_class=HTMLResponse)
async def index():
    return FRONTEND_HTML

@app.get("/api/health")
async def health():
    last_update = None
    try:
        df = Database.query_df("SELECT MAX(dt) as max_dt FROM quotes")
        if not df.empty and df.iloc[0]['max_dt'] is not None:
            last_update = df.iloc[0]['max_dt'].isoformat()
    except Exception:
        pass
    return {"status": "ok", "app": APP_NAME, "version": APP_VERSION,
            "port": app.state.port if hasattr(app.state, 'port') else config.PORT,
            "last_update": last_update, "timestamp": datetime.now().isoformat()}

# ---- 行情 ----
@app.get("/api/quotes")
async def get_quotes(limit: int = Query(100, ge=1, le=500)):
    try:
        df = Database.get_latest_quotes()
        if df.empty:
            return []
        df = df.head(limit)
        quotes = []
        for _, row in df.iterrows():
            history = Database.get_quote_history(row['ticker'], days=10)
            prev_close = None
            if len(history) >= 2:
                prev_close = float(history.iloc[-2]['close'])
            else:
                prev_close = float(row['open']) if row['open'] is not None else None
            change_pct = ((float(row['close']) - prev_close) / prev_close * 100) if prev_close else 0
            quotes.append({
                "ticker": row['ticker'],
                "dt": row['dt'].isoformat() if hasattr(row['dt'], 'isoformat') else str(row['dt']),
                "open": float(row['open']) if row['open'] is not None else None,
                "high": float(row['high']) if row['high'] is not None else None,
                "low": float(row['low']) if row['low'] is not None else None,
                "close": float(row['close']) if row['close'] is not None else None,
                "volume": float(row['volume']) if row['volume'] is not None else None,
                "change_pct": round(change_pct, 2),
            })
        return quotes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/quotes/history/{ticker}")
async def get_quote_history(ticker: str, days: int = Query(30, ge=1, le=365)):
    df = Database.get_quote_history(ticker, days)
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records", date_format="iso"))

@app.post("/api/quotes/refresh")
async def refresh_quotes(force: bool = Query(False)):
    try:
        if force:
            MarketDataFetcher.clear_cache()
        df = MarketDataFetcher.fetch_batch_daily(config.DEFAULT_TICKERS, days=365)
        if df.empty:
            return {"success": False, "message": "所有股票抓取失败"}
        Database.insert_quotes_batch(df)
        signals = SignalCalculator.detect_extreme_signals(df)
        for s in signals:
            Database.insert_signal(s['ticker'], s['signal_type'], s['strength'])
        fund_data = FundamentalFetcher.fetch_batch(config.DEFAULT_TICKERS)
        for ticker, data in fund_data.items():
            Database.upsert_fundamental(ticker, data)
        all_news = NewsFetcher.fetch_batch(config.DEFAULT_TICKERS, limit=10)
        for n in all_news:
            Database.insert_news(n['ticker'], n['title'], n['publisher'], n['link'],
                                 n['sentiment'], n['sentiment_score'], n['published_at'])
        return {"success": True, "message": f"成功抓取 {len(df)} 条行情, {len(fund_data)} 条基本面, {len(all_news)} 条新闻"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/quotes/refresh/{ticker}")
async def refresh_single_ticker(ticker: str):
    """手动刷新单只股票"""
    try:
        df = MarketDataFetcher.fetch_daily(ticker, days=365)
        if df is None or df.empty:
            raise HTTPException(status_code=404, detail=f"无法获取 {ticker} 数据")
        Database.insert_quotes_batch(df)
        fund = FundamentalFetcher.fetch(ticker)
        if fund:
            Database.upsert_fundamental(ticker, fund)
        news_list = NewsFetcher.fetch(ticker, limit=10)
        for n in news_list:
            Database.insert_news(n['ticker'], n['title'], n['publisher'], n['link'],
                                 n['sentiment'], n['sentiment_score'], n['published_at'])
        return {"success": True, "message": f"已刷新 {ticker}"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---- 信号 ----
@app.get("/api/signals")
async def get_signals(limit: int = Query(50, ge=1, le=200)):
    df = Database.get_recent_signals(limit)
    if df.empty:
        return []
    records = []
    for _, row in df.iterrows():
        records.append({
            "id": int(row['id']), "ticker": row['ticker'], "signal_type": row['signal_type'],
            "strength": float(row['strength']) if row['strength'] is not None else None,
            "dt": row['dt'].isoformat() if hasattr(row['dt'], 'isoformat') else str(row['dt']),
        })
    return records

# ---- 基本面 ----
@app.get("/api/fundamentals")
async def get_all_fundamentals():
    df = Database.get_all_fundamentals()
    if df.empty:
        return []
    records = []
    for _, row in df.iterrows():
        rec = {}
        for col in df.columns:
            val = row[col]
            if pd.isna(val): rec[col] = None
            elif hasattr(val, 'isoformat'): rec[col] = val.isoformat()
            elif isinstance(val, (np.integer,)): rec[col] = int(val)
            elif isinstance(val, (np.floating,)): rec[col] = float(val)
            else: rec[col] = val
        records.append(rec)
    return records

@app.get("/api/fundamentals/{ticker}")
async def get_fundamental(ticker: str):
    cached = FundamentalFetcher._get_cached(ticker)
    if cached:
        return cached
    db_data = Database.get_fundamental(ticker)
    if db_data:
        return db_data
    data = FundamentalFetcher.fetch(ticker)
    if data:
        Database.upsert_fundamental(ticker, data)
        return data
    return {}

# ---- 资金流向 ----
@app.get("/api/fundflow/{ticker}")
async def get_fundflow(ticker: str):
    df = Database.get_quote_history(ticker, days=60)
    if df.empty:
        df = MarketDataFetcher.fetch_daily(ticker, days=60)
    return FundFlowCalculator.calculate_flow(df)

@app.get("/api/fundflow/all")
async def get_all_fundflow():
    df = Database.get_latest_quotes()
    if df.empty:
        return []
    results = []
    for _, row in df.iterrows():
        ticker = row['ticker']
        hist = Database.get_quote_history(ticker, days=60)
        if len(hist) >= 5:
            flow = FundFlowCalculator.calculate_flow(hist)
            if 'error' not in flow:
                flow['ticker'] = ticker
                results.append(flow)
    return results

# ---- 新闻 ----
@app.get("/api/news")
async def get_news(limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)):
    df = Database.get_all_news(limit, offset)
    if df.empty:
        return []
    records = []
    for _, row in df.iterrows():
        records.append({
            "id": int(row['id']), "ticker": row['ticker'], "title": row['title'],
            "publisher": row['publisher'], "link": row['link'],
            "sentiment": row['sentiment'],
            "sentiment_score": float(row['sentiment_score']) if row['sentiment_score'] is not None else 0,
            "published_at": row['published_at'].isoformat() if row['published_at'] and hasattr(row['published_at'], 'isoformat') else None,
        })
    return records

@app.get("/api/news/{ticker}")
async def get_ticker_news(ticker: str, limit: int = Query(20, ge=1, le=50)):
    df = Database.get_news_by_ticker(ticker, limit)
    if df.empty:
        return []
    records = []
    for _, row in df.iterrows():
        records.append({
            "id": int(row['id']), "ticker": row['ticker'], "title": row['title'],
            "publisher": row['publisher'], "link": row['link'],
            "sentiment": row['sentiment'],
            "sentiment_score": float(row['sentiment_score']) if row['sentiment_score'] is not None else 0,
            "published_at": row['published_at'].isoformat() if row['published_at'] and hasattr(row['published_at'], 'isoformat') else None,
        })
    return records

# ---- 技术指标（V3.9 新增）----
@app.get("/api/indicators/{ticker}")
async def get_indicators(ticker: str):
    df = Database.get_quote_history(ticker, days=120)
    if df.empty:
        df = MarketDataFetcher.fetch_daily(ticker, days=120)
    return TechnicalIndicators.get_all_indicators(df)

# ---- CSV 导出（V3.9 新增）----
@app.get("/api/export/csv")
async def export_csv():
    """导出最新行情为 CSV"""
    df = Database.get_latest_quotes()
    if df.empty:
        raise HTTPException(status_code=404, detail="暂无数据")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Ticker', 'Datetime', 'Open', 'High', 'Low', 'Close', 'Volume'])
    for _, row in df.iterrows():
        writer.writerow([
            row['ticker'], row['dt'].isoformat(),
            round(float(row['open']), 2) if row['open'] is not None else '',
            round(float(row['high']), 2) if row['high'] is not None else '',
            round(float(row['low']), 2) if row['low'] is not None else '',
            round(float(row['close']), 2) if row['close'] is not None else '',
            int(row['volume']) if row['volume'] is not None else '',
        ])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=quotes.csv"}
    )

# ---- 股票管理 ----
@app.get("/api/tickers")
async def get_tickers():
    return {"tickers": config.DEFAULT_TICKERS}

@app.post("/api/tickers")
async def add_ticker(req: TickerAddRequest):
    ticker = req.ticker.strip().upper()
    if not ticker:
        raise HTTPException(status_code=400, detail="股票代码不能为空")
    if ticker in config.DEFAULT_TICKERS:
        raise HTTPException(status_code=400, detail="股票已在监控列表中")
    config.DEFAULT_TICKERS.append(ticker)
    try:
        cfg_file = BASE_DIR / "config.json"
        current = {}
        if cfg_file.exists():
            with open(cfg_file, "r", encoding="utf-8") as f:
                current = json.load(f)
        current["DEFAULT_TICKERS"] = config.DEFAULT_TICKERS
        with open(cfg_file, "w", encoding="utf-8") as f:
            json.dump(current, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    df = MarketDataFetcher.fetch_daily(ticker, days=365)
    if df is not None and not df.empty:
        Database.insert_quotes_batch(df)
    fund = FundamentalFetcher.fetch(ticker)
    if fund:
        Database.upsert_fundamental(ticker, fund)
    news_list = NewsFetcher.fetch(ticker, limit=10)
    for n in news_list:
        Database.insert_news(n['ticker'], n['title'], n['publisher'], n['link'],
                             n['sentiment'], n['sentiment_score'], n['published_at'])
    return {"success": True, "tickers": config.DEFAULT_TICKERS, "message": f"已添加 {ticker}"}

@app.delete("/api/tickers/{ticker}")
async def remove_ticker(ticker: str):
    """V3.9 新增：删除自选股"""
    ticker = ticker.strip().upper()
    if ticker not in config.DEFAULT_TICKERS:
        raise HTTPException(status_code=404, detail="股票不在监控列表中")
    config.DEFAULT_TICKERS.remove(ticker)
    Database.delete_ticker_data(ticker)
    try:
        cfg_file = BASE_DIR / "config.json"
        current = {}
        if cfg_file.exists():
            with open(cfg_file, "r", encoding="utf-8") as f:
                current = json.load(f)
        current["DEFAULT_TICKERS"] = config.DEFAULT_TICKERS
        with open(cfg_file, "w", encoding="utf-8") as f:
            json.dump(current, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    return {"success": True, "message": f"已移除 {ticker}"}

# ---- 回测 ----
@app.post("/api/backtest/run")
async def run_backtest(req: BacktestRequest):
    try:
        if req.short_window >= req.long_window:
            raise HTTPException(status_code=400, detail="短均线窗口必须小于长均线窗口")
        df = MarketDataFetcher.fetch_daily(req.ticker, config.BACKTEST_LOOKBACK_DAYS)
        if df is None or df.empty:
            raise HTTPException(status_code=404, detail=f"无法获取 {req.ticker} 的历史数据")
        result = BacktestEngine.run_ma_cross(df, req.short_window, req.long_window, req.initial_capital)
        if 'error' in result:
            raise HTTPException(status_code=400, detail=result['error'])
        Database.insert_backtest("MA_CROSS", result['params'], result)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/backtests")
async def get_backtests(limit: int = Query(20, ge=1, le=100)):
    df = Database.get_backtests(limit)
    if df.empty:
        return []
    records = []
    for _, row in df.iterrows():
        try:
            result = json.loads(row['result']) if row['result'] else None
        except Exception:
            result = None
        records.append({
            "id": int(row['id']), "strategy": row['strategy'], "params": row['params'],
            "result": result,
            "created_at": row['created_at'].isoformat() if hasattr(row['created_at'], 'isoformat') else str(row['created_at']),
        })
    return records

@app.post("/api/archive")
async def archive_data(days: int = Query(365, ge=30, le=3650)):
    try:
        count = Database.archive_old_quotes(days)
        return {"success": True, "archived": count, "cutoff_days": days}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# 第十二部分：单实例互斥
# ============================================================
def _acquire_single_instance() -> bool:
    if sys.platform != "win32":
        return True
    try:
        import ctypes
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        mutex_name = f"Global\\{APP_NAME.replace(' ', '_')}_Mutex_V3"
        handle = kernel32.CreateMutexW(None, False, mutex_name)
        if not handle:
            return True
        return ctypes.get_last_error() != 183
    except Exception:
        return True

def _find_existing_instance() -> Optional[str]:
    for p in range(config.PORT, config.PORT + 11):
        url = f"http://127.0.0.1:{p}/api/health"
        try:
            with urllib.request.urlopen(url, timeout=0.5) as r:
                if r.status == 200:
                    return f"http://localhost:{p}"
        except Exception:
            continue
    return None

def _free_port(start: int, tries: int = 10) -> int:
    for p in range(start, start + tries):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", p)) != 0:
                return p
    return start

# ============================================================
# 第十三部分：主入口
# ============================================================
def main():
    multiprocessing.freeze_support()
    if not _acquire_single_instance():
        existing = _find_existing_instance()
        if existing:
            print(f"[提示] {APP_NAME} 已在运行({existing})，已为你打开看板")
            webbrowser.open(existing)
        else:
            print(f"[提示] {APP_NAME} 已在运行，但未能找到看板地址")
        time.sleep(1)
        return

    port = _free_port(config.PORT)
    url = f"http://localhost:{port}"
    print("=" * 62)
    print(f"   {APP_NAME} V{APP_VERSION}")
    print(f"   看板地址 : {url}    （浏览器将自动打开）")
    print(f"   API 文档 : {url}/docs")
    print(f"   数据位置 : {config.DATA_DIR.resolve()}")
    print("   功能：行情 / 信号 / 基本面 / 资金流 / 消息面 / 技术指标 / 回测 / 导出")
    print("=" * 62)

    if os.getenv("AUTO_OPEN", "1") == "1":
        threading.Timer(2.0, lambda: webbrowser.open(url)).start()

    # 预热
    try:
        df_count = Database.query_df("SELECT COUNT(*) as cnt FROM quotes").iloc[0]['cnt']
        if df_count == 0:
            logger.info("数据库为空，执行首次抓取...")
            df = MarketDataFetcher.fetch_batch_daily(config.DEFAULT_TICKERS, days=365)
            if not df.empty:
                Database.insert_quotes_batch(df)
    except Exception as e:
        logger.warning(f"预热失败: {e}")

    start_scheduler()
    try:
        app.state.port = port
        uvicorn.run(app, host=config.HOST, port=port, log_level="warning")
    except KeyboardInterrupt:
        print("\n已安全退出。")
    except SystemExit:
        raise
    except Exception as e:
        crash_log = config.DATA_DIR / "crash.log"
        with open(crash_log, "w", encoding="utf-8") as f:
            f.write(traceback.format_exc())
        print(f"\n[启动失败] 详细原因已写入: {crash_log}")
        try:
            input("\n按回车键退出...")
        except Exception:
            pass
    finally:
        stop_scheduler()
        Database.close()

if __name__ == "__main__":
    main()
