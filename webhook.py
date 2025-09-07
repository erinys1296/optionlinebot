# -*- coding: utf-8 -*-
import os, json, threading, time, logging
from pathlib import Path
from typing import Set, List, Optional

from flask import Flask, request, abort, jsonify, send_from_directory, make_response
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage, ImageSendMessage

import sqlite3, requests, pandas as pd, numpy as np, io, time, os
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ========= 設定 =========
CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]
REGISTER_CODE = os.environ.get("REGISTER_CODE", "abc123")     # 註冊驗證碼
CRON_KEY = os.environ.get("CRON_KEY")                         # Admin/Cron 金鑰（必填）
WHITE_LIST_FILE = Path(os.environ.get("WHITELIST_PATH", "whitelist.json"))

# 圖片儲存與公開網址（建議有 Persistent Disk）
IMAGES_DIR = Path(os.environ.get("IMAGES_DIR", "/var/data/images"))
PUBLIC_BASE = os.environ.get("PUBLIC_BASE")  # 例如 https://your-service.onrender.com

# ===== SQLite 來源與快取（請到 Render 環境變數設定）=====
EQUITY_SQLITE_URL   = os.environ.get("EQUITY_SQLITE_URL")   # 主圖資料.sqlite3 的 RAW 連結
FUTURES_SQLITE_URL  = os.environ.get("FUTURES_SQLITE_URL")  # FutureData.sqlite3 的 RAW 連結
EQUITY_SQLITE_CACHE = Path(os.environ.get("EQUITY_SQLITE_CACHE", "/var/data/cache/equity.sqlite"))
FUTURES_SQLITE_CACHE= Path(os.environ.get("FUTURES_SQLITE_CACHE","/var/data/cache/future.sqlite"))
SQLITE_TTL_SEC      = int(os.environ.get("SQLITE_TTL_SEC", "900"))  # 15分鐘快取
GITHUB_TOKEN        = os.environ.get("GITHUB_TOKEN")  # 私有 repo 才需要，可留空

# （若你的資料庫內表名與我預設不同，可用這三個環境變數覆蓋；不填就用你 SQL 寫的原名）
EQ_COST_TABLE      = os.environ.get("EQ_COST_TABLE", "cost")
EQ_LIMIT_TABLE     = os.environ.get("EQ_LIMIT_TABLE", "limit")
FUT_HOURLY_TABLE   = os.environ.get("FUT_HOURLY_TABLE", "futurehourly")

# 簡單配色
increasing_color = 'rgb(255, 0, 0)'
decreasing_color = 'rgb(0, 0, 245)'

red_color = 'rgba(255, 0, 0, 0.1)'
green_color = 'rgba(30, 144, 255,0.1)'

no_color = 'rgba(256, 256, 256,0)'

blue_color = 'rgb(30, 144, 255)'
red_color_full = 'rgb(255, 0, 0)'

orange_color = 'rgb(245, 152, 59)'
green_color_full = 'rgb(52, 186, 7)'

gray_color = 'rgb(188, 194, 192)'
black_color = 'rgb(0, 0, 0)'

# ========= App / SDK =========
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("webhook")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# --- Persistent Disk 初始化與一次性搬遷（白名單） ---
DEFAULT_WHITE_LIST_FILE = Path("whitelist.json")
WHITE_LIST_FILE.parent.mkdir(parents=True, exist_ok=True)
IMAGES_DIR.mkdir(parents=True, exist_ok=True)

def _bootstrap_storage():
    """若掛了磁碟但檔案不存在，且舊檔存在，則把舊檔搬到新位置；否則建立空白檔。"""
    try:
        if WHITE_LIST_FILE.exists():
            return
        if WHITE_LIST_FILE != DEFAULT_WHITE_LIST_FILE and DEFAULT_WHITE_LIST_FILE.exists():
            data = json.loads(DEFAULT_WHITE_LIST_FILE.read_text("utf-8"))
            WHITE_LIST_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
            print(f"[BOOTSTRAP] migrated whitelist from {DEFAULT_WHITE_LIST_FILE} -> {WHITE_LIST_FILE}")
        else:
            WHITE_LIST_FILE.write_text("[]", "utf-8")
            print(f"[BOOTSTRAP] created empty whitelist at {WHITE_LIST_FILE}")
    except Exception as e:
        print("[BOOTSTRAP] error:", e)

_bootstrap_storage()

# ========= 白名單存取 =========
_lock = threading.Lock()

def _load_whitelist() -> Set[str]:
    if not WHITE_LIST_FILE.exists():
        return set()
    try:
        with WHITE_LIST_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data if isinstance(data, list) else [])
    except Exception as e:
        logger.exception("load whitelist error: %s", e)
        return set()

def _save_whitelist(ids: Set[str]):
    """原子寫檔，避免中斷造成 JSON 壞掉。"""
    tmp = WHITE_LIST_FILE.with_suffix(".json.tmp")
    data = json.dumps(sorted(list(ids)), ensure_ascii=False, indent=2)
    with _lock:
        with tmp.open("w", encoding="utf-8") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, WHITE_LIST_FILE)

def _require_key():
    key = request.args.get("key") or request.headers.get("X-CRON-KEY")
    if not CRON_KEY or key != CRON_KEY:
        abort(401)

# ========= SQLite 下載與快取 =========
def _ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def _download_to(path: Path, url: str):
    """把 url 下載到 path（原子寫入，支援私有 repo Token）。"""
    headers = {"User-Agent": "render-linebot"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
        headers["Accept"] = "application/vnd.github.raw"
    _ensure_parent(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with requests.get(url, headers=headers, stream=True, timeout=120) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_content(8192):
                if chunk:
                    f.write(chunk)
            f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)

def _get_cached_sqlite(url: str, cache_path: Path) -> Path:
    """TTL 內直接用快取；過期就重新下載。"""
    if not url:
        raise RuntimeError(f"missing SQLITE url for {cache_path}")
    if cache_path.exists() and (time.time() - cache_path.stat().st_mtime) < SQLITE_TTL_SEC:
        return cache_path
    _download_to(cache_path, url)
    return cache_path

def get_sqlite_connection(which: str) -> sqlite3.Connection:
    """
    which: 'equity' 或 'futures'
    回傳 sqlite3.connect(...) 連線；呼叫方負責 con.close()
    """
    if which == "equity":
        p = _get_cached_sqlite(EQUITY_SQLITE_URL, EQUITY_SQLITE_CACHE)
    elif which == "futures":
        p = _get_cached_sqlite(FUTURES_SQLITE_URL, FUTURES_SQLITE_CACHE)
    else:
        raise ValueError("which must be 'equity' or 'futures'")
    return sqlite3.connect(str(p))


# ========= 圖片：生成 & 提供 =========
def generate_plot_png(filename: str = "latest.png") -> Path:
    """
    讀取：
      - equity DB 的 {EQ_COST_TABLE}(Date, Cost)、{EQ_LIMIT_TABLE}(日期, 身份別, 上極限, 下極限)
      - futures DB 的 {FUT_HOURLY_TABLE}(ts, Open, High, Low, Close, Volume)
    產生雙列（60分/300分）的 Plotly 圖，輸出為 PNG。
    """

    # ---- 顏色（若外部有同名變數會用外部的）----
    global red_color, green_color, increasing_color, decreasing_color, no_color
    red_color        = locals().get("red_color",   "rgba(220, 53, 69, 0.9)")
    green_color      = locals().get("green_color", "rgba(25, 135, 84, 0.9)")
    increasing_color = locals().get("increasing_color", "rgba(13, 110, 253, 1.0)")
    decreasing_color = locals().get("decreasing_color", "rgba(33, 37, 41, 1.0)")
    no_color         = locals().get("no_color", "rgba(0,0,0,0)")

    # ---- 目錄（若未定義 IMAGES_DIR，用預設）----
    images_dir = globals().get("IMAGES_DIR", Path(os.environ.get("IMAGES_DIR", "/var/data/images")))
    images_dir.mkdir(parents=True, exist_ok=True)

    # ---- 表名（可由環境變數覆蓋）----
    EQ_COST_TABLE     = os.environ.get("EQ_COST_TABLE", "cost")
    EQ_LIMIT_TABLE    = os.environ.get("EQ_LIMIT_TABLE", "limit")
    FUT_HOURLY_TABLE  = os.environ.get("FUT_HOURLY_TABLE", "futurehourly")

    # ---- 連線 ----
    con_eq = get_sqlite_connection("equity")
    con_fu = get_sqlite_connection("futures")

    try:
        # =========================
        # 1) 讀取 equity（cost / limit）
        # =========================
        cost_df = pd.read_sql(
            f'SELECT DISTINCT Date AS 日期, Cost AS 外資成本 FROM "{EQ_COST_TABLE}"',
            con_eq, parse_dates=['日期']
        ).dropna()
        cost_df["外資成本"] = cost_df["外資成本"].astype(int)

        limit_df = pd.read_sql(
            f'SELECT DISTINCT * FROM "{EQ_LIMIT_TABLE}"',
            con_eq, parse_dates=['日期']
        )
        inves_limit  = limit_df[limit_df["身份別"] == "外資"][['日期','上極限','下極限']].copy()
        dealer_limit = limit_df[limit_df["身份別"] == "自營商"][['日期','上極限','下極限']].copy()
        inves_limit.columns  = ['日期',"外資上極限","外資下極限"]
        dealer_limit.columns = ['日期',"自營商上極限","自營商下極限"]

        # =========================
        # 2) 讀取 futures（分時）
        # =========================
        FutureData = pd.read_sql(
            f'SELECT DISTINCT * FROM "{FUT_HOURLY_TABLE}"',
            con_fu, parse_dates=['ts'], index_col=['ts']
        )
        df_ts = FutureData.reset_index()

        # ====== 60分彙整 ======
        FutureData = FutureData.reset_index()
        mask = (FutureData.ts.dt.hour < 14) & (FutureData.ts.dt.hour >= 8)
        FutureData.loc[mask, 'ts'] = FutureData.loc[mask].ts - timedelta(minutes=46)
        FutureData.index = FutureData.ts

        FutureData["date"] = pd.to_datetime(FutureData.index)
        FutureData["hourdate"] = FutureData.date.dt.date.astype(str) + FutureData.date.dt.hour.astype(str)
        FutureData = FutureData.dropna(subset=['Open'])
        FutureData.index = FutureData['date']

        tempdf = FutureData[['hourdate','Volume']].copy()
        Final60Tdata = (
            FutureData.groupby('hourdate').max()[["High"]]
            .join(FutureData.groupby('hourdate').min()[["Low"]])
            .join(tempdf.groupby('hourdate').sum()[["Volume"]])
        )
        # 開/收
        tempopen  = FutureData.loc[FutureData.groupby('hourdate').min()['date'].values][["Open","date"]]
        tempopen.index = FutureData.loc[FutureData.groupby('hourdate').min()['date'].values].hourdate.values
        tempclose = FutureData.loc[FutureData.groupby('hourdate').max()['date'].values][["Close"]]
        tempclose.index = FutureData.loc[FutureData.groupby('hourdate').max()['date'].values].hourdate.values

        Final60Tdata = Final60Tdata.join(tempopen[["Open",'date']]).join(tempclose[["Close"]])
        Final60Tdata.index = Final60Tdata.date
        Final60Tdata.columns = ['max','min','Volume','open','date','close']

        # 對齊交易日 + 假日特例
        Final60Tdata['dateonly'] = pd.to_datetime((Final60Tdata.date - timedelta(hours=15)).dt.date)
        Final60Tdata.loc[(Final60Tdata.date - timedelta(hours=13)).dt.weekday == 6, 'dateonly'] = pd.to_datetime(
            (Final60Tdata[(Final60Tdata.date - timedelta(hours=13)).dt.weekday == 6].date - timedelta(hours=63)).dt.date
        )
        manual = {
            datetime(2023, 9,29): datetime(2023, 9,28),
            datetime(2024, 4, 5): datetime(2024, 4, 3),
            datetime(2024, 5, 1): datetime(2024, 4,30),
            datetime(2024, 6,10): datetime(2024, 6, 7),
            datetime(2025, 2,28): datetime(2025, 2,27),
            datetime(2025, 4, 4): datetime(2025, 4, 2),
            datetime(2025, 5, 1): datetime(2025, 4,30),
        }
        for k, v in manual.items():
            Final60Tdata.loc[Final60Tdata.dateonly == k, 'dateonly'] = v

        # 併入成本/極限
        Final60Tdata = pd.merge(Final60Tdata, cost_df,     left_on="dateonly", right_on="日期", how='left')
        Final60Tdata = pd.merge(Final60Tdata, inves_limit, on="日期",          how='left')
        Final60Tdata = pd.merge(Final60Tdata, dealer_limit,on="日期",          how='left')

        # 微調 1 分鐘
        idx_min1 = Final60Tdata.date.dt.minute == 1
        Final60Tdata.loc[idx_min1, 'date'] = Final60Tdata.loc[idx_min1, 'date'] - timedelta(minutes=1)
        Final60Tdata.index = Final60Tdata.date
        Final60Tdata = Final60Tdata.sort_index()

        # 指標（20MA / 布林 / K、D / uline,dline / IC）
        Final60Tdata['20MA'] = Final60Tdata['close'].rolling(20).mean()
        Final60Tdata['std']  = Final60Tdata['close'].rolling(20).std()
        Final60Tdata['upper_band']  = Final60Tdata['20MA'] + 2 * Final60Tdata['std']
        Final60Tdata['lower_band']  = Final60Tdata['20MA'] - 2 * Final60Tdata['std']
        Final60Tdata['upper_band1'] = Final60Tdata['20MA'] + 1 * Final60Tdata['std']
        Final60Tdata['lower_band1'] = Final60Tdata['20MA'] - 1 * Final60Tdata['std']
        Final60Tdata['IC'] = Final60Tdata['close'] + 2*Final60Tdata['close'].shift(1) - Final60Tdata['close'].shift(3) - Final60Tdata['close'].shift(4)

        low_list  = Final60Tdata['min'].rolling(9, min_periods=9).min().fillna(Final60Tdata['min'].expanding().min())
        high_list = Final60Tdata['max'].rolling(9, min_periods=9).max().fillna(Final60Tdata['max'].expanding().max())
        rsv = (Final60Tdata['close'] - low_list) / (high_list - low_list) * 100
        Final60Tdata['K'] = pd.DataFrame(rsv).ewm(com=2).mean()
        Final60Tdata['D'] = Final60Tdata['K'].ewm(com=2).mean()
        ds = 2
        Final60Tdata['uline'] = Final60Tdata['max'].rolling(ds, min_periods=1).max()
        Final60Tdata['dline'] = Final60Tdata['min'].rolling(ds, min_periods=1).min()
        Final60Tdata["all_kk"] = 0
        Final60Tdata['labelb'] = 1
        Final60Tdata = Final60Tdata[~Final60Tdata.index.duplicated(keep='first')]

        barssince5 = barssince6 = 0
        for i in range(2, len(Final60Tdata.index)):
            try:
                condition51 = (Final60Tdata['max'].iloc[i-1] < Final60Tdata['min'].iloc[i-2]) and \
                      (Final60Tdata['min'].iloc[i]   > Final60Tdata['max'].iloc[i-1])
                condition53 = (Final60Tdata['close'].iloc[i]   > Final60Tdata['uline'].iloc[i-1]) and \
                      (Final60Tdata['close'].iloc[i-1] <= Final60Tdata['uline'].iloc[i-1])
                condition61 = (Final60Tdata['min'].iloc[i-1] > Final60Tdata['max'].iloc[i-2]) and \
                      (Final60Tdata['max'].iloc[i]   < Final60Tdata['min'].iloc[i-1])
                condition63 = (Final60Tdata['close'].iloc[i]   < Final60Tdata['dline'].iloc[i-1]) and \
                      (Final60Tdata['close'].iloc[i-1] >= Final60Tdata['dline'].iloc[i-1])
            except Exception:
                condition51=condition53=condition61=condition63=True
            condition54 = condition51 or condition53
            condition64 = condition61 or condition63

            if Final60Tdata.iloc[i].close > Final60Tdata.iloc[i].upper_band1:
                Final60Tdata.iloc[i, Final60Tdata.columns.get_loc('labelb')] = 1
            elif Final60Tdata.iloc[i].close < Final60Tdata.iloc[i].lower_band1:
                Final60Tdata.iloc[i, Final60Tdata.columns.get_loc('labelb')] = -1
            else:
                Final60Tdata.iloc[i, Final60Tdata.columns.get_loc('labelb')] = Final60Tdata.iloc[i-1].labelb

            barssince5 = 1 if condition54 else (barssince5 + 1)
            barssince6 = 1 if condition64 else (barssince6 + 1)
            Final60Tdata.iloc[i, Final60Tdata.columns.get_loc('all_kk')] = 1 if barssince5 < barssince6 else -1

        # 只留最近 130 根
        Final60Tdata = Final60Tdata.iloc[-130:].copy()

        # ====== 300分彙整 ======
        start_times = [timedelta(hours=1), timedelta(hours=8, minutes=45), timedelta(hours=15), timedelta(hours=20)]
        data_300 = []
        current_date = datetime.combine(df_ts.iloc[0]['ts'].date(), dtime(0, 0))
        while current_date.date() <= df_ts['ts'].iloc[-1].date():
            for stt in start_times:
                start = current_date + stt
                end   = start + timedelta(hours=5)
                period = df_ts[(df_ts['ts'] > start) & (df_ts['ts'] < end)].dropna(subset=['Open'])
                if period.shape[0]:
                    data_300.append([start, period.iloc[0]['Open'], period.iloc[-1]['Close'],
                                     period['High'].max(), period['Low'].min(), period['Volume'].sum()])
                else:
                    data_300.append([start, None, None, None, None, None])
            current_date += timedelta(days=1)

        df_300 = pd.DataFrame(data_300, columns=['ts','open','close','max','min','Volume'])
        df_300 = df_300.dropna(subset=['open'])
        df_300['date'] = df_300['ts']
        df_300.set_index('ts', inplace=True)

        df_300['dateonly'] = pd.to_datetime((df_300.index - timedelta(hours=15)).date)
        df_300.loc[(df_300.date - timedelta(hours=13)).dt.weekday == 6, 'dateonly'] = pd.to_datetime(
            (df_300[(df_300.date - timedelta(hours=13)).dt.weekday == 6].date - timedelta(hours=63)).dt.date
        )
        for k, v in manual.items():
            df_300.loc[df_300.dateonly == k, 'dateonly'] = v

        df_300 = pd.merge(df_300, cost_df,     left_on="dateonly", right_on="日期", how='left')
        df_300 = pd.merge(df_300, inves_limit, on="日期",          how='left')
        df_300 = pd.merge(df_300, dealer_limit,on="日期",          how='left')
        df_300 = df_300.sort_index()

        df_300['20MA'] = df_300['close'].rolling(20).mean()
        df_300['std']  = df_300['close'].rolling(20).std()
        df_300['upper_band']  = df_300['20MA'] + 2 * df_300['std']
        df_300['lower_band']  = df_300['20MA'] - 2 * df_300['std']
        df_300['upper_band1'] = df_300['20MA'] + 1 * df_300['std']
        df_300['lower_band1'] = df_300['20MA'] - 1 * df_300['std']

        low_list2  = df_300['min'].rolling(9, min_periods=9).min().fillna(df_300['min'].expanding().min())
        high_list2 = df_300['max'].rolling(9, min_periods=9).max().fillna(df_300['max'].expanding().max())
        rsv2 = (df_300['close'] - low_list2) / (high_list2 - low_list2) * 100
        df_300['K'] = pd.DataFrame(rsv2).ewm(com=2).mean()
        df_300['D'] = df_300['K'].ewm(com=2).mean()
        ds2 = 2
        df_300['uline'] = df_300['max'].rolling(ds2, min_periods=1).max()
        df_300['dline'] = df_300['min'].rolling(ds2, min_periods=1).min()
        df_300["all_kk"] = 0
        df_300['labelb'] = 1
        df_300 = df_300[~df_300.index.duplicated(keep='first')]

        # 簡化同樣標註流程（不需再次 barssince；直接繼續沿用上面的方式亦可）
        for i in range(2, len(df_300.index)):
            if df_300.iloc[i].close > df_300.iloc[i].upper_band1:
                df_300.iloc[i, df_300.columns.get_loc('labelb')] = 1
            elif df_300.iloc[i].close < df_300.iloc[i].lower_band1:
                df_300.iloc[i, df_300.columns.get_loc('labelb')] = -1
            else:
                df_300.iloc[i, df_300.columns.get_loc('labelb')] = df_300.iloc[i-1].labelb
            # all_kk （與 60分一致的邏輯：用 uline/dline 觸發，這裡保持簡化可行版本）
            # all_kk（用與 60 分相同的條件，但全都改成欄位取值）
            cond54 = ( (df_300['max'].iloc[i-1] < df_300['min'].iloc[i-2]) and
                    (df_300['min'].iloc[i]   > df_300['max'].iloc[i-1]) ) or \
                    ( (df_300['close'].iloc[i]   > df_300['uline'].iloc[i-1]) and
                    (df_300['close'].iloc[i-1] <= df_300['uline'].iloc[i-1]) )

            cond64 = ( (df_300['min'].iloc[i-1] > df_300['max'].iloc[i-2]) and
                    (df_300['max'].iloc[i]   < df_300['min'].iloc[i-1]) ) or \
                    ( (df_300['close'].iloc[i]   < df_300['dline'].iloc[i-1]) and
                    (df_300['close'].iloc[i-1] >= df_300['dline'].iloc[i-1]) )

            df_300.loc[df_300.index[i], 'all_kk'] = 1 if cond54 and not cond64 else \
                                                (-1 if cond64 and not cond54 else df_300['all_kk'].iloc[i-1])
            # 用近兩根作簡化決策
            # df_300.iloc[i, df_300.columns.get_loc('all_kk')] = 1 if cond54 and not cond64 else (-1 if cond64 and not cond54 else df_300.iloc[i-1].get('all_kk', 0))

        df_300 = df_300.iloc[-80:].copy()

        # =========================
        # 3) 作圖
        # =========================
        fig = make_subplots(
            rows=2, cols=1,
            specs=[[{"secondary_y": True}], [{"secondary_y": True}]],
            vertical_spacing=0.2,
            subplot_titles=["TAIEX FUTURE 60分", "TAIEX FUTURE 300分"]
        )

        # --- 第一列：60分 ---
        x60 = Final60Tdata.index.strftime("%m-%d-%Y %H:%M")
        volume_colors1 = [red_color if Final60Tdata['close'].iloc[i] > Final60Tdata['close'].iloc[i-1] else green_color for i in range(len(Final60Tdata['close']))]
        if len(volume_colors1): volume_colors1[0] = green_color
        fig.add_trace(go.Bar(x=x60, y=Final60Tdata['Volume'], name='成交量', marker=dict(color=volume_colors1)), row=1, col=1, secondary_y=False)

        # 帶區塊（依 labelb 決定填色）
        idxs = Final60Tdata.index
        for bandstart in range(1, len(idxs)):
            if bandstart >= len(idxs)-1: break
            label_now = Final60Tdata['labelb'].iloc[bandstart]
            checkidx = bandstart
            while checkidx < len(idxs)-1 and Final60Tdata['labelb'].iloc[checkidx] == Final60Tdata['labelb'].iloc[checkidx+1]:
                checkidx += 1
            bandend = min(checkidx+1, len(idxs))
            color = 'rgba(256,256,0,0.2)' if label_now == 1 else 'rgba(137, 207, 240, 0.2)'
            xseg = idxs[bandstart:bandend].strftime("%m-%d-%Y %H:%M")
            fig.add_trace(go.Scatter(x=xseg, y=Final60Tdata['lower_band'].iloc[bandstart:bandend], line=dict(color='rgba(0,0,0,0)'),
                                     showlegend=False), row=1, col=1, secondary_y=True)
            fig.add_trace(go.Scatter(x=xseg, y=Final60Tdata['upper_band'].iloc[bandstart:bandend], line=dict(color='rgba(0,0,0,0)'),
                                     fill='tonexty', fillcolor=color, showlegend=False), row=1, col=1, secondary_y=True)

        # 成本、均線
        fig.add_trace(go.Scatter(x=x60, y=Final60Tdata['外資成本'], mode='lines', name='外資成本'), row=1, col=1, secondary_y=True)
        fig.add_trace(go.Scatter(x=x60, y=Final60Tdata['20MA'],     mode='lines', line=dict(color='green'), name='MA20'), row=1, col=1, secondary_y=True)

        # K 線（四種情境）
        def cs_masked(df, msk, color, width=1):
            return go.Candlestick(
                x=df.index[msk].strftime("%m-%d-%Y %H:%M"),
                open=df['open'][msk], high=df['max'][msk], low=df['min'][msk], close=df['close'][msk],
                increasing_line_color=color, increasing_fillcolor=no_color,
                decreasing_line_color=color, decreasing_fillcolor=no_color,
                line=dict(width=width), name='OHLC', showlegend=False
            )
        up1 = (Final60Tdata['all_kk'] == -1) & (Final60Tdata['close'] > Final60Tdata['open'])
        up2 = (Final60Tdata['all_kk'] ==  1) & (Final60Tdata['close'] > Final60Tdata['open'])
        dn1 = (Final60Tdata['all_kk'] == -1) & (Final60Tdata['close'] < Final60Tdata['open'])
        dn2 = (Final60Tdata['all_kk'] ==  1) & (Final60Tdata['close'] < Final60Tdata['open'])
        fig.add_trace(cs_masked(Final60Tdata, up1, decreasing_color, 2), row=1, col=1, secondary_y=True)
        fig.add_trace(cs_masked(Final60Tdata, up2, increasing_color, 1), row=1, col=1, secondary_y=True)
        fig.add_trace(cs_masked(Final60Tdata, dn1, decreasing_color, 1), row=1, col=1, secondary_y=True)
        fig.add_trace(cs_masked(Final60Tdata, dn2, increasing_color, 1), row=1, col=1, secondary_y=True)

        # --- 第二列：300分 ---
        x300 = df_300.index.strftime("%m-%d-%Y %H:%M")
        volume_colors2 = [red_color if df_300['close'].iloc[i] > df_300['close'].iloc[i-1] else green_color for i in range(len(df_300['close']))]
        if len(volume_colors2): volume_colors2[0] = green_color
        fig.add_trace(go.Bar(x=x300, y=df_300['Volume'], name='成交量', marker=dict(color=volume_colors2)), row=2, col=1, secondary_y=False)
        fig.add_trace(go.Scatter(x=x300, y=df_300['外資成本'], mode='lines', name='外資成本'), row=2, col=1, secondary_y=True)
        fig.add_trace(go.Scatter(x=x300, y=df_300['20MA'],     mode='lines', line=dict(color='green'), name='MA20'), row=2, col=1, secondary_y=True)

        # 軸與樣式
        fig.update_xaxes(rangeslider={'visible': False},
                         rangebreaks=[dict(bounds=[6,8], pattern="hour"),
                                      dict(bounds=[13,15], pattern="hour"),
                                      dict(bounds=['sun','mon'])],
                         row=1, col=1)
        fig.update_xaxes(rangeslider={'visible': False},
                         rangebreaks=[dict(bounds=['sat','mon'])],
                         row=2, col=1)
        fig.update_yaxes(range=[0, max(1, Final60Tdata['Volume'].max())*1.1], showgrid=False, secondary_y=False, row=1, col=1)
        fig.update_yaxes(range=[Final60Tdata['min'].min()-200, Final60Tdata['max'].max()+200], showgrid=False, secondary_y=True, row=1, col=1)
        fig.update_yaxes(range=[0, max(1, df_300['Volume'].max())*1.1], showgrid=False, secondary_y=False, row=2, col=1)
        fig.update_yaxes(range=[df_300['min'].min()-200, df_300['max'].max()+200], showgrid=False, secondary_y=True, row=2, col=1)

        fig.update_annotations(font_size=12)
        fig.update_layout(
            title_text="", hovermode='x unified',
            width=1200, height=1200,
            hoverlabel_namelength=-1,
            hoverlabel=dict(align='left', bgcolor='rgba(255,255,255,0.5)', font=dict(color='black')),
            legend_traceorder="reversed"
        )

        # =========================
        # 4) 原子輸出 PNG
        # =========================
        out = images_dir / filename
        tmp = out.with_suffix(".png.tmp")
        fig.write_image(str(tmp), format="png", scale=2)   # 需要 plotly + kaleido (+ Chrome)
        os.replace(tmp, out)
        return out

    finally:
        try: con_eq.close()
        except Exception: pass
        try: con_fu.close()
        except Exception: pass

@app.route("/images/<path:fname>", methods=["GET"])
def serve_image(fname):
    """從 Persistent Disk 回傳圖片（/images/固定路徑）"""
    safe = Path(fname).name  # 防止目錄跳脫
    full = IMAGES_DIR / safe
    if not full.exists():
        abort(404)
    resp = make_response(send_from_directory(IMAGES_DIR, safe, mimetype="image/png"))
    resp.headers["Cache-Control"] = "public, max-age=300"
    return resp

# ========= 共用推播 =========
def _push_to_uids(uids: List[str], msg: str) -> int:
    sent = 0
    for uid in uids:
        try:
            line_bot_api.push_message(uid, TextSendMessage(text=msg))
        except Exception as e:
            logger.warning("push fail uid=%s err=%s", uid, e)
        else:
            sent += 1
    return sent

def _push_image_to_uids(uids: List[str], img_url: str) -> int:
    sent = 0
    for uid in uids:
        try:
            line_bot_api.push_message(uid, ImageSendMessage(
                original_content_url=img_url,
                preview_image_url=img_url
            ))
        except Exception as e:
            logger.warning("push image fail uid=%s err=%s", uid, e)
        else:
            sent += 1
    return sent

def _push_to_whitelist(msg: str) -> int:
    wl = sorted(list(_load_whitelist()))
    return _push_to_uids(wl, msg)

def _push_image_to_whitelist(img_url: str) -> int:
    wl = sorted(list(_load_whitelist()))
    return _push_image_to_uids(wl, img_url)

def _public_base() -> str:
    # 若未設定 PUBLIC_BASE，就用 request.host_url
    return (PUBLIC_BASE or request.host_url).rstrip("/")

# ========= 健康檢查 =========
@app.route("/health")
def health():
    return "OK"

@app.route("/admin/ping")
def admin_ping():
    return jsonify(ok=True, ts=int(time.time()))

# ========= LINE Webhook =========
@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return "OK"


@handler.add(MessageEvent, message=TextMessage)
def on_message(event: MessageEvent):
    text = (event.message.text or "").strip()
    uid = event.source.user_id
    wl = _load_whitelist()

    def reply(msg: str):
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))

    # 註冊
    if text.lower().startswith("註冊") or text.lower().startswith("register"):
        parts = text.replace("　", " ").split()
        if len(parts) < 2:
            reply(f"請輸入：註冊 <驗證碼>\n例如：註冊 abc123")
            return
        code = parts[1]
        if code == REGISTER_CODE:
            if uid in wl:
                reply("你已經完成訂閱囉 ✅")
            else:
                wl.add(uid)
                _save_whitelist(wl)
                reply("訂閱成功！之後會收到定時通知 ✅")
        else:
            reply("驗證碼錯誤 ❌，請再試一次。")
        return

    # 取消
    if text in ["取消訂閱", "退訂", "unsubscribe"]:
        if uid in wl:
            wl.remove(uid)
            _save_whitelist(wl)
            reply("已取消訂閱 ✅ 之後不會再收到定時通知。")
        else:
            reply("你目前不在訂閱名單中。")
        return

    # 狀態
    if text in ["狀態", "status"]:
        reply("目前狀態：{}".format("✅ 已訂閱" if uid in wl else "未訂閱"))
        return

    # 導引
    reply(
        "嗨！\n"
        f"若要接收定時通知，請輸入：\n「註冊 <驗證碼>」\n"
        "取消請輸入：「取消訂閱」\n"
        "查詢請輸入：「狀態」"
    )

# ========= 兩段式：先生圖 → 再推圖 =========
@app.route("/cron/genplot", methods=["POST", "GET"])
def cron_genplot():
    _require_key()
    filename = request.args.get("filename") or "latest.png"
    p = generate_plot_png(filename)
    img_url = f"{_public_base()}/images/{p.name}"
    return jsonify(ok=True, filename=p.name, path=str(p), url=img_url)

@app.route("/cron/push", methods=["POST", "GET"])
def cron_push():
    _require_key()
    mode = (request.args.get("mode") or "text").lower()   # 'text' | 'image'
    if mode == "image":
        filename = request.args.get("filename") or "latest.png"
        p = IMAGES_DIR / filename
        if not p.exists():
            abort(404)  # 沒先生圖就推，直接 404（也可改成自動 gen）
        img_url = f"{_public_base()}/images/{p.name}"
        count = _push_image_to_whitelist(img_url)
        return f"OK, pushed image to {count} subscribers. url={img_url}"

    # 預設：純文字
    msg = request.args.get("message") or "⏰ 固定時間提醒來囉！"
    count = _push_to_whitelist(msg)
    return f"OK, pushed to {count} subscribers"

# ========= 管理 / 除錯端點（需 CRON_KEY） =========
@app.route("/admin/whitelist", methods=["GET"])
def admin_whitelist():
    _require_key()
    wl = sorted(list(_load_whitelist()))
    return jsonify(count=len(wl), user_ids=wl)

@app.route("/admin/whitelist/add", methods=["POST", "GET"])
def admin_whitelist_add():
    _require_key()
    uid = request.args.get("uid") or request.form.get("uid")
    if not uid:
        abort(400)
    wl = _load_whitelist()
    wl.add(uid)
    _save_whitelist(wl)
    return jsonify(ok=True, count=len(wl))

@app.route("/admin/whitelist/remove", methods=["POST", "GET"])
def admin_whitelist_remove():
    _require_key()
    uid = request.args.get("uid") or request.form.get("uid")
    if not uid:
        abort(400)
    wl = _load_whitelist()
    wl.discard(uid)
    _save_whitelist(wl)
    return jsonify(ok=True, count=len(wl))

@app.route("/admin/push", methods=["POST", "GET"])
def admin_push():
    _require_key()
    msg = request.args.get("message") or request.form.get("message") or "管理者手動推播"
    count = _push_to_whitelist(msg)
    return jsonify(ok=True, pushed=count)

@app.route("/admin/testpush", methods=["POST", "GET"])
def admin_testpush():
    _require_key()
    uid = request.args.get("uid") or request.form.get("uid")
    if not uid:
        abort(400)
    msg = request.args.get("message") or request.form.get("message") or "單人測試推播"
    count = _push_to_uids([uid], msg)
    return jsonify(ok=True, pushed=count)

@app.route("/admin/env", methods=["GET"])
def admin_env():
    _require_key()
    def masked(tok: str) -> str:
        if not tok: return ""
        if len(tok) <= 8: return "*" * len(tok)
        return tok[:4] + "*" * (len(tok) - 8) + tok[-4:]
    return jsonify(
        has_token=bool(CHANNEL_ACCESS_TOKEN),
        has_secret=bool(CHANNEL_SECRET),
        register_code=REGISTER_CODE,
        cron_key_set=bool(CRON_KEY),
        whitelist_path=str(WHITE_LIST_FILE),
        images_dir=str(IMAGES_DIR),
        public_base=PUBLIC_BASE or "(auto from request)",
        token_masked=masked(CHANNEL_ACCESS_TOKEN),
        secret_masked=masked(CHANNEL_SECRET),
    )

# ========= 本機啟動 =========
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)