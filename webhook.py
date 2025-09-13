# -*- coding: utf-8 -*-
import os, json, threading, time, logging
from pathlib import Path
from typing import Any, Dict, Set, List, Optional

from flask import Flask, request, abort, jsonify, send_from_directory, make_response
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage, ImageSendMessage

import sqlite3, requests, pandas as pd, numpy as np, io, time, os
from datetime import date, datetime, timedelta, time as dtime
from pathlib import Path
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

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
ANALYSIS_SQLITE_URL = os.environ.get("ANALYSIS_SQLITE_URL") # 選擇權分析.sqlite3 的 RAW 連結（可留空不使用）
EQUITY_SQLITE_CACHE = Path(os.environ.get("EQUITY_SQLITE_CACHE", "/var/data/cache/equity.sqlite"))
FUTURES_SQLITE_CACHE= Path(os.environ.get("FUTURES_SQLITE_CACHE","/var/data/cache/future.sqlite"))
ANALYSIS_SQLITE_CACHE=Path(os.environ.get("ANALYSIS_SQLITE_CACHE","/var/data/cache/analysis.sqlite"))
SQLITE_TTL_SEC      = int(os.environ.get("SQLITE_TTL_SEC", "900"))  # 15分鐘快取
GITHUB_TOKEN        = os.environ.get("GITHUB_TOKEN")  # 私有 repo 才需要，可留空

# （若你的資料庫內表名與我預設不同，可用這三個環境變數覆蓋；不填就用你 SQL 寫的原名）
EQ_COST_TABLE      = os.environ.get("EQ_COST_TABLE", "cost")
EQ_LIMIT_TABLE     = os.environ.get("EQ_LIMIT_TABLE", "limit")
FUT_HOURLY_TABLE   = os.environ.get("FUT_HOURLY_TABLE", "futurehourly")

# 簡單配色
increasing_color = 'rgb(255, 0, 0)'
decreasing_color = 'rgb(0, 0, 245)'

red_color = 'rgba(255, 0, 0, 0.2)'
green_color = 'rgba(30, 144, 255,0.2)'

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
    elif which == "analysis":
        p = _get_cached_sqlite(ANALYSIS_SQLITE_URL, ANALYSIS_SQLITE_CACHE)
    else:
        raise ValueError("which must be 'equity' or 'futures'")
    return sqlite3.connect(str(p))

# ========= 表格：生成 & 成圖 =========
def _df_to_table_png(df: pd.DataFrame, filename: str, title: Optional[str] = None) -> Path:
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    h = max(2.5, 0.55 * (len(df) + 2))
    w = min(22, max(6, len(df.columns) * 1.2))
    fig, ax = plt.subplots(figsize=(w, h), dpi=180)
    ax.axis('off')
    if title:
        ax.set_title(title, fontsize=14, pad=10)
    tbl = ax.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    tbl.scale(1, 1.2)
    out = IMAGES_DIR / filename
    plt.tight_layout()
    fig.savefig(out, bbox_inches='tight')
    plt.close(fig)
    return out

def _push_images_to_uids(uids: List[str], img_urls: List[str]) -> int:
    MAX_PER_PUSH = 5
    sent_total = 0
    batches = [img_urls[i:i+MAX_PER_PUSH] for i in range(0, len(img_urls), MAX_PER_PUSH)]
    for uid in uids:
        for batch in batches:
            try:
                messages = [ImageSendMessage(original_content_url=u, preview_image_url=u) for u in batch]
                line_bot_api.push_message(uid, messages)
                sent_total += len(batch)
            except Exception as e:
                logger.warning("push images fail uid=%s err=%s", uid, e)
    return sent_total

def _push_images_to_whitelist(img_urls: List[str]) -> int:
    wl = sorted(list(_load_whitelist()))
    return _push_images_to_uids(wl, img_urls)


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
    red_color        = locals().get("red_color",   'rgba(255, 0, 0, 0.2)')
    green_color      = locals().get("green_color", 'rgba(30, 144, 255,0.2)')
    increasing_color = locals().get("increasing_color", 'rgb(255, 0, 0)')
    decreasing_color = locals().get("decreasing_color", 'rgb(0, 0, 245)')
    no_color         = locals().get("no_color", 'rgba(256, 256, 256,0)')


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
    con_an = get_sqlite_connection("analysis")

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
        FutureData.loc[(FutureData.ts.dt.hour<14)&(FutureData.ts.dt.hour>=8),'ts'] = FutureData.loc[(FutureData.ts.dt.hour<14)&(FutureData.ts.dt.hour>=8)].ts - timedelta(minutes=46)
        FutureData.index = FutureData.ts

        #FutureData.index = FutureData.ts
        FutureData.date = pd.to_datetime(FutureData.index)
        FutureData["hourdate"] = np.array(FutureData.date.date.astype(str)) +  np.array(FutureData.date.hour.astype(str))
        FutureData['date'] = np.array(pd.to_datetime(FutureData.index).values)
        #FutureData
        FutureData = FutureData.dropna(subset = ['Open'])
        FutureData.index = FutureData['date']
        #FutureData
        tempdf = FutureData[['hourdate','Volume']]
        tempdf = tempdf.reset_index()
        tempdf = tempdf[['hourdate','Volume']]
        #tempdf

        Final60Tdata = FutureData.groupby('hourdate').max()[["High"]].join(FutureData.groupby('hourdate').min()[["Low"]]).join(tempdf.groupby('hourdate').sum()[["Volume"]])
        #Final60Tdata
        #Final60Tdata.index = Final60Tdata['hourdate']
        tempopen = FutureData.loc[FutureData.groupby('hourdate').min()['date'].values]
        tempopen.index = tempopen.hourdate
        tempclose = FutureData.loc[FutureData.groupby('hourdate').max()['date'].values]
        tempclose.index = tempclose.hourdate
        #tempvolume = FutureData.loc[FutureData.groupby('hourdate').sum()['Volume'].values]
        #tempvolume.index = tempvolume.hourdate
        Final60Tdata = Final60Tdata.join(tempopen[["Open",'date']]).join(tempclose[["Close"]])
        Final60Tdata.index = Final60Tdata.date
        Final60Tdata.columns = ['max','min','Volume','open','date','close']


        Final60Tdata['dateonly'] = pd.to_datetime((Final60Tdata.date- timedelta(hours=15)).dt.date)
        Final60Tdata.loc[(Final60Tdata.date - timedelta(hours=13)).dt.weekday ==6,'dateonly'] = pd.to_datetime((Final60Tdata[(Final60Tdata.date - timedelta(hours=13)).dt.weekday ==6].date- timedelta(hours=63)).dt.date)
        Final60Tdata.loc[Final60Tdata.dateonly ==datetime(2023, 9, 29, 0, 0),'dateonly'] = datetime(2023, 9, 28, 0, 0)

        Final60Tdata.loc[Final60Tdata.dateonly ==datetime(2024, 4,5, 0, 0),'dateonly'] = datetime(2024, 4, 3, 0, 0)
        Final60Tdata.loc[Final60Tdata.dateonly ==datetime(2024, 5,1, 0, 0),'dateonly'] = datetime(2024, 4, 30, 0, 0)
        Final60Tdata.loc[Final60Tdata.dateonly ==datetime(2024, 6,10, 0, 0),'dateonly'] = datetime(2024, 6, 7, 0, 0)
        Final60Tdata.loc[Final60Tdata.dateonly ==datetime(2025, 2,28, 0, 0),'dateonly'] = datetime(2025, 2, 27, 0, 0)
        Final60Tdata.loc[Final60Tdata.dateonly ==datetime(2025, 4,4, 0, 0),'dateonly'] = datetime(2025, 4, 2, 0, 0)
        Final60Tdata.loc[Final60Tdata.dateonly ==datetime(2025, 5,1, 0, 0),'dateonly'] = datetime(2025, 4, 30, 0, 0)

        #Final60Tdata.loc[Final60Tdata.dateonly ==datetime(2023, 10, 10, 0, 0),'dateonly'] = datetime(2023, 10, 7, 0, 0)
        Final60Tdata = pd.merge(Final60Tdata, cost_df, left_on="dateonly", right_on="日期", how='left')
        Final60Tdata = pd.merge(Final60Tdata, inves_limit, on="日期", how='left')
        Final60Tdata = pd.merge(Final60Tdata, dealer_limit, on="日期", how='left')
        
        #Final60Tdata.loc[Final60Tdata["外資成本"]==None,['外資成本','外資上極限','外資下極限','自營商上極限','自營商下極限']] = [16347,16673,16227,16645,16155]
        Final60Tdata.loc[Final60Tdata.date.dt.minute == 1 ,'date'] = Final60Tdata.loc[Final60Tdata.date.dt.minute == 1 ,'date'] - timedelta(minutes = 1)
        Final60Tdata.index = Final60Tdata.date
        Final60Tdata = Final60Tdata.sort_index()
        #Final60Tdata
        Final60Tdata[Final60Tdata.index == Final60Tdata.index[-1]][["日期","外資成本","外資上極限","外資下極限","自營商上極限","自營商下極限"]]





        # 計算布林帶指標
        # 計算布林帶指標
        Final60Tdata['20MA'] = Final60Tdata['close'].rolling(20).mean()
        #Final60Tdata['60MA'] = Final60Tdata['close'].rolling(60).mean()
        #Final60Tdata['200MA'] = Final60Tdata['close'].rolling(200).mean()
        Final60Tdata['std'] = Final60Tdata['close'].rolling(20).std()
        Final60Tdata['upper_band'] = Final60Tdata['20MA'] + 2 * Final60Tdata['std']
        Final60Tdata['lower_band'] = Final60Tdata['20MA'] - 2 * Final60Tdata['std']
        Final60Tdata['upper_band1'] = Final60Tdata['20MA'] + 1 * Final60Tdata['std']
        Final60Tdata['lower_band1'] = Final60Tdata['20MA'] - 1 * Final60Tdata['std']

        Final60Tdata['IC'] = Final60Tdata['close'] + 2 * Final60Tdata['close'].shift(1) - Final60Tdata['close'].shift(3) -Final60Tdata['close'].shift(4)



        # 在k线基础上计算KDF，并将结果存储在df上面(k,d,j)
        low_list = Final60Tdata['min'].rolling(9, min_periods=9).min()
        low_list.fillna(value=Final60Tdata['min'].expanding().min(), inplace=True)
        high_list = Final60Tdata['max'].rolling(9, min_periods=9).max()
        high_list.fillna(value=Final60Tdata['max'].expanding().max(), inplace=True)
        rsv = (Final60Tdata['close'] - low_list) / (high_list - low_list) * 100
        Final60Tdata['K'] = pd.DataFrame(rsv).ewm(com=2).mean()
        Final60Tdata['D'] = Final60Tdata['K'].ewm(com=2).mean()

        #enddatemonth = enddate[~enddate["契約月份"].str.contains("W")]['最後結算日']
        Final60Tdata['end_low'] = 0
        Final60Tdata['end_high'] = 0

        #詢問
        ds = 2
        Final60Tdata['uline'] = Final60Tdata['max'].rolling(ds, min_periods=1).max()
        Final60Tdata['dline'] = Final60Tdata['min'].rolling(ds, min_periods=1).min()

        Final60Tdata["all_kk"] = 0
        barssince5 = 0
        barssince6 = 0
        Final60Tdata['labelb'] = 1
        Final60Tdata = Final60Tdata[~Final60Tdata.index.duplicated(keep='first')]

        for i in range(2,len(Final60Tdata.index)):
            try:
                #(Final60Tdata.loc[Final60Tdata.index[i],'close'] > Final60Tdata.loc[Final60Tdata.index[i-1],"uline"])
                condition51 = (Final60Tdata.loc[Final60Tdata.index[i-1],"max"] < Final60Tdata.loc[Final60Tdata.index[i-2],"min"] ) and (Final60Tdata.loc[Final60Tdata.index[i],"min"] > Final60Tdata.loc[Final60Tdata.index[i-1],"max"] )
                #condition52 = (Final60Tdata.loc[Final60Tdata.index[i-1],'close'] < Final60Tdata.loc[Final60Tdata.index[i-2],"min"]) and (Final60Tdata.loc[Final60Tdata.index[i-1],'成交金額'] > Final60Tdata.loc[Final60Tdata.index[i-2],'成交金額']) and (Final60Tdata.loc[Final60Tdata.index[i],'close']>Final60Tdata.loc[Final60Tdata.index[i-1],"max"] )
                condition53 = (Final60Tdata.loc[Final60Tdata.index[i],'close'] > Final60Tdata.loc[Final60Tdata.index[i-1],"uline"]) and (Final60Tdata.loc[Final60Tdata.index[i-1],'close'] <= Final60Tdata.loc[Final60Tdata.index[i-1],"uline"])

                condition61 = (Final60Tdata.loc[Final60Tdata.index[i-1],"min"] > Final60Tdata.loc[Final60Tdata.index[i-2],"max"] ) and (Final60Tdata.loc[Final60Tdata.index[i],"max"] < Final60Tdata.loc[Final60Tdata.index[i-1],"min"] )
                #condition62 = (Final60Tdata.loc[Final60Tdata.index[i-1],'close'] > Final60Tdata.loc[Final60Tdata.index[i-2],"max"]) and (Final60Tdata.loc[Final60Tdata.index[i-1],'成交金額'] > Final60Tdata.loc[Final60Tdata.index[i-2],'成交金額']) and (Final60Tdata.loc[Final60Tdata.index[i],'close']<Final60Tdata.loc[Final60Tdata.index[i-1],"min"] )
                condition63 = (Final60Tdata.loc[Final60Tdata.index[i],'close'] < Final60Tdata.loc[Final60Tdata.index[i-1],"dline"]) and (Final60Tdata.loc[Final60Tdata.index[i-1],'close'] >= Final60Tdata.loc[Final60Tdata.index[i-1],"dline"])
            except:
                condition51 = True
                condition52 = True
                condition53 = True
                condition61 = True
                condition63 = True
            condition54 = condition51 or condition53 #or condition52
            condition64 = condition61 or condition63 #or condition62 

            #Final60Tdata['labelb'] = np.where((Final60Tdata['close']> Final60Tdata['upper_band1']) , 1, np.where((Final60Tdata['close']< Final60Tdata['lower_band1']),-1,1))

            #print(i)
            if Final60Tdata.loc[Final60Tdata.index[i],'close'] > Final60Tdata.loc[Final60Tdata.index[i],'upper_band1']:
                Final60Tdata.loc[Final60Tdata.index[i],'labelb'] = 1
            elif Final60Tdata.loc[Final60Tdata.index[i],'close'] < Final60Tdata.loc[Final60Tdata.index[i],'lower_band1']:
                Final60Tdata.loc[Final60Tdata.index[i],'labelb'] = -1
            else:
                Final60Tdata.loc[Final60Tdata.index[i],'labelb'] = Final60Tdata.loc[Final60Tdata.index[i-1],'labelb']

            if condition54 == True:
                barssince5 = 1
            else:
                barssince5 += 1

            if condition64 == True:
                barssince6 = 1
            else:
                barssince6 += 1


            if barssince5 < barssince6:
                Final60Tdata.loc[Final60Tdata.index[i],"all_kk"] = 1
            else:
                Final60Tdata.loc[Final60Tdata.index[i],"all_kk"] = -1

        Final60Tdata = Final60Tdata[Final60Tdata.index>Final60Tdata.index[-130]]
        IChour = []
        finalhour = list(Final60Tdata['IC'].index)[-1]
        plusi = 1
        while (finalhour + timedelta(hours = plusi)).hour in [6,7,13,14] or  (finalhour + timedelta(hours = plusi)-timedelta(hours = 5)).weekday in [5,6]:
            plusi = plusi + 1
        IChour.append((finalhour + timedelta(hours = plusi)).strftime("%m-%d %H"))
        IChour.append((finalhour + timedelta(hours = plusi+1)).strftime("%m-%d %H"))

        #Final60Tdata.index = Final60Tdata.index.astype('str')
        Final60Tdata.index = Final60Tdata.index.strftime("%m-%d %H")
        #Final60Tdata




        #Final60Tdata
        fig3_1 = make_subplots(
            rows = 2, 
            cols = 1, 
            horizontal_spacing = 0.05,
            #vertical_spacing=0.2,
            vertical_spacing=0.25,   # 拉開上下子圖距離（預設 0.2 → 可以改 0.15~0.25）
            row_heights=[0.55, 0.45],  # 上圖 60%，下圖 40%，避免擠在一起
            subplot_titles = ["TAIEX FUTURE 60/300",""],
            shared_yaxes=False,
            
            #subplot_titles=subtitle,
            #y_title = "test"# subtitle,
            specs = [[{"secondary_y":True}]]*2
        )

        ### 成交量圖製作 ###
        volume_colors1 = [red_color if Final60Tdata['close'][i] > Final60Tdata['close'][i-1] else green_color for i in range(len(Final60Tdata['close']))]
        volume_colors1[0] = green_color

        #fig.add_trace(go.Bar(x=Final60Tdata.index, y=Final60Tdata['成交金額'], name='成交數量', marker=dict(color=volume_colors),showlegend=False), row=optvrank[0], col=1)
        fig3_1.add_trace(go.Bar(x=Final60Tdata.index, y=Final60Tdata['Volume'], name='成交量', marker=dict(color=volume_colors1)), row=1, col=1)
        #Final60Tdata.index = Final60Tdata.index - timedelta(hours = 6)

        checkb = Final60Tdata["labelb"].values[0]
        bandstart = 1
        bandidx = 1
        checkidx = 0
        while bandidx < len(Final60Tdata["labelb"].values):
            #checkidx = bandidx
            bandstart = bandidx-1
            checkidx = bandstart+1
            if checkidx >=len(Final60Tdata["labelb"].values)-1:
                break
            while Final60Tdata["labelb"].values[checkidx] == Final60Tdata["labelb"].values[checkidx+1]:
                checkidx +=1
                if checkidx >=len(Final60Tdata["labelb"].values)-1:
                    break
            bandend = checkidx+1
            #print(bandstart,bandend)
            if Final60Tdata["labelb"].values[bandstart+1] == 1:
                fig3_1.add_traces(go.Scatter(x=Final60Tdata.index[bandstart:bandend], y = Final60Tdata['lower_band'].values[bandstart:bandend],
                                            line = dict(color='rgba(0,0,0,0)'),showlegend=False,hoverinfo='none'),rows=[1], cols=[1], secondary_ys= [True])
                    
                fig3_1.add_traces(go.Scatter(x=Final60Tdata.index[bandstart:bandend], y = Final60Tdata['upper_band'].values[bandstart:bandend],
                                            line = dict(color='rgba(0,0,0,0)'),
                                            fill='tonexty', 
                                            fillcolor = 'rgba(256,256,0,0.2)',showlegend=False,hoverinfo='none'
                                            ),rows=[1], cols=[1], secondary_ys= [True])
            else:


                fig3_1.add_traces(go.Scatter(x=Final60Tdata.index[bandstart:bandend], y = Final60Tdata['lower_band'].values[bandstart:bandend],
                                            line = dict(color='rgba(0,0,0,0)'),showlegend=False,hoverinfo='none'),rows=[1], cols=[1], secondary_ys= [True])
                    
                fig3_1.add_traces(go.Scatter(x=Final60Tdata.index[bandstart:bandend], y = Final60Tdata['upper_band'].values[bandstart:bandend],
                                            line = dict(color='rgba(0,0,0,0)'),
                                            fill='tonexty', 
                                            fillcolor = 'rgba(137, 207, 240,0.2)',showlegend=False,hoverinfo='none'
                                            ),rows=[1], cols=[1], secondary_ys= [True])
            bandidx =checkidx +1
            if bandidx >=len(Final60Tdata["labelb"].values):
                break




        fig3_1.add_trace(go.Scatter(x=Final60Tdata.index,
                                y=Final60Tdata['外資成本'],
                                mode='lines',
                                line=dict(width=0.5),
                                name='外資成本'),row=1, col=1, secondary_y= True)

        #fig3_1.add_trace(go.Scatter(x=Final60Tdata.index,
        #                        y=Final60Tdata['外資下極限'],
        #                        mode='lines',
        #                        #line=dict(color='green'),
        #                        name='外資下極限'),row=1, col=1)



        fig3_1.add_traces(go.Scatter(x=Final60Tdata.index, y = Final60Tdata['外資上極限'].values,
                                            line = dict(color='rgba(0,0,0,0)'),showlegend=False,name='外資上極限'),rows=[1], cols=[1], secondary_ys= [True])
                    
        fig3_1.add_traces(go.Scatter(x=Final60Tdata.index, y = Final60Tdata['自營商上極限'].values,
                                    line = dict(color='rgba(0,0,0,0)'),
                                    fill='tonexty', 
                                    fillcolor = 'rgba(0,0,256,0.2)',showlegend=False,name='自營商上極限'
                                    ),rows=[1], cols=[1], secondary_ys= [True])
        fig3_1.add_traces(go.Scatter(x=Final60Tdata.index, y = Final60Tdata['外資下極限'].values,
                                            line = dict(color='rgba(0,0,0,0)'),showlegend=False,name='外資下極限'),rows=[1], cols=[1], secondary_ys= [True])
                    
        fig3_1.add_traces(go.Scatter(x=Final60Tdata.index, y = Final60Tdata['自營商下極限'].values,
                                    line = dict(color='rgba(0,0,0,0)'),
                                    fill='tonexty', 
                                    fillcolor = 'rgba(256,0,0,0.2)',showlegend=False,name='自營商下極限'
                                    ),rows=[1], cols=[1], secondary_ys= [True])



        fig3_1.add_trace(go.Scatter(x=Final60Tdata.index,
                                y=Final60Tdata['20MA'],
                                mode='lines',
                                line=dict(color='green', width=0.5),
                                name='MA20'),row=1, col=1, secondary_y= True)
        #fig3_1.add_trace(go.Scatter(x=Final60Tdata.index,
        #                        y=Final60Tdata['200MA'],
        #                        mode='lines',
        #                        line=dict(color='blue'),
        #                        name='MA200'),row=1, col=1)
        #fig3_1.add_trace(go.Scatter(x=Final60Tdata.index,
        #                        y=Final60Tdata['60MA'],
        #                        mode='lines',
        #                        line=dict(color='orange'),
        #                        name='MA60'),row=1, col=1)

        #fig3_1.add_trace(go.Scatter(x=np.array(list(Final60Tdata['IC'].index)[2:]+IChour).astype('str'),
        #                        y=Final60Tdata['IC'].values,
        #                        mode='lines',
        #                        line=dict(color='orange'),
        #                        name='IC操盤線'),row=1, col=1, secondary_y= True)





        ### K線圖製作 ###
        fig3_1.add_trace(
            go.Candlestick(
                x=Final60Tdata[(Final60Tdata['all_kk'] == -1)&(Final60Tdata['close'] >Final60Tdata['open'] )].index,
                open=Final60Tdata[(Final60Tdata['all_kk'] == -1)&(Final60Tdata['close'] >Final60Tdata['open'] )]['open'],
                high=Final60Tdata[(Final60Tdata['all_kk'] == -1)&(Final60Tdata['close'] >Final60Tdata['open'] )]['max'],
                low=Final60Tdata[(Final60Tdata['all_kk'] == -1)&(Final60Tdata['close'] >Final60Tdata['open'] )]['min'],
                close=Final60Tdata[(Final60Tdata['all_kk'] == -1)&(Final60Tdata['close'] >Final60Tdata['open'] )]['close'],
                increasing_line_color=decreasing_color,
                increasing_fillcolor=no_color, #fill_increasing_color(Final60Tdata.index>Final60Tdata.index[50])
                decreasing_line_color=decreasing_color,
                decreasing_fillcolor=no_color,#decreasing_color,
                line=dict(width=0.5),
                name='OHLC',showlegend=False
            )#,

            ,row=1, col=1, secondary_y= True
        )


        fig3_1.add_trace(
            go.Candlestick(
                x=Final60Tdata[(Final60Tdata['all_kk'] == 1)&(Final60Tdata['close'] >Final60Tdata['open'] )].index,
                open=Final60Tdata[(Final60Tdata['all_kk'] == 1)&(Final60Tdata['close'] >Final60Tdata['open'] )]['open'],
                high=Final60Tdata[(Final60Tdata['all_kk'] == 1)&(Final60Tdata['close'] >Final60Tdata['open'] )]['max'],
                low=Final60Tdata[(Final60Tdata['all_kk'] == 1)&(Final60Tdata['close'] >Final60Tdata['open'] )]['min'],
                close=Final60Tdata[(Final60Tdata['all_kk'] == 1)&(Final60Tdata['close'] >Final60Tdata['open'] )]['close'],
                increasing_line_color=increasing_color,
                increasing_fillcolor=no_color, #fill_increasing_color(Final60Tdata.index>Final60Tdata.index[50])
                decreasing_line_color=increasing_color,
                decreasing_fillcolor=no_color,#decreasing_color,
                line=dict(width=0.5),
                name='OHLC',showlegend=False
            )#,

            ,row=1, col=1, secondary_y= True
        )

        ### K線圖製作 ###
        fig3_1.add_trace(
            go.Candlestick(
                x=Final60Tdata[(Final60Tdata['all_kk'] == -1)&(Final60Tdata['close'] <Final60Tdata['open'] )].index,
                open=Final60Tdata[(Final60Tdata['all_kk'] == -1)&(Final60Tdata['close'] <Final60Tdata['open'] )]['open'],
                high=Final60Tdata[(Final60Tdata['all_kk'] == -1)&(Final60Tdata['close'] <Final60Tdata['open'] )]['max'],
                low=Final60Tdata[(Final60Tdata['all_kk'] == -1)&(Final60Tdata['close'] <Final60Tdata['open'] )]['min'],
                close=Final60Tdata[(Final60Tdata['all_kk'] == -1)&(Final60Tdata['close'] <Final60Tdata['open'] )]['close'],
                increasing_line_color=decreasing_color,
                increasing_fillcolor=decreasing_color, #fill_increasing_color(Final60Tdata.index>Final60Tdata.index[50])
                decreasing_line_color=decreasing_color,
                decreasing_fillcolor=decreasing_color,#decreasing_color,
                line=dict(width=0.5),
                name='OHLC',showlegend=False
            )#,

            ,row=1, col=1, secondary_y= True
        )


        fig3_1.add_trace(
            go.Candlestick(
                x=Final60Tdata[(Final60Tdata['all_kk'] == 1)&(Final60Tdata['close'] <Final60Tdata['open'] )].index,
                open=Final60Tdata[(Final60Tdata['all_kk'] == 1)&(Final60Tdata['close'] <Final60Tdata['open'] )]['open'],
                high=Final60Tdata[(Final60Tdata['all_kk'] == 1)&(Final60Tdata['close'] <Final60Tdata['open'] )]['max'],
                low=Final60Tdata[(Final60Tdata['all_kk'] == 1)&(Final60Tdata['close'] <Final60Tdata['open'] )]['min'],
                close=Final60Tdata[(Final60Tdata['all_kk'] == 1)&(Final60Tdata['close'] <Final60Tdata['open'] )]['close'],
                increasing_line_color=increasing_color,
                increasing_fillcolor=increasing_color, #fill_increasing_color(Final60Tdata.index>FinalＷeekdata.index[50])
                decreasing_line_color=increasing_color,
                decreasing_fillcolor=increasing_color,#decreasing_color,
                line=dict(width=0.5),
                name='OHLC',showlegend=False
            )#,

            ,row=1, col=1, secondary_y= True
        )


        start_times = [timedelta(hours=1), timedelta(hours=8, minutes=45),
                    timedelta(hours=15), timedelta(hours=20)]
        data_300 = []



        current_date = datetime.combine(df_ts.iloc[0]['ts'].date(), dtime(0, 0, 0))

        while current_date.date() <= df_ts['ts'].iloc[-1].date():
            for start_time in start_times:
                start = current_date + start_time
                end = start + timedelta(hours=5)

                period = df_ts[(df_ts['ts'] > start) & (df_ts['ts'] < end)].dropna(subset = ['Open'])

                if period.shape[0]:
                    data_300.append([start, period.iloc[0]['Open'], period.iloc[-1]['Close'], period['High'].max(),
                                    period['Low'].min(),period['Volume'].sum()])
                else:
                    data_300.append([start, None, None, None, None,None])

            current_date += timedelta(days=1)

        df_300 = pd.DataFrame(data_300, columns=['ts', 'open','close','max','min','Volume'])
        df_300['date'] = df_300['ts']

        df_300 = df_300.dropna(subset = ['open'])

        df_300.set_index('ts', inplace=True)
        df_300['dateonly'] = pd.to_datetime((df_300.index- timedelta(hours=15)).date)
        df_300.loc[(df_300.date - timedelta(hours=13)).dt.weekday ==6,'dateonly'] = pd.to_datetime((df_300[(df_300.date - timedelta(hours=13)).dt.weekday ==6].date- timedelta(hours=63)).dt.date)

        df_300.loc[df_300.dateonly ==datetime(2024, 2, 28, 0, 0),'dateonly'] = datetime(2024, 2, 27, 0, 0)
        df_300.loc[df_300.dateonly ==datetime(2024, 4, 5, 0, 0),'dateonly'] = datetime(2024, 4, 3, 0, 0)
        df_300.loc[df_300.dateonly ==datetime(2024, 5, 1, 0, 0),'dateonly'] = datetime(2024, 4, 30, 0, 0)
        df_300.loc[df_300.dateonly ==datetime(2024, 6, 10, 0, 0),'dateonly'] = datetime(2024, 6, 7, 0, 0)
        df_300.loc[df_300.dateonly ==datetime(2025, 2,28, 0, 0),'dateonly'] = datetime(2025, 2, 27, 0, 0)
        df_300.loc[df_300.dateonly ==datetime(2025, 4,4, 0, 0),'dateonly'] = datetime(2025, 4, 2, 0, 0)
        df_300.loc[df_300.dateonly ==datetime(2025, 5, 1, 0, 0),'dateonly'] = datetime(2025, 4, 30, 0, 0)

        
        df_300 = pd.merge(df_300, cost_df, left_on="dateonly", right_on="日期",how='left')
        df_300 = pd.merge(df_300, inves_limit, on="日期",how='left')
        df_300 = pd.merge(df_300, dealer_limit, on="日期",how='left')
        
        #df_300.loc[Final60Tdata["外資成本"]==None,['外資成本','外資上極限','外資下極限','自營商上極限','自營商下極限']] = [16347,16673,16227,16645,16155]
        df_300.index = df_300.date

        df_300 = df_300.sort_index()

        # 計算布林帶指標
        df_300['20MA'] = df_300['close'].rolling(20).mean()
        #df_300['60MA'] = df_300['close'].rolling(60).mean()
        #df_300['200MA'] = df_300['close'].rolling(200).mean()
        df_300['std'] = df_300['close'].rolling(20).std()
        df_300['upper_band'] = df_300['20MA'] + 2 * df_300['std']
        df_300['lower_band'] = df_300['20MA'] - 2 * df_300['std']
        df_300['upper_band1'] = df_300['20MA'] + 1 * df_300['std']
        df_300['lower_band1'] = df_300['20MA'] - 1 * df_300['std']






        # 在k线基础上计算KDF，并将结果存储在df上面(k,d,j)
        low_list = df_300['min'].rolling(9, min_periods=9).min()
        low_list.fillna(value=df_300['min'].expanding().min(), inplace=True)
        high_list = df_300['max'].rolling(9, min_periods=9).max()
        high_list.fillna(value=df_300['max'].expanding().max(), inplace=True)
        rsv = (df_300['close'] - low_list) / (high_list - low_list) * 100
        df_300['K'] = pd.DataFrame(rsv).ewm(com=2).mean()
        df_300['D'] = df_300['K'].ewm(com=2).mean()

        #enddatemonth = enddate[~enddate["契約月份"].str.contains("W")]['最後結算日']
        df_300['end_low'] = 0
        df_300['end_high'] = 0

        #詢問
        ds = 2
        df_300['uline'] = df_300['max'].rolling(ds, min_periods=1).max()
        df_300['dline'] = df_300['min'].rolling(ds, min_periods=1).min()

        df_300["all_kk"] = 0
        barssince5 = 0
        barssince6 = 0
        df_300['labelb'] = 1
        df_300 = df_300[~df_300.index.duplicated(keep='first')]

        #df_300 = df_300.dropna(subset = ['open','close','max','min'])

        df_300['IC'] = df_300['close'] + 2 * df_300['close'].shift(1) - df_300['close'].shift(3) -df_300['close'].shift(4)
        #df_300 = df_300[df_300.index>df_300.index[-80]]

        for i in range(2,len(df_300.index)):
            try:
                #(df_300.loc[df_300.index[i],'close'] > df_300.loc[df_300.index[i-1],"uline"])
                condition51 = (df_300.loc[df_300.index[i-1],"max"] < df_300.loc[df_300.index[i-2],"min"] ) and (df_300.loc[df_300.index[i],"min"] > df_300.loc[df_300.index[i-1],"max"] )
                #condition52 = (df_300.loc[df_300.index[i-1],'close'] < df_300.loc[df_300.index[i-2],"min"]) and (df_300.loc[df_300.index[i-1],'成交金額'] > df_300.loc[df_300.index[i-2],'成交金額']) and (df_300.loc[df_300.index[i],'close']>df_300.loc[df_300.index[i-1],"max"] )
                condition53 = (df_300.loc[df_300.index[i],'close'] > df_300.loc[df_300.index[i-1],"uline"]) and (df_300.loc[df_300.index[i-1],'close'] <= df_300.loc[df_300.index[i-1],"uline"])

                condition61 = (df_300.loc[df_300.index[i-1],"min"] > df_300.loc[df_300.index[i-2],"max"] ) and (df_300.loc[df_300.index[i],"max"] < df_300.loc[df_300.index[i-1],"min"] )
                #condition62 = (df_300.loc[df_300.index[i-1],'close'] > df_300.loc[df_300.index[i-2],"max"]) and (df_300.loc[df_300.index[i-1],'成交金額'] > df_300.loc[df_300.index[i-2],'成交金額']) and (df_300.loc[df_300.index[i],'close']<df_300.loc[df_300.index[i-1],"min"] )
                condition63 = (df_300.loc[df_300.index[i],'close'] < df_300.loc[df_300.index[i-1],"dline"]) and (df_300.loc[df_300.index[i-1],'close'] >= df_300.loc[df_300.index[i-1],"dline"])
            except:
                condition51 = True
                condition52 = True
                condition53 = True
                condition61 = True
                condition63 = True
            condition54 = condition51 or condition53 #or condition52
            condition64 = condition61 or condition63 #or condition62 

            #df_300['labelb'] = np.where((df_300['close']> df_300['upper_band1']) , 1, np.where((df_300['close']< df_300['lower_band1']),-1,1))

            #print(i)
            if df_300.loc[df_300.index[i],'close'] > df_300.loc[df_300.index[i],'upper_band1']:
                df_300.loc[df_300.index[i],'labelb'] = 1
            elif df_300.loc[df_300.index[i],'close'] < df_300.loc[df_300.index[i],'lower_band1']:
                df_300.loc[df_300.index[i],'labelb'] = -1
            else:
                df_300.loc[df_300.index[i],'labelb'] = df_300.loc[df_300.index[i-1],'labelb']

            if condition54 == True:
                barssince5 = 1
            else:
                barssince5 += 1

            if condition64 == True:
                barssince6 = 1
            else:
                barssince6 += 1


            if barssince5 < barssince6:
                df_300.loc[df_300.index[i],"all_kk"] = 1
            else:
                df_300.loc[df_300.index[i],"all_kk"] = -1


        df_300 = df_300[df_300.index>df_300.index[-80]]






        IChour2 = []
        finalhour = list(df_300['IC'].index)[-1]
        plusi = 1
        while (finalhour + timedelta(hours = plusi)).hour in [6,7,13,14] or  (finalhour + timedelta(hours = plusi)-timedelta(hours = 5)).weekday in [5,6]:
            plusi = plusi + 1
        IChour2.append((finalhour + timedelta(hours = plusi)).strftime("%m-%d %H"))
        IChour2.append((finalhour + timedelta(hours = plusi+5)).strftime("%m-%d %H"))

        df_300.index = df_300.index.strftime(("%m-%d %H"))
        #df_300

        checkb = df_300["labelb"].values[0]
        bandstart = 1
        bandidx = 1
        checkidx = 0
        while bandidx < len(df_300["labelb"].values):
            #checkidx = bandidx
            bandstart = bandidx-1
            checkidx = bandstart+1
            if checkidx >=len(df_300["labelb"].values)-1:
                break
            while df_300["labelb"].values[checkidx] == df_300["labelb"].values[checkidx+1]:
                checkidx +=1
                if checkidx >=len(df_300["labelb"].values)-1:
                    break
            bandend = checkidx+1
            #print(bandstart,bandend)
            if df_300["labelb"].values[bandstart+1] == 1:
                fig3_1.add_traces(go.Scatter(x=df_300.index[bandstart:bandend], y = df_300['lower_band'].values[bandstart:bandend],
                                            line = dict(color='rgba(0,0,0,0)'),showlegend=False),rows=[2], cols=[1], secondary_ys= [True])
                    
                fig3_1.add_traces(go.Scatter(x=df_300.index[bandstart:bandend], y = df_300['upper_band'].values[bandstart:bandend],
                                            line = dict(color='rgba(0,0,0,0)'),
                                            fill='tonexty', 
                                            fillcolor = 'rgba(256,256,0,0.2)',showlegend=False
                                            ),rows=[2], cols=[1], secondary_ys= [True])
            else:


                fig3_1.add_traces(go.Scatter(x=df_300.index[bandstart:bandend], y = df_300['lower_band'].values[bandstart:bandend],
                                            line = dict(color='rgba(0,0,0,0)'),showlegend=False),rows=[2], cols=[1], secondary_ys= [True])
                    
                fig3_1.add_traces(go.Scatter(x=df_300.index[bandstart:bandend], y = df_300['upper_band'].values[bandstart:bandend],
                                            line = dict(color='rgba(0,0,0,0)'),
                                            fill='tonexty', 
                                            fillcolor = 'rgba(137, 207, 240,0.2)',showlegend=False
                                            ),rows=[2], cols=[1], secondary_ys= [True])
            bandidx =checkidx +1
            if bandidx >=len(df_300["labelb"].values):
                break
        ### 成交量圖製作 ###
        volume_colors1 = [red_color if df_300['close'][i] > df_300['close'][i-1] else green_color for i in range(len(df_300['close']))]
        volume_colors1[0] = green_color

        #fig.add_trace(go.Bar(x=df_300.index, y=df_300['成交金額'], name='成交量', marker=dict(color=volume_colors),showlegend=False), row=optvrank[0], col=1)
        fig3_1.add_trace(go.Bar(x=df_300.index, y=df_300['Volume'], name='成交量', marker=dict(color=volume_colors1)), row=2, col=1)

        #df_300.index = df_300.index - timedelta(hours = 6)

        fig3_1.add_trace(go.Scatter(x=df_300.index,
                                y=df_300['外資成本'],
                                mode='lines',
                                line=dict( width=0.5),
                                name='外資成本'),row=2, col=1, secondary_y= True)
        #fig3_1.add_trace(go.Scatter(x=df_300.index,
        #                        y=df_300['外資上極限'],
        #                        mode='lines',
        #                        #line=dict(color='green'),
        #                        name='外資上極限'),row=2, col=1)
        #fig3_1.add_trace(go.Scatter(x=df_300.index,
        #                        y=df_300['外資下極限'],
        #                        mode='lines',
        #                        #line=dict(color='green'),
        #                        name='外資下極限'),row=2, col=1)

        fig3_1.add_traces(go.Scatter(x=df_300.index, y = df_300['外資上極限'].values,
                                            line = dict(color='rgba(0,0,0,0)'),showlegend=False),rows=[2], cols=[1], secondary_ys= [True])
                    
        fig3_1.add_traces(go.Scatter(x=df_300.index, y = df_300['自營商上極限'].values,
                                    line = dict(color='rgba(0,0,0,0)'),
                                    fill='tonexty', 
                                    fillcolor = 'rgba(0,0,256,0.2)',showlegend=False
                                    ),rows=[2], cols=[1], secondary_ys= [True])
        fig3_1.add_traces(go.Scatter(x=df_300.index, y = df_300['外資下極限'].values,
                                            line = dict(color='rgba(0,0,0,0)'),showlegend=False),rows=[2], cols=[1], secondary_ys= [True])
                    
        fig3_1.add_traces(go.Scatter(x=df_300.index, y = df_300['自營商下極限'].values,
                                    line = dict(color='rgba(0,0,0,0)'),
                                    fill='tonexty', 
                                    fillcolor = 'rgba(256,0,0,0.2)',showlegend=False
                                    ),rows=[2], cols=[1], secondary_ys= [True])



        fig3_1.add_trace(go.Scatter(x=df_300.index,
                                y=df_300['20MA'],
                                mode='lines',
                                line=dict(color='green', width=0.5),
                                name='MA20'),row=2, col=1, secondary_y= True)
        #fig3_1.add_trace(go.Scatter(x=df_300.index,
        #                        y=df_300['200MA'],
        #                        mode='lines',
        #                        line=dict(color='blue'),
        #                        name='MA200'),row=1, col=1)
        #fig3_1.add_trace(go.Scatter(x=df_300.index,
        #                        y=df_300['60MA'],
        #                        mode='lines',
        #                        line=dict(color='orange'),
        #                        name='MA60'),row=1, col=1)

        #fig3_1.add_trace(go.Scatter(x=list(df_300['IC'].index)[2:]+IChour2,
        #                        y=df_300['IC'].values,
        #                        mode='lines',
        #                        line=dict(color='orange'),
        #                        name='IC操盤線'),row=2, col=1, secondary_y= True)



        ### K線圖製作 ###
        fig3_1.add_trace(
            go.Candlestick(
                x=df_300[(df_300['all_kk'] == -1)&(df_300['close'] >df_300['open'] )].index,
                open=df_300[(df_300['all_kk'] == -1)&(df_300['close'] >df_300['open'] )]['open'],
                high=df_300[(df_300['all_kk'] == -1)&(df_300['close'] >df_300['open'] )]['max'],
                low=df_300[(df_300['all_kk'] == -1)&(df_300['close'] >df_300['open'] )]['min'],
                close=df_300[(df_300['all_kk'] == -1)&(df_300['close'] >df_300['open'] )]['close'],
                increasing_line_color=decreasing_color,
                increasing_fillcolor=no_color, #fill_increasing_color(df_300.index>df_300.index[50])
                decreasing_line_color=decreasing_color,
                decreasing_fillcolor=no_color,#decreasing_color,
                line=dict(width=0.5),
                name='OHLC',showlegend=False
            )#,

            ,row=2, col=1, secondary_y= True
        )


        fig3_1.add_trace(
            go.Candlestick(
                x=df_300[(df_300['all_kk'] == 1)&(df_300['close'] >df_300['open'] )].index,
                open=df_300[(df_300['all_kk'] == 1)&(df_300['close'] >df_300['open'] )]['open'],
                high=df_300[(df_300['all_kk'] == 1)&(df_300['close'] >df_300['open'] )]['max'],
                low=df_300[(df_300['all_kk'] == 1)&(df_300['close'] >df_300['open'] )]['min'],
                close=df_300[(df_300['all_kk'] == 1)&(df_300['close'] >df_300['open'] )]['close'],
                increasing_line_color=increasing_color,
                increasing_fillcolor=no_color, #fill_increasing_color(df_300.index>df_300.index[50])
                decreasing_line_color=increasing_color,
                decreasing_fillcolor=no_color,#decreasing_color,
                line=dict(width=0.5),
                name='OHLC',showlegend=False
            )#,

            ,row=2, col=1, secondary_y= True
        )

        ### K線圖製作 ###
        fig3_1.add_trace(
            go.Candlestick(
                x=df_300[(df_300['all_kk'] == -1)&(df_300['close'] <df_300['open'] )].index,
                open=df_300[(df_300['all_kk'] == -1)&(df_300['close'] <df_300['open'] )]['open'],
                high=df_300[(df_300['all_kk'] == -1)&(df_300['close'] <df_300['open'] )]['max'],
                low=df_300[(df_300['all_kk'] == -1)&(df_300['close'] <df_300['open'] )]['min'],
                close=df_300[(df_300['all_kk'] == -1)&(df_300['close'] <df_300['open'] )]['close'],
                increasing_line_color=decreasing_color,
                increasing_fillcolor=decreasing_color, #fill_increasing_color(df_300.index>df_300.index[50])
                decreasing_line_color=decreasing_color,
                decreasing_fillcolor=decreasing_color,#decreasing_color,
                line=dict(width=0.5),
                name='OHLC',showlegend=False
            )#,

            ,row=2, col=1, secondary_y= True
        )


        fig3_1.add_trace(
            go.Candlestick(
                x=df_300[(df_300['all_kk'] == 1)&(df_300['close'] <df_300['open'] )].index,
                open=df_300[(df_300['all_kk'] == 1)&(df_300['close'] <df_300['open'] )]['open'],
                high=df_300[(df_300['all_kk'] == 1)&(df_300['close'] <df_300['open'] )]['max'],
                low=df_300[(df_300['all_kk'] == 1)&(df_300['close'] <df_300['open'] )]['min'],
                close=df_300[(df_300['all_kk'] == 1)&(df_300['close'] <df_300['open'] )]['close'],
                increasing_line_color=increasing_color,
                increasing_fillcolor=increasing_color, #fill_increasing_color(Final60Tdata.index>FinalＷeekdata.index[50])
                decreasing_line_color=increasing_color,
                decreasing_fillcolor=increasing_color,#decreasing_color,
                line=dict(width=0.5),
                name='OHLC',showlegend=False
            )#,

            ,row=2, col=1, secondary_y= True
        )

        #T60noshow = []
        #curdatetime = Final60Tdata.index[0]
        #while curdatetime < datetime.today():
        #    if curdatetime not in Final60Tdata.index:
        #        T60noshow.append(datetime.strftime(curdatetime,'%Y-%m-%d %H:%M:%S'))
        #    curdatetime = curdatetime + timedelta(hours = 1)

        #T60noshow




        fig3_1.update_xaxes(
            rangeslider= {'visible':False},
            rangebreaks=[
                dict(bounds=[6, 8], pattern="hour"),
                #dict(bounds=[6,24],pattern = "sat"),
                dict(bounds=[13, 15], pattern="hour"),
                #dict(bounds=[6,24], pattern="hour"),#,bounds=['sat','sun']),# hide weekends, eg. hide sat to before mon
                dict(bounds=['sun','mon']),
                #dict(values=[str(holiday) for holiday in holidf[~(holidf["說明"].str.contains('開始交易') | holidf["說明"].str.contains('最後交易'))]["日期"].values]+['2023-08-03'])
                #dict(dvalues=T60noshow[10:], pattern="hour")
            ],
                        row = 1, 
                        col = 1
        )


        fig3_1.update_xaxes(
            rangeslider= {'visible':False},
            rangebreaks=[
                #dict(bounds=[6, 8], pattern="hour"),

                dict(bounds=['sat', 'mon']),# hide weekends, eg. hide sat to before mon
                #dict(values=T300noshow)
            ],
                        row = 2, 
                        col = 1
        )

        fig3_1.update_yaxes(
            range=[0, Final60Tdata['Volume'].max()+100],showgrid=False,
            secondary_y=False,
                        row = 1, 
                        col = 1
        )
        fig3_1.update_yaxes(
            range=[Final60Tdata['外資下極限'].min() - 30, Final60Tdata['自營商上極限'].max() + 30],showgrid=False,
            secondary_y=True,
                        row = 1, 
                        col = 1
        )
        fig3_1.update_yaxes(
            range=[df_300['min'].min() - 200, df_300['max'].max() + 200],showgrid=False,
                secondary_y=True,
                        row = 2, 
                        col = 1
        )
        fig3_1.update_yaxes(
            range=[0, df_300['Volume'].max()+100],showgrid=False,
            secondary_y=False,
                        row = 2, 
                        col = 1,showticklabels=False
        )




        # 設定圖的標題跟長寬
        fig3_1.update_annotations(font_size=8)
        fig3_1.update_layout( hovermode='x unified',
                        yaxis = dict(showgrid=False,showticklabels=False),#,tickformat = ",.0f",range=[Final60Tdata['min'].min() - 50, Final60Tdata['max'].max() + 50]),
                        yaxis2 = dict(showgrid=False),#showticklabels=False,range=[0, Final60Tdata['Volume'].max()+100]),
                        #yaxis = dict(showgrid=False,showticklabels=False),

                        width = 800, 
                        height = 1800,
                        hoverlabel_namelength=-1,
                        plot_bgcolor="rgb(256,256,256)",    # 繪圖區背景白色
                        paper_bgcolor="rgb(256,256,256)",   # 外框背景白
                        font=dict(color="black", size=10),          # 改字體顏色成黑色
                        showlegend = False,
                    
                        #legend_traceorder="reversed",
                        )

        # =========================
        # 4) 原子輸出 PNG
        # =========================
        out = images_dir / filename
        tmp = out.with_suffix(".png.tmp")
        fig3_1.write_image(str(tmp), format="png", scale=2)   # 需要 plotly + kaleido (+ Chrome)
        os.replace(tmp, out)
        return out

    finally:
        try: con_eq.close()
        except Exception: pass
        try: con_fu.close()
        except Exception: pass

# --- 工具：取得最後一個工作日（只避開週末） ---
def _prev_weekday(d: date) -> date:
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d

# ---（可選）FinMind 指數漲跌點數，失敗就不加入 ---
def _fetch_taiex_delta_map(start: str, end: str) -> Dict[str, Any]:
    try:
        token = os.getenv("FINMIND_TOKEN") or ""
        url = "https://api.finmindtrade.com/api/v4/data"
        params = {
            "dataset": "TaiwanStockPrice",
            "data_id": "TAIEX",
            "start_date": start,
            "end_date": end,
            "token": token,
        }
        r = requests.get(url, params=params, timeout=8)
        js = r.json()
        df = pd.DataFrame(js.get("data", []))
        if df.empty or "date" not in df or "spread" not in df:
            # 有些版本欄位名是 change / spread / close-previous_close
            if "close" in df and "open" in df:
                df["spread"] = pd.to_numeric(df["close"], errors="coerce") - pd.to_numeric(df["open"], errors="coerce")
            else:
                return {}
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        return dict(zip(df["date"], df["spread"]))
    except Exception:
        return {}

@app.route("/cron/gentables_options", methods=["GET", "POST"])
def cron_gentables_options():
    """
    產生「選擇權三大法人籌碼」相關表格為 PNG：
      1) 選擇日期的數據（options_futures_bs）
      2) 日變動（外資/自營商/散戶 各一張），依 putcallsum_sep 計算『成交位置』
      3) 參考數據（價平和/外資成本/指數漲跌點數）
      4) 上下極限（df_option_limit）
    Query 參數：
      - date: YYYY-MM-DD（預設=今天；若是假日→往前找工作日）
      - tag: 檔名標籤，預設=同上日期
    回傳：各圖檔名與可公開網址
    """
    _require_key()

    # 1) 解析日期
    now_local = datetime.now().date()
    q_date = request.args.get("date")
    if q_date:
        try:
            base_day = datetime.strptime(q_date, "%Y-%m-%d").date()
        except Exception:
            abort(400, "bad date")
    else:
        base_day = now_local
    sel_day = _prev_weekday(base_day)
    tag = request.args.get("tag") or sel_day.strftime("%Y%m%d")
    sel_str = sel_day.strftime("%Y-%m-%d")

    # 2) 讀取 SQLite
    con_an = get_sqlite_connection("analysis")
    try:
        df_bs = pd.read_sql('select distinct * from options_futures_bs', con_an)
        df_daygap = pd.read_sql('select distinct * from df_options_futures_daygap', con_an)
        df_putcall = pd.read_sql('select distinct * from putcallsum_sep', con_an)
        df_cost = pd.read_sql('select distinct * from df_cost', con_an)
        df_limit = pd.read_sql('select distinct * from df_option_limit', con_an)
    finally:
        con_an.close()

    # 3) 選擇日期的數據
    df_sel = df_bs[df_bs["日期"] == sel_str].copy()
    if df_sel.empty:
        # 若該日仍無資料，就一路往前找最近一個有資料的工作日
        back = 1
        while df_sel.empty and back <= 5:
            d2 = _prev_weekday(sel_day - timedelta(days=back))
            sel_str = d2.strftime("%Y-%m-%d")
            df_sel = df_bs[df_bs["日期"] == sel_str].copy()
            back += 1

    # 4) 日變動（以 sel_str 當天），並計算「成交位置」
    gap_day_df = df_daygap[df_daygap["日期"] == sel_str].copy()
    ref_row = df_putcall[df_putcall["日期"] == sel_str].copy()

    # 容錯：若當天 put/call 參考不存在，用最近一筆
    if ref_row.empty and not df_putcall.empty:
        ref_row = df_putcall.sort_values("日期").tail(1).copy()

    call_num = int(ref_row["價平和買權成交價"].iloc[0]) if not ref_row.empty else 0
    put_num = int(ref_row["價平和賣權成交價"].iloc[0]) if not ref_row.empty else 0

    if not gap_day_df.empty:
        # 保留原始單價的絕對值邏輯
        gap_day_df = gap_day_df.copy()
        gap_day_df["成交位置"] = ""
        # 單價有可能是字串，先轉數值
        gap_day_df["單價"] = pd.to_numeric(gap_day_df["單價"], errors="coerce").fillna(0)
        for idx in gap_day_df.index:
            k = gap_day_df.at[idx, "種類"]
            px = abs(gap_day_df.at[idx, "單價"])
            if k == "買權":
                gap_day_df.at[idx, "成交位置"] = "價內" if px > call_num else "價外"
            elif k == "賣權":
                gap_day_df.at[idx, "成交位置"] = "價內" if px > put_num else "價外"
            else:
                gap_day_df.at[idx, "成交位置"] = "不分"

    foreign_df = gap_day_df[gap_day_df["身份"] == "外資"].copy()
    dealer_df  = gap_day_df[gap_day_df["身份"] == "自營商"].copy()
    retail_df  = gap_day_df[gap_day_df["身份"] == "散戶"].copy()

    # 5) 參考數據（僅當天）
    ref_df = df_putcall[df_putcall["日期"] == sel_str].copy()
    ref_df = ref_df.rename(columns={
        "日期":"日期",
        "價平和履約價":"價平和履約價",
        "價平和買權成交價":"價平和買權成交價",
        "價平和賣權成交價":"價平和賣權成交價",
    })

    # 外資成本
    if "外資成本" in df_cost.columns:
        df_cost["外資成本"] = pd.to_numeric(df_cost["外資成本"], errors="coerce").astype("Int64")
    ref_df = ref_df.merge(df_cost[["日期","外資成本"]], how="left", on="日期")

    # 指數漲跌點數（用 FinMind，可設環境變數 FINMIND_TOKEN；取不到就略過）
    taiex_map = _fetch_taiex_delta_map(sel_str, sel_str)
    ref_df["漲跌點數"] = ref_df["日期"].map(taiex_map) if taiex_map else pd.NA

    # 6) 上下極限（只取當天）
    limit_df = df_limit[df_limit["日期"] == sel_str].copy()
    # 若有『身份別』欄位，常用外資
    if "身份別" in limit_df.columns:
        limit_df = limit_df[limit_df["身份別"].isin(["外資","自營商","散戶"])].copy()

    # 7) 轉成 PNG（共 1 + 3 + 1 + 1 = 最多 6 張）
    imgs = []

    tbl1 = _df_to_table_png(
        df_sel.reset_index(drop=True),
        f"opt_bs_{tag}.png",
        title=f"選擇日期的數據（{sel_str}）"
    ); imgs.append(tbl1)

    def _safe_png(df, fname, title):
        if df is None or df.empty:
            # 給一張『無資料』空表，避免整體中斷
            tmp = pd.DataFrame({"訊息": [f"{sel_str} 無資料"]})
            return _df_to_table_png(tmp, fname, title=title)
        return _df_to_table_png(df.reset_index(drop=True), fname, title=title)

    imgs.append(_safe_png(foreign_df, f"opt_day_foreign_{tag}.png", f"日變動：外資（{sel_str}）"))
    imgs.append(_safe_png(dealer_df,  f"opt_day_dealer_{tag}.png",  f"日變動：自營商（{sel_str}）"))
    imgs.append(_safe_png(retail_df,  f"opt_day_retail_{tag}.png",  f"日變動：散戶（{sel_str}）"))

    imgs.append(_safe_png(ref_df,     f"opt_ref_{tag}.png",         f"參考數據（{sel_str}）"))

    imgs.append(_safe_png(limit_df,   f"opt_limit_{tag}.png",       f"上下極限（{sel_str}）"))

    base = _public_base()
    urls = [f"{base}/images/{p.name}" for p in imgs]

    return jsonify(ok=True, date=sel_str, files=[p.name for p in imgs], urls=urls)




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

#推播表格
@app.route("/cron/push_options_tables", methods=["GET", "POST"])
def cron_push_options_tables():
    """
    推播「選擇權三大法人籌碼」的所有表格 PNG。
    Query:
      - date: YYYY-MM-DD（預設=今天；遇週末將往前一個工作日）
      - files: 逗號分隔檔名；若未提供，會先呼叫 /cron/gentables_options 生成。
    """
    _require_key()
    files_param = request.args.get("files")
    q_date = request.args.get("date")

    if files_param:
        filenames = [x.strip() for x in files_param.split(",") if x.strip()]
        sel_date = "N/A"
        urls = [f"{_public_base()}/images/{fn}" for fn in filenames if (IMAGES_DIR / fn).exists()]
    else:
        q = f"/cron/gentables_options?key={CRON_KEY}"
        if q_date:
            q += f"&date={q_date}"
        with app.test_request_context(q):
            resp = cron_gentables_options().json
        filenames = resp.get("files", [])
        sel_date = resp.get("date")
        urls = resp.get("urls", [])

    count = _push_images_to_whitelist(urls)
    return f"OK, pushed {len(urls)} images for {sel_date} (actually sent {count})."


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