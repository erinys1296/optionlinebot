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
from matplotlib import font_manager
import matplotlib

# --- 專案隨包字型（Variable TrueType） ---
# 檔案放在 fonts/NotoSansTC-VariableFont_wght.ttf
_FONT_PATH = Path(__file__).parent / "NotoSansTC-VariableFont_wght.ttf"
def _load_cjk_prop():
    if not _FONT_PATH.exists():
        print(f"[warn] CJK font not found: {_FONT_PATH}")
        return None
    # 1) 註冊到 fontManager
    font_manager.fontManager.addfont(str(_FONT_PATH))
    # 2) 強制重建快取，避免舊 cache 讓 matplotlib 還是用 DejaVu
    try:
        font_manager._load_fontmanager(try_read_cache=False)  # matplotlib 3.7+
    except Exception:
        pass
    # 3) 建 FontProperties
    prop = font_manager.FontProperties(fname=str(_FONT_PATH))
    # 4) 設成全域：家族= sans-serif，候選只有我們這顆
    matplotlib.rcParams["font.family"] = "sans-serif"
    matplotlib.rcParams["font.sans-serif"] = [prop.get_name()]
    matplotlib.rcParams["axes.unicode_minus"] = False
    print(f"[font] using: {prop.get_name()} @ {_FONT_PATH}")
    return prop

_CJK_PROP = _load_cjk_prop()
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

def _reply_images(reply_token: str, img_urls: list[str]) -> None:
    """一次回覆最多 5 張，多的請用 push。"""
    messages = [ImageSendMessage(original_content_url=u, preview_image_url=u) for u in img_urls[:5]]
    line_bot_api.reply_message(reply_token, messages)

def _push_images_to_user(user_id: str, img_urls: list[str]) -> int:
    """對單一使用者 push 多張（自動分批，每批最多 5）。"""
    total = 0
    for i in range(0, len(img_urls), 5):
        batch = [ImageSendMessage(original_content_url=u, preview_image_url=u) for u in img_urls[i:i+5]]
        line_bot_api.push_message(user_id, batch)
        total += len(batch)
    return total

def _url_for_file(filename: str) -> str:
    return f"{_public_base()}/images/{filename}"

def _ensure_latest_png() -> str:
    """產『即時盤』圖（沿用你現有 generate_plot_png），固定檔名 latest.png"""
    p = generate_plot_png("latest.png")  # 如果你原本函式叫別名，改這行
    return _url_for_file(p.name)

def _ensure_main_png() -> str:
    """產『主圖』圖（你新增的 generate_plot_png_main），固定檔名 main.png"""
    p = generate_plot_png_main("main.png")
    return _url_for_file(p.name)

def _ensure_tables_pngs() -> list[str]:
    """產表格並回傳圖片 URL 陣列（覆蓋固定檔名）"""
    paths = cron_gentables_options()     # 之前我們做的函式，回傳 List[Path]
    return [_url_for_file(p.name) for p in paths]

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
    回傳 sqlite3.connect(...) 連線；呼叫方負責  scon.close()
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
        if _CJK_PROP:
            ax.set_title(title, fontsize=14, pad=10, fontproperties=_CJK_PROP)
        else:
            ax.set_title(title, fontsize=14, pad=10)

    tbl = ax.table(cellText=df.values,
                   colLabels=df.columns,
                   cellLoc='center',
                   loc='center')
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    tbl.scale(1, 1.2)

    # 逐格把字型套上（正確 API：get_text().set_fontproperties）
    if _CJK_PROP:
        for _, cell in tbl.get_celld().items():
            cell.get_text().set_fontproperties(_CJK_PROP)

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

def generate_plot_png_main(filename: str = "latest_main.png") -> Path:
    # ---- 連線 ----
    con_eq = get_sqlite_connection("equity")
    con_fu = get_sqlite_connection("futures")
    con_an = get_sqlite_connection("analysis")
    # ---- 目錄（若未定義 IMAGES_DIR，用預設）----
    images_dir = globals().get("IMAGES_DIR", Path(os.environ.get("IMAGES_DIR", "/var/data/images")))
    images_dir.mkdir(parents=True, exist_ok=True)
    try:
        #週線
        url = "https://api.finmindtrade.com/api/v4/data?"
        token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRlIjoiMjAyMy0wNy0zMCAyMzowMTo0MSIsInVzZXJfaWQiOiJqZXlhbmdqYXUiLCJpcCI6IjExNC4zNC4xMjEuMTA0In0.WDAZzKGv4Du5JilaAR7o7M1whpnGaR-vMDuSeTBXhhA", # 參考登入，獲取金鑰

        parameter = {
        "dataset": "TaiwanStockPrice",
        "data_id": "TAIEX",
        "start_date": "2025-03-02",
        "end_date": datetime.strftime(datetime.today(),'%Y-%m-%d'),
        "token": token, # 參考登入，獲取金鑰
        }
        data = requests.get(url, params=parameter)
        data = data.json()
        WeekTAIEXdata = pd.DataFrame(data['data'])

        taiex_fin = pd.DataFrame(data['data'])
        taiex_fin.date = pd.to_datetime(taiex_fin.date)
        taiex_fin.index = taiex_fin.date
        taiex_fin.columns = ['日期', 'stock_id', '成交股數', '成交金額', '開盤指數', '最高指數',
            '最低指數', '收盤指數', '漲跌點數', '成交筆數']

        #taiex = pd.read_sql("select distinct * from taiex", con_eq, parse_dates=['日期'], index_col=['日期'])
        #taiex_vol = pd.read_sql("select distinct * from taiex_vol", con_eq, parse_dates=['日期'], index_col=['日期'])

        #taiex
        #taiex_vol
        cost_df = pd.read_sql("select distinct Date as [日期], Cost as [外資成本] from cost", con_eq, parse_dates=['日期'], index_col=['日期']).dropna()
        cost_df["外資成本"] = cost_df["外資成本"].astype('int')
        limit_df = pd.read_sql("select distinct * from [limit]", con_eq, parse_dates=['日期'], index_col=['日期'])

        inves_limit = limit_df[limit_df["身份別"] == "外資"][['上極限', '下極限']]
        dealer_limit = limit_df[limit_df["身份別"] == "自營商"][['上極限', '下極限']]

        inves_limit.columns = ["外資上極限","外資下極限"]
        dealer_limit.columns = ["自營商上極限","自營商下極限"]

        kbars = taiex_fin.join(cost_df).join(dealer_limit).join(inves_limit)
        enddate = pd.read_sql("select * from end_date order by 最後結算日 desc", con_eq, parse_dates=['最後結算日'])


        holidf = pd.read_sql("select * from holiday", con_eq)

        holilist = [str(holiday) for holiday in holidf[~(holidf["說明"].str.contains('開始交易') | holidf["說明"].str.contains('最後交易'))]["日期"].values]
        #holilist["日期"] = pd.to_datetime(holilist["日期"])

        ordervolumn = pd.read_sql("select distinct * from ordervolumn", con_eq, parse_dates=['日期'], index_col=['日期'])
        putcallsum = pd.read_sql("select 日期, max(價平和) as 價平和 from putcallsum group by 日期", con_eq, parse_dates=['日期'], index_col=['日期'])
        putcallsum_month = pd.read_sql("select 日期, max(月選擇權價平和) as 月價平和 from putcallsum_month group by 日期", con_eq, parse_dates=['日期'], index_col=['日期'])
        putcallgap = pd.read_sql("select 日期, max(價外買賣權價差) as 價外買賣權價差 from putcallgap group by 日期", con_eq, parse_dates=['日期'], index_col=['日期'])

        putcallgap_month = pd.read_sql("select 日期, max(價外買賣權價差) as 月價外買賣權價差 from putcallgap_month group by 日期", con_eq, parse_dates=['日期'], index_col=['日期'])


        print(putcallsum_month.tail())
        kbars = kbars.join(ordervolumn).join(putcallsum).join(putcallsum_month)
        kbars = kbars.join(putcallgap).join(putcallgap_month)

        # 計算布林帶指標
        kbars['20MA'] = kbars['收盤指數'].rolling(20).mean()
        kbars['std'] = kbars['收盤指數'].rolling(20).std()
        kbars['60MA'] = kbars['收盤指數'].rolling(60).mean()
        kbars['200MA'] = kbars['收盤指數'].rolling(200).mean()

        kbars = kbars[kbars.index > kbars.index[-100]]

        kbars['upper_band'] = kbars['20MA'] + 2 * kbars['std']
        kbars['lower_band'] = kbars['20MA'] - 2 * kbars['std']
        kbars['upper_band1'] = kbars['20MA'] + 1 * kbars['std']
        kbars['lower_band1'] = kbars['20MA'] - 1 * kbars['std']


        kbars['IC'] = kbars['收盤指數'] + 2 * kbars['收盤指數'].shift(1) - kbars['收盤指數'].shift(3) -kbars['收盤指數'].shift(4)

        kbars['月價平和日差'] = kbars['月價平和'] - kbars['月價平和'].shift(1)

        # 在k线基础上计算KDF，并将结果存储在df上面(k,d,j)
        low_list = kbars['最低指數'].rolling(9, min_periods=9).min()
        low_list.fillna(value=kbars['最低指數'].expanding().min(), inplace=True)
        high_list = kbars['最高指數'].rolling(9, min_periods=9).max()
        high_list.fillna(value=kbars['最高指數'].expanding().max(), inplace=True)
        rsv = (kbars['收盤指數'] - low_list) / (high_list - low_list) * 100
        kbars['K'] = pd.DataFrame(rsv).ewm(com=2).mean()
        kbars['D'] = kbars['K'].ewm(com=2).mean()

        enddatemonth = enddate[~enddate["契約月份"].str.contains("W")]['最後結算日']
        kbars['end_low'] = 0
        kbars['end_high'] = 0
        #print(enddatemonth)
        #kbars
        baseline_high = 0
        baseline_low = 0
        previous_high = 0
        previous_low = 0
        for datei in kbars.index:
            

            if datei ==enddatemonth[enddatemonth<=datei].max():
                kbars.loc[datei,'end_low'] =  0
                kbars.loc[datei,'end_high'] = 0
                baseline_high = kbars[(kbars.index==datei)]["收盤指數"].values[0]
                baseline_low = kbars[(kbars.index==datei)]["收盤指數"].values[0]
                previous_high = 0
                previous_low = 0
                current_high = 0
                current_low = 0
            else:
                current_low = kbars[(kbars.index==datei)]["最低指數"].min() - baseline_high
                current_high = kbars[(kbars.index==datei)]['最高指數'].max() - baseline_low
                baseline_high = max(baseline_high,kbars[(kbars.index==datei)]["最高指數"].values[0])
                baseline_low = min(baseline_low,kbars[(kbars.index==datei)]["最低指數"].values[0])

            print(datei,kbars[(kbars.index==datei)]["最低指數"].min(),kbars[(kbars.index==datei)]['最高指數'].max(),current_low,current_high,previous_high,previous_low)

            kbars.loc[datei,'end_high'] =  max(current_high,previous_high)
            kbars.loc[datei,'end_low'] = min(current_low,previous_low)

            
            previous_high = max(current_high,previous_high)
            previous_low = min(current_low,previous_low)

            
        kbars["MAX_MA"] = kbars["最高指數"] - kbars["20MA"]
        kbars["MIN_MA"] = kbars["最低指數"] - kbars["20MA"]

        #詢問
        ds = 2
        kbars['uline'] = kbars['最高指數'].rolling(ds, min_periods=1).max()
        kbars['dline'] = kbars['最低指數'].rolling(ds, min_periods=1).min()

        kbars["all_kk"] = 0
        barssince5 = 0
        barssince6 = 0
        kbars['labelb'] = 1
        kbars = kbars[~kbars.index.duplicated(keep='first')]
        for i in range(2,len(kbars.index)):
            try:
                #(kbars.loc[kbars.index[i],'收盤指數'] > kbars.loc[kbars.index[i-1],"uline"])
                condition51 = (kbars.loc[kbars.index[i-1],"最高指數"] < kbars.loc[kbars.index[i-2],"最低指數"] ) and (kbars.loc[kbars.index[i],"最低指數"] > kbars.loc[kbars.index[i-1],"最高指數"] )
                #condition52 = (kbars.loc[kbars.index[i-1],'收盤指數'] < kbars.loc[kbars.index[i-2],"最低指數"]) and (kbars.loc[kbars.index[i-1],'成交金額'] > kbars.loc[kbars.index[i-2],'成交金額']) and (kbars.loc[kbars.index[i],'收盤指數']>kbars.loc[kbars.index[i-1],"最高指數"] )
                condition53 = (kbars.loc[kbars.index[i],'收盤指數'] > kbars.loc[kbars.index[i-1],"uline"]) and (kbars.loc[kbars.index[i-1],'收盤指數'] <= kbars.loc[kbars.index[i-1],"uline"])

                condition61 = (kbars.loc[kbars.index[i-1],"最低指數"] > kbars.loc[kbars.index[i-2],"最高指數"] ) and (kbars.loc[kbars.index[i],"最高指數"] < kbars.loc[kbars.index[i-1],"最低指數"] )
                #condition62 = (kbars.loc[kbars.index[i-1],'收盤指數'] > kbars.loc[kbars.index[i-2],"最高指數"]) and (kbars.loc[kbars.index[i-1],'成交金額'] > kbars.loc[kbars.index[i-2],'成交金額']) and (kbars.loc[kbars.index[i],'收盤指數']<kbars.loc[kbars.index[i-1],"最低指數"] )
                condition63 = (kbars.loc[kbars.index[i],'收盤指數'] < kbars.loc[kbars.index[i-1],"dline"]) and (kbars.loc[kbars.index[i-1],'收盤指數'] >= kbars.loc[kbars.index[i-1],"dline"])
            except:
                condition51 = True
                condition52 = True
                condition53 = True
                condition61 = True
                condition63 = True
            condition54 = condition51 or condition53 #or condition52
            condition64 = condition61 or condition63 #or condition62 

            #kbars['labelb'] = np.where((kbars['收盤指數']> kbars['upper_band1']) , 1, np.where((kbars['收盤指數']< kbars['lower_band1']),-1,1))

            #print(i)
            if kbars.loc[kbars.index[i],'收盤指數'] > kbars.loc[kbars.index[i],'upper_band1']:
                kbars.loc[kbars.index[i],'labelb'] = 1
            elif kbars.loc[kbars.index[i],'收盤指數'] < kbars.loc[kbars.index[i],'lower_band1']:
                kbars.loc[kbars.index[i],'labelb'] = -1
            else:
                kbars.loc[kbars.index[i],'labelb'] = kbars.loc[kbars.index[i-1],'labelb']

            if condition54 == True:
                barssince5 = 1
            else:
                barssince5 += 1

            if condition64 == True:
                barssince6 = 1
            else:
                barssince6 += 1


            if barssince5 < barssince6:
                kbars.loc[kbars.index[i],"all_kk"] = 1
            else:
                kbars.loc[kbars.index[i],"all_kk"] = -1


        max_days20_list =  []
        max_days20_x = []
        min_days20_list =  []
        min_days20_x = []

        for dateidx in range(0,len(kbars.index[-59:])):

            try:
                datei = kbars.index[-59:][dateidx]
                days19 = kbars[(kbars.index> kbars.index[-79:][dateidx]) & (kbars.index<datei )]
                max_days19 = days19["九點累積委託賣出數量"].values.max()
                min_days19 = days19["九點累積委託賣出數量"].values.min()
                curday = kbars[kbars.index == datei]["九點累積委託賣出數量"].values[0]
                yesday = kbars[kbars.index == kbars.index[-59:][dateidx-1]]["九點累積委託賣出數量"].values[0]
                tomday = kbars[kbars.index == kbars.index[-59:][dateidx+1]]["九點累積委託賣出數量"].values[0]

                if curday >= max_days19 and curday > yesday and curday > tomday:
                    max_days20_list.append(curday)
                    max_days20_x.append(datei)

                if curday <= min_days19 and curday < yesday and curday < tomday:
                    min_days20_list.append(curday)
                    min_days20_x.append(datei)
            except:
                pass

        

        #max_days20_list
        #max_days20

        #max_days20_x
        notshowdate = []
        for datei in enddate[~enddate["契約月份"].str.contains("W")]['最後結算日']:
            try:
                kbarsdi = np.where(kbars.index == datei)[0]
                notshowdate.append(kbars.index[kbarsdi+1][0])
            except:
                continue
        #kbars = kbars.dropna()
        kbars = kbars[kbars.index > kbars.index[-60]]


        def fillcol(label):
            if label >= 1:
                return 'rgba(0,250,0,0.2)'
            else:
                return 'rgba(0,256,256,0.2)'

        ICdate = []
        datechecki = 1
        #(kbars['IC'].index[-1] + timedelta(days = 1)).weekday() == 5
        while (kbars['IC'].index[-1] + timedelta(days = datechecki)).weekday() in [5,6] or (kbars['IC'].index[-1] + timedelta(days = datechecki)) in pd.to_datetime(holidf["日期"]).values:
            datechecki +=1
        ICdate.append((kbars['IC'].index[-1] + timedelta(days = datechecki)))
        datechecki +=1
        while (kbars['IC'].index[-1] + timedelta(days = datechecki)).weekday() in [5,6] or (kbars['IC'].index[-1] + timedelta(days = datechecki)) in pd.to_datetime(holidf["日期"]).values:
            datechecki +=1
        ICdate.append((kbars['IC'].index[-1] + timedelta(days = datechecki)))



        #[kbars['IC'].index[-1] + timedelta(days = 1),kbars['IC'].index[-1] + timedelta(days = 2)]

        CPratio = pd.read_sql("select distinct * from putcallratio", con_eq, parse_dates=['日期'], index_col=['日期'])
        CPratio = CPratio[CPratio.index>kbars.index[0]]

        bank8 = pd.read_sql("select distinct * from bank", con_eq, parse_dates=['日期'], index_col=['日期'])
        bank8 = bank8[bank8.index>kbars.index[0]]

        dfMTX = pd.read_sql("select distinct * from dfMTX", con_eq, parse_dates=['Date'], index_col=['Date'])
        dfMTX = dfMTX[dfMTX.index>kbars.index[0]]

        futdf = pd.read_sql("select distinct * from futdf", con_eq, parse_dates=['日期'], index_col=['日期'])
        futdf = futdf[futdf.index>kbars.index[0]]

        TXOOIdf = pd.read_sql("select distinct * from TXOOIdf", con_eq, parse_dates=['日期'], index_col=['日期'])
        TXOOIdf = TXOOIdf[TXOOIdf.index>kbars.index[0]]

        dfbuysell = pd.read_sql("select distinct * from dfbuysell order by Date", con_eq, parse_dates=['Date'], index_col=['Date'])
        dfbuysell = dfbuysell[dfbuysell.index>kbars.index[0]]

        dfMargin = pd.read_sql("select distinct * from dfMargin order by Date", con_eq, parse_dates=['Date'], index_col=['Date'])
        dfMargin = dfMargin[dfMargin.index>kbars.index[0]]

        options_vice = [ True ]*4

        optvn = 0
        optvrank = []
        for opv in options_vice:
            if opv == True:
                optvn += 1
                optvrank.append(optvn+1)
            else:
                optvrank.append(0)
        subtitle_all = ['OHLC',   '開盤賣張','價平和','月價平和日差','月結趨勢']
        subtitle =['OHLC']
        for i in range(1,5):
            if optvrank[i-1] != 0:
                subtitle.append(subtitle_all[i])    



        #subtitle
        enddate = pd.read_sql("select * from end_date", con_eq, parse_dates=['最後結算日'])


        rowcount = optvn + 1 + 8 + 2
        rowh = [0.2] + [ 0.6/(rowcount - 3)] * (rowcount - 3)+[0.1,0.1]
        fig = make_subplots(
            rows=rowcount, cols=1,
            shared_xaxes=True, 
            vertical_spacing=0.02,
            row_heights= rowh[:rowcount],
            shared_yaxes=False,
            #subplot_titles=subtitle,
            #y_title = "test"# subtitle,
            specs = [[{"secondary_y":True}]]*rowcount
        )

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


        ### 成本價及上下極限 ###
        #fig.add_trace(go.Scatter(x=list(kbars['IC'].index)[1:]+[ICdate[0]],
        #                y=kbars['外資成本'].shift(1).values,
        #                mode='lines',
        #                line=dict(color='yellow'),
        #                name='外資成本'),row=1, col=1, secondary_y= True)


        #自營商外資上極限
        #fig.add_scatter(x=np.concatenate([kbars.index,kbars.index[::-1]]), y=np.concatenate([kbars['外資上極限'], kbars['自營商上極限'][::-1]]), 
        #                fill='toself',fillcolor= 'rgba(0,0,256,0.1)', line_width=0,name='上極限',row=1, col=1 )



        #自營商外資下極限
        #fig.add_scatter(x=np.concatenate([kbars.index,kbars.index[::-1]]), y=np.concatenate([kbars['外資下極限'], kbars['自營商下極限'][::-1]]), 
        #                fill='toself',fillcolor= 'rgba(256,0,0,0.1)', line_width=0,name='下極限',row=1, col=1)

        #上下極限
        #kbars[kbars['收盤指數']> kbars['upper_band1']]

        #np.concatenate([kbars[kbars['收盤指數'] > kbars['upper_band1']].index,kbars[kbars['收盤指數']> kbars['upper_band1']].index[::-1]])
        #buling_colors = ['rgba(0,256,0,0.1)' if kbars['收盤指數'][i] > kbars['upper_band1'][i] else 'rgba(0,256,256,0.1)' for i in range(len(kbars['lower_band']))]
        #fillcol(kbars['labelb'].iloc[0])
        #fig.add_scatter(x=np.concatenate([kbars.index,kbars.index[::-1]]), y=np.concatenate([kbars['lower_band'], kbars['upper_band'][::-1]]), 
        #               fill='toself',fillcolor= kbars['labelb'].iloc[0], line_width=0,name='布林上下極限',row=1, col=1)


        checkb = kbars["labelb"].values[0]
        bandstart = 1
        bandidx = 1
        checkidx = 0
        while bandidx < len(kbars["labelb"].values):
            #checkidx = bandidx
            bandstart = bandidx-1
            checkidx = bandstart+1
            if checkidx >=len(kbars["labelb"].values)-1:
                break
            while kbars["labelb"].values[checkidx] == kbars["labelb"].values[checkidx+1]:
                checkidx +=1
                if checkidx >=len(kbars["labelb"].values)-1:
                    break
            bandend = checkidx+1
            #print(bandstart,bandend)
            if kbars["labelb"].values[bandstart+1] == 1:
                fig.add_traces(go.Scatter(x=kbars.index[bandstart:bandend], y = kbars['lower_band'].values[bandstart:bandend],
                                            line = dict(color='rgba(0,0,0,0)'),showlegend=False,hoverinfo='none'),secondary_ys= [True,True])
                    
                fig.add_traces(go.Scatter(x=kbars.index[bandstart:bandend], y = kbars['upper_band'].values[bandstart:bandend],
                                            line = dict(color='rgba(0,0,0,0)'),
                                            fill='tonexty', 
                                            fillcolor = 'rgba(256,256,0,0.2)',showlegend=False,hoverinfo='none'
                                            ),secondary_ys= [True,True])
            else:


                fig.add_traces(go.Scatter(x=kbars.index[bandstart:bandend], y = kbars['lower_band'].values[bandstart:bandend],
                                            line = dict(color='rgba(0,0,0,0)'),showlegend=False,hoverinfo='none'), secondary_ys= [True,True])
                    
                fig.add_traces(go.Scatter(x=kbars.index[bandstart:bandend], y = kbars['upper_band'].values[bandstart:bandend],
                                            line = dict(color='rgba(0,0,0,0)'),
                                            fill='tonexty', 
                                            fillcolor = 'rgba(137, 207, 240,0.2)',showlegend=False,hoverinfo='none'
                                            ),secondary_ys= [True,True])
            bandidx =checkidx +1
            if bandidx >=len(kbars["labelb"].values):
                break

        #fig.add_scatter(x=np.concatenate([kbars[kbars['收盤指數'] <= kbars['upper_band1']].index,kbars[kbars['收盤指數']<= kbars['upper_band1']].index[::-1]]), y=np.concatenate([kbars[kbars['收盤指數']<= kbars['upper_band1']]['lower_band'], kbars[kbars['收盤指數'] <= kbars['upper_band1']]['upper_band'][::-1]]), 
        #                fill='toself',fillcolor= 'rgba(0,256,256,0.1)', line_width=0,name='布林上下極限',row=1, col=1)

        #fig.add_trace(go.Scatter(x=kbars.index,
        #                 y=kbars['外資上極限'],
        #                 mode='lines',
        #                 line=dict(color='#9467bd'),
        #                 name='外資上極限'))

        #fig.add_trace(go.Scatter(x=kbars.index,
        #                 y=kbars['外資下極限'],
        #                 mode='lines',
        #                 line=dict(color='#17becf'),
        #                 name='外資下極限'))

        ### 成交量圖製作 ###
        volume_colors = [red_color if kbars['收盤指數'][i] > kbars['收盤指數'][i-1] else green_color for i in range(len(kbars['收盤指數']))]
        volume_colors[0] = green_color

        #fig.add_trace(go.Bar(x=kbars.index, y=kbars['成交金額'], name='Volume', marker=dict(color=volume_colors),showlegend=False), row=optvrank[0], col=1)
        fig.add_trace(go.Bar(x=kbars.index, y=kbars['成交金額'], name='成交金額', marker=dict(color=volume_colors)), row=1, col=1)


        fig.add_trace(go.Scatter(x=kbars.index,
                                y=kbars['20MA'],
                                mode='lines',
                                line=dict(color='green', width=0.5),
                                name='20MA'),row=1, col=1, secondary_y= True)

        fig.add_trace(go.Scatter(x=list(kbars['IC'].index)[2:]+ICdate,
                                y=kbars['IC'].values,
                                mode='lines',
                                line=dict(color='orange', width=0.5),
                                name='IC操盤線'),row=1, col=1, secondary_y= True)
        
        fig.add_trace(go.Scatter(x=kbars.index,
                                y=kbars['200MA'],
                                mode='lines',
                                line=dict(color='blue', width=0.5),
                                name='MA200'),row=1, col=1, secondary_y= True)
        fig.add_trace(go.Scatter(x=kbars.index,
                                y=kbars['60MA'],
                                mode='lines',
                                line=dict(color='orange', width=0.5),
                                name='MA60'),row=1, col=1, secondary_y= True)

    
            
        # fig.add_trace(go.Scatter(x=[kbars.index[0],kbars.index[0]],y=[15500,17500], line_width=0.1, line_color="green",name='月結算日',showlegend=False),row=1, col=1)
        # #if option_month == True:
        # for i in enddate[~enddate["契約月份"].str.contains("W")]['最後結算日']:
        #     if i > kbars.index[0] :#and i!=enddate[~enddate["契約月份"].str.contains("W")]['最後結算日'].values[6]:
        #         fig.add_vline(x=i, line_width=1, line_color="green",name='月結算日',row=1, col=1)

        # #enddate['最後結算日'].values
        # #enddate.groupby(enddate['最後結算日'].dt.month)['最後結算日'].max()
        # #list(enddate['最後結算日'].values)[:3]
        # #if option_week == True:
        # for i in enddate['最後結算日']:
        #     if i > kbars.index[0] :# and i!=enddate.groupby(enddate['最後結算日'].dt.month)['最後結算日'].max()[6] and i not in enddate.groupby(enddate['最後結算日'].dt.month)['最後結算日'].max():
        #         fig.add_vline(x=i, line_width=1,line_dash="dash", line_color="blue",name='週結算日')#, line_dash="dash"
        #     #fig.add_hrect(y0=0.9, y1=2.6, line_width=0, fillcolor="red", opacity=0.2)
        


        ### K線圖製作 ###
        fig.add_trace(
            go.Candlestick(
                x=kbars[(kbars['all_kk'] == -1)&(kbars['收盤指數'] >kbars['開盤指數'] )].index,
                open=kbars[(kbars['all_kk'] == -1)&(kbars['收盤指數'] >kbars['開盤指數'] )]['開盤指數'],
                high=kbars[(kbars['all_kk'] == -1)&(kbars['收盤指數'] >kbars['開盤指數'] )]['最高指數'],
                low=kbars[(kbars['all_kk'] == -1)&(kbars['收盤指數'] >kbars['開盤指數'] )]['最低指數'],
                close=kbars[(kbars['all_kk'] == -1)&(kbars['收盤指數'] >kbars['開盤指數'] )]['收盤指數'],
                increasing_line_color=decreasing_color,
                increasing_fillcolor=no_color, #fill_increasing_color(kbars.index>kbars.index[50])
                decreasing_line_color=decreasing_color,
                decreasing_fillcolor=no_color,#decreasing_color,
                line=dict(width=2),
                name='OHLC',showlegend=False
            )#,
            
            ,row=1, col=1, secondary_y= True
        )


        fig.add_trace(
            go.Candlestick(
                x=kbars[(kbars['all_kk'] == 1)&(kbars['收盤指數'] >kbars['開盤指數'] )].index,
                open=kbars[(kbars['all_kk'] == 1)&(kbars['收盤指數'] >kbars['開盤指數'] )]['開盤指數'],
                high=kbars[(kbars['all_kk'] == 1)&(kbars['收盤指數'] >kbars['開盤指數'] )]['最高指數'],
                low=kbars[(kbars['all_kk'] == 1)&(kbars['收盤指數'] >kbars['開盤指數'] )]['最低指數'],
                close=kbars[(kbars['all_kk'] == 1)&(kbars['收盤指數'] >kbars['開盤指數'] )]['收盤指數'],
                increasing_line_color=increasing_color,
                increasing_fillcolor=no_color, #fill_increasing_color(kbars.index>kbars.index[50])
                decreasing_line_color=increasing_color,
                decreasing_fillcolor=no_color,#decreasing_color,
                line=dict(width=1),
                name='OHLC',showlegend=False
            )#,
            
            ,row=1, col=1, secondary_y= True
        )

        ### K線圖製作 ###
        fig.add_trace(
            go.Candlestick(
                x=kbars[(kbars['all_kk'] == -1)&(kbars['收盤指數'] <kbars['開盤指數'] )].index,
                open=kbars[(kbars['all_kk'] == -1)&(kbars['收盤指數'] <kbars['開盤指數'] )]['開盤指數'],
                high=kbars[(kbars['all_kk'] == -1)&(kbars['收盤指數'] <kbars['開盤指數'] )]['最高指數'],
                low=kbars[(kbars['all_kk'] == -1)&(kbars['收盤指數'] <kbars['開盤指數'] )]['最低指數'],
                close=kbars[(kbars['all_kk'] == -1)&(kbars['收盤指數'] <kbars['開盤指數'] )]['收盤指數'],
                increasing_line_color=decreasing_color,
                increasing_fillcolor=decreasing_color, #fill_increasing_color(kbars.index>kbars.index[50])
                decreasing_line_color=decreasing_color,
                decreasing_fillcolor=decreasing_color,#decreasing_color,
                line=dict(width=1),
                name='OHLC',showlegend=False
            )#,
            
            ,row=1, col=1, secondary_y= True
        )


        fig.add_trace(
            go.Candlestick(
                x=kbars[(kbars['all_kk'] == 1)&(kbars['收盤指數'] <kbars['開盤指數'] )].index,
                open=kbars[(kbars['all_kk'] == 1)&(kbars['收盤指數'] <kbars['開盤指數'] )]['開盤指數'],
                high=kbars[(kbars['all_kk'] == 1)&(kbars['收盤指數'] <kbars['開盤指數'] )]['最高指數'],
                low=kbars[(kbars['all_kk'] == 1)&(kbars['收盤指數'] <kbars['開盤指數'] )]['最低指數'],
                close=kbars[(kbars['all_kk'] == 1)&(kbars['收盤指數'] <kbars['開盤指數'] )]['收盤指數'],
                increasing_line_color=increasing_color,
                increasing_fillcolor=increasing_color, #fill_increasing_color(kbars.index>kbars.index[50])
                decreasing_line_color=increasing_color,
                decreasing_fillcolor=increasing_color,#decreasing_color,
                line=dict(width=1),
                name='OHLC',showlegend=False
            )#,
            
            ,row=1, col=1, secondary_y= True
        )



        
        ### KD線 ###
        #if optvrank[0] != 0:
        #    fig.add_trace(go.Scatter(x=kbars.index, y=kbars['K'], name='K', line=dict(width=1, color='rgb(41, 98, 255)'),showlegend=False), row=optvrank[0], col=1)
        #    fig.add_trace(go.Scatter(x=kbars.index, y=kbars['D'], name='D', line=dict(width=1, color='rgb(255, 109, 0)'),showlegend=False), row=optvrank[0], col=1)

        ## 委賣數量 ##
        if optvrank[0] != 0:
            days20 = kbars[(kbars.index> (kbars.index[-1] + timedelta(days = -20)))]
            max_days20 = days20["九點累積委託賣出數量"].values.max()
            
            min_days20 = days20["九點累積委託賣出數量"].values.min()
            
            #volume_colors = [increasing_color if kbars['九點累積委託賣出數量'][i] > kbars['收盤指數'][i-1] else decreasing_color for i in range(len(kbars['收盤指數']))]
            fig.add_trace(go.Scatter(x=kbars.index, y=kbars['九點累積委託賣出數量'],
                                     line=dict(width=0.5),
                                      name='成交數量',showlegend=False), row=optvrank[0], col=1)
            fig.add_scatter(x=np.array(max_days20_x), y=np.array(max_days20_list),marker=dict(color = blue_color,size=5),showlegend=False,mode = 'markers', row=optvrank[0], col=1)
            fig.add_scatter(x=np.array(min_days20_x), y=np.array(min_days20_list),marker=dict(color = orange_color,size=5),showlegend=False,mode = 'markers', row=optvrank[0], col=1)
            
            fig.update_yaxes(title_text="開盤賣張", tickvals=[1300000, 1800000], ticktext=['1.3M', '1.8M'], showgrid=True, gridcolor='lightgray', row=optvrank[0], col=1)
        
        charti = 3
        ## 價平和

        PCsum_colors = [increasing_color if kbars['價平和'][i] > kbars['價平和'][i-1] else decreasing_color for i in range(len(kbars['價平和']))]
        PCsum_colors[0] = decreasing_color
        fig.add_trace(go.Bar(x=kbars.index, y=kbars['價平和'], name='PCsum', marker=dict(color=PCsum_colors),showlegend=False), row=charti, col=1)
        #fig.add_hline(y = 50, line_width=0.2,line_dash="dash", line_color="blue", row=charti, col=1)
        for i in range(1,int(max(kbars['價平和'].values)//50)+1):
            fig.add_trace(go.Scatter(x=kbars.index,y=[i*50]*len(kbars.index),showlegend=False,hoverinfo='none', line_width=0.5,line_dash="dash", line_color="black"), row=charti, col=1)
        fig.update_yaxes(title_text="價平和", row=charti, col=1)

        charti = charti +1
        ## 價外買賣權價差

        fig.add_trace(go.Bar(x=kbars[(kbars['價外買賣權價差']>0)].index, y=(kbars[(kbars['價外買賣權價差']>0)]['價外買賣權價差']), name='價外買賣權價差',marker=dict(color = red_color_full),showlegend=False), row=charti, col=1)
        fig.add_trace(go.Bar(x=kbars[(kbars['價外買賣權價差']<=0)].index, y=(kbars[(kbars['價外買賣權價差']<=0)]['價外買賣權價差']), name='價外買賣權價差',marker=dict(color = blue_color),showlegend=False), row=charti, col=1)
        #fig.add_hline(y = 50, line_width=0.2,line_dash="dash", line_color="blue", row=charti, col=1)
        fig.update_yaxes(title_text="價外買賣權價差", row=charti, col=1)
            

        charti = charti +1
        

        fig.add_trace(go.Bar(x=kbars[(kbars['月價平和日差']>0)&(~kbars.index.isin(notshowdate))].index, y=(kbars[(kbars['月價平和日差']>0)&(~kbars.index.isin(notshowdate))]['月價平和日差']), name='月價平和日差',marker=dict(color = red_color_full),showlegend=False), row=charti, col=1)
        fig.add_trace(go.Bar(x=kbars[(kbars['月價平和日差']<=0)&(~kbars.index.isin(notshowdate))].index, y=(kbars[(kbars['月價平和日差']<=0)&(~kbars.index.isin(notshowdate))]['月價平和日差']), name='月價平和日差',marker=dict(color = blue_color),showlegend=False), row=charti, col=1)
        fig.update_yaxes(title_text="月價平和日差", row=charti, col=1)

        charti = charti +1
        ## 月價外買賣權價差

        fig.add_trace(go.Bar(x=kbars[(kbars['月價外買賣權價差']>0)].index, y=(kbars[(kbars['月價外買賣權價差']>0)]['月價外買賣權價差']), name='月價外買賣權價差',marker=dict(color = red_color_full),showlegend=False), row=charti, col=1)
        fig.add_trace(go.Bar(x=kbars[(kbars['月價外買賣權價差']<=0)].index, y=(kbars[(kbars['月價外買賣權價差']<=0)]['月價外買賣權價差']), name='月價外買賣權價差',marker=dict(color = blue_color),showlegend=False), row=charti, col=1)
        #fig.add_hline(y = 50, line_width=0.2,line_dash="dash", line_color="blue", row=charti, col=1)
        fig.update_yaxes(title_text="月價外買賣權價差", row=charti, col=1)
        
        
        charti = charti +1
        ## 月結趨勢

        fig.add_trace(go.Bar(x=kbars.index, y=kbars['end_high'], name='MAX_END',marker=dict(color = black_color),showlegend=False), row=charti, col=1)
        fig.add_trace(go.Bar(x=kbars.index, y=kbars['end_low'], name='MIN_END',marker=dict(color = gray_color),showlegend=False), row=charti, col=1)
        fig.update_yaxes(title_text="月結趨勢", row=charti, col=1, tickfont=dict(size=8))

        
        ##外資買賣超
        fig.add_trace(go.Bar(x=dfbuysell[dfbuysell['ForeBuySell']>0].index, y=(dfbuysell[dfbuysell['ForeBuySell']>0]["ForeBuySell"]).round(2), name='外資買賣超',marker=dict(color = red_color_full),showlegend=False), row=charti+1, col=1)
        fig.add_trace(go.Bar(x=dfbuysell[dfbuysell['ForeBuySell']<=0].index, y=(dfbuysell[dfbuysell['ForeBuySell']<=0]["ForeBuySell"]).round(2), name='外資買賣超',marker=dict(color = blue_color),showlegend=False), row=charti+1, col=1)
        #fig.add_trace(go.Bar(x=bank8.index, y=bank8["八大行庫買賣超金額"]/10000, name='eightbank',showlegend=False), row=charti+2, col=1)
        fig.update_yaxes(title_text="外資買賣超(億元)", row=charti+1, col=1)


        
        ## 外資臺股期貨未平倉淨口數
        #fut_colors = [red_color_full if kbars['收盤指數'][i] > kbars['收盤指數'][i-1] else blue_color for i in range(len(kbars['收盤指數']))]
        #fut_colors[0] = blue_color
        fut_colors = [decreasing_color if futdf['多空未平倉口數淨額'][i] > futdf['多空未平倉口數淨額'][i-1] else increasing_color for i in range(len(futdf['多空未平倉口數淨額']))]
        fut_colors[0] = decreasing_color
        #fig.add_trace(go.Bar(x=kbars.index, y=kbars['成交金額'], name='成交金額', marker=dict(color=volume_colors)), row=1, col=1, secondary_y= True)
        fig.add_trace(go.Bar(x=futdf.index, y=futdf['多空未平倉口數淨額'], name='fut', marker=dict(color=fut_colors),showlegend=False), row=charti+2, col=1)
        #fig.add_trace(go.Bar(x=bank8.index, y=bank8["八大行庫買賣超金額"]/10000, name='eightbank',showlegend=False), row=charti+2, col=1)
        fig.update_yaxes(title_text="外資未平倉淨口數", row=charti+2, col=1)

        
        

        #put call ratio
        #fig.add_trace(go.Scatter(x=kbars.index,y=kbars['收盤指數'],
        #                mode='lines',
        #                line=dict(color='black'),
        #                name='收盤指數',showlegend=False),row=charti+1, col=1)
        #fig.add_trace(go.Bar(x=CPratio.index, y=CPratio['買賣權未平倉量比率%']-100, name='PC_Ratio',showlegend=False), row=charti+4, col=1)
        #fig.update_yaxes(title_text="PutCallRatio", row=charti+4, col=1)
        

        #選擇權外資OI
        fig.add_trace(go.Bar(x=TXOOIdf.index, y=(TXOOIdf["買買賣賣"]), name='買買權+賣賣權',marker=dict(color = red_color_full),showlegend=False), row=charti+3, col=1)
        fig.add_trace(go.Bar(x=TXOOIdf.index, y=(TXOOIdf["買賣賣買"]), name='買賣權+賣買權',marker=dict(color = blue_color),showlegend=False), row=charti+3, col=1)
        #fig.add_trace(go.Bar(x=bank8.index, y=bank8["八大行庫買賣超金額"]/10000, name='eightbank',showlegend=False), row=charti+2, col=1)3
        fig.update_yaxes(title_text="選擇權外資OI", row=charti+3, col=1)

        #心態
        fin = []
        find = []
        for idx in range(1,len(dfbuysell.index)) :
            try:
                datei = dfbuysell.index[idx]
                one = dfbuysell.loc[datei,'ForeBuySell']

                
                two = (int(futdf.loc[datei,'多空未平倉口數淨額']) - int(futdf.loc[dfbuysell.index[idx-1],'多空未平倉口數淨額']))*kbars.loc[datei,'收盤指數']*200
                three = TXOOIdf.loc[datei,'買買賣賣'] - TXOOIdf.loc[datei,'買賣賣買']
                find.append(datei)
                fin.append(one+two/100000000+three/100000000)
                #print(datei,one,two/100000000,three/100000000)
            except:
                continue
        fin = np.array(fin)
        find = np.array(find)
        fig.add_trace(go.Bar(x=find[fin>0], y=fin[fin>0], name='外資期現選心態',marker=dict(color = red_color_full),showlegend=False), row=charti+4, col=1)
        fig.add_trace(go.Bar(x=find[fin<=0], y=fin[fin<=0], name='外資期現選心態',marker=dict(color = blue_color),showlegend=False), row=charti+4, col=1)
        fig.update_yaxes(title_text="外資期現選心態", row=charti+4, col=1)


        ## 小台散戶多空比
        
        fig.add_trace(go.Bar(x=dfMTX[dfMTX['MTXRatio']>0].index, y=(dfMTX[dfMTX['MTXRatio']>0]['MTXRatio']*100).round(2), name='小台散戶多空比',marker=dict(color = orange_color),showlegend=False), row=charti+8, col=1)
        fig.add_trace(go.Bar(x=dfMTX[dfMTX['MTXRatio']<=0].index, y=(dfMTX[dfMTX['MTXRatio']<=0]['MTXRatio']*100).round(2), name='小台散戶多空比',marker=dict(color = green_color_full),showlegend=False), row=charti+8, col=1)
        #fig.add_trace(go.Bar(x=bank8.index, y=bank8["八大行庫買賣超金額"]/10000, name='eightbank',showlegend=False), row=charti+2, col=1)
        fig.update_yaxes(title_text="小台散戶多空比", row=charti+8, col=1)

        

        #八大行庫買賣超
        fig.add_trace(go.Bar(x=bank8[bank8["八大行庫買賣超金額"]>0].index, y=(bank8[bank8["八大行庫買賣超金額"]>0]["八大行庫買賣超金額"]/100000).round(2), name='八大行庫買賣超',marker=dict(color = orange_color),showlegend=False), row=charti+6, col=1)
        fig.add_trace(go.Bar(x=bank8[bank8["八大行庫買賣超金額"]<=0].index, y=(bank8[bank8["八大行庫買賣超金額"]<=0]["八大行庫買賣超金額"]/100000).round(2), name='八大行庫買賣超',marker=dict(color = green_color_full),showlegend=False), row=charti+6, col=1)
        #fig.add_trace(go.Bar(x=bank8.index, y=bank8["八大行庫買賣超金額"]/10000, name='eightbank',showlegend=False), row=charti+2, col=1)
        fig.update_yaxes(title_text="八大行庫", row=charti+6, col=1)


        
        fig.add_trace(go.Scatter(x=dfMargin.index, y=dfMargin['MarginRate'],marker=dict(color = gray_color),line_width=3, name='MarginRate',showlegend=False), row=charti+7, col=1)
        fig.update_yaxes(title_text="大盤融資資維持率", row=charti+7, col=1)    



        #美元匯率
        url = "https://api.finmindtrade.com/api/v4/data?"
        parameter = {
        "dataset": "TaiwanExchangeRate",
        "data_id":'USD',
        "start_date": '2023-01-02',
        "end_date": datetime.strftime(datetime.today(),'%Y-%m-%d'),
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRlIjoiMjAyMy0wNy0zMCAyMzowMTo0MSIsInVzZXJfaWQiOiJqZXlhbmdqYXUiLCJpcCI6IjExNC4zNC4xMjEuMTA0In0.WDAZzKGv4Du5JilaAR7o7M1whpnGaR-vMDuSeTBXhhA", # 參考登入，獲取金鑰
        }
        data = requests.get(url, params=parameter)
        data = data.json()
        TaiwanExchangeRate = pd.DataFrame(data['data'])
        TaiwanExchangeRate.date = pd.to_datetime(TaiwanExchangeRate.date)
        TaiwanExchangeRate = TaiwanExchangeRate[~(TaiwanExchangeRate['spot_buy']==-1)]

        fig.add_trace(go.Scatter(x=TaiwanExchangeRate[(TaiwanExchangeRate.date>kbars.index[0])&(TaiwanExchangeRate.date!=datetime.strptime('2023-08-03', '%Y-%m-%d'))].date, y=TaiwanExchangeRate[(TaiwanExchangeRate.date>kbars.index[0])&(TaiwanExchangeRate.date!=datetime.strptime('2023-08-03', '%Y-%m-%d'))]['spot_buy'],marker=dict(color = gray_color,width=0.5), name='ExchangeRate',line_width=3,showlegend=False), row=charti+5, col=1)
        fig.update_yaxes(title_text="美元匯率", row=charti+5, col=1)  

        token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRlIjoiMjAyMy0wNy0zMCAyMzowMTo0MSIsInVzZXJfaWQiOiJqZXlhbmdqYXUiLCJpcCI6IjExNC4zNC4xMjEuMTA0In0.WDAZzKGv4Du5JilaAR7o7M1whpnGaR-vMDuSeTBXhhA"
        url = "https://api.finmindtrade.com/api/v4/data?"

        

        

        

        

        ymin = kbars.min()[['開盤指數', '最高指數', '最低指數', '收盤指數',
            '20MA',  '60MA', '200MA', 'upper_band',
        'lower_band', 'upper_band1', 'lower_band1', 'IC']].min()

        ymax = kbars.max()[['開盤指數', '最高指數', '最低指數', '收盤指數',
                '20MA',  '60MA', '200MA', 'upper_band',
            'lower_band', 'upper_band1', 'lower_band1', 'IC']].max()
                
        ### 圖表設定 ###
        fig.update(layout_xaxis_rangeslider_visible=False)
        fig.update_annotations(font_size=12)

        fig.update_layout(
            title=u'大盤指數技術分析圖',
            #title_x=0.5,
            #title_y=0.93,
            hovermode='x unified',
            height=350 + 150* rowcount,
            width = 1200,
            hoverlabel_namelength=-1,
            hoverlabel_align = "left",
            xaxis2=dict(showgrid=False),
            yaxis2=dict(showgrid=False,tickformat = ",.0f",range=[ymin - 200, ymax + 200]),
            yaxis = dict(showgrid=False,showticklabels=False,range=[0, kbars['成交金額'].max()*3]),
            #yaxis = dict(range=[kbars['min'].min() - 2000, kbars['最高指數'].max() + 500]),
            dragmode = 'drawline',
            hoverlabel=dict(align='left',bgcolor='rgba(255,255,255,0.5)',font=dict(color='black')),
            legend_traceorder="reversed",
            font=dict(
            family="Noto Sans TC",   # 全域字型
            size=12,
            color="black"
            )
        )

        fig.update_traces(xaxis='x1',hoverlabel=dict(align='left'))

        # 隱藏周末與市場休市日期 ### 導入台灣的休市資料

        noshowdate = []
        for delta_day in range((datetime.now() - datetime.strptime(kbars.index.strftime('%Y-%m-%d')[0], '%Y-%m-%d')).days):
            if (datetime.now() - timedelta(days=delta_day)).strftime('%Y-%m-%d') not in kbars.index.strftime('%Y-%m-%d').values:
                noshowdate.append((datetime.now() - timedelta(days=delta_day)).strftime('%Y-%m-%d'))
            
        fig.update_xaxes(
            rangebreaks=[
                #dict(bounds=['sat', 'mon']), # hide weekends, eg. hide sat to before mon
                dict(values= noshowdate)
            ]
        )
        #noshowdate




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
        #df_limit = pd.read_sql('select distinct * from df_option_limit', con_an)
        df_option_limit = pd.read_sql("select distinct * from df_option_limit", con_an)
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
    #gap_day_df = df_daygap[df_daygap["日期"] == sel_str].copy()
    ref_row = df_putcall[df_putcall["日期"] == sel_str].copy()

    # 容錯：若當天 put/call 參考不存在，用最近一筆
    if ref_row.empty and not df_putcall.empty:
        ref_row = df_putcall.sort_values("日期").tail(1).copy()

    call_num = pd.to_numeric(ref_row["價平和買權成交價"].iloc[0], errors="coerce")
    call_num = 0 if pd.isna(call_num) else float(call_num)  # 需要比較時用 float
    put_num = pd.to_numeric(ref_row["價平和賣權成交價"].iloc[0], errors="coerce")
    put_num = 0 if pd.isna(put_num) else float(put_num)

     #for i in range(num_days):
    foreign_df = pd.DataFrame()
    dealer_df = pd.DataFrame()
    retail_df = pd.DataFrame()
    num_days = 7
    num_i = 0
    day_i = 0
    while num_i < num_days:
        gap_day_df = df_daygap[df_daygap["日期"] ==datetime.strftime(sel_day - timedelta(days=day_i), '%Y-%m-%d') ]
        callputtemp = df_putcall[df_putcall["日期"] ==datetime.strftime(sel_day - timedelta(days=day_i), '%Y-%m-%d')]
        day_i += 1
        if len(gap_day_df) !=0:
            num_i += 1
            call_num = int(callputtemp["價平和買權成交價"])
            put_num = int(callputtemp["價平和賣權成交價"])
            gap_day_df["成交位置"]=""
            for idx in gap_day_df.index:
                if gap_day_df.loc[idx,"種類"]=="買權":
                    if np.abs(gap_day_df.loc[idx,"單價"])>call_num:
                        gap_day_df.loc[idx,"成交位置"] = "價內"
                    else:
                        gap_day_df.loc[idx,"成交位置"] = "價外"

                elif gap_day_df.loc[idx,"種類"]=="賣權":
                    if np.abs(gap_day_df.loc[idx,"單價"])>put_num:
                        gap_day_df.loc[idx,"成交位置"] = "價內"
                    else:
                        gap_day_df.loc[idx,"成交位置"] = "價外"

                else:
                    gap_day_df.loc[idx,"成交位置"] = "不分"
                    
                #gap_day_df.loc[idx,"成交位置"]
            foreign_df = pd.concat([foreign_df,gap_day_df[gap_day_df["身份"]=="外資"]])
            dealer_df = pd.concat([dealer_df,gap_day_df[gap_day_df["身份"]=="自營商"]])
            retail_df = pd.concat([retail_df,gap_day_df[gap_day_df["身份"]=="散戶"]])
            #foreign_df = foreign_df.append(gap_day_df[gap_day_df["身份"]=="外資"])
            #dealer_df = dealer_df.append(gap_day_df[gap_day_df["身份"]=="自營商"])
            #retail_df = retail_df.append(gap_day_df[gap_day_df["身份"]=="散戶"])

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
        df_cost["外資成本"] = pd.to_numeric(df_cost["外資成本"], errors="coerce")
    ref_df = ref_df.merge(df_cost[["日期","外資成本"]], how="left", on="日期")

    # 指數漲跌點數（用 FinMind，可設環境變數 FINMIND_TOKEN；取不到就略過）
    callputtemp = df_putcall[(df_putcall["日期"] <=datetime.strftime(sel_day, '%Y-%m-%d'))]
    callputtemp = callputtemp[callputtemp["日期"] >=datetime.strftime(sel_day - timedelta(days=day_i-1), '%Y-%m-%d')]
    callputtemp = callputtemp.sort_values(by="日期",ascending=False)
    callputtemp = callputtemp.reset_index(drop=True)
    callputtemp.columns = ["日期","價平和履約價","價平和買權成交價","價平和賣權成交價"]
    df_cost["外資成本"] = df_cost["外資成本"].astype(int)

    callputtemp = callputtemp.join(df_cost.set_index("日期"),on="日期")


    #df_option_limit = pd.read_sql("select distinct * from df_option_limit", con_an)
    limit_temp = df_option_limit[(df_option_limit["日期"] <=datetime.strftime(sel_day, '%Y-%m-%d'))]
    limit_temp = limit_temp[limit_temp["日期"] >=datetime.strftime(sel_day - timedelta(days=day_i-1), '%Y-%m-%d')]
    limit_temp = limit_temp.sort_values(by="日期",ascending=False)
    limit_temp = limit_temp.reset_index(drop=True)


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
    # 7. 存檔（固定檔名，覆蓋）
    
    imgs.append(_safe_png(foreign_df, f"opt_day_foreign.png", f"日變動：外資（{sel_str}）"))
    imgs.append(_safe_png(dealer_df,  f"opt_day_dealer.png",  f"日變動：自營商（{sel_str}）"))
    imgs.append(_safe_png(retail_df,  f"opt_day_retail.png",  f"日變動：散戶（{sel_str}）"))

    imgs.append(_safe_png(callputtemp ,     f"opt_ref.png",         f"參考數據（{sel_str}）"))

    imgs.append(_safe_png(limit_temp,   f"opt_limit.png",       f"上下極限（{sel_str}）"))

    base = _public_base()
    urls = [f"{base}/images/{p.name}" for p in imgs]

    return jsonify(ok=True, date=sel_str, files=[p.name for p in imgs], urls=urls)

@app.route("/cron/genplot_main", methods=["GET", "POST"])
def cron_genplot_main():
    """
    產生主圖 (generate_plot_png_main) 並存檔
    Query:
      - filename: 檔名（預設 main.png）
    """
    _require_key()
    filename = request.args.get("filename", "main.png")
    p = generate_plot_png_main(filename)   # 這裡呼叫你自己寫好的 function
    base = _public_base()
    url = f"{base}/images/{p.name}"
    return jsonify(ok=True, file=p.name, url=url)

@app.route("/cron/push_main", methods=["GET", "POST"])
def cron_push_main():
    """
    推播 generate_plot_png_main 產生的圖
    Query:
      - filename: 檔名（預設 main.png）
    """
    _require_key()
    filename = request.args.get("filename", "main.png")
    p = IMAGES_DIR / filename
    if not p.exists():
        # 沒檔案時就先呼叫生成
        with app.test_request_context(f"/cron/genplot_main?key={CRON_KEY}&filename={filename}"):
            resp = cron_genplot_main().json
        url = resp["url"]
    else:
        url = f"{_public_base()}/images/{filename}"

    count = _push_images_to_whitelist([url])
    return f"OK, pushed main plot to whitelist ({count} image sent)."

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
    
    # 如果沒有 user_id 或不在白名單 → 拒絕
    if not uid or uid not in wl:
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="抱歉，你沒有權限使用這個功能。")
        )
        # 導引
        reply(
            f"若要接收定時通知和使用查詢功能，請輸入：\n「註冊 <驗證碼>」\n"
            "取消請輸入：「取消訂閱」\n"
            "查詢請輸入：「狀態」"
        )
        return

    key = text.replace(" ", "").replace("\u3000","")

    if key in ("即時盤", "即時盘", "即時", "盤中"):
        url = _ensure_latest_png()
        _reply_images(event.reply_token, [url])
        return

    elif key in ("主圖", "主图", "main"):
        url = _ensure_main_png()
        _reply_images(event.reply_token, [url])
        return

    elif key in ("資料", "数据", "表格", "tables"):
        urls = _ensure_tables_pngs()
        if urls:
            _reply_images(event.reply_token, urls)
            if len(urls) > 5:
                _push_images_to_user(uid, urls[5:])
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