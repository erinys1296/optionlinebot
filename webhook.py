# -*- coding: utf-8 -*-
import os, json, threading, time, logging
from pathlib import Path
from typing import Set, List

from flask import Flask, request, abort, jsonify
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

# ========= 設定 =========
CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]
REGISTER_CODE = os.environ.get("REGISTER_CODE", "abc123")   # 使用者註冊用驗證碼
CRON_KEY = os.environ.get("CRON_KEY")                       # Admin/Cron 金鑰（必填）
WHITE_LIST_FILE = Path(os.environ.get("WHITELIST_PATH", "whitelist.json"))

# ========= App / SDK =========
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("webhook")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# --- Persistent Disk 初始化與一次性搬遷 ---
DEFAULT_WHITE_LIST_FILE = Path("whitelist.json")  # 容器本地舊路徑
WHITE_LIST_FILE.parent.mkdir(parents=True, exist_ok=True)

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
    """原子寫檔：先寫暫存檔，再 os.replace 覆蓋，避免中途中斷導致 JSON 壞掉。"""
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

def _push_to_whitelist(msg: str) -> int:
    wl = sorted(list(_load_whitelist()))
    return _push_to_uids(wl, msg)

# ========= 基本健康檢查 =========
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

# ========= 定時推播（給 Render Cron Job 用） =========
@app.route("/cron/push", methods=["POST", "GET"])
def cron_push():
    _require_key()
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
        token_masked=masked(CHANNEL_ACCESS_TOKEN),
        secret_masked=masked(CHANNEL_SECRET),
    )

# ========= 本機啟動 =========
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)