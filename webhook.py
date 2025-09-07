# webhook.py
import os, json, threading
from pathlib import Path
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]
REGISTER_CODE = os.environ.get("REGISTER_CODE", "abc123")  # 你的驗證碼

# ✅ 新增：Cron 驗證用的金鑰（Cron Job 觸發 /cron/push 必須帶上）
CRON_KEY = os.environ.get("CRON_KEY")

WHITE_LIST_FILE = Path(os.environ.get("WHITELIST_PATH", "whitelist.json"))  # 可用環境變數改路徑
_lock = threading.Lock()

app = Flask(__name__)
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

def load_whitelist():
    if not WHITE_LIST_FILE.exists():
        return set()
    try:
        data = json.loads(WHITE_LIST_FILE.read_text("utf-8"))
        return set(data if isinstance(data, list) else [])
    except Exception:
        return set()

def save_whitelist(ids: set):
    with _lock:
        WHITE_LIST_FILE.write_text(
            json.dumps(sorted(list(ids)), ensure_ascii=False, indent=2),
            "utf-8"
        )

@app.route("/health")
def health():
    return "OK"

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers.get('X-Line-Signature', '')
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
    wl = load_whitelist()

    def reply(msg):
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))

    # 指令：註冊 <code>
    if text.lower().startswith("註冊") or text.lower().startswith("register"):
        parts = text.replace("　", " ").split()
        if len(parts) >= 2:
            code = parts[1]
        else:
            reply("請輸入：註冊 <驗證碼>\n例如：註冊 " + REGISTER_CODE)
            return

        if code == REGISTER_CODE:
            if uid in wl:
                reply("你已經完成訂閱囉 ✅")
            else:
                wl.add(uid)
                save_whitelist(wl)
                reply("訂閱成功！之後會收到定時通知 ✅")
        else:
            reply("驗證碼錯誤 ❌，請再試一次。")
        return

    # 指令：取消訂閱 / 退訂
    if text in ["取消訂閱", "退訂", "unsubscribe"]:
        if uid in wl:
            wl.remove(uid)
            save_whitelist(wl)
            reply("已取消訂閱 ✅ 之後不會再收到定時通知。")
        else:
            reply("你目前不在訂閱名單中。")
        return

    # 指令：狀態
    if text in ["狀態", "status"]:
        reply("目前狀態：{}".format("✅ 已訂閱" if uid in wl else "未訂閱"))
        return

    # 其他訊息：給個導引
    reply(
        "嗨！\n"
        "若要接收定時通知，請輸入：\n"
        f"「註冊 {REGISTER_CODE}」\n"
        "取消請輸入：「取消訂閱」\n"
        "查詢請輸入：「狀態」"
    )

# =========================
# ✅ 新增：定時推播端點
# =========================
def _push_to_whitelist(message: str) -> int:
    targets = sorted(list(load_whitelist()))
    sent = 0
    for uid in targets:
        try:
            line_bot_api.push_message(uid, TextSendMessage(text=message))
            sent += 1
        except Exception as e:
            print("Push failed for", uid, e)
    return sent

@app.route("/cron/push", methods=["POST", "GET"])
def cron_push():
    # 簡單金鑰保護：?key=... 或 Header: X-CRON-KEY: ...
    key = request.args.get("key") or request.headers.get("X-CRON-KEY")
    if not CRON_KEY or key != CRON_KEY:
        abort(401)

    # 支援動態訊息（可用 ?message=... 帶入），預設給一段文字
    msg = request.args.get("message") or "⏰ 固定時間提醒來囉！"
    count = _push_to_whitelist(msg)
    return f"OK, pushed to {count} subscribers"

if __name__ == "__main__":
    # 本地開發：python webhook.py
    app.run(host="0.0.0.0", port=8000)