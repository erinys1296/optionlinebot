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

WHITE_LIST_FILE = Path("whitelist.json")
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
        WHITE_LIST_FILE.write_text(json.dumps(sorted(list(ids)), ensure_ascii=False, indent=2), "utf-8")

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
        # 支援「註冊 abc123」或「註冊」後再回覆
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

if __name__ == "__main__":
    # 本地開發：python webhook.py
    # 之後用 ngrok 暴露：ngrok http 8000
    app.run(host="0.0.0.0", port=8000)