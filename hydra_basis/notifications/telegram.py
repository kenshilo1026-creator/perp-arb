from __future__ import annotations

import asyncio
import json
import os
import urllib.request


def send_telegram_sync(message: str) -> None:
    telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    if not telegram_bot_token or not telegram_chat_id:
        print("[telegram disabled]", message)
        return

    url = f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage"
    payload = {
        "chat_id": telegram_chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        if getattr(response, "status", 200) >= 400:
            raise RuntimeError(f"telegram http status={response.status}")
        body = response.read()
        if body:
            json.loads(body.decode("utf-8"))


async def send_telegram(message: str) -> None:
    await asyncio.to_thread(send_telegram_sync, message)
