from __future__ import annotations

import os

import aiohttp

from hydra_basis.adapters.base import fetch_json


async def send_telegram(message: str) -> None:
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
    async with aiohttp.ClientSession() as session:
        await fetch_json(session, "POST", url, json=payload)
