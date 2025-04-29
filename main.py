import asyncio
from functions_framework import http
from telegram.ext import Application, CommandHandler, MessageHandler, filters
from services.bot_service import BotService
from config.loader import load_allowed_user_ids, load_bot_config
import os
from telegram import Update

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

@http
def telegram_bot(request):
    return asyncio.run(main(request))


async def main(request):
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    allowed_users = load_allowed_user_ids()
    bot_config = load_bot_config()
    bot_service = BotService(app.bot, allowed_users, bot_config)

    app.add_handler(CommandHandler("start", bot_service.handle_start))
    app.add_handler(MessageHandler(filters.TEXT, bot_service.handle_message))

    if request.method == 'GET':
        await app.bot.set_webhook(f'https://{request.host}/telegram_bot')
        return "Webhook set"

    async with app:
        update = Update.de_json(request.json, app.bot)
        await app.process_update(update)

    return "ok"