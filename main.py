import asyncio
from functions_framework import http
from telegram.ext import Application, CommandHandler, MessageHandler, filters
from services.bot_service import BotService
from config.loader import FirestoreLoader
from utils.bigquery_utils import BigQueryUtils
import os
from telegram import Update
import pytz

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

@http
def telegram_bot(request):
    return asyncio.run(main(request))


async def main(request):
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    firestore_loader = FirestoreLoader()
    allowed_users = firestore_loader.load_allowed_user_ids()
    bot_config = firestore_loader.load_bot_config()
    owner_id = firestore_loader.load_owner_id()

    timezone_obj = pytz.timezone("America/El_Salvador")
    bigquery_utils = BigQueryUtils(timezone_obj)

    bot_service = BotService(app.bot, allowed_users, bot_config, owner_id, bigquery_utils)

    app.add_handler(CommandHandler("start", bot_service.handle_start))
    app.add_handler(MessageHandler(filters.TEXT, bot_service.handle_message))

    if request.method == 'GET':
        await app.bot.set_webhook(f'https://{request.host}/telegram_bot')
        return "Webhook set"

    async with app:
        update = Update.de_json(request.json, app.bot)
        update_id = update.update_id

        if await asyncio.to_thread(firestore_loader.is_duplicate_update, update_id):
            print(f"Duplicate update received: {update_id}")
            return "ok"

        await asyncio.to_thread(firestore_loader.mark_update_processed, update_id)
        await app.process_update(update)

    return "ok"