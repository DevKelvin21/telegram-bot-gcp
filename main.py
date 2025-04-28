import asyncio
import os
import json
import requests
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from google.cloud import firestore
from functions_framework import http
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram import Update
from openai import OpenAI

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")
GPT_MODEL = os.getenv("GPT_MODEL", "gpt-3.5-turbo")


def load_allowed_user_ids():
    db = firestore.Client()
    docs = db.collection("allowedUserIDs").stream()
    allowed_users = set()
    for doc in docs:
        data = doc.to_dict()
        allowed_users.add(int(data["ID"]))
    return allowed_users

@http
def telegram_bot(request):
    return asyncio.run(main(request))


async def main(request):
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    bot = app.bot

    app.add_handler(CommandHandler("start", on_start))
    app.add_handler(MessageHandler(filters.TEXT, on_message))

    if request.method == 'GET':
        await bot.set_webhook(f'https://{request.host}/telegram_bot')
        return "Webhook set"

    async with app:
        update = Update.de_json(request.json, bot)
        await app.process_update(update)

    return "ok"


async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Hola, soy tu bot de ventas y gastos para la floristería Morale's 🌸"
    )


async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message.text
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    allowed_users = load_allowed_user_ids()
    if user_id not in allowed_users:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"Tu ID de usuario de Telegram es: `{user_id}`\nCompártelo con el administrador para que te dé acceso.",
            parse_mode="Markdown"
        )
        log_to_bigquery({
            "timestamp": current_utc_iso(),
            "user_id": user_id,
            "chat_id": chat_id,
            "operation_type": "unauthorized_access",
            "message_content": message
        })
        return

    try:
        gpt_response = interpret_message_with_gpt(message)
        structured_data = json.loads(gpt_response)
        if not structured_data.get("sales") and not structured_data.get("expenses"):
            await context.bot.send_message(
                chat_id=chat_id,
                text="No se encontró ninguna venta ni gasto en el mensaje."
            )
            return
        structured_data.setdefault("date", datetime.now(timezone(timedelta(hours=-6))).strftime("%Y-%m-%d"))
        insert_to_bigquery(structured_data)

        log_to_bigquery({
            "timestamp": current_utc_iso(),
            "user_id": user_id,
            "chat_id": chat_id,
            "operation_type": "data_insert",
            "message_content": message
        })

        await context.bot.send_message(
            chat_id=chat_id,
            text=f"Registro guardado correctamente:\n{json.dumps(structured_data, indent=2)}"
        )
    except Exception as e:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"Hubo un error al procesar el mensaje: {str(e)}"
        )


def interpret_message_with_gpt(message: str) -> str:
    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an assistant that extracts structured sales and expenses data from flower shop messages.\n\n"
                    "Each message may include sales (sold products) or expenses (purchases or operational costs) in free-text form.\n\n"
                    "Output a JSON object in the following structure:\n\n"
                    "{\n"
                    "  \"total_sale_price\": float or null, // Sum of all sales; null if only expenses\n"
                    "  \"payment_method\": \"cash\" | \"bank_transfer\" | null, // Payment method for sales default to cash; null if only expenses\n"
                    "  \"sales\": [\n"
                    "    {\n"
                    "      \"item\": \"string\",\n"
                    "      \"quantity\": int or null,\n"
                    "      \"unit_price\": float or null\n"
                    "    }\n"
                    "  ],\n"
                    "  \"expenses\": [\n"
                    "    {\n"
                    "      \"description\": \"string\",\n"
                    "      \"amount\": float\n"
                    "    }\n"
                    "  ]\n"
                    "}\n\n"
                    "Rules:\n"
                    "- If the message describes a **purchase**, **buying**, or **operational cost** (e.g., 'compramos', 'gastamos', 'pagamos'), create an entry under \"expenses\".\n"
                    "- If the message describes a **sale** (e.g., 'vendimos', 'se vendió'), create an entry under \"sales\" and set \"total_sale_price\".\n"
                    "- If the message describes only an expense, \"total_sale_price\" must be null.\n"
                    "- If no payment method is mentioned and it is not a sale, set \"payment_method\" to null.\n"
                    "- Always output only valid JSON without additional explanations."
                )
            },
            {"role": "user", "content": message}
        ],
        temperature=0.2
    )
    return response.choices[0].message.content


def insert_to_bigquery(row: dict):
    client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    errors = client.insert_rows_json(table_id, [row])
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")


def log_to_bigquery(log_entry: dict):
    client = bigquery.Client()
    log_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.audit_logs"
    errors = client.insert_rows_json(log_table_id, [log_entry])
    if errors:
        print(f"Audit log insert errors: {errors}")


def current_utc_iso():
    return datetime.now(timezone.utc).isoformat()