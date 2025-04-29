import asyncio
import os
import json
import requests
import re
import uuid
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


def load_allowed_user_ids():
    db = firestore.Client()
    docs = db.collection("allowedUserIDs").stream()
    allowed_users = set()
    for doc in docs:
        data = doc.to_dict()
        allowed_users.add(int(data["ID"]))
    return allowed_users

def load_bot_config():
    db = firestore.Client()
    doc = db.collection("configs").document("telegram-bot").get()
    if not doc.exists:
        raise RuntimeError("Config document not found in Firestore.")
    return doc.to_dict()

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
            "timestamp": current_cst_iso(),
            "user_id": user_id,
            "chat_id": chat_id,
            "operation_type": "unauthorized_access",
            "message_content": message,
            "user_name": update.effective_user.full_name
        })
        return

    # Lowercase for easier matching
    command = message.strip().lower()

    if command.startswith("eliminar"):
        parts = message.split()
        if len(parts) != 2:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Formato incorrecto. Usa: eliminar <transaction_id>"
            )
            return
        transaction_id = parts[1]
        try:
            safe_delete(transaction_id)
            await safe_send_message(
                context.bot,
                chat_id,
                f"✅ *ID de Transacción:*\n`{transaction_id}` eliminada correctamente."
            )
            log_to_bigquery({
                "timestamp": current_cst_iso(),
                "user_id": user_id,
                "chat_id": chat_id,
                "operation_type": "delete_transaction",
                "message_content": message,
                "user_name": update.effective_user.full_name
            })
            try:
                config = load_bot_config()
                if config.get("liveNotifications"):
                    db = firestore.Client()
                    owner_doc = next(
                        (doc.to_dict() for doc in db.collection("allowedUserIDs").stream() if doc.to_dict().get("Role") == "Owner"),
                        None
                    )
                    if owner_doc:
                        owner_id = int(owner_doc["ID"])
                        await safe_send_message(
                            context.bot,
                            owner_id,
                            f"🔔 Notificación de administración:\n\n"
                            f"Operación realizada por {escape_user_text(update.effective_user.full_name)} (ID: {user_id}).\n"
                            f"Acción: Eliminar\n"
                            f"🆔 *ID de Transacción:*\n`{transaction_id}`"
                        )
            except Exception as notify_error:
                print(f"Error notificando al Owner: {notify_error}")
        except Exception as e:
            await safe_send_message(context.bot, chat_id, f"❌ Error al eliminar:\n{escape_user_text(str(e))}")
        return

    if command.startswith("editar"):
        parts = message.split(maxsplit=2)
        if len(parts) != 3:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Formato incorrecto. Usa: editar <transaction_id> <nuevo mensaje>"
            )
            return
        transaction_id, new_text = parts[1], parts[2]
        try:
            gpt_response = interpret_message_with_gpt(new_text)
            new_data = json.loads(gpt_response)
            new_data.setdefault("date", datetime.now(timezone(timedelta(hours=-6))).strftime("%Y-%m-%d"))
            new_data["transaction_id"] = transaction_id
            safe_edit(transaction_id, new_data)

            await safe_send_message(
                context.bot,
                chat_id,
                f"✅ *ID de Transacción:*\n`{transaction_id}` actualizada correctamente."
            )
            log_to_bigquery({
                "timestamp": current_cst_iso(),
                "user_id": user_id,
                "chat_id": chat_id,
                "operation_type": "edit_transaction",
                "message_content": message,
                "user_name": update.effective_user.full_name
            })
            try:
                config = load_bot_config()
                if config.get("liveNotifications"):
                    db = firestore.Client()
                    owner_doc = next(
                        (doc.to_dict() for doc in db.collection("allowedUserIDs").stream() if doc.to_dict().get("Role") == "Owner"),
                        None
                    )
                    if owner_doc:
                        owner_id = int(owner_doc["ID"])
                        await safe_send_message(
                            context.bot,
                            owner_id,
                            f"🔔 Notificación de administración:\n\n"
                            f"Operación realizada por {escape_user_text(update.effective_user.full_name)} (ID: {user_id})\n"
                            f"Acción: Editar\n"
                            f"🆔 *ID de Transacción:*\n`{transaction_id}`"
                        )
            except Exception as notify_error:
                print(f"Error notificando al Owner: {notify_error}")
        except Exception as e:
            await safe_send_message(context.bot, chat_id, f"❌ Error al editar:\n{escape_user_text(str(e))}")
        return

    if command.startswith("cierre"):
        client = bigquery.Client()
        query = f"""
        WITH latest_transactions AS (
        SELECT *
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE operation IS NULL
        )

        , ventas_efectivo AS (
        SELECT
            SUM(total_sale_price) AS efectivo_sales
        FROM latest_transactions
        WHERE payment_method = 'cash'
            AND date = CURRENT_DATE()
        )

        , ventas_transferencia AS (
        SELECT
            SUM(total_sale_price) AS transfer_sales
        FROM latest_transactions
        WHERE payment_method = 'bank_transfer'
            AND date = CURRENT_DATE()
        )

        , gastos_totales AS (
        SELECT
            SUM(expense.amount) AS total_expenses
        FROM latest_transactions,
        UNNEST(expenses) AS expense
        WHERE date = CURRENT_DATE()
        )

        SELECT
        (SELECT efectivo_sales FROM ventas_efectivo) AS efectivo_sales,
        (SELECT transfer_sales FROM ventas_transferencia) AS transfer_sales,
        (SELECT total_expenses FROM gastos_totales) AS total_expenses
        """
        query_job = client.query(query)
        results = query_job.result()

        for row in results:
            efectivo_sales = row.efectivo_sales if row.efectivo_sales is not None else 0
            transfer_sales = row.transfer_sales if row.transfer_sales is not None else 0
            total_expenses = row.total_expenses if row.total_expenses is not None else 0
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🔔 Resumen del cierre de caja:\n\n"
                     f"🏦 Ventas por transferencia bancaria: ${transfer_sales}\n"
                     f"💵 Ventas en efectivo: ${efectivo_sales}\n"
                     f"💰 Gastos del día: ${total_expenses}\n"
                     f"💵 Total efectivo en caja: ${efectivo_sales - total_expenses}\n\n"
            )

        log_to_bigquery({
            "timestamp": current_cst_iso(),
            "user_id": user_id,
            "chat_id": chat_id,
            "operation_type": "closure_report",
            "message_content": message,
            "user_name": update.effective_user.full_name
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
            "timestamp": current_cst_iso(),
            "user_id": user_id,
            "chat_id": chat_id,
            "operation_type": "data_insert",
            "message_content": message,
            "user_name": update.effective_user.full_name
        })

        # Use safe_send_message for confirmation message with dynamic content (escape only user JSON)
        await safe_send_message(
            context.bot,
            chat_id,
            f"Registro guardado correctamente\n\n"
            f"```json\n{escape_user_text(json.dumps(structured_data, indent=2))}\n```"
            f"\n\n🆔 *ID de Transacción:*\n`{structured_data['transaction_id']}`"
        )

        config = load_bot_config()
        if config.get("liveNotifications"):
            db = firestore.Client()
            owner_doc = next(
                (doc.to_dict() for doc in db.collection("allowedUserIDs").stream() if doc.to_dict().get("Role") == "Owner"),
                None
            )
            if owner_doc:
                owner_id = int(owner_doc["ID"])
                await safe_send_message(
                    context.bot,
                    owner_id,
                    f"🔔 Nueva operación registrada por {escape_user_text(update.effective_user.full_name)} (ID: {user_id}):\n\n{escape_user_text(message)}\n\n"
                    f"🆔 *ID de Transacción:*\n`{structured_data['transaction_id']}`"
                )
    except Exception as e:
        await safe_send_message(context.bot, chat_id, f"Hubo un error al procesar el mensaje: {escape_user_text(str(e))}")


def interpret_message_with_gpt(message: str) -> str:
    config = load_bot_config()
    model = config.get("gptModel", "gpt-3.5-turbo")
    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model=model,
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
    row.setdefault("transaction_id", str(uuid.uuid4()))
    errors = client.insert_rows_json(table_id, [row])
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")


def log_to_bigquery(log_entry: dict):
    client = bigquery.Client()
    log_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.audit_logs"
    errors = client.insert_rows_json(log_table_id, [log_entry])
    if errors:
        print(f"Audit log insert errors: {errors}")


def current_cst_iso():
    return datetime.now(timezone(timedelta(hours=-6))).isoformat()

def safe_delete(transaction_id: str):
    client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    query = f"""
    SELECT *
    FROM `{table_id}`
    WHERE transaction_id = @transaction_id
      AND operation IS NULL
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("transaction_id", "STRING", transaction_id)
        ]
    )
    result = client.query(query, job_config=job_config).result()
    original = list(result)[0]

    shadow = dict(original)
    shadow["operation"] = "deleted"
    shadow["is_deleted"] = True
    shadow["date"] = datetime.now(timezone(timedelta(hours=-6))).strftime("%Y-%m-%d")
    insert_to_bigquery(shadow)

def safe_edit(transaction_id: str, new_data: dict):
    safe_delete(transaction_id)

    new_data.setdefault("date", datetime.now(timezone(timedelta(hours=-6))).strftime("%Y-%m-%d"))
    new_data["transaction_id"] = transaction_id
    new_data["operation"] = None
    new_data["is_deleted"] = False
    insert_to_bigquery(new_data)

def escape_markdown(text: str) -> str:
    """
    Escape Telegram MarkdownV2 reserved characters.
    """
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

def escape_user_text(text: str) -> str:
    """
    Escape user-provided text for MarkdownV2 without affecting full message structure.
    """
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

async def safe_send_message(bot, chat_id: int, text: str, markdown_v2=True):
    from telegram.constants import ParseMode
    await bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=ParseMode.MARKDOWN_V2 if markdown_v2 else None
    )