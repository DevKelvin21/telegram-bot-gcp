import functions_framework
import requests
import os
import json
from google.cloud import bigquery

# Set your Telegram and OpenAI keys via env vars
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")

# Allow switching between GPT models (e.g., gpt-3.5-turbo or gpt-4) for cost optimization
GPT_MODEL = os.getenv("GPT_MODEL", "gpt-3.5-turbo")  # Default to cost-efficient model

@functions_framework.http
def telegram_webhook(request):
    try:
        # Parse the Telegram message
        body = request.get_json()
        message = body.get('message', {}).get('text')
        chat_id = body.get('message', {}).get('chat', {}).get('id')

        if not message or not chat_id:
            return "No message found", 400

        # Interpret the message using ChatGPT
        gpt_response = interpret_message_with_gpt(message)

        # Parse the response from GPT to extract structured data
        structured_data = parse_gpt_response(gpt_response)

        # Insert the structured data into BigQuery
        insert_to_bigquery(structured_data)

        # Send confirmation back to the user
        reply_text = f"Got it! Here's what I recorded:\n{json.dumps(structured_data, indent=2)}"
        send_telegram_message(chat_id, reply_text)

        return "OK", 200

    except Exception as e:
        print(f"Error: {str(e)}")
        return f"Internal error: {str(e)}", 500

def interpret_message_with_gpt(message: str) -> str:
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": GPT_MODEL,
        "messages": [
            {"role": "system", "content": "You are an assistant that converts flower shop sales and expenses into structured JSON. Output only JSON."},
            {"role": "user", "content": message}
        ],
        "temperature": 0.2
    }
    resp = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=data)
    resp.raise_for_status()
    return resp.json()['choices'][0]['message']['content']

def parse_gpt_response(gpt_response: str) -> dict:
    return json.loads(gpt_response)

def insert_to_bigquery(row: dict):
    client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    errors = client.insert_rows_json(table_id, [row])
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

def send_telegram_message(chat_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    requests.post(url, json=payload)