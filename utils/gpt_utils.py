from openai import OpenAI
import os

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

def interpret_message_with_gpt(message: str, config) -> str:
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
                    "- If the message contains \"docena\" take it as 12 units, but don't calculate the price for total_sale_price; just leave what the user passed.\n"
                    "- Always output only valid JSON without additional explanations."
                )
            },
            {"role": "user", "content": message}
        ],
        temperature=0.2
    )
    return response.choices[0].message.content
