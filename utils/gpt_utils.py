import os
import json
from openai import OpenAI

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


class GPTMessageInterpreter:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(GPTMessageInterpreter, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.client = OpenAI(api_key=OPENAI_API_KEY)

    def interpret_message_with_gpt(self, message: str, config) -> str:
        model = config.get("gptModel", "gpt-3.5-turbo")
        response = self.client.chat.completions.create(
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

    def generate_summary_in_spanish(self, json_output: str) -> str:
        """
        Generates a plain text summary in Spanish based on the JSON output.

        Args:
            json_output (str): The JSON string output from interpret_message_with_gpt.

        Returns:
            str: A plain text summary in Spanish.
        """
        try:
            data = json.loads(json_output)
            total_sale_price = data.get("total_sale_price")
            payment_method = data.get("payment_method", "efectivo")  # Default to "efectivo"
            sales = data.get("sales", [])
            expenses = data.get("expenses", [])

            # Translate payment methods to Spanish
            payment_method_translation = {
                "cash": "efectivo",
                "bank_transfer": "transferencia bancaria",
                None: "efectivo"  # Default to "efectivo" if not provided
            }
            payment_method = payment_method_translation.get(payment_method, "efectivo")

            if sales:
                items = []
                for sale in sales:
                    item = sale.get("item", "producto")
                    quantity = sale.get("quantity", 1)
                    unit_price = sale.get("unit_price", 0)

                    # Handle ambiguous or missing details
                    if not item or item.strip() == "":
                        item = "¿producto?"
                    if not quantity:
                        quantity = "¿cantidad?"
                    if not unit_price:
                        unit_price = "¿precio unitario?"

                    # Infer "docena" as 12 units
                    if "docena" in item.lower() and quantity == "¿cantidad?":
                        quantity = 12

                    items.append(f"{quantity} {item}")
                items_text = ", ".join(items)
                return f"Esto es lo que entendí: vendiste {items_text} por un total de ${total_sale_price} en {payment_method}."

            if expenses:
                items = []
                for expense in expenses:
                    description = expense.get("description", "gasto")
                    amount = expense.get("amount", 0)

                    # Handle ambiguous or missing details
                    if not description or description.strip() == "":
                        description = "¿descripción?"
                    if not amount:
                        amount = "¿monto?"

                    items.append(f"{description} por ${amount}")
                items_text = ", ".join(items)
                return f"Esto es lo que entendí: registraste los siguientes gastos: {items_text}."

            return "No se encontró ninguna venta ni gasto en el mensaje. ¿Podrías dar más contexto?"

        except json.JSONDecodeError:
            return "Hubo un error al procesar el mensaje. Por favor, inténtalo de nuevo."
