import os
import json
import re
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

        # Extract sender's name from the message
        name_match = re.search(r"(?i)nombre[:\-]?\s*(\w+)", message)
        if name_match:
            sender_name = name_match.group(1)
            message_without_name = re.sub(r"(?i)nombre[:\-]?\s*\w+", "", message).strip()
        else:
            # Assume the last word(s) in the message is the sender's name if no prefix is found
            parts = message.rsplit(" ", 1)
            if len(parts) > 1 and not re.search(r"\d", parts[1]):  # Ensure it's not a number
                sender_name = parts[1]
                message_without_name = parts[0].strip()
            else:
                sender_name = None
                message_without_name = message

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
                        "      \"unit_price\": float or null,\n"
                        "      \"quality\": \"string\" (either \"regular\" or \"special\", default: \"regular\")\n"
                        "    }\n"
                        "  ],\n"
                        "  \"expenses\": [\n"
                        "    {\n"
                        "      \"description\": \"string\",\n"
                        "      \"amount\": float\n"
                        "    }\n"
                        "  ],\n"
                        "  \"sender_name\": string or null // Extracted sender's name from the message, null if not found\n"
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
                {"role": "user", "content": message_without_name}
            ],
            temperature=0.2
        )

        # Add sender's name to the response JSON
        response_json = json.loads(response.choices[0].message.content)
        response_json["sender_name"] = sender_name
        return json.dumps(response_json)

    def generate_summary_in_spanish(self, json_output: str, original_message: str) -> str:
        """
        Generates a plain text summary in Spanish based on the JSON output and refines it using ChatGPT.

        Args:
            json_output (str): The JSON string output from interpret_message_with_gpt.
            original_message (str): The original user input message.

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

            summary = ""

            if sales:
                items = []
                for sale in sales:
                    item = sale.get("item", "producto")
                    quantity = sale.get("quantity")
                    is_quantity_missing = quantity is None
                    if is_quantity_missing:
                        quantity = 1  # Default to 1 if missing
                    unit_price = sale.get("unit_price", 0)

                    # Handle ambiguous or missing details
                    if not item or item.strip() == "":
                        item = f"{item} (¿producto?)"
                    if is_quantity_missing:
                        quantity_placeholder = "¿cantidad?"
                    else:
                        quantity_placeholder = None
                    if not unit_price:
                        unit_price = f"{unit_price} (¿precio unitario?)"

                    # Infer "docena" as 12 units
                    if "docena" in item.lower() and is_quantity_missing:
                        quantity = 12

                    items.append(f"{quantity} {item}")
                items_text = ", ".join(items)
                summary = f"Esto es lo que entendí: vendiste {items_text} por un total de ${total_sale_price} en {payment_method}."

            elif expenses:
                items = []
                for expense in expenses:
                    description = expense.get("description", "gasto")
                    amount = expense.get("amount", 0)

                    # Handle ambiguous or missing details
                    if not description or description.strip() == "":
                        description = f"{description} (¿descripción?)"
                    if not amount:
                        amount = f"{amount} (¿monto?)"

                    items.append(f"{description} por ${amount}")
                items_text = ", ".join(items)
                summary = f"Esto es lo que entendí: registraste los siguientes gastos: {items_text}."

            else:
                summary = "No se encontró ninguna venta ni gasto en el mensaje. ¿Podrías dar más contexto?"

            # Refine the summary using ChatGPT
            refined_summary = self._refine_summary_with_gpt(original_message, summary)
            return refined_summary

        except json.JSONDecodeError:
            return "Hubo un error al procesar el mensaje. Por favor, inténtalo de nuevo."

    def _refine_summary_with_gpt(self, original_message: str, draft_summary: str) -> str:
        """
        Refines the draft summary using ChatGPT.

        Args:
            original_message (str): The original user input message.
            draft_summary (str): The draft summary generated from the JSON output.

        Returns:
            str: A refined summary in Spanish.
        """
        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "Eres un asistente que ayuda a refinar resúmenes en español basados en mensajes de texto "
                            "de una floristería. El mensaje original puede contener errores gramaticales o falta de contexto. "
                            "Tu tarea es mejorar el resumen manteniendo un tono conciso y claro, y asegurándote de que sea fácil de entender."
                        )
                    },
                    {"role": "user", "content": f"Mensaje original: {original_message}\nResumen inicial: {draft_summary}"}
                ],
                temperature=0.2
            )
            return response.choices[0].message.content
        except Exception as e:
            import logging
            logging.error("Error while refining summary with GPT: %s", str(e))
            return f"{draft_summary}\n\nNota: No se pudo refinar el resumen automáticamente debido a un error inesperado."

    def interpret_bulk_inventory_with_gpt(self, message: str, config) -> str:
        """
        Interprets a bulk inventory message using GPT to extract structured inventory data.

        Args:
            message (str): The bulk inventory message.
            config (dict): The bot configuration.

        Returns:
            str: A JSON string containing the structured inventory data.
        """
        model = config.get("gptModel", "gpt-3.5-turbo")

        response = self.client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an assistant that extracts structured inventory data from messages.\n\n"
                        "Each message may include one or more inventory entries in free-text form.\n\n"
                        "Output a JSON object in the following structure:\n\n"
                        "{\n"
                        "  \"inventory\": [\n"
                        "    {\n"
                        "      \"item\": \"string\",\n"
                        "      \"quantity\": int,\n"
                        "      \"quality\": \"string\" (default to \"regular\" if not provided)\n"
                        "    }\n"
                        "  ]\n"
                        "}\n\n"
                        "Rules:\n"
                        "- If the message contains multiple lines, treat each line as a separate inventory entry.\n"
                        "- Extract the item name, quantity, and optional quality from each line.\n"
                        "- If quality is not mentioned, default it to \"regular\".\n"
                        "- Always output only valid JSON without additional explanations."
                    )
                },
                {"role": "user", "content": message}
            ],
            temperature=0.2
        )
        return response.choices[0].message.content
