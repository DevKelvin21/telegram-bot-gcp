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

        # Improved sender name detection: check for known names (case-insensitive) as last word
        KNOWN_SENDERS = {"josue", "mila", "maria", "michel"}
        words = message.strip().split()
        sender_name = None
        message_without_name = message
        if words and words[-1].lower() in KNOWN_SENDERS:
            sender_name = words[-1]
            message_without_name = " ".join(words[:-1]).strip()

        # Fallback: Extract sender's name from the message (if not already found)
        if sender_name is None:
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
                        "You are an assistant that extracts structured sales and expenses data from flower shop messages written in informal Spanish.\n\n"
                        "Each message may include free-text descriptions of sales (e.g. sold items) or expenses (e.g. purchases or operational costs). Messages may contain spelling mistakes or lack formal structure.\n\n"
                        "Output a valid JSON object with this structure:\n\n"
                        "{\n"
                        "  \"total_sale_price\": float or null, // Sum of all sales; null if only expenses\n"
                        "  \"payment_method\": \"cash\" | \"bank_transfer\" | null, // Payment method, default to \"cash\" for sales; null if only expenses\n"
                        "  \"sales\": [\n"
                        "    {\n"
                        "      \"item\": \"string\",\n"
                        "      \"quantity\": int or null,\n"
                        "      \"unit_price\": float or null,\n"
                        "      \"quality\": \"regular\" | \"special\" // default to \"regular\" if not specified\n"
                        "    }\n"
                        "  ],\n"
                        "  \"expenses\": [\n"
                        "    {\n"
                        "      \"description\": \"string\",\n"
                        "      \"amount\": float\n"
                        "    }\n"
                        "  ],\n"
                        "  \"sender_name\": string or null\n"
                        "}\n\n"
                        "Interpretation rules:\n"
                        "- If the message refers to selling products (e.g., \"ramo\", \"rosa\", \"bon\", \"oasis\", \"listón\") or is not clearly defined, classify as a sale.\n"
                        "- If the message refers to purchases or costs (e.g., \"compramos\", \"gastamos\"), classify as expenses.\n"
                        "- Always extract as many individual sale items as possible.\n"
                        "- Always extract items as singular (e.g., \"rosas\" -> \"rosa\").\n"
                        "- If the message includes \"x \" before the price then treat it as a total sale price. unless \"cada uno\" or \"cada una\" is present \n"
                        "- Treat \"cada uno\" or \"cada una\" as unit price references.\n"
                        "- Handle combined items (e.g., \"ramo 12 rosas y 12 chocolates bon $19\") as a single line item.\n"
                        "- If quantity is not clear but price per unit is mentioned, infer quantity if possible.\n"
                        "- If unit price is ambiguous, leave it null.\n"
                        "- Accept and normalize product names beyond just flowers, including party gifts or accessories (e.g. chocolates, listón, oasis, varitas, claveles, helechos).\n"
                        "- If quality is not mentioned, default to \"regular\".\n"
                        "- If quality indicators such as \"de ecuador\", \"especial\", or \"premium\" appear, assign quality as \"special\".\n"
                        "- If indicators like \"de guatemala\", \"chapina\", or nothing are mentioned, assign \"regular\".\n"
                        "- Always return only valid JSON — no explanation, no commentary."
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
                        "You are a structured data extraction assistant specialized in processing inventory messages in Spanish.\n\n"
                        "Each input message may include one or more inventory entries written in free-text, often using informal expressions such as \"docena\", \"media docena\", etc.\n\n"
                        "Your goal is to extract a structured list of items with name, quantity (as integer units), and quality. Always respond with a valid JSON object using this structure:\n\n"
                        "{\n"
                        "  \"inventory\": [\n"
                        "    {\n"
                        "      \"item\": \"string\",\n"
                        "      \"quantity\": int,\n"
                        "      \"quality\": \"string\" // either \"regular\" or \"special\"\n"
                        "    }\n"
                        "  ]\n"
                        "}\n\n"
                        "Parsing rules:\n"
                        "1. If the message includes multiple lines or multiple items in one line (comma-separated), treat each as a separate inventory entry.\n"
                        "2. Interpret expressions such as:\n"
                        "   - \"1 docena\" or \"1 doc\" = 12 units\n"
                        "   - \"media docena\" = 6 units\n"
                        "   - \"3 doc\" = 36 units\n"
                        "   - Support both numeric and textual variants: \"una docena\", \"tres docenas\", \"12 unidades\", \"6 flores\"\n"
                        "   - Always convert quantities to individual units\n"
                        "3. Normalize and extract:\n"
                        "   - \"item\": name of the flower (e.g., \"rosas\", \"girasoles\")\n"
                        "   - \"quantity\": integer quantity, converting docenas or fractions to unit count\n"
                        "   - \"quality\": should be \"special\" only if terms like \"de ecuador\", \"especial\", or \"premium\" are mentioned.\n"
                        "     Otherwise, if terms like \"de guatemala\", \"chapina\", or nothing is mentioned, use \"regular\".\n"
                        "4. if the message includes some weird text that does not match as a known flower name take the item anyways as an inventory entry.\n"
                        "5. Always respond with clean JSON only — no comments, no markdown, no explanations.\n"
                        "6. when extracting items always take it as singular:\n"
                        "7. Recognize and include uncommon or informal flower names as valid inventory entries. Examples include: \"Astromelia\", \"Dragón\", \"Monte casino\", \"Magnus\", \"Gradiola\", \"Ginger\", \"Ave de paraíso\", \"Amor seco\", \"estandar\". These may appear with inconsistent spelling or casing and should still be recorded as valid items.\n\n"
                        "Example input:\n"
                        "1 doc rosas\n"
                        "1 docena de girasoles\n"
                        "15 docenas de rosas especiales\n"
                        "media docena de gerberas de ecuador\n\n"
                        "Expected output:\n"
                        "{\n"
                        "  \"inventory\": [\n"
                        "    { \"item\": \"rosa\", \"quantity\": 12, \"quality\": \"regular\" },\n"
                        "    { \"item\": \"girasol\", \"quantity\": 12, \"quality\": \"regular\" },\n"
                        "    { \"item\": \"rosa\", \"quantity\": 180, \"quality\": \"special\" },\n"
                        "    { \"item\": \"gerbera\", \"quantity\": 6, \"quality\": \"special\" }\n"
                        "  ]\n"
                        "}"
                    )
                },
                {"role": "user", "content": message}
            ],
            temperature=0.2
        )
        return response.choices[0].message.content
