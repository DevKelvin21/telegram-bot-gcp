import json
from datetime import datetime
from utils.helpers import safe_send_message
from utils.gpt_utils import GPTMessageInterpreter
from utils.firestore_utils import FirestoreInventoryManager
from concurrent.futures import ThreadPoolExecutor
import asyncio


class BotService:
    def __init__(self, bot, allowed_users, config, owner_id, bigquery_utils):
        """
        Initializes the BotService with the bot instance, allowed users, configuration,
        owner ID, and BigQuery utilities.

        Args:
            bot: The Telegram bot instance.
            allowed_users (set): A set of allowed Telegram user IDs.
            config (dict): The bot configuration.
            owner_id (int): The Telegram user ID of the owner.
            bigquery_utils (BigQueryUtils): An instance of BigQueryUtils for database operations.
        """
        self.bot = bot
        self.allowed_users = allowed_users
        self.owner_id = owner_id
        self.config = config
        self.bigquery_utils = bigquery_utils
        self.timezone = bigquery_utils.timezone
        self.developer_id = self.config.get("developerID", None)
        self.gpt_interpreter = GPTMessageInterpreter()
        self.inventory_manager = FirestoreInventoryManager()
        self.executor = ThreadPoolExecutor()

    async def handle_start(self, update, context):
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Hola, soy tu bot de ventas y gastos para la floristería Morale's 🌸"
        )

    async def handle_message(self, update, context):
        message = update.message.text
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id

        if user_id not in self.allowed_users:
            await self._handle_unauthorized_access(update, context, message, chat_id, user_id)
            return

        command = message.strip().lower()
        if command.startswith("eliminar"):
            await self._handle_delete(update, context, message, chat_id, user_id)
        elif command.startswith("editar"):
            await self._handle_edit(update, context, message, chat_id, user_id)
        elif command.startswith("cierre"):
            await self._handle_closure_report(update, context, message, chat_id, user_id)
        elif command.startswith("inventario:"):
            await self._handle_inventory_update(update, context, message, chat_id, user_id)
        elif command.startswith("perdida:"):
            await self._handle_inventory_loss(update, context, message, chat_id, user_id)
        else:
            await self._handle_data_insert(update, context, message, chat_id, user_id)

    async def _handle_unauthorized_access(self, update, context, message, chat_id, user_id):
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"Tu ID de usuario de Telegram es: {user_id}\nCompártelo con el administrador para que te dé acceso."
        )
        self.bigquery_utils.log_to_bigquery({
            "timestamp": datetime.now(self.timezone).isoformat(),
            "user_id": user_id,
            "chat_id": chat_id,
            "operation_type": "unauthorized_access",
            "message_content": message,
            "user_name": update.effective_user.full_name,
            "transaction_id": None
        })

    async def _handle_delete(self, update, context, message, chat_id, user_id):
        parts = message.split()
        if len(parts) != 3:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Formato incorrecto. Usa: eliminar <transaction_id> <nombre del usuario>"
            )
            return
        transaction_id = parts[1]
        user_name = parts[2] if len(parts) > 2 else update.effective_user.full_name
        try:
            # Use asyncio.get_running_loop().run_in_executor instead of context.application.run_in_executor
            loop = asyncio.get_running_loop()
            transaction = await loop.run_in_executor(
                self.executor, self.bigquery_utils.get_transaction_by_id, transaction_id
            )
            
            if not transaction:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Transacción no encontrada."
                )
                return

            if transaction.get("sales"):
                for sale in transaction["sales"]:
                    item = sale.get("item")
                    quality = sale.get("quality", "regular")
                    quantity = sale.get("quantity", 0)
                    self.inventory_manager.restore_inventory(item, quality, quantity)

            self.bigquery_utils.safe_delete(transaction_id)
            await safe_send_message(
                context.bot,
                chat_id,
                f"✅ ID de Transacción eliminada correctamente."
            )
            await safe_send_message(
                context.bot,
                chat_id,
                f"`{transaction_id}`",
                parse_mode="MarkdownV2"
            )
            self.bigquery_utils.log_to_bigquery({
                "timestamp": datetime.now(self.timezone).isoformat(),
                "user_id": user_id,
                "chat_id": chat_id,
                "operation_type": "delete_transaction",
                "message_content": message,
                "user_name": user_name,
                "transaction_id": transaction_id
            })
            try:
                if self.config.get("liveNotifications"):
                    await safe_send_message(
                        context.bot,
                        self.owner_id,
                        f"🔔 Notificación de administración:\n\n"
                        f"Operación realizada por {user_name} (ID: {user_id}).\n"
                        f"Acción: Eliminar\n"
                        f"ID de Transacción: {transaction_id}"
                    )
            except Exception as notify_error:
                print(f"Error notificando al Owner: {notify_error}")
        except Exception as e:
            await self._notify_error(
                context.bot,
                chat_id,
                self.developer_id,
                update.effective_user.full_name,
                user_id,
                "eliminar",
                str(e)
            )
        return

    async def _handle_edit(self, update, context, message, chat_id, user_id):
        parts = message.split(maxsplit=2)
        if len(parts) != 3:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Formato incorrecto. Usa: editar <transaction_id> <nuevo mensaje>"
            )
            return
        transaction_id, new_text = parts[1], parts[2]
        try:
            gpt_response = self.gpt_interpreter.interpret_message_with_gpt(new_text, self.config)
            new_data = json.loads(gpt_response)
            self.bigquery_utils.safe_edit(transaction_id, new_data)
            user_name = new_data.get("sender_name", update.effective_user.full_name)
            await safe_send_message(
                context.bot,
                chat_id,
                f"✅ ID de Transacción actualizada correctamente."
            )
            await safe_send_message(
                context.bot,
                chat_id,
                f"`{new_data['transaction_id']}`",
                parse_mode="MarkdownV2"
            )
            self.bigquery_utils.log_to_bigquery({
                "timestamp": datetime.now(self.timezone).isoformat(),
                "user_id": user_id,
                "chat_id": chat_id,
                "operation_type": "edit_transaction",
                "message_content": message,
                "user_name": user_name,
                "transaction_id": new_data['transaction_id']
            })
            try:
                if self.config.get("liveNotifications"):
                    await safe_send_message(
                        context.bot,
                        self.owner_id,
                        f"🔔 Notificación de administración:\n\n"
                        f"Operación realizada por {user_name} (ID: {user_id})\n"
                        f"Acción: Editar\n"
                        f"ID de Transacción: {new_data['transaction_id']}"
                    )
            except Exception as notify_error:
                print(f"Error notificando al Owner: {notify_error}")
        except Exception as e:
            await self._notify_error(
                context.bot,
                chat_id,
                self.developer_id,
                update.effective_user.full_name,
                user_id,
                "editar",
                str(e)
            )
        return

    async def _handle_closure_report(self, update, context, message, chat_id, user_id):
        parts = message.split()
        if len(parts) != 2:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Formato incorrecto. Usa: cierre <nombre del usuario>"
            )
            return
        user_name = parts[1] if len(parts) > 1 else update.effective_user.full_name  # Ensure user_name is initialized
        try:
            today = datetime.now(self.timezone).strftime("%Y-%m-%d")
            report = self.bigquery_utils.get_closure_report_by_date(today)
            if not report:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="No hay datos para el cierre de hoy."
                )
                return

            efectivo_sales = report.efectivo_sales if report.efectivo_sales is not None else 0
            transfer_sales = report.transfer_sales if report.transfer_sales is not None else 0
            total_expenses = report.total_expenses if report.total_expenses is not None else 0
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🔔 Resumen del cierre de caja:\n\n"
                    f"🏦 Ventas por transferencia bancaria: ${transfer_sales}\n"
                    f"💵 Ventas en efectivo: ${efectivo_sales}\n"
                    f"💰 Gastos del día: ${total_expenses}\n"
                    f"💵 Total efectivo en caja: ${efectivo_sales - total_expenses}\n\n"
            )

            self.bigquery_utils.log_to_bigquery({
                "timestamp": datetime.now(self.timezone).isoformat(),
                "user_id": user_id,
                "chat_id": chat_id,
                "operation_type": "closure_report",
                "message_content": f"Cierre de caja para {today}",
                "user_name": user_name,
                "transaction_id": None
            })
            try:
                if self.config.get("liveNotifications"):
                    await safe_send_message(
                        context.bot,
                        self.owner_id,
                        f"🔔 Notificación de administración:\n\n"
                        f"Operación realizada por {user_name} (ID: {user_id})\n"
                        f"Acción: Cierre de caja\n"
                        f"Fecha: {today}\n\n"
                        f"🏦 Ventas por transferencia bancaria: ${transfer_sales}\n"
                        f"💵 Ventas en efectivo: ${efectivo_sales}\n"
                        f"💰 Gastos del día: ${total_expenses}\n"
                        f"💵 Total efectivo en caja: ${efectivo_sales - total_expenses}\n\n"
                    )
            except Exception as notify_error:
                print(f"Error notificando al Owner: {notify_error}")
        except Exception as e:
            await self._notify_error(
                context.bot,
                chat_id,
                self.developer_id,
                update.effective_user.full_name,
                user_id,
                "cierre",
                str(e)
            )
        return

    async def _handle_data_insert(self, update, context, message, chat_id, user_id):
        try:
            gpt_response = self.gpt_interpreter.interpret_message_with_gpt(message, self.config)
            structured_data = json.loads(gpt_response)
            if not structured_data.get("sales") and not structured_data.get("expenses"):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="No se encontró ninguna venta ni gasto en el mensaje."
                )
                return

            # Set the date and insert data into BigQuery before inventory deductions
            # to ensure an audit trail is preserved even if inventory operations fail.
            structured_data.setdefault("date", datetime.now(self.timezone).strftime("%Y-%m-%d"))
            try:
                self.bigquery_utils.insert_to_bigquery(structured_data)
            except Exception as insert_error:
                await self._notify_error(
                    context.bot,
                    chat_id,
                    self.developer_id,
                    update.effective_user.full_name,
                    user_id,
                    "insertar",
                    f"Error al insertar en BigQuery: {insert_error}"
                )
                return

            if structured_data.get("sales"):
                inventory_issues = self.inventory_manager.deduct_inventory(
                    structured_data["sales"], structured_data["transaction_id"]
                )
                if inventory_issues:
                    await context.bot.send_message(
                        chat_id=self.owner_id,
                        text="⚠️ Problemas con el inventario:\n" +
                             "\n".join([f"- {issue['item']} ({issue['quality']}): {issue['reason']}" for issue in inventory_issues])
                    )
  
            user_name = structured_data.get("sender_name", update.effective_user.full_name)
            self.bigquery_utils.log_to_bigquery({
                "timestamp": datetime.now(self.timezone).isoformat(),
                "user_id": user_id,
                "chat_id": chat_id,
                "operation_type": "data_insert",
                "message_content": message,
                "user_name": user_name,
                "transaction_id": structured_data["transaction_id"]
            })

            summary = self.gpt_interpreter.generate_summary_in_spanish(gpt_response, original_message=message)
            await safe_send_message(
                context.bot,
                chat_id,
                f"{summary}\n\n✅ ID de Transacción guardada correctamente."
            )
            await safe_send_message(
                context.bot,
                chat_id,
                f"`{structured_data['transaction_id']}`",
                parse_mode="MarkdownV2"
            )
            try:
                if self.config.get("liveNotifications"):
                    await safe_send_message(
                        context.bot,
                        self.owner_id,
                        f"🔔 Nueva operación registrada por {user_name} (ID: {user_id}):\n\n{message}\n\n"
                        f"ID de Transacción: {structured_data['transaction_id']}"
                    )
            except Exception as notify_error:
                print(f"Error notificando al Owner: {notify_error}")
        except Exception as e:
            await self._notify_error(
                context.bot,
                chat_id,
                self.developer_id,
                update.effective_user.full_name,
                user_id,
                "insertar",
                str(e)
            )
            return

    async def _handle_inventory_update(self, update, context, message, chat_id, user_id):
        try:
            parts = message.split(":", 1)[1].strip()
            await self._handle_bulk_inventory_update(update, context, parts, chat_id)
            
        except Exception as e:
            await self._notify_error(
                context.bot,
                chat_id,
                self.developer_id,
                update.effective_user.full_name,
                user_id,
                "inventario",
                str(e)
            )
            return
        try:
            if self.config.get("liveNotifications"):
                await safe_send_message(
                    context.bot,
                    self.owner_id,
                    f"🔔 Notificación de administración:\n\n"
                    f"Operación realizada por {update.effective_user.full_name} (ID: {user_id})\n"
                    f"Acción: Actualización de inventario\n"
                    f"Mensaje: {message}"
                )
        except Exception as notify_error:
            print(f"Error notificando al Owner: {notify_error}")
        return

    async def _handle_inventory_loss(self, update, context, message, chat_id, user_id):
        """
        Handles the 'perdida:' command to deduct items from inventory due to loss/disposal.
        """
        try:
            parts = message.split(":", 1)[1].strip()
            # Reuse the GPT bulk inventory parser
            gpt_response = self.gpt_interpreter.interpret_bulk_inventory_with_gpt(parts, self.config)
            inventory_entries = json.loads(gpt_response).get("inventory", [])

            if not inventory_entries:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="No se encontraron entradas válidas para la pérdida en el mensaje."
                )
                return

            issues = []
            timestamp = datetime.now(self.timezone).isoformat()
            user_name = update.effective_user.full_name
            for entry in inventory_entries:
                item = entry.get("item")
                quality = entry.get("quality", "regular")
                quantity = entry.get("quantity", 0)
                # Deduct inventory (like a sale)
                deduct_issues = self.inventory_manager.deduct_inventory(
                    [{"item": item, "quality": quality, "quantity": quantity}], transaction_id="PERDIDA"
                )
                issues.extend(deduct_issues)
                # Log to Firestore
                self.inventory_manager.log_inventory_loss(
                    user_id=user_id,
                    user_name=user_name,
                    chat_id=chat_id,
                    item=item,
                    quality=quality,
                    quantity=quantity,
                    original_message=message,
                    timestamp=timestamp
                )

            self.bigquery_utils.log_to_bigquery({
                "timestamp": timestamp,
                "user_id": user_id,
                "chat_id": chat_id,
                "operation_type": "inventory_loss",
                "message_content": message,
                "user_name": user_name,
                "transaction_id": None
            })

            await context.bot.send_message(
                chat_id=chat_id,
                text=f"✅ Inventario actualizado. Se registró la pérdida de {len(inventory_entries)} entradas."
            )
            if issues:
                await context.bot.send_message(
                    chat_id=self.owner_id,
                    text="⚠️ Problemas al registrar la pérdida:\n" +
                         "\n".join([f"- {issue['item']} ({issue['quality']}): {issue['reason']}" for issue in issues])
                )
            try:
                if self.config.get("liveNotifications"):
                    await safe_send_message(
                        context.bot,
                        self.owner_id,
                        f"🔔 Notificación de administración:\n\n"
                        f"Operación realizada por {user_name} (ID: {user_id})\n"
                        f"Acción: Pérdida de inventario\n"
                        f"Mensaje: {message}"
                    )
            except Exception as notify_error:
                print(f"Error notificando al Owner: {notify_error}")
        except Exception as e:
            await self._notify_error(
                context.bot,
                chat_id,
                self.developer_id,
                update.effective_user.full_name,
                user_id,
                "perdida",
                str(e)
            )
            return

    async def _handle_bulk_inventory_update(self, update, context, message, chat_id):
        try:
            gpt_response = self.gpt_interpreter.interpret_bulk_inventory_with_gpt(message, self.config)
            inventory_entries = json.loads(gpt_response).get("inventory", [])

            if not inventory_entries:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="No se encontraron entradas válidas para el inventario en el mensaje."
                )
                return

            for entry in inventory_entries:
                item = entry.get("item")
                quality = entry.get("quality", "regular")
                quantity = entry.get("quantity", 0)
                self.inventory_manager.update_inventory(item, quality, quantity)

            self.bigquery_utils.log_to_bigquery({
                "timestamp": datetime.now(self.timezone).isoformat(),
                "user_id": update.effective_user.id,
                "chat_id": chat_id,
                "operation_type": "bulk_inventory_update",
                "message_content": message,
                "user_name": update.effective_user.full_name,
                "transaction_id": None
            })

            await context.bot.send_message(
                chat_id=chat_id,
                text=f"✅ Inventario actualizado con {len(inventory_entries)} entradas."
            )
        except Exception as e:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Error al procesar la carga masiva de inventario: {str(e)}"
            )

    async def _notify_error(self, bot, chat_id, developer_id, user_name, user_id, action, error_message):
        """
        Notifies the developer about an error that occurred during a bot operation.

        Args:
            bot: The Telegram bot instance.
            chat_id (int): The chat ID where the error occurred.
            developer_id (int): The Telegram user ID of the developer.
            user_name (str): The name of the user who triggered the action.
            user_id (int): The Telegram user ID of the user who triggered the action.
            action (str): The action that caused the error.
            error_message (str): The error message to be sent to the developer.
        """
        if not developer_id:
            return
        await safe_send_message(
            bot,
            developer_id,
            f"🚨 Error Report:\n\n"
            f"User: {user_name} (ID: {user_id})\n"
            f"Action: {action}\n"
            f"Error: {error_message}"
        )
        await safe_send_message(
            bot,
            chat_id,
            f"❌ Hubo un error al procesar tu solicitud. El desarollador ha sido notificado, Por favor intenta mas tarde."
        )
        return

