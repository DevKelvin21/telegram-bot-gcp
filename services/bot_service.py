import json
from datetime import datetime
from utils.helpers import safe_send_message, escape_user_text
from utils.gpt_utils import interpret_message_with_gpt
import pytz


class BotService:
    def __init__(self, bot, allowed_users, config, owner_id, bigquery_utils):
        self.bot = bot
        self.allowed_users = allowed_users
        self.owner_id = owner_id
        self.config = config
        self.bigquery_utils = bigquery_utils
        self.cst = pytz.timezone("America/El_Salvador")

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
            await self._handle_closure_report(update, context, chat_id, user_id)
        else:
            await self._handle_data_insert(update, context, message, chat_id, user_id)

    async def _handle_unauthorized_access(self, update, context, message, chat_id, user_id):
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"Tu ID de usuario de Telegram es: {user_id}\nCompártelo con el administrador para que te dé acceso."
        )
        self.bigquery_utils.log_to_bigquery({
            "timestamp": datetime.now(self.cst).isoformat(),
            "user_id": user_id,
            "chat_id": chat_id,
            "operation_type": "unauthorized_access",
            "message_content": message,
            "user_name": update.effective_user.full_name
        })

    async def _handle_delete(self, update, context, message, chat_id, user_id):
        parts = message.split()
        if len(parts) != 2:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Formato incorrecto. Usa: eliminar <transaction_id>"
            )
            return
        transaction_id = parts[1]
        try:
            self.bigquery_utils.safe_delete(transaction_id)
            await safe_send_message(
                context.bot,
                chat_id,
                f"✅ ID de Transacción:\n{transaction_id} eliminada correctamente."
            )
            self.bigquery_utils.log_to_bigquery({
                "timestamp": datetime.now(self.cst).isoformat(),
                "user_id": user_id,
                "chat_id": chat_id,
                "operation_type": "delete_transaction",
                "message_content": message,
                "user_name": update.effective_user.full_name
            })
            try:
                if self.config.get("liveNotifications"):
                    await safe_send_message(
                        context.bot,
                        self.owner_id,
                        f"🔔 Notificación de administración:\n\n"
                        f"Operación realizada por {update.effective_user.full_name} (ID: {user_id}).\n"
                        f"Acción: Eliminar\n"
                        f"ID de Transacción: {transaction_id}"
                    )
            except Exception as notify_error:
                print(f"Error notificando al Owner: {notify_error}")
        except Exception as e:
            await safe_send_message(context.bot, chat_id, f"❌ Error al eliminar:\n{str(e)}", escape_user_input=True)
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
            gpt_response = interpret_message_with_gpt(new_text)
            new_data = json.loads(gpt_response)
            new_data.setdefault("date", datetime.now(self.cst).strftime("%Y-%m-%d"))
            new_data["transaction_id"] = transaction_id
            self.bigquery_utils.safe_edit(transaction_id, new_data)

            await safe_send_message(
                context.bot,
                chat_id,
                f"✅ ID de Transacción:\n{transaction_id} actualizada correctamente."
            )
            self.bigquery_utils.log_to_bigquery({
                "timestamp": datetime.now(self.cst).isoformat(),
                "user_id": user_id,
                "chat_id": chat_id,
                "operation_type": "edit_transaction",
                "message_content": message,
                "user_name": update.effective_user.full_name
            })
            try:
                if self.config.get("liveNotifications"):
                    await safe_send_message(
                        context.bot,
                        self.owner_id,
                        f"🔔 Notificación de administración:\n\n"
                        f"Operación realizada por {update.effective_user.full_name} (ID: {user_id})\n"
                        f"Acción: Editar\n"
                        f"ID de Transacción: {transaction_id}"
                    )
            except Exception as notify_error:
                print(f"Error notificando al Owner: {notify_error}")
        except Exception as e:
            await safe_send_message(context.bot, chat_id, f"❌ Error al editar:\n{str(e)}", escape_user_input=True)
        return

    async def _handle_closure_report(self, update, context, chat_id, user_id):
        try:
            today = datetime.now(self.cst).strftime("%Y-%m-%d")
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
                "timestamp": datetime.now(self.cst).isoformat(),
                "user_id": user_id,
                "chat_id": chat_id,
                "operation_type": "closure_report",
                "message_content": f"Cierre de caja para {today}",
                "user_name": update.effective_user.full_name
            })
            try:
                if self.config.get("liveNotifications"):
                    await safe_send_message(
                        context.bot,
                        self.owner_id,
                        f"🔔 Notificación de administración:\n\n"
                        f"Operación realizada por {update.effective_user.full_name} (ID: {user_id})\n"
                        f"Acción: Cierre de caja\n"
                        f"Fecha: {today}"
                    )
            except Exception as notify_error:
                print(f"Error notificando al Owner: {notify_error}")
        except Exception as e:
            await safe_send_message(context.bot, chat_id, f"❌ Error al generar el cierre:\n{str(e)}", escape_user_input=True)
        return

    async def _handle_data_insert(self, update, context, message, chat_id, user_id):
        try:
            gpt_response = interpret_message_with_gpt(message)
            structured_data = json.loads(gpt_response)
            if not structured_data.get("sales") and not structured_data.get("expenses"):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="No se encontró ninguna venta ni gasto en el mensaje."
                )
                return
            structured_data.setdefault("date", datetime.now(self.cst).strftime("%Y-%m-%d"))
            self.bigquery_utils.insert_to_bigquery(structured_data)

            self.bigquery_utils.log_to_bigquery({
                "timestamp": datetime.now(self.cst).isoformat(),
                "user_id": user_id,
                "chat_id": chat_id,
                "operation_type": "data_insert",
                "message_content": message,
                "user_name": update.effective_user.full_name
            })

            await safe_send_message(
                context.bot,
                chat_id,
                f"Registro guardado correctamente\n\n"
                f"{json.dumps(structured_data, indent=2)}\n\n"
                f"ID de Transacción:\n{structured_data['transaction_id']}"
            )

            if self.config.get("liveNotifications"):
                await safe_send_message(
                    context.bot,
                    self.owner_id,
                    f"🔔 Nueva operación registrada por {update.effective_user.full_name} (ID: {user_id}):\n\n{message}\n\n"
                    f"ID de Transacción: {structured_data['transaction_id']}"
                )
        except Exception as e:
            await safe_send_message(context.bot, chat_id, f"❌ Hubo un error al procesar el mensaje:\n{str(e)}", escape_user_input=True)

