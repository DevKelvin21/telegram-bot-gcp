import re
from telegram.constants import ParseMode


def escape_user_text(text: str) -> str:
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)


async def safe_send_message(bot, chat_id: int, text: str, escape_user_input=False, parse_mode=None):
    """
    Sends a message safely with optional MarkdownV2 formatting.

    Args:
        bot: The Telegram bot instance.
        chat_id (int): The chat ID to send the message to.
        text (str): The message text.
        escape_user_input (bool): Whether to escape user input for MarkdownV2.
        parse_mode (str): The parse mode for the message (e.g., "MarkdownV2").
    """
    if escape_user_input:
        text = escape_user_text(text)
    await bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=parse_mode or ParseMode.HTML  # Default to HTML if no parse_mode is provided
    )
