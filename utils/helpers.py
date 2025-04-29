import re


def escape_user_text(text: str) -> str:
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)


async def safe_send_message(bot, chat_id: int, text: str, escape_user_input=False):
    if escape_user_input:
        text = escape_user_text(text)
    await bot.send_message(
        chat_id=chat_id,
        text=text
    )
