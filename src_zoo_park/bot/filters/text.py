from aiogram.filters import Filter
from aiogram.types import InlineQuery, Message
from tools import get_text_button, find_integers


class GetTextButton(Filter):
    def __init__(self, name: str) -> None:
        self.name = name

    async def __call__(self, message: Message) -> bool:
        return message.text == await get_text_button(self.name)


class FindIntegers(Filter):
    async def __call__(self, inline_query: InlineQuery) -> bool:
        try:
            return bool(await find_integers(inline_query.query.split()[0]))
        except Exception:
            return False
