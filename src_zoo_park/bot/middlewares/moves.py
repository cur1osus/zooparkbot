import json
from datetime import datetime
from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram.types import Message
from config import CHAT_ID
from db import User
from sqlalchemy.ext.asyncio import AsyncSession
from tools import get_value


class RegMove(BaseMiddleware):

    async def __call__(
        self,
        handler: Callable[[Message, dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: dict[str, Any],
    ) -> Any:
        user: User = data["user"]
        if user and event.chat.id != CHAT_ID:
            session: AsyncSession = data["session"]
            LIMIT_ON_WRITE_MOVES = await get_value(
                session=session, value_name="LIMIT_ON_WRITE_MOVES"
            )
            decoded_dict: dict = json.loads(user.history_moves)
            key = datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")
            decoded_dict[key] = event.text
            if len(decoded_dict) > LIMIT_ON_WRITE_MOVES:
                # Преобразуем словарь в список кортежей
                items = list(decoded_dict.items())
                # Удаляем первый элемент
                items.pop(0)
                # Преобразуем список кортежей обратно в словарь
                decoded_dict = dict(items)
            encoded = json.dumps(decoded_dict, ensure_ascii=False, separators=(",", ":"))
            max_history_chars = 20_000
            while len(encoded) > max_history_chars and decoded_dict:
                first_key = next(iter(decoded_dict))
                del decoded_dict[first_key]
                encoded = json.dumps(decoded_dict, ensure_ascii=False, separators=(",", ":"))

            user.history_moves = encoded
            user.moves += 1
            await session.commit()
        return await handler(event, data)
