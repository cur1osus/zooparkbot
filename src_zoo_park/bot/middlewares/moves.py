from datetime import datetime
from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram.types import Message
from config import CHAT_ID
from db import User
from db.structured_state import append_user_message_history
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
            await append_user_message_history(
                session=session,
                user=user,
                message_text=event.text,
                limit=int(LIMIT_ON_WRITE_MOVES),
            )
            user.moves += 1
            await session.commit()
        return await handler(event, data)
