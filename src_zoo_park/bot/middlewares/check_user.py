from datetime import datetime
from pprint import pprint
from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message, ReplyKeyboardRemove
from db import BlackList, User
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from tools import get_text_message


class CheckUser(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: dict[str, Any],
    ) -> Any:
        session: AsyncSession = data["session"]
        if await session.scalar(
            select(BlackList).where(BlackList.id_user == event.from_user.id)
        ):
            return
        user = await session.scalar(
            select(User).where(User.id_user == event.from_user.id)
        )
        data["user"] = user
        if isinstance(event, Message) and not user:
            if (event.text and event.text.startswith("/start")) or data.get(
                "raw_state"
            ) == "UserState:start_reg_step":
                return await handler(event, data)
            return await event.answer(
                text=await get_text_message("press_start_to_play"),
                reply_markup=ReplyKeyboardRemove(),
            )
        if isinstance(event, CallbackQuery) and not user:
            return await event.answer(
                text=await get_text_message("press_start_to_play"),
                show_alert=True,
            )
        return await handler(event, data)
