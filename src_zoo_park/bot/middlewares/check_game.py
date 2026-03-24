from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message
from bot.keyboards import rk_main_menu
from bot.states import UserState
from db import Game
from sqlalchemy.ext.asyncio import AsyncSession
from tools import get_text_message


class CheckGame(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: dict[str, Any],
    ) -> Any:
        session: AsyncSession = data["session"]
        state = data.get("state")
        d = await state.get_data()
        idpk_game = d.get("idpk_game")
        if idpk_game is None:
            return await handler(event, data)

        game = await session.get(Game, idpk_game)
        if game.end and data.get("raw_state") == "UserState:game":
            await state.clear()
            await state.set_state(UserState.main_menu)
            text = await get_text_message("main_menu")
            user = data.get("user")
            reply_markup = await rk_main_menu(
                user_id=user.id_user if user else event.from_user.id
            )

            if isinstance(event, Message):
                return await event.answer(text=text, reply_markup=reply_markup)
            if isinstance(event, CallbackQuery):
                if event.message:
                    await event.message.delete()
                    return await event.message.answer(text=text, reply_markup=reply_markup)
                return await event.answer()

        return await handler(event, data)
