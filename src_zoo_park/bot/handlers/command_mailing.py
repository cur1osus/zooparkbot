import asyncio

from aiogram import Router
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import any_state
from aiogram.types import Message
from bot.states import AdminState, UserState
from config import ADMIN_ID
from db import User
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from tools import get_text_message, mention_html

router = Router()
flags = {"throttling_key": "default"}


@router.message(Command(commands="m"), StateFilter(any_state))
async def command_mailing(
    message: Message,
    state: FSMContext,
    command: CommandObject,
    session: AsyncSession,
    user: User | None,
) -> None:
    if not user or user.id_user != ADMIN_ID:
        await message.answer("У вас нет прав")
        return
    await state.set_state(AdminState.get_mess_mailing)
    await message.answer(text=await get_text_message("send_mess_for_mailing"))


@router.message(AdminState.get_mess_mailing)
async def get_mess_mailing(
    message: Message,
    state: FSMContext,
    session: AsyncSession,
    user: User | None,
) -> None:
    if not user or user.id_user != ADMIN_ID:
        await state.set_state(UserState.zoomarket_menu)
        await message.answer("У вас нет прав")
        return

    users = await session.execute(select(User.id_user, User.username))
    users = users.all()
    not_sended = []
    for id_user, username in users:
        try:
            await message.send_copy(chat_id=id_user)
        except Exception:
            not_sended.append(mention_html(id_user, username or str(id_user)))
        await asyncio.sleep(0.1)
    amount_got_message = len(users) - len(not_sended)
    amount_not_got_message = len(not_sended)
    not_sended = ", ".join(not_sended) if not_sended else "Нет"
    await state.set_state(UserState.zoomarket_menu)
    await message.answer(
        text=await get_text_message(
            "mess_mailing_finish",
            amount_got_message=amount_got_message,
            amount_not_got_message=amount_not_got_message,
            not_sended=not_sended,
        )
    )
