import contextlib
from aiogram import Router
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import any_state
from aiogram.types import Message
from config import ADMIN_ID
from db import User
from sqlalchemy.ext.asyncio import AsyncSession
from tools import get_events_list, sort_events_by_time

router = Router()
flags = {"throttling_key": "default"}


@router.message(Command(commands="h"), StateFilter(any_state))
async def command_history(
    message: Message,
    state: FSMContext,
    command: CommandObject,
    session: AsyncSession,
    user: User | None,
) -> None:
    if not user or user.id_user != ADMIN_ID:
        await message.answer("У вас нет прав")
        return
    if not command.args:
        await message.answer("Не указано время")
        return
    try:
        mins = int(command.args)
    except ValueError:
        await message.answer("Время должно быть числом в минутах")
        return

    if mins <= 0:
        await message.answer("Время должно быть больше нуля")
        return

    events_list = await get_events_list(session, user.id_user)
    sev = sort_events_by_time(events_list=events_list, time=mins)
    if not sev:
        await message.answer(text="Нет событий")
        return
    for text_event in sev:
        with contextlib.suppress(Exception):
            await message.answer(text=text_event)
