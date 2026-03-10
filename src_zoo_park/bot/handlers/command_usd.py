from aiogram import Router
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import any_state
from aiogram.types import Message
from config import ADMIN_ID
from db import User
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    mention_html,
)

router = Router()
flags = {"throttling_key": "default"}


@router.message(Command(commands="usd"), StateFilter(any_state))
async def command_reset(
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
        await message.answer("Не указана сумма и username")
        return
    args = command.args.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Не указана сумма или username")
        return
    try:
        amount = int(args[0])
    except ValueError:
        await message.answer("Сумма должна быть числом")
        return

    username = args[1].removeprefix("@")
    user_to_add = await session.scalar(select(User).where(User.username == username))
    if not user_to_add:
        await message.answer("Пользователь не найден")
        return
    mention = mention_html(id_user=user_to_add.id_user, name=user_to_add.nickname)
    text = f"Добавлено {amount} USD игроку {user_to_add.nickname}"
    if amount < 0:
        text = f"Убавлено {amount} USD у игрока {mention}"
    user_to_add.usd += amount
    await session.commit()
    await message.answer(text)
    mention = mention_html(id_user=user.id_user, name=user.nickname)
    await message.bot.send_message(
        chat_id=user_to_add.id_user,
        text=f"{mention} изменил ваш баланс на {amount} USD",
    )
