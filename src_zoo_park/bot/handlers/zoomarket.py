from aiogram import Router
from aiogram.fsm.context import FSMContext
from aiogram.types import Message
from bot.filters import GetTextButton
from bot.keyboards import rk_main_menu, rk_zoomarket_menu
from bot.states import UserState
from db import User
from sqlalchemy.ext.asyncio import AsyncSession
from tools import disable_not_main_window, get_text_message

flags = {"throttling_key": "default"}
router = Router()


@router.message(UserState.main_menu, GetTextButton("zoomarket"))
async def zoomarket(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await state.set_state(UserState.zoomarket_menu)
    await message.answer(
        text=await get_text_message("zoomarket_menu"),
        reply_markup=await rk_zoomarket_menu(),
    )


@router.message(UserState.zoomarket_menu, GetTextButton("back"), flags=flags)
async def back(message: Message, state: FSMContext, user: User):
    await disable_not_main_window(data=await state.get_data(), message=message)
    await state.clear()
    await state.set_state(UserState.main_menu)
    await message.answer(
        text=await get_text_message("main_menu"),
        reply_markup=await rk_main_menu(user_id=user.id_user),
    )
