import contextlib
import base64
from datetime import datetime

from aiogram import F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import any_state
from aiogram.types import CallbackQuery, Message
from bot.callbacks import InlineRateUpdateCallback
from bot.keyboards import ik_update_inline_rate
from config import ADMIN_ID, CHAT_ID
from db import Photo, User
from jobs import create_game_for_chat
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    contains_any_pattern,
    fetch_and_parse_str_value,
    formatter,
    get_text_message,
    get_value,
)

router = Router()
flags = {"throttling_key": "default"}


@router.message(
    Command(commands="reset"),
    StateFilter(any_state),
    flags=flags,
)
async def reset(
    message: Message,
    state: FSMContext,
):
    await state.clear()
    await message.answer(text=await get_text_message("reset_done"))


@router.message(
    Command(commands="cg"),
    StateFilter(any_state),
    flags=flags,
)
async def create_game_(
    message: Message,
    state: FSMContext,
):
    if message.from_user.id != ADMIN_ID:
        return
    await create_game_for_chat()


@router.message(F.content_type == "photo", F.chat.id != CHAT_ID)
async def photo_catch(message: Message, session: AsyncSession) -> None:
    photo_id = message.photo[-1].file_id
    session.add(Photo(name=f"new_photo_{message.message_id}", photo_id=photo_id))
    await session.commit()
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer(text=await get_text_message("photo_saved"))


@router.message(F.chat.id == CHAT_ID)
async def any_unknown_message_test(
    message: Message, state: FSMContext, session: AsyncSession
) -> None:
    patterns = await fetch_and_parse_str_value(
        session=session, value_name="pattern_ban_word", func_to_element=str
    )
    if contains_any_pattern(text=message.text, patterns=patterns):
        await message.delete()


@router.message()
async def any_unknown_message(message: Message, state: FSMContext) -> None:
    await message.answer(text=await get_text_message("answer_on_unknown_message"))
    # print(message.effect_id)


@router.callback_query(InlineRateUpdateCallback.filter())
async def update_inline_rate(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: InlineRateUpdateCallback,
) -> None:
    rate = await get_value(session=session, value_name="RATE_RUB_USD", cache_=False)
    time_to_update_bank = 60 - datetime.now().second
    bank_storage = await get_value(
        session=session, value_name="BANK_STORAGE", cache_=False, value_type="str"
    )
    bank_storage = formatter.format_large_number(float(bank_storage))
    inline_message_id = base64.urlsafe_b64decode(callback_data.token).decode("utf-8")
    with contextlib.suppress(Exception):
        await query.bot.edit_message_text(
            inline_message_id=inline_message_id,
            text=await get_text_message(
                "inline_rate",
                r=rate,
                t=time_to_update_bank,
                s=bank_storage,
            ),
            reply_markup=await ik_update_inline_rate(inline_message_id),
        )


@router.callback_query()
async def any_unknown_callback(query: CallbackQuery) -> None:
    await query.message.edit_reply_markup(reply_markup=None)
