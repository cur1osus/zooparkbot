import contextlib

import bot.keyboards as kb
from aiogram.types import CallbackQuery, Message
from config import CHAT_ID
from db import User
from db.structured_state import get_user_aviaries_map
from sqlalchemy.ext.asyncio import AsyncSession

import tools


async def disable_not_main_window(data: dict, message: Message):
    if data.get("active_window"):
        with contextlib.suppress(Exception):
            await message.bot.edit_message_reply_markup(
                chat_id=message.chat.id,
                message_id=data["active_window"],
                reply_markup=None,
            )


async def m_choice_quantity_avi(
    session: AsyncSession, aviary: str, state, query: CallbackQuery, user: User
):
    aviaries_state = await get_user_aviaries_map(session=session, user=user)
    aviary_price = await tools.get_price_aviaries(
        session=session,
        code_name_aviary=aviary,
        aviaries=aviaries_state,
        info_about_items=user.info_about_items,
    )
    await state.update_data(code_name_aviary=aviary, aviary_price=aviary_price)
    await query.message.edit_text(
        text=await tools.get_text_message(
            "choice_quantity_aviaries", price_one_aviary=aviary_price, usd=user.usd
        ),
        reply_markup=await kb.ik_choice_quantity_aviary_avi(
            session=session, aviary_price=aviary_price
        ),
    )


def get_id_for_edit_message(id_message: str) -> dict:
    if id_message.isdigit():
        return {"chat_id": CHAT_ID, "message_id": id_message}
    return {"inline_message_id": id_message}
