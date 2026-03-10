import contextlib

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, ReplyParameters
from bot.callbacks import SupportAction, SupportTakeCallback
from bot.keyboards import (
    ik_confirm_or_cancel,
    ik_im_take,
    ik_link_on_member_support,
)
from bot.states import UserState
from config import CHAT_SUPPORT_ID
from db import MessageToSupport, User
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    get_text_message,
    mention_html,
)
from aiogram.exceptions import TelegramBadRequest

flags = {"throttling_key": "default"}
router = Router()


@router.callback_query(SupportTakeCallback.filter(F.action == SupportAction.take))
async def im_take(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: SupportTakeCallback,
):
    idpk_message_to_support = callback_data.message_idpk
    message_to_support = await session.get(MessageToSupport, idpk_message_to_support)
    await query.bot.copy_message(
        chat_id=user.id_user,
        from_chat_id=CHAT_SUPPORT_ID,
        message_id=message_to_support.id_message,
        reply_markup=await ik_confirm_or_cancel(idpk_message_to_support),
    )
    func = {
        False: query.message.edit_text,
        True: query.message.edit_caption,
    }
    text = await get_text_message(
        "mess_taken",
        user=mention_html(id_user=user.id_user, name=query.from_user.full_name),
    )

    mess_data = {"caption": text} if message_to_support.photo_id else {"text": text}
    await func[bool(message_to_support.photo_id)](
        **mess_data,
        reply_markup=None,
    )


@router.callback_query(SupportTakeCallback.filter(F.action == SupportAction.cancel))
async def cancel_im_take(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: SupportTakeCallback,
):
    idpk_message_to_support = callback_data.message_idpk
    message_to_support = await session.get(MessageToSupport, idpk_message_to_support)
    user_to_answer = await session.get(User, message_to_support.idpk_user)
    func = {
        False: query.bot.send_message,
        True: query.bot.send_photo,
    }
    text = await get_text_message(
        "new_mess_to_support",
        user=mention_html(id_user=user_to_answer.id_user, name=user_to_answer.nickname),
        text=message_to_support.question,
        idpk_user=message_to_support.idpk_user,
    )
    mess_data = (
        {"photo": message_to_support.photo_id, "caption": text}
        if message_to_support.photo_id
        else {"text": text}
    )
    msg: Message = await func[bool(message_to_support.photo_id)](
        chat_id=CHAT_SUPPORT_ID,
        **mess_data,
        reply_markup=await ik_im_take(idpk_message_to_support=message_to_support.idpk),
    )

    await query.answer(
        text=await get_text_message("im_take_canceled"),
        show_alert=True,
    )
    with contextlib.suppress(Exception):
        await query.bot.delete_message(
            chat_id=user.id_user, message_id=query.message.message_id
        )
        await query.bot.delete_message(
            chat_id=CHAT_SUPPORT_ID, message_id=message_to_support.id_message
        )
    message_to_support.id_message = msg.message_id
    await session.commit()


@router.callback_query(SupportTakeCallback.filter(F.action == SupportAction.confirm))
async def cancel_im_take(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: SupportTakeCallback,
):
    idpk_message_to_support = callback_data.message_idpk
    await state.set_state(UserState.answer_on_question)
    await state.update_data(
        idpk_message_to_support=idpk_message_to_support,
        id_message_question=query.message.message_id,
    )
    await query.message.delete_reply_markup()
    await query.message.answer(
        text=await get_text_message("write_answer"),
    )


@router.message(UserState.answer_on_question)
async def get_answer_on_question(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    data = await state.get_data()
    message_to_support = await session.get(
        MessageToSupport, data["idpk_message_to_support"]
    )
    message_to_support.id_message_answer = message.message_id
    await session.commit()
    try:
        await message.bot.delete_message(
            chat_id=CHAT_SUPPORT_ID,
            message_id=message_to_support.id_message,
        )
    except TelegramBadRequest:
        await message.bot.edit_message_text(
            chat_id=CHAT_SUPPORT_ID,
            message_id=message_to_support.id_message,
            text="[...]",
        )
    except Exception as e:
        await message.answer(text=str(e))
    msg = await message.bot.copy_message(
        chat_id=CHAT_SUPPORT_ID,
        from_chat_id=user.id_user,
        message_id=data["id_message_question"],
    )
    message_to_support.id_message = msg.message_id
    await session.commit()
    await message.bot.copy_message(
        chat_id=CHAT_SUPPORT_ID,
        from_chat_id=user.id_user,
        message_id=message_to_support.id_message_answer,
        reply_parameters=ReplyParameters(
            message_id=message_to_support.id_message,
            chat_id=CHAT_SUPPORT_ID,
        ),
        reply_markup=await ik_link_on_member_support(
            link=f"tg://user?id={user.id_user}", name=message.from_user.full_name
        ),
    )
    user_to_answer = await session.get(User, message_to_support.idpk_user)
    await message.bot.copy_message(
        chat_id=user_to_answer.id_user,
        from_chat_id=user.id_user,
        message_id=message_to_support.id_message_answer,
        reply_parameters=ReplyParameters(
            message_id=message_to_support.id_message_question,
            chat_id=user_to_answer.id_user,
        ),
    )
    await state.set_state(UserState.main_menu)
    await message.answer(
        text=await get_text_message(
            "answer_sent",
        )
    )
