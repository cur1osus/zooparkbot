from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.types import Message
from bot.filters import GetTextButton
from bot.keyboards import ik_im_take, rk_cancel, rk_main_menu
from bot.states import UserState
from config import CHAT_SUPPORT_ID
from db import MessageToSupport, User
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    get_photo_from_message,
    get_text_message,
    mention_html,
)

router = Router()
flags = {"throttling_key": "default"}


@router.message(UserState.main_menu, Command(commands="support"))
async def support(
    message: Message,
    state: FSMContext,
    command: CommandObject,
    session: AsyncSession,
    user: User | None,
) -> None:
    await message.answer(
        text=await get_text_message("send_mess_to_support"),
        reply_markup=await rk_cancel(),
    )
    await state.set_state(UserState.send_mess_to_support)


@router.message(Command(commands="support"))
async def support_error(
    message: Message,
    state: FSMContext,
    command: CommandObject,
    session: AsyncSession,
    user: User | None,
) -> None:
    await message.answer(text=await get_text_message("support_call_from_main_menu"))


@router.message(UserState.send_mess_to_support, GetTextButton("cancel"), flags=flags)
async def cancel_send_mess_to_supp(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await state.set_state(UserState.main_menu)
    await message.answer(
        text=await get_text_message("canceled"),
        reply_markup=await rk_main_menu(user_id=user.id_user),
    )


@router.message(UserState.send_mess_to_support)
async def get_sended_message(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    photo_id = await get_photo_from_message(message)
    if photo_id and not message.caption:
        await message.answer(
            text=await get_text_message("error_message_without_caption"),
        )
        return
    message_to_support = MessageToSupport(
        idpk_user=user.idpk,
        question=message.caption if photo_id else message.text,
        id_message_question=message.message_id,
        photo_id=photo_id,
    )
    session.add(message_to_support)
    await session.flush()
    await state.set_state(UserState.main_menu)
    await message.answer(
        text=await get_text_message("wait_answer"),
        reply_markup=await rk_main_menu(user_id=user.id_user),
    )
    func = {
        False: message.bot.send_message,
        True: message.bot.send_photo,
    }
    text = await get_text_message(
        "new_mess_to_support",
        user=mention_html(id_user=user.id_user, name=user.nickname),
        text=message_to_support.question,
        idpk_user=user.idpk,
    )
    mess_data = {"photo": photo_id, "caption": text} if photo_id else {"text": text}
    msg: Message = await func[bool(photo_id)](
        chat_id=CHAT_SUPPORT_ID,
        **mess_data,
        reply_markup=await ik_im_take(idpk_message_to_support=message_to_support.idpk),
    )
    message_to_support.id_message = msg.message_id
    await session.commit()
