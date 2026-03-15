from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext

from bot.states import UserState
from db import Unity, User
from sqlalchemy.ext.asyncio import AsyncSession
from tools.unity_projects import (
    contribute_to_project,
    format_project_text,
    get_or_create_project,
    get_active_clan_buff,
)

router = Router()
flags = {"throttling_key": "default"}

PROJECT_BUTTON_TEXT = "🏗 Проект клана"


def _kb():
    b = InlineKeyboardBuilder()
    b.button(text="💸 +100k RUB", callback_data="clprj:rub:100000")
    b.button(text="💵 +10k USD", callback_data="clprj:usd:10000")
    b.button(text="Ввести RUB ⌨️", callback_data="clprj:custom:rub")
    b.button(text="Ввести USD ⌨️", callback_data="clprj:custom:usd")
    b.button(text="🔄 Обновить", callback_data="clprj:refresh")
    b.adjust(2, 2, 1)
    return b.as_markup()


@router.message(UserState.unity_menu, F.text == PROJECT_BUTTON_TEXT, flags=flags)
async def open_project(message: Message, session: AsyncSession, user: User):
    if not user.current_unity:
        await message.answer("Ты не состоишь в клане")
        return
    unity_idpk = int(str(user.current_unity).split(":")[-1])
    unity: Unity | None = await session.get(Unity, unity_idpk)
    if not unity:
        await message.answer("Клан не найден")
        return
    project = await get_or_create_project(session=session, unity=unity)
    active_buff = await get_active_clan_buff(session=session, unity_idpk=unity.idpk)
    await session.commit()
    await message.answer(format_project_text(project, active_buff), reply_markup=_kb())


@router.callback_query(StateFilter(UserState.unity_menu), F.data.startswith("clprj:"))
async def on_project_cb(query: CallbackQuery, session: AsyncSession, user: User, state: FSMContext):
    if not user.current_unity:
        await query.answer("Ты не состоишь в клане", show_alert=True)
        return
    unity_idpk = int(str(user.current_unity).split(":")[-1])
    unity: Unity | None = await session.get(Unity, unity_idpk)
    if not unity:
        await query.answer("Клан не найден", show_alert=True)
        return

    parts = (query.data or "").split(":")
    action = parts[1] if len(parts) > 1 else "refresh"

    if action == "custom":
        currency = parts[2] if len(parts) > 2 else "rub"
        if currency == "rub":
            await state.set_state(UserState.unity_project_custom_rub)
            await query.message.answer("Введите сумму в RUB, которую хотите внести:")
        elif currency == "usd":
            await state.set_state(UserState.unity_project_custom_usd)
            await query.message.answer("Введите сумму в USD, которую хотите внести:")
        await query.answer()
        return

    if action == "refresh":
        project = await get_or_create_project(session=session, unity=unity)
        active_buff = await get_active_clan_buff(session=session, unity_idpk=unity.idpk)
        await session.commit()
        await query.message.edit_text(format_project_text(project, active_buff), reply_markup=_kb())
        await query.answer("Обновлено")
        return

    currency = action
    amount = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
    rub = amount if currency == "rub" else 0
    usd = amount if currency == "usd" else 0
    ok, msg, project = await contribute_to_project(
        session=session,
        user=user,
        unity=unity,
        rub=rub,
        usd=usd,
    )
    await session.commit()
    if not ok:
        await query.answer(msg, show_alert=True)
        return

    active_buff = await get_active_clan_buff(session=session, unity_idpk=unity.idpk)
    await query.message.edit_text(format_project_text(project, active_buff), reply_markup=_kb())
    await query.answer(msg)


async def _handle_custom_contribution(message: Message, session: AsyncSession, user: User, state: FSMContext, currency: str):
    if not user.current_unity:
        await message.answer("Ты не состоишь в клане")
        await state.set_state(UserState.unity_menu)
        return
    unity_idpk = int(str(user.current_unity).split(":")[-1])
    unity: Unity | None = await session.get(Unity, unity_idpk)
    if not unity:
        await message.answer("Клан не найден")
        await state.set_state(UserState.unity_menu)
        return

    text = message.text.strip()
    if not text.isdigit():
        await message.answer("Пожалуйста, введите целое положительное число.")
        return

    amount = int(text)
    if amount <= 0:
        await message.answer("Сумма должна быть больше нуля.")
        return

    rub = amount if currency == "rub" else 0
    usd = amount if currency == "usd" else 0

    ok, msg, project = await contribute_to_project(
        session=session,
        user=user,
        unity=unity,
        rub=rub,
        usd=usd,
    )
    await session.commit()
    
    await message.answer(msg)
    if ok:
        active_buff = await get_active_clan_buff(session=session, unity_idpk=unity.idpk)
        await message.answer(format_project_text(project, active_buff), reply_markup=_kb())
        await state.set_state(UserState.unity_menu)


@router.message(StateFilter(UserState.unity_project_custom_rub), flags=flags)
async def on_custom_project_rub(message: Message, session: AsyncSession, user: User, state: FSMContext):
    await _handle_custom_contribution(message, session, user, state, "rub")


@router.message(StateFilter(UserState.unity_project_custom_usd), flags=flags)
async def on_custom_project_usd(message: Message, session: AsyncSession, user: User, state: FSMContext):
    await _handle_custom_contribution(message, session, user, state, "usd")
