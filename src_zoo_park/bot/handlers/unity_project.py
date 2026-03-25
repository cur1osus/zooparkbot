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
    get_user_project_stats,
)

router = Router()
flags = {"throttling_key": "default"}

PROJECT_BUTTON_TEXT = "🏗 Проект клана"


def _remaining_by_currency(project: dict) -> tuple[int, int]:
    progress = project.get("progress", {}) or {}
    target = project.get("target", {}) or {}
    rub_left = max(0, int(target.get("rub", 0)) - int(progress.get("rub", 0)))
    usd_left = max(0, int(target.get("usd", 0)) - int(progress.get("usd", 0)))
    return rub_left, usd_left


def _kb(project: dict):
    b = InlineKeyboardBuilder()

    rub_left, usd_left = _remaining_by_currency(project)

    def _fmt(n: int) -> str:
        return f"{n:,}".replace(",", " ")

    # RUB row: 3 short buttons on one line
    if rub_left > 0:
        rub_10 = max(1, rub_left // 10)
        rub_50 = max(1, rub_left // 2)
        b.button(text="💰 10%", callback_data=f"clprj:rub:{rub_10}")
        b.button(text="💰 50%", callback_data=f"clprj:rub:{rub_50}")
        b.button(text="💰 100%", callback_data=f"clprj:rub:{rub_left}")
    else:
        b.button(text="✅ RUB закрыт", callback_data="clprj:noop:rub")

    # USD row: 3 short buttons on one line
    if usd_left > 0:
        usd_10 = max(1, usd_left // 10)
        usd_50 = max(1, usd_left // 2)
        b.button(text="💵 10%", callback_data=f"clprj:usd:{usd_10}")
        b.button(text="💵 50%", callback_data=f"clprj:usd:{usd_50}")
        b.button(text="💵 100%", callback_data=f"clprj:usd:{usd_left}")
    else:
        b.button(text="✅ USD закрыт", callback_data="clprj:noop:usd")

    if rub_left > 0:
        b.button(text="✏️ Своя ₽", callback_data="clprj:custom:rub")
    if usd_left > 0:
        b.button(text="✏️ Своя $", callback_data="clprj:custom:usd")
    b.button(text="🔄 Обновить", callback_data="clprj:refresh")

    rub_row = 3 if rub_left > 0 else 1
    usd_row = 3 if usd_left > 0 else 1
    custom_count = int(rub_left > 0) + int(usd_left > 0)
    b.adjust(rub_row, usd_row, custom_count, 1)

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
    user_stats = await get_user_project_stats(session=session, user_idpk=user.idpk)
    await session.commit()
    await message.answer(
        format_project_text(project, active_buff, user_idpk=user.idpk, user_stats=user_stats),
        reply_markup=_kb(project),
    )


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
        user_stats = await get_user_project_stats(session=session, user_idpk=user.idpk)
        await session.commit()
        await query.message.edit_text(
            format_project_text(project, active_buff, user_idpk=user.idpk, user_stats=user_stats),
            reply_markup=_kb(project),
        )
        await query.answer("Обновлено")
        return

    if action == "noop":
        currency = parts[2] if len(parts) > 2 else ""
        if currency == "rub":
            await query.answer("Цель по RUB уже закрыта")
        elif currency == "usd":
            await query.answer("Цель по USD уже закрыта")
        else:
            await query.answer("Эта цель уже закрыта")
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
    user_stats = await get_user_project_stats(session=session, user_idpk=user.idpk)
    await query.message.edit_text(
        format_project_text(project, active_buff, user_idpk=user.idpk, user_stats=user_stats),
        reply_markup=_kb(project),
    )
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
        user_stats = await get_user_project_stats(session=session, user_idpk=user.idpk)
        await message.answer(
            format_project_text(project, active_buff, user_idpk=user.idpk, user_stats=user_stats),
            reply_markup=_kb(project),
        )
        await state.set_state(UserState.unity_menu)


@router.message(StateFilter(UserState.unity_project_custom_rub), flags=flags)
async def on_custom_project_rub(message: Message, session: AsyncSession, user: User, state: FSMContext):
    await _handle_custom_contribution(message, session, user, state, "rub")


@router.message(StateFilter(UserState.unity_project_custom_usd), flags=flags)
async def on_custom_project_usd(message: Message, session: AsyncSession, user: User, state: FSMContext):
    await _handle_custom_contribution(message, session, user, state, "usd")
