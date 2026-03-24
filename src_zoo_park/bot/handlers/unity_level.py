from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from bot.filters import GetTextButton
from bot.keyboards import (
    ik_update_level_unity,
)
from bot.states import UserState
from db import Unity, User
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    check_condition_1st_lvl,
    check_condition_2nd_lvl,
    check_condition_3rd_lvl,
    disable_not_main_window,
    get_data_by_lvl_unity,
    get_text_message,
    sync_user_income,
)

flags = {"throttling_key": "default"}
router = Router()


@router.message(UserState.unity_menu, GetTextButton("level"), flags=flags)
async def unity_level(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await disable_not_main_window(data=await state.get_data(), message=message)
    data = await state.get_data()
    unity = await session.get(Unity, data["idpk_unity"])
    data_for_text = await get_data_by_lvl_unity(
        session=session, lvl=unity.level, unity=unity
    )
    msg = await message.answer(
        text=await get_text_message(f"unity_level_{unity.level}", **data_for_text),
        reply_markup=await ik_update_level_unity(),
    )
    await state.update_data(active_window=msg.message_id)


@router.callback_query(UserState.unity_menu, F.data == "update_level_unity")
async def update_unity_level(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    data = await state.get_data()
    unity = await session.get(Unity, data["idpk_unity"])
    match unity.level:
        case 0:
            pass_to_up = await check_condition_1st_lvl(session=session, unity=unity)
            if not pass_to_up:
                await query.answer(
                    text=await get_text_message("conditions_are_not_met"),
                    show_alert=True,
                )
                return
            unity.level = 1
        case 1:
            pass_to_up = await check_condition_2nd_lvl(session=session, unity=unity)
            if not pass_to_up:
                await query.answer(
                    text=await get_text_message("conditions_are_not_met"),
                    show_alert=True,
                )
                return
            unity.level = 2
        case 2:
            pass_to_up = await check_condition_3rd_lvl(session=session, unity=unity)
            if not pass_to_up:
                await query.answer(
                    text=await get_text_message("conditions_are_not_met"),
                    show_alert=True,
                )
                return

            unity.level = 3
            await session.commit()
            # After reaching level 3, require specialization choice
            if not unity.specialization:
                await query.message.edit_text(
                    text=_specialization_text(),
                    reply_markup=_specialization_kb(),
                )
                return
        case 3:
            if not unity.specialization:
                await query.message.edit_text(
                    text=_specialization_text(),
                    reply_markup=_specialization_kb(),
                )
                return
            await query.answer(
                text=await get_text_message("unity_level_max"),
                show_alert=True,
            )
            return
    await session.commit()
    data_for_text = await get_data_by_lvl_unity(
        session=session, lvl=unity.level, unity=unity
    )
    await query.message.edit_text(
        text=await get_text_message(f"unity_level_{unity.level}", **data_for_text),
        reply_markup=await ik_update_level_unity(),
    )


_SPECIALIZATIONS = {
    "specialist": (
        "🔬 Редкий зверинец",
        "+50% доход от эпических/мифических/лег животных\n-20% доход от редких\n\nИдеально для коллекционеров редкостей",
    ),
    "megapark": (
        "🏟 Мегапарк",
        "+1% дохода на каждые 10 животных (макс. +60%)\n+15% к расходам на содержание\n\nИдеально для тех, кто хочет огромный зоопарк",
    ),
    "wild": (
        "🌿 Дикий заповедник",
        "+3% дохода за каждый уникальный вид (дополнительно к базовому бонусу)\n-30% стоимость лечения животных\n\nИдеально для разнообразного зоопарка",
    ),
}


def _specialization_text() -> str:
    lines = ["🌟 Выберите специализацию клана\n\nДостигнув 3-го уровня, клан получает уникальный путь развития. Выбор нельзя изменить!\n"]
    for key, (name, desc) in _SPECIALIZATIONS.items():
        lines.append(f"{name}\n{desc}\n")
    return "\n".join(lines)


def _specialization_kb():
    b = InlineKeyboardBuilder()
    for key, (name, _) in _SPECIALIZATIONS.items():
        b.button(text=name, callback_data=f"choose_specialization:{key}")
    b.adjust(1)
    return b.as_markup()


@router.callback_query(UserState.unity_menu, F.data.startswith("choose_specialization:"))
async def choose_specialization(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    spec = (query.data or "").split(":", 1)[-1]
    if spec not in _SPECIALIZATIONS:
        await query.answer("Неизвестная специализация", show_alert=True)
        return

    data = await state.get_data()
    unity = await session.get(Unity, data["idpk_unity"])
    if not unity:
        await query.answer("Клан не найден", show_alert=True)
        return
    if unity.idpk_user != user.idpk:
        await query.answer("Только лидер клана может выбрать специализацию", show_alert=True)
        return
    if unity.specialization:
        await query.answer("Специализация уже выбрана", show_alert=True)
        return

    unity.specialization = spec
    # Recalculate income for clan leader to apply new specialization bonuses
    await sync_user_income(session=session, user=user)
    await session.commit()

    name, desc = _SPECIALIZATIONS[spec]
    await query.message.edit_text(
        text=f"✅ Специализация выбрана!\n\n{name}\n{desc}",
        reply_markup=None,
    )
    await query.answer(f"Выбрано: {name}")
