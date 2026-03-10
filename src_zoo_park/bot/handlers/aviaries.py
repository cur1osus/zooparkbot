from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from bot.callbacks import (
    AviaryBackCallback,
    AviaryChoiceCallback,
    AviaryQuantityCallback,
)
from bot.filters import GetTextButton
from bot.keyboards import (
    ik_choice_aviary,
    ik_choice_quantity_aviary_avi,
    rk_back,
    rk_zoomarket_menu,
)
from bot.states import UserState
from db import User
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    add_aviary,
    disable_not_main_window,
    find_integers,
    get_text_message,
    m_choice_quantity_avi,
)

flags = {"throttling_key": "default"}
router = Router()


@router.message(UserState.zoomarket_menu, GetTextButton("aviaries"), flags=flags)
async def aviaries_menu(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await disable_not_main_window(data=await state.get_data(), message=message)
    msg = await message.answer(
        text=await get_text_message("aviaries_menu"),
        reply_markup=await ik_choice_aviary(session=session),
    )
    await state.set_data({})
    await state.update_data(active_window=msg.message_id)


@router.callback_query(UserState.zoomarket_menu, AviaryChoiceCallback.filter())
async def choice_quantity_avi(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: AviaryChoiceCallback,
):
    aviary = callback_data.aviary
    await m_choice_quantity_avi(
        session=session,
        aviary=aviary,
        state=state,
        query=query,
        user=user,
    )


@router.callback_query(UserState.zoomarket_menu, AviaryQuantityCallback.filter())
async def get_qa_to_buy_avi(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: AviaryQuantityCallback,
):
    data = await state.get_data()
    quantity = callback_data.quantity
    finite_price = data["aviary_price"] * quantity
    if user.usd < finite_price:
        return await query.answer(
            text=await get_text_message("not_enough_money"),
            show_alert=True,
        )
    user.usd -= finite_price
    user.amount_expenses_usd += finite_price
    await add_aviary(
        session=session,
        self=user,
        code_name_aviary=data["code_name_aviary"],
        quantity=quantity,
    )
    await session.commit()
    await query.answer(
        await get_text_message("aviary_bought_successfully"), show_alert=True
    )
    await m_choice_quantity_avi(
        user=user,
        session=session,
        aviary=data["code_name_aviary"],
        state=state,
        query=query,
    )


@router.callback_query(UserState.zoomarket_menu, AviaryBackCallback.filter())
async def back_distributor_avi(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: AviaryBackCallback,
):
    back_to = callback_data.target
    match back_to:
        case "to_choice_aviary":
            return await query.message.edit_text(
                text=await get_text_message("aviaries_menu"),
                reply_markup=await ik_choice_aviary(session=session),
            )


@router.callback_query(UserState.zoomarket_menu, F.data == "cqa_avi")
async def custom_quantity_aviaries(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await query.message.delete_reply_markup()
    data = await state.get_data()
    aviary_are_available = user.usd // data["aviary_price"]
    await query.message.answer(
        text=await get_text_message(
            "enter_custom_quantity_aviary", available=aviary_are_available
        ),
        reply_markup=await rk_back(),
    )
    await state.set_state(UserState.avi_enter_custom_qa_step)


@router.message(UserState.avi_enter_custom_qa_step, GetTextButton("back"))
async def back_to_choice_quantity_avi(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    data = await state.get_data()
    await message.answer(
        text=await get_text_message("backed"), reply_markup=await rk_zoomarket_menu()
    )
    msg = await message.answer(
        text=await get_text_message(
            "choice_quantity_aviaries",
            price_one_aviary=data["aviary_price"],
            usd=user.usd,
        ),
        reply_markup=await ik_choice_quantity_aviary_avi(
            session=session, aviary_price=data["aviary_price"]
        ),
    )
    await state.update_data(active_window=msg.message_id)
    await state.set_state(UserState.zoomarket_menu)


@router.message(UserState.avi_enter_custom_qa_step)
async def get_custom_quantity_aviary(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    num = await find_integers(message.text)
    if not num:
        await message.answer(text=await get_text_message("enter_digit"))
        return
    if num < 1:
        await message.answer(text=await get_text_message("enter_digit"))
        return
    data = await state.get_data()
    finite_price = num * data["aviary_price"]
    if user.usd < finite_price:
        await message.answer(text=await get_text_message("not_enough_money"))
        return
    user.usd -= finite_price
    user.amount_expenses_usd += finite_price
    await add_aviary(
        session=session,
        self=user,
        code_name_aviary=data["code_name_aviary"],
        quantity=num,
    )
    await session.commit()
    await message.answer(
        text=await get_text_message("aviary_bought_successfully"),
        reply_markup=await rk_zoomarket_menu(),
    )
    await state.set_state(UserState.zoomarket_menu)
