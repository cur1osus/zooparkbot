from datetime import datetime

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from bot.filters import GetTextButton
from bot.keyboards import ik_bank, rk_exchange_bank, rk_main_menu
from bot.states import UserState
from db import User
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    exchange,
    find_integers,
    ft_bank_exchange_info,
    get_rate,
    get_text_message,
    get_value,
    mention_html_by_username,
    formatter,
)

flags = {"throttling_key": "default"}
router = Router()


@router.message(UserState.main_menu, GetTextButton("bank"), flags=flags)
async def bank(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    edit: bool = False,
):
    rate = await get_rate(session=session, user=user)
    base_rate = await get_value(
        session=session, value_name="RATE_RUB_USD", cache_=False
    )
    time_to_update_bank = 60 - datetime.now().second
    dict_func = {
        True: message.edit_text,
        False: message.answer,
    }
    bank_storage = await get_value(
        session=session, value_name="BANK_STORAGE", cache_=False, value_type="str"
    )
    bank_storage = formatter.format_large_number(float(bank_storage))
    keyboard = await ik_bank()
    # if d := await get_values_from_item(info_about_items=user.info_about_items, code_name_item="item_6"):
    #     if d["is_activate"] and not d.get("date_end"):
    #         keyboard = await ik_bank_modify()
    text = await get_text_message(
        "bank_info",
        r=rate,
        ub=time_to_update_bank,
        rub=user.rub,
        usd=user.usd,
        bank_storage=bank_storage,
    )

    # If item properties modified the effective rate, show original bank rate in brackets.
    if int(rate) != int(base_rate):
        marker = f"{int(rate)}₽"
        replacement = f"{int(rate)}₽ ({int(base_rate)}₽)"
        if marker in text:
            text = text.replace(marker, replacement, 1)
        else:
            text += f"\n\n(Оригинальный курс банка: 1$ = {int(base_rate)}₽)"

    await dict_func[edit](
        text=text,
        reply_markup=keyboard,
    )


# @router.callback_query(UserState.main_menu, F.data == "market_collapse")
# async def market_collapse(
#     query: CallbackQuery,
#     session: AsyncSession,
#     state: FSMContext,
#     user: User,
# ):
#     # if await get_value_from_item(
#     #     info_about_items=user.info_about_items,
#     #     code_name_item="item_6",
#     #     value_name="date_end",
#     # ):
#     #     await query.answer(
#     #         await get_text_message("market_collapse_used"), show_alert=True
#     #     )
#     # else:
#     #     await add_value_to_item(
#     #         self=user,
#     #         code_name_item="item_6",
#     #         value_name="date_end",
#     #         value=(datetime.now() + timedelta(seconds=60)).isoformat(),
#     #     )
#     #     await session.commit()
#     await bank(
#         message=query.message,
#         session=session,
#         state=state,
#         user=user,
#         edit=True,
#     )


@router.callback_query(UserState.main_menu, F.data == "update_bank")
async def update_bank(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await query.answer(cache_time=3)
    await bank(
        message=query.message,
        session=session,
        state=state,
        user=user,
        edit=True,
    )


@router.callback_query(UserState.main_menu, F.data == "exchange_bank")
async def exchange_bank(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    rate = await get_rate(session=session, user=user)
    await state.update_data(rate=rate)
    if user.rub < rate:
        await query.answer(await get_text_message("no_money"), show_alert=True)
        return
    await query.message.delete_reply_markup()
    await query.message.answer(
        text=await get_text_message("enter_amount_to_exchange"),
        reply_markup=await rk_exchange_bank(),
    )
    await state.set_state(UserState.exchange_bank_step)


@router.message(
    UserState.exchange_bank_step, GetTextButton("exchange_all_amount"), flags=flags
)
async def exchange_all_amount(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    data = await state.get_data()
    if user.rub < data["rate"]:
        await message.answer(await get_text_message("no_money"))
        return

    you_change, bank_fee, you_got = await exchange(
        session=session,
        user=user,
        amount=int(user.rub),
        rate=data["rate"],
    )

    referrer_got = None

    if user.id_referrer:
        percent = await get_value(session=session, value_name="REFERRAL_PERCENT")
        referrer_got = int(you_got * (percent / 100))
        referrer = await session.get(User, user.id_referrer)
        you_got -= referrer_got
        referrer.usd += referrer_got

    user.usd += you_got
    if referrer_got and referrer_got > 0:
        await message.bot.send_message(
            chat_id=referrer.id_user,
            text=await get_text_message(
                "referral_bonus",
                referral=mention_html_by_username(
                    username=user.username, name=user.nickname
                ),
                amount=referrer_got,
            ),
            disable_notification=True,
            disable_web_page_preview=True,
        )
    await message.answer(
        text=await get_text_message(
            "exchange_bank_success",
            t=await ft_bank_exchange_info(
                you_change=you_change,
                you_got=you_got,
                rate=data["rate"],
                referrer_got=referrer_got,
                bank_got=bank_fee,
            ),
        ),
        reply_markup=await rk_main_menu(user_id=user.id_user),
    )
    await state.set_state(UserState.main_menu)
    await session.commit()


@router.message(UserState.exchange_bank_step, GetTextButton("back"))
async def back_to_bank(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await message.answer(
        text=await get_text_message("back_to_bank"),
        reply_markup=await rk_main_menu(user_id=user.id_user),
    )
    await state.set_state(UserState.main_menu)
    await bank(
        message=message,
        session=session,
        state=state,
        user=user,
        edit=False,
    )


@router.message(UserState.exchange_bank_step)
async def get_amount(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    amount = await find_integers(message.text)
    if not amount:
        await message.answer(await get_text_message("enter_amount_to_exchange"))
        return
    data = await state.get_data()
    rate = data["rate"]
    if amount < rate:
        await message.answer(await get_text_message("no_money"))
        return
    if amount > user.rub:
        await message.answer(await get_text_message("no_money"))
        return

    you_change, bank_fee, you_got = await exchange(
        session=session,
        user=user,
        amount=amount,
        rate=data["rate"],
        all=False,
    )
    referrer_got = None

    if user.id_referrer:
        percent = await get_value(session=session, value_name="REFERRAL_PERCENT")
        referrer_got = int(you_got * (percent / 100))
        referrer = await session.get(User, user.id_referrer)
        you_got -= referrer_got
        referrer.usd += referrer_got

    user.usd += you_got
    if referrer_got and referrer_got > 0:
        await message.bot.send_message(
            chat_id=referrer.id_user,
            text=await get_text_message(
                "referral_bonus",
                referral=mention_html_by_username(
                    username=user.username, name=user.nickname
                ),
                amount=referrer_got,
            ),
            disable_notification=True,
            disable_web_page_preview=True,
        )
    await message.answer(
        text=await get_text_message(
            "exchange_bank_success",
            t=await ft_bank_exchange_info(
                you_change=you_change,
                you_got=you_got,
                rate=data["rate"],
                referrer_got=referrer_got,
                bank_got=bank_fee,
            ),
        ),
        reply_markup=await rk_main_menu(user_id=user.id_user),
    )
    await state.set_state(UserState.main_menu)
    await session.commit()
