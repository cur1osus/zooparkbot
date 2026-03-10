import asyncio
import contextlib
import json

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from bot.callbacks import (
    AccountBackCallback,
    AccountBackTarget,
    AccountItemPageCallback,
    AccountItemViewCallback,
)
from bot.filters import GetTextButton
from bot.keyboards import (
    ik_account_menu,
    ik_back,
    ik_item_activate_menu,
    ik_menu_items,
    ik_yes_or_not_sell_item,
)
from bot.states import UserState
from db import Item, User
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    count_page_items,
    disable_not_main_window,
    factory_text_account_animals,
    factory_text_account_aviaries,
    ft_item_props,
    get_remain_seats,
    get_text_message,
    get_total_number_animals,
    get_total_number_seats,
    get_value,
    income_,
    synchronize_info_about_items,
)

flags = {"throttling_key": "default"}
router = Router()


@router.message(UserState.main_menu, GetTextButton("account"), flags=flags)
async def account(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await disable_not_main_window(data=await state.get_data(), message=message)

    income, ik_account_menu_k = await asyncio.gather(
        income_(session=session, user=user),
        ik_account_menu(),
    )

    text_message = await get_text_message(
        "account_info",
        nn=user.nickname,
        rub=user.rub,
        usd=user.usd,
        pawc=user.paw_coins,
        income=income,
    )

    msg = await message.answer(
        text=text_message,
        reply_markup=ik_account_menu_k,
    )

    await state.set_data({})
    await state.update_data(active_window=msg.message_id)


@router.callback_query(UserState.main_menu, F.data == "account_animals")
async def account_animals(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    if user.animals == "{}":
        await query.answer(
            text=await get_text_message("no_animals"),
            show_alert=True,
        )
        return
    text = await factory_text_account_animals(session=session, animals=user.animals)
    await query.message.edit_text(
        text=await get_text_message(
            "account_animals",
            t=text,
            total_animals=await get_total_number_animals(user),
        ),
        reply_markup=await ik_back(custom_callback_data="to_account:back_account"),
    )


@router.callback_query(UserState.main_menu, F.data == "account_aviaries")
async def account_aviaries(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    if user.aviaries == "{}":
        await query.answer(
            text=await get_text_message("no_aviaries"),
            show_alert=True,
        )
        return
    text, total_places, remain_places = await asyncio.gather(
        factory_text_account_aviaries(session=session, aviaries=user.aviaries),
        get_total_number_seats(session=session, aviaries=user.aviaries),
        get_remain_seats(
            session=session,
            user=user,
        ),
    )
    await query.message.edit_text(
        text=await get_text_message(
            "account_aviaries",
            t=text,
            total_places=total_places,
            remain_places=remain_places,
        ),
        reply_markup=await ik_back(custom_callback_data="to_account:back_account"),
    )


@router.callback_query(UserState.main_menu, F.data == "items")
async def account_items(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    amount_items = await session.scalar(
        select(func.count()).select_from(Item).where(Item.id_user == user.id_user)
    )
    if amount_items == 0:
        await query.answer(
            text=await get_text_message("no_items"),
            show_alert=True,
        )
        return
    q_page = await count_page_items(session=session, amount_items=amount_items)
    all_stat_props = await ft_item_props(item_props=user.info_about_items)
    await state.update_data(page=1, q_page=q_page, all_stat_props=all_stat_props)
    await query.message.edit_text(
        text=await get_text_message(
            "menu_items", q_page=q_page, page=1, all_stat_props=all_stat_props
        ),
        reply_markup=await ik_menu_items(
            session=session,
            id_user=user.id_user,
        ),
    )


@router.callback_query(UserState.main_menu, AccountItemPageCallback.filter())
async def process_turn_right(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: AccountItemPageCallback,
) -> None:
    data = await state.get_data()
    page = data["page"]
    if callback_data.direction.value == "left":
        page = page - 1 if page > 1 else data["q_page"]
    else:
        page = page + 1 if page < data["q_page"] else 1
    await state.update_data(page=page)
    with contextlib.suppress(Exception):
        await query.message.edit_text(
            text=await get_text_message(
                "menu_items",
                q_page=data["q_page"],
                page=page,
                all_stat_props=data["all_stat_props"],
            ),
            reply_markup=await ik_menu_items(
                session=session,
                id_user=user.id_user,
                page=page,
            ),
        )


@router.callback_query(UserState.main_menu, AccountBackCallback.filter())
async def process_back_to_menu(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: AccountBackCallback,
) -> None:
    back_to = callback_data.target
    match back_to:
        case AccountBackTarget.account:
            await query.message.edit_text(
                text=await get_text_message(
                    "account_info",
                    nn=user.nickname,
                    rub=user.rub,
                    usd=user.usd,
                    pawc=user.paw_coins,
                    income=await income_(session=session, user=user),
                ),
                reply_markup=await ik_account_menu(),
            )
        case AccountBackTarget.items:
            data = await state.get_data()
            await query.message.edit_text(
                text=await get_text_message(
                    "menu_items",
                    q_page=data["q_page"],
                    page=data["page"],
                    all_stat_props=data["all_stat_props"],
                ),
                reply_markup=await ik_menu_items(
                    session=session, id_user=user.id_user, page=data["page"]
                ),
            )


@router.callback_query(UserState.main_menu, AccountItemViewCallback.filter())
async def process_viewing_item(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: AccountItemViewCallback,
) -> None:
    id_item = callback_data.item_id
    item: Item = await session.scalar(select(Item).where(Item.id_item == id_item))
    await state.update_data(id_item=id_item)
    props = await ft_item_props(item_props=item.properties)
    await query.message.edit_text(
        text=await get_text_message(
            "description_item",
            name_=item.name_with_emoji,
            description=props,
        ),
        reply_markup=await ik_item_activate_menu(is_activate=item.is_active),
    )


@router.callback_query(
    UserState.main_menu, F.data.in_(["item_activate", "item_deactivate"])
)
async def process_viewing_recipes(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
) -> None:
    data = await state.get_data()
    is_activate = True
    items = await session.scalars(
        select(Item).where(Item.id_user == user.id_user, Item.is_active == True)
    )
    items = list(items.all())
    if query.data == "item_activate":
        if len(items) == 3:
            await query.answer(
                text=await get_text_message("max_active_items"),
                show_alert=True,
            )
            return
        item: Item = await session.scalar(
            select(Item).where(Item.id_item == data["id_item"])
        )
        item.is_active = True
        items.append(item)
    elif query.data == "item_deactivate":
        is_activate = False
        item: Item = await session.scalar(
            select(Item).where(Item.id_item == data["id_item"])
        )
        item.is_active = False
        items.remove(item)
    user.info_about_items = await synchronize_info_about_items(items=items)
    await session.commit()
    all_stat_props = await ft_item_props(item_props=user.info_about_items)
    await state.update_data(all_stat_props=all_stat_props)
    await query.message.edit_reply_markup(
        reply_markup=await ik_item_activate_menu(is_activate=is_activate),
    )


@router.callback_query(UserState.main_menu, F.data == "sell_item")
async def sell_item(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await query.message.edit_reply_markup(
        reply_markup=await ik_yes_or_not_sell_item(),
    )


@router.callback_query(UserState.main_menu, F.data == "sell_item_no")
async def sell_item_no(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await query.message.edit_reply_markup(
        reply_markup=await ik_item_activate_menu(is_activate=False),
    )


@router.callback_query(UserState.main_menu, F.data == "sell_item_yes")
async def sell_item_yes(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    data = await state.get_data()
    item: Item = await session.scalar(
        select(Item).where(Item.id_item == data["id_item"])
    )
    item.id_user = 0
    USD_TO_CREATE_ITEM = await get_value(
        session=session, value_name="USD_TO_CREATE_ITEM"
    )
    PERCENT_MARKDOWN_ITEM = await get_value(
        session=session, value_name="PERCENT_MARKDOWN_ITEM"
    )
    sell_price = int(USD_TO_CREATE_ITEM * (PERCENT_MARKDOWN_ITEM / 100))
    user.usd += sell_price
    await session.commit()
    await query.answer(
        text=await get_text_message("item_sold", sell_price=sell_price), show_alert=True
    )
    amount_items = await session.scalar(
        select(func.count()).select_from(Item).where(Item.id_user == user.id_user)
    )
    if amount_items == 0:
        await query.message.edit_reply_markup(
            reply_markup=await ik_back(
                custom_callback_data=AccountBackCallback(
                    target=AccountBackTarget.account
                ).pack()
            ),
        )
        return
    q_page = await count_page_items(session=session, amount_items=amount_items)
    await state.update_data(q_page=q_page)
    page = data["page"] if data["page"] <= q_page else q_page
    await query.message.edit_text(
        text=await get_text_message(
            "menu_items",
            q_page=q_page,
            page=page,
            all_stat_props=data["all_stat_props"],
        ),
        reply_markup=await ik_menu_items(
            session=session,
            id_user=user.id_user,
            page=page,
        ),
    )
