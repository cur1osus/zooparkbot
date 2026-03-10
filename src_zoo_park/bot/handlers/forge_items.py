import contextlib
import json

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from bot.callbacks import (
    ForgeBackCallback,
    ForgeBackTarget,
    ForgeItemSelectCallback,
    ForgeItemsPageCallback,
    ForgePageMode,
)
from bot.filters import GetTextButton
from bot.keyboards import (
    ik_choice_items_to_merge,
    ik_create_item,
    ik_forge_items_menu,
    ik_menu_items_for_merge,
    ik_menu_items_for_up,
    ik_up_lvl_item,
    ik_upgrade_item,
    ik_yes_or_not_sell_item,
)
from bot.states import UserState
from db import Item, User
from game_variables import prop_quantity_by_rarity, translated_rarities
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    CREATE_ITEM_PAW_PRICE,
    able_to_enhance,
    add_item_to_db,
    calculate_percent_to_enhance,
    count_page_items,
    create_item,
    disable_not_main_window,
    fetch_and_parse_str_value,
    ft_item_props,
    ft_item_props_for_update,
    gen_price_to_create_item,
    get_text_message,
    get_value,
    merge_items,
    random_up_property_item,
    synchronize_info_about_items,
    update_prop_iai,
)

flags = {"throttling_key": "default"}
router = Router()


def _build_create_item_info_note(user: User) -> str:
    return (
        f"\n\nАльтернатива: создать предмет за {CREATE_ITEM_PAW_PRICE}🐾."
        f"\nСейчас у тебя {int(user.paw_coins)}🐾."
    )


def _build_create_item_spent_text(currency: str, amount: int) -> str:
    return f"{amount}$" if currency == "usd" else f"{amount}🐾"


async def _finish_create_item(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    currency: str,
    amount: int,
) -> None:
    item_info, item_props = await create_item(session=session)
    await add_item_to_db(
        session=session,
        item_info=item_info,
        item_props=item_props,
        id_user=user.id_user,
    )
    await session.commit()
    usd_to_create_item = await gen_price_to_create_item(
        session=session, id_user=user.id_user
    )
    await state.update_data(
        USD_TO_CREATE_ITEM=usd_to_create_item,
        id_item=item_info["key"],
    )
    text_props = await ft_item_props(item_props=item_props)
    await query.message.edit_text(
        text=(
            await get_text_message(
                "item_created",
                name_=item_info["name"],
                emoji=item_info["emoji"],
                rarity=translated_rarities[item_info["rarity"]],
                text_props=text_props,
            )
            + f"\n\nПотрачено: {_build_create_item_spent_text(currency=currency, amount=amount)}"
        ),
        reply_markup=await ik_create_item(
            uci=usd_to_create_item,
            pci=CREATE_ITEM_PAW_PRICE,
            is_sell=True,
        ),
    )


@router.message(UserState.zoomarket_menu, GetTextButton("forge_items"), flags=flags)
async def forge_items_menu(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await disable_not_main_window(data=await state.get_data(), message=message)
    msg = await message.answer(
        text=await get_text_message("forge_items_menu", usd=user.usd),
        reply_markup=await ik_forge_items_menu(),
    )
    await state.set_data({})
    await state.update_data(active_window=msg.message_id)


@router.callback_query(UserState.zoomarket_menu, F.data == "create_item_info")
async def fi_create_item_info(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    USD_TO_CREATE_ITEM = await gen_price_to_create_item(
        session=session, id_user=user.id_user
    )
    common, rare, epic, mythical = await fetch_and_parse_str_value(
        session=session,
        value_name="WEIGHT_RARITIES_ITEM",
        func_to_element=lambda x: int(float(x) * 100),
    )
    await state.update_data(USD_TO_CREATE_ITEM=USD_TO_CREATE_ITEM)
    await query.message.edit_text(
        text=(
            await get_text_message(
                "item_probability_info",
                uci=int(USD_TO_CREATE_ITEM),
                common_weight=common,
                rare_weight=rare,
                epic_weight=epic,
                mythical_weight=mythical,
            )
            + _build_create_item_info_note(user=user)
        ),
        reply_markup=await ik_create_item(
            uci=int(USD_TO_CREATE_ITEM),
            pci=CREATE_ITEM_PAW_PRICE,
        ),
    )


@router.callback_query(UserState.zoomarket_menu, F.data == "create_item")
async def fi_create_item(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    USD_TO_CREATE_ITEM = (await state.get_data())["USD_TO_CREATE_ITEM"]
    if user.usd < USD_TO_CREATE_ITEM:
        await query.answer(
            text=await get_text_message("not_enough_usd"), show_alert=True
        )
        return
    user.usd -= USD_TO_CREATE_ITEM
    user.amount_expenses_usd += USD_TO_CREATE_ITEM
    await _finish_create_item(
        query=query,
        session=session,
        state=state,
        user=user,
        currency="usd",
        amount=USD_TO_CREATE_ITEM,
    )


@router.callback_query(UserState.zoomarket_menu, F.data == "create_item_paw")
async def fi_create_item_paw(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    if int(user.paw_coins) < CREATE_ITEM_PAW_PRICE:
        await query.answer(text="Недостаточно paw coins", show_alert=True)
        return
    user.paw_coins -= CREATE_ITEM_PAW_PRICE
    user.amount_expenses_paw_coins += CREATE_ITEM_PAW_PRICE
    await _finish_create_item(
        query=query,
        session=session,
        state=state,
        user=user,
        currency="paw",
        amount=CREATE_ITEM_PAW_PRICE,
    )


@router.callback_query(UserState.zoomarket_menu, F.data == "sell_item")
async def sell_item_in_create(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await query.message.edit_reply_markup(
        reply_markup=await ik_yes_or_not_sell_item(),
    )


@router.callback_query(UserState.zoomarket_menu, F.data == "sell_item_no")
async def sell_item_no_in_create(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    data = await state.get_data()
    await query.message.edit_reply_markup(
        reply_markup=await ik_create_item(
            uci=data["USD_TO_CREATE_ITEM"],
            pci=CREATE_ITEM_PAW_PRICE,
            is_sell=True,
        ),
    )


@router.callback_query(UserState.zoomarket_menu, F.data == "sell_item_yes")
async def sell_item_yes_in_create(
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
    USD_TO_CREATE_ITEM = await gen_price_to_create_item(
        session=session, id_user=user.id_user
    )
    await state.update_data(USD_TO_CREATE_ITEM=USD_TO_CREATE_ITEM)
    await query.message.edit_text(
        text=await get_text_message("item_sold", sell_price=sell_price),
        reply_markup=await ik_create_item(
            uci=USD_TO_CREATE_ITEM,
            pci=CREATE_ITEM_PAW_PRICE,
        ),
    )


@router.callback_query(UserState.zoomarket_menu, ForgeBackCallback.filter())
async def process_back_forge_items(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: ForgeBackCallback,
) -> None:
    back_to = callback_data.target
    match back_to:
        case ForgeBackTarget.forge_menu:
            await query.message.edit_text(
                text=await get_text_message("forge_items_menu", usd=user.usd),
                reply_markup=await ik_forge_items_menu(),
            )
        case ForgeBackTarget.upgrade_info:
            PERCENTAGE_DECREASE_ENHANCE_BY_LVL = await get_value(
                session=session, value_name="PERCENTAGE_DECREASE_ENHANCE_BY_LVL"
            )
            await query.message.edit_text(
                text=await get_text_message(
                    "up_lvl_item_info",
                    usd=user.usd,
                    pd=PERCENTAGE_DECREASE_ENHANCE_BY_LVL,
                ),
                reply_markup=await ik_up_lvl_item(),
            )
        case ForgeBackTarget.choice_item:
            data = await state.get_data()
            page = data["page"]
            await query.message.edit_text(
                text=await get_text_message("up_menu_items"),
                reply_markup=await ik_menu_items_for_up(
                    session=session,
                    id_user=user.id_user,
                    page=page,
                ),
            )
        case ForgeBackTarget.merge_info:
            PERCENT_MERGE_BY_PROP = await get_value(
                session=session, value_name="PERCENT_MERGE_BY_PROP"
            )
            await query.message.edit_text(
                text=await get_text_message(
                    "merge_items_info", usd=user.usd, pm=PERCENT_MERGE_BY_PROP
                ),
                reply_markup=await ik_choice_items_to_merge(),
            )


@router.callback_query(UserState.zoomarket_menu, F.data == "up_lvl_item_info")
async def fi_up_lvl_item_info(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    PERCENTAGE_DECREASE_ENHANCE_BY_LVL = await get_value(
        session=session, value_name="PERCENTAGE_DECREASE_ENHANCE_BY_LVL"
    )
    await query.message.edit_text(
        text=await get_text_message(
            "up_lvl_item_info", usd=user.usd, pd=PERCENTAGE_DECREASE_ENHANCE_BY_LVL
        ),
        reply_markup=await ik_up_lvl_item(),
    )


@router.callback_query(UserState.zoomarket_menu, F.data == "choice_item_for_upgrade")
async def fi_choice_item_for_upgrade(
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
    await state.update_data(page=1, q_page=q_page)
    await query.message.edit_text(
        text=await get_text_message("up_menu_items"),
        reply_markup=await ik_menu_items_for_up(
            session=session,
            id_user=user.id_user,
        ),
    )


@router.callback_query(
    UserState.zoomarket_menu,
    ForgeItemsPageCallback.filter(F.mode == ForgePageMode.upgrade),
)
async def process_turn_up_item(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: ForgeItemsPageCallback,
) -> None:
    data = await state.get_data()
    page = data["page"]
    turn_to = callback_data.direction.value
    if turn_to == "left":
        page = page - 1 if page > 1 else data["q_page"]
    else:
        page = page + 1 if page < data["q_page"] else 1
    await state.update_data(page=page)
    with contextlib.suppress(Exception):
        await query.message.edit_reply_markup(
            reply_markup=await ik_menu_items_for_up(
                session=session,
                id_user=user.id_user,
                page=page,
            ),
        )


@router.callback_query(
    UserState.zoomarket_menu,
    ForgeItemSelectCallback.filter(F.mode == ForgePageMode.upgrade),
)
async def process_viewing_to_up_item(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: ForgeItemSelectCallback,
) -> None:
    id_item = callback_data.item_id
    item: Item = await session.scalar(select(Item).where(Item.id_item == id_item))
    MAX_LVL_ITEM = await get_value(session=session, value_name="MAX_LVL_ITEM")
    if item.lvl == MAX_LVL_ITEM:
        await query.answer(
            text=await get_text_message("item_reached_max_lvl"), show_alert=True
        )
        return
    USD_TO_UP_ITEM = await get_value(session=session, value_name="USD_TO_UP_ITEM")
    USD_TO_UP_ITEM_view = USD_TO_UP_ITEM * (item.lvl + 1)
    await state.update_data(
        id_item=id_item,
        item_lvl=item.lvl,
        MAX_LVL_ITEM=MAX_LVL_ITEM,
        USD_TO_UP_ITEM=USD_TO_UP_ITEM,
    )
    penh = await calculate_percent_to_enhance(
        session=session, current_item_lvl=item.lvl
    )
    props = await ft_item_props(item_props=item.properties)
    await query.message.edit_text(
        text=await get_text_message(
            "description_item_to_up",
            name_=item.name_with_emoji,
            description=props,
            utui=USD_TO_UP_ITEM_view,
            penh=penh,
        ),
        reply_markup=await ik_upgrade_item(),
    )


@router.callback_query(UserState.zoomarket_menu, F.data == "upgrade_item")
async def fi_upgrade_item(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    data = await state.get_data()
    if data["item_lvl"] == data["MAX_LVL_ITEM"]:
        await query.answer(
            text=await get_text_message("item_reached_max_lvl"), show_alert=True
        )
        return
    USD_TO_UP_ITEM = data["USD_TO_UP_ITEM"] * (data["item_lvl"] + 1)
    if user.usd < USD_TO_UP_ITEM:
        await query.answer(
            text=await get_text_message("not_enough_usd"), show_alert=True
        )
        return
    user.usd -= USD_TO_UP_ITEM
    user.amount_expenses_usd += USD_TO_UP_ITEM
    await session.commit()
    if not await able_to_enhance(session=session, current_item_lvl=data["item_lvl"]):
        await query.answer(text=await get_text_message("not_updated"), show_alert=False)
        return
    id_item = data["id_item"]
    item: Item = await session.scalar(select(Item).where(Item.id_item == id_item))
    new_item_properties, updated_property, parameter = await random_up_property_item(
        session=session, item_properties=item.properties
    )
    if item.is_active:
        user.info_about_items = await update_prop_iai(
            info_about_items=user.info_about_items,
            prop=updated_property,
            value=parameter,
        )
    item.properties = new_item_properties
    item.lvl += 1
    await state.update_data(item_lvl=item.lvl)
    await session.commit()
    utui = data["USD_TO_UP_ITEM"] * (item.lvl + 1)
    text_props = await ft_item_props_for_update(
        item_props=new_item_properties,
        updated_prop=updated_property,
        parameter=parameter,
    )
    penh = await calculate_percent_to_enhance(
        session=session, current_item_lvl=item.lvl
    )
    await query.message.edit_text(
        text=await get_text_message(
            "item_upgraded",
            name_=item.name_with_emoji,
            item_lvl=item.lvl,
            text_props=text_props,
            utui=utui,
            penh=penh,
        ),
        reply_markup=await ik_upgrade_item(),
    )


@router.callback_query(UserState.zoomarket_menu, F.data == "merge_items_info")
async def fi_merge_items_info(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    PERCENT_MERGE_BY_PROP = await get_value(
        session=session, value_name="PERCENT_MERGE_BY_PROP"
    )
    await query.message.edit_text(
        text=await get_text_message(
            "merge_items_info", usd=user.usd, pm=PERCENT_MERGE_BY_PROP
        ),
        reply_markup=await ik_choice_items_to_merge(),
    )


@router.callback_query(UserState.zoomarket_menu, F.data == "choice_item_to_merge")
async def fi_choice_item_to_merge(
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
    await state.update_data(
        page=1,
        q_page=q_page,
        id_chosen_items=[],
        status_chosen_items=[],
        q_props_items=[],
        lvl_items=[],
        ft_props=[],
    )
    await query.message.edit_text(
        text=await get_text_message("merge_menu_items"),
        reply_markup=await ik_menu_items_for_merge(
            session=session,
            id_user=user.id_user,
        ),
    )


@router.callback_query(
    UserState.zoomarket_menu,
    ForgeItemsPageCallback.filter(F.mode == ForgePageMode.merge),
)
async def process_turn_merge_items(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: ForgeItemsPageCallback,
) -> None:
    data = await state.get_data()
    page = data["page"]
    turn_to = callback_data.direction.value
    if turn_to == "left":
        page = page - 1 if page > 1 else data["q_page"]
    else:
        page = page + 1 if page < data["q_page"] else 1
    await state.update_data(page=page)
    with contextlib.suppress(Exception):
        await query.message.edit_reply_markup(
            reply_markup=await ik_menu_items_for_merge(
                session=session,
                id_user=user.id_user,
                page=page,
                id_chosen_items=data["id_chosen_items"],
            ),
        )


@router.callback_query(
    UserState.zoomarket_menu,
    ForgeItemSelectCallback.filter(F.mode == ForgePageMode.merge),
)
async def process_viewing_to_merge_item(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
    callback_data: ForgeItemSelectCallback,
) -> None:
    id_item = callback_data.item_id
    data = await state.get_data()
    if len(data["id_chosen_items"]) == 2 and id_item not in data["id_chosen_items"]:
        await query.answer(
            text=await get_text_message("max_two_items"), show_alert=True
        )
        return
    item: Item = await session.scalar(select(Item).where(Item.id_item == id_item))
    count_props = len(json.loads(item.properties))
    if count_props > prop_quantity_by_rarity["mythical"]:
        await query.answer(
            text=await get_text_message("too_many_props"), show_alert=True
        )
        return
    if id_item in data["id_chosen_items"]:
        data["id_chosen_items"].remove(id_item)
        data["status_chosen_items"].remove(item.is_active)
        data["q_props_items"].remove(count_props)
        data["lvl_items"].remove(item.lvl)
        data["ft_props"].remove(await ft_item_props(item_props=item.properties))
    else:
        data["id_chosen_items"].append(id_item)
        data["status_chosen_items"].append(item.is_active)
        data["q_props_items"].append(count_props)
        data["lvl_items"].append(item.lvl)
        data["ft_props"].append(await ft_item_props(item_props=item.properties))
    await state.update_data(
        id_chosen_items=data["id_chosen_items"],
        status_chosen_items=data["status_chosen_items"],
        q_props_items=data["q_props_items"],
        lvl_items=data["lvl_items"],
        ft_props=data["ft_props"],
    )
    USD_TO_MERGE_ITEMS = 0
    t1 = data["ft_props"][0] if len(data["ft_props"]) > 0 else ""
    t2 = data["ft_props"][1] if len(data["ft_props"]) == 2 else ""
    if len(data["id_chosen_items"]) == 2:
        USD_TO_MERGE_ITEMS = await get_value(
            session=session, value_name="USD_TO_MERGE_ITEMS"
        )
        q_props = sum(data["q_props_items"])
        lvl_sum = sum(data["lvl_items"])
        lvl_sum = 1 if lvl_sum == 0 else lvl_sum
        main_k = q_props + lvl_sum
        USD_TO_MERGE_ITEMS = USD_TO_MERGE_ITEMS * main_k
        await state.update_data(USD_TO_MERGE_ITEMS=USD_TO_MERGE_ITEMS)
    await query.message.edit_text(
        text=await get_text_message(
            "chosen_items_to_merge",
            t1=t1,
            t2=t2,
            utmi=USD_TO_MERGE_ITEMS,
        ),
        reply_markup=await ik_menu_items_for_merge(
            session=session,
            id_user=user.id_user,
            id_chosen_items=data["id_chosen_items"],
            page=data["page"],
        ),
    )


@router.callback_query(UserState.zoomarket_menu, F.data == "merge_items")
async def fi_merge_items(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    USD_TO_MERGE_ITEMS = (await state.get_data())["USD_TO_MERGE_ITEMS"]
    if user.usd < USD_TO_MERGE_ITEMS:
        await query.answer(
            text=await get_text_message("not_enough_usd"), show_alert=True
        )
        return
    user.usd -= USD_TO_MERGE_ITEMS
    user.amount_expenses_usd += USD_TO_MERGE_ITEMS
    data = await state.get_data()
    new_item = await merge_items(
        session=session,
        id_item_1=data["id_chosen_items"][0],
        id_item_2=data["id_chosen_items"][1],
    )
    new_item.id_user = user.id_user
    session.add(new_item)
    await session.commit()
    if any(data["status_chosen_items"]):
        items = await session.scalars(
            select(Item).where(
                and_(Item.id_user == user.id_user, Item.is_active == True)  # noqa: E712
            )
        )
        user.info_about_items = await synchronize_info_about_items(items=list(items))
        await session.commit()
    t = await ft_item_props(item_props=new_item.properties)
    await query.message.edit_text(
        text=await get_text_message(
            "items_merged", name_=new_item.name_with_emoji, t=t
        ),
        reply_markup=await ik_choice_items_to_merge(),
    )
