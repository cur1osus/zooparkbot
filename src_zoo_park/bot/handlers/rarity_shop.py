from pathlib import Path

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InputMediaPhoto,
    Message,
)
from bot.callbacks import (
    RarityShopAnimalCallback,
    RarityShopBackCallback,
    RarityShopBackTarget,
    RarityShopQuantityCallback,
    RarityShopRarityCallback,
    RarityShopSwitchCallback,
)
from bot.filters import GetTextButton
from bot.keyboards import (
    ik_choice_animal_rshop,
    ik_choice_quantity_animals_rshop,
    ik_choice_rarity_rshop,
    rk_back,
    rk_zoomarket_menu,
)
from bot.states import UserState
from db import Animal, User
from game_variables import rarities
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    add_animal,
    disable_not_main_window,
    find_integers,
    formatter,
    get_dict_animals,
    get_income_animal,
    get_price_animal,
    get_remain_seats,
    get_text_message,
    get_top_unity_by_animal,
    get_value,
    get_value_prop_from_iai,
    magic_count_animal_for_kb,
)

flags = {"throttling_key": "default"}
router = Router()
PHOTO_ROOT = Path("src_photos")


def get_animal_photo_path(animal_group: str, animal_code_name: str) -> Path | None:
    photo_path = PHOTO_ROOT / animal_group / f"{animal_code_name}.jpg"
    return photo_path if photo_path.exists() else None


async def get_rarity_shop_caption(
    session: AsyncSession,
    user: User,
    animal: Animal,
    animal_price: int,
    unity_idpk: int | None,
) -> str:
    base_income = int(animal.income)
    quantity_animals = (await get_dict_animals(user, session=session)).get(
        animal.code_name, 0
    )

    # Item bonus for this animal
    item_pct = int(get_value_prop_from_iai(
        info_about_items=user.info_about_items,
        name_prop=f"{animal.code_name}:animal_income",
    ) or 0)

    # Unity bonus (top unity by this animal type)
    unity_pct = 0
    if unity_idpk:
        unity_idpk_top, animal_code_top = await get_top_unity_by_animal(session=session)
        if unity_idpk_top == unity_idpk and animal_code_top == animal.code_name:
            unity_pct = int(await get_value(session=session, value_name="BONUS_FOR_AMOUNT_ANIMALS"))

    # Milestone bonus
    _milestones = [10**i for i in range(1, 19)]
    milestone_pct = sum(1 for m in _milestones if quantity_animals >= m)

    income = await get_income_animal(
        session=session,
        animal=animal,
        unity_idpk=unity_idpk,
        info_about_items=user.info_about_items,
    )

    bonuses = []
    if item_pct:
        bonuses.append(f"🎒 Предметы: +{item_pct}%")
    if unity_pct:
        bonuses.append(f"🏰 Клан: +{unity_pct}%")
    if milestone_pct:
        bonuses.append(f"📈 Прирост: +{milestone_pct}%")
    bonuses_text = "\n".join(bonuses) if bonuses else "—"

    return await get_text_message(
        "choice_quantity_rarity_shop_menu",
        name_=animal.name,
        price=formatter.format_large_number(animal_price),
        base_income=formatter.format_large_number(base_income),
        income=formatter.format_large_number(income),
        bonuses=bonuses_text,
        usd=formatter.format_large_number(user.usd),
        quantity_animals=quantity_animals,
    )


async def answer_rarity_shop_offer(
    message: Message,
    session: AsyncSession,
    user: User,
    animal_group: str,
    animal: Animal,
    animal_price: int,
    unity_idpk: int | None,
    magic_count_animal: int,
):
    caption = await get_rarity_shop_caption(
        session=session,
        user=user,
        animal=animal,
        animal_price=animal_price,
        unity_idpk=unity_idpk,
    )
    reply_markup = await ik_choice_quantity_animals_rshop(
        session=session,
        animal_price=animal_price,
        magic_count_animal=magic_count_animal,
    )
    photo_path = get_animal_photo_path(
        animal_group=animal_group, animal_code_name=animal.code_name
    )
    if photo_path:
        return await message.answer_photo(
            photo=FSInputFile(path=photo_path),
            caption=caption,
            reply_markup=reply_markup,
            protect_content=True,
        )
    return await message.answer(
        text=caption,
        reply_markup=reply_markup,
        protect_content=True,
    )


async def edit_rarity_shop_offer(
    message: Message,
    session: AsyncSession,
    user: User,
    animal_group: str,
    animal: Animal,
    animal_price: int,
    unity_idpk: int | None,
    magic_count_animal: int,
):
    caption = await get_rarity_shop_caption(
        session=session,
        user=user,
        animal=animal,
        animal_price=animal_price,
        unity_idpk=unity_idpk,
    )
    reply_markup = await ik_choice_quantity_animals_rshop(
        session=session,
        animal_price=animal_price,
        magic_count_animal=magic_count_animal,
    )
    photo_path = get_animal_photo_path(
        animal_group=animal_group, animal_code_name=animal.code_name
    )
    if photo_path:
        await message.edit_media(
            media=InputMediaPhoto(
                media=FSInputFile(path=photo_path),
                caption=caption,
            ),
            reply_markup=reply_markup,
            protect_content=True,
        )
        return
    await message.edit_text(text=caption, reply_markup=reply_markup)


@router.message(UserState.zoomarket_menu, GetTextButton("rarity_shop"), flags=flags)
async def rarity_shop_menu(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    data = await state.get_data()
    await disable_not_main_window(message=message, data=data)
    msg = await message.answer(
        text=await get_text_message("rarity_shop_menu"),
        reply_markup=await ik_choice_animal_rshop(session=session),
    )
    await state.set_data({})
    await state.update_data(active_window=msg.message_id)


@router.callback_query(UserState.zoomarket_menu, RarityShopAnimalCallback.filter())
async def get_animal_rshop(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: RarityShopAnimalCallback,
):
    animal = callback_data.animal
    await state.update_data(animal=animal)
    await query.message.edit_text(
        text=await get_text_message("choice_rarity_shop_menu"),
        reply_markup=await ik_choice_rarity_rshop(),
    )


@router.callback_query(UserState.zoomarket_menu, RarityShopRarityCallback.filter())
async def get_rarity_rshop(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: RarityShopRarityCallback,
):
    data = await state.get_data()
    rarity = callback_data.rarity
    unity_idpk = int(user.current_unity.split(":")[-1]) if user.current_unity else None
    animal_price = await get_price_animal(
        session=session,
        animal_code_name=data["animal"] + rarity,
        unity_idpk=unity_idpk,
        info_about_items=user.info_about_items,
        user=user,
    )
    animal = await session.scalar(
        select(Animal).where(Animal.code_name == data["animal"] + rarity)
    )

    animal_income = await get_income_animal(
        session=session,
        animal=animal,
        unity_idpk=unity_idpk,
        info_about_items=user.info_about_items,
    )
    await query.message.delete()
    remain_seats = await get_remain_seats(session=session, user=user)
    magic_count_animal = await magic_count_animal_for_kb(
        remain_seats=remain_seats, balance=user.usd, price_per_one_animal=animal_price
    )
    await state.update_data(
        animal_price=animal_price,
        animal=data["animal"],
        rarity=rarity,
        unity_idpk=unity_idpk,
        remain_seats=remain_seats,
    )
    msg = await answer_rarity_shop_offer(
        message=query.message,
        session=session,
        user=user,
        animal_group=data["animal"],
        animal=animal,
        animal_price=animal_price,
        unity_idpk=unity_idpk,
        magic_count_animal=magic_count_animal,
    )
    await state.update_data(active_window=msg.message_id)


@router.callback_query(UserState.zoomarket_menu, RarityShopSwitchCallback.filter())
async def rshop_switch_rarity(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: RarityShopSwitchCallback,
):
    data = await state.get_data()
    switch_to = callback_data.direction.value
    if switch_to == "right":
        rarity = (
            rarities[rarities.index(data["rarity"]) + 1]
            if data["rarity"] != rarities[-1]
            else rarities[0]
        )
    elif switch_to == "left":
        rarity = (
            rarities[rarities.index(data["rarity"]) - 1]
            if data["rarity"] != rarities[0]
            else rarities[-1]
        )
    animal_price = await get_price_animal(
        session=session,
        animal_code_name=data["animal"] + rarity,
        unity_idpk=data["unity_idpk"],
        info_about_items=user.info_about_items,
        user=user,
    )
    animal = await session.scalar(
        select(Animal).where(Animal.code_name == data["animal"] + rarity)
    )
    await state.update_data(rarity=rarity, animal_price=animal_price)
    animal_income = await get_income_animal(
        session=session,
        animal=animal,
        unity_idpk=data["unity_idpk"],
        info_about_items=user.info_about_items,
    )
    magic_count_animal = await magic_count_animal_for_kb(
        remain_seats=data["remain_seats"],
        balance=user.usd,
        price_per_one_animal=animal_price,
    )
    await edit_rarity_shop_offer(
        message=query.message,
        session=session,
        user=user,
        animal_group=data["animal"],
        animal=animal,
        animal_price=animal_price,
        unity_idpk=data["unity_idpk"],
        magic_count_animal=magic_count_animal,
    )


@router.callback_query(UserState.zoomarket_menu, RarityShopBackCallback.filter())
async def back_to_rarity_shop_menu(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: RarityShopBackCallback,
):
    back_to = callback_data.target
    match back_to:
        case RarityShopBackTarget.choice_animal:
            return await query.message.edit_text(
                text=await get_text_message("rarity_shop_menu"),
                reply_markup=await ik_choice_animal_rshop(session=session),
            )
        case RarityShopBackTarget.choice_rarity:
            await query.message.delete()
            msg = await query.message.answer(
                text=await get_text_message("choice_rarity_shop_menu"),
                reply_markup=await ik_choice_rarity_rshop(),
            )
            await state.update_data(active_window=msg.message_id)


@router.callback_query(UserState.zoomarket_menu, RarityShopQuantityCallback.filter())
async def get_quantity_rshop(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: RarityShopQuantityCallback,
):
    quantity_animal = callback_data.quantity
    data = await state.get_data()
    remain_seats = data["remain_seats"]
    if remain_seats < quantity_animal:
        await query.answer(await get_text_message("not_enough_seats"), show_alert=True)
        return
    finite_price = data["animal_price"] * quantity_animal
    if user.usd < finite_price:
        return await query.answer(
            text=await get_text_message("not_enough_money"),
            show_alert=True,
        )
    user.usd -= finite_price
    user.amount_expenses_usd += finite_price
    await add_animal(
        self=user,
        code_name_animal=data["animal"] + data["rarity"],
        quantity=quantity_animal,
        session=session,
    )
    await query.answer(
        await get_text_message("offer_bought_successfully"), show_alert=True
    )
    await session.commit()
    animal = await session.scalar(
        select(Animal).where(Animal.code_name == data["animal"] + data["rarity"])
    )
    await edit_rarity_shop_offer(
        message=query.message,
        session=session,
        user=user,
        animal_group=data["animal"],
        animal=animal,
        animal_price=data["animal_price"],
        unity_idpk=data["unity_idpk"],
        magic_count_animal=0,
    )


@router.callback_query(UserState.zoomarket_menu, F.data == "cqa_rshop")
async def custom_quantity_animals_rshop(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await query.message.delete_reply_markup()
    data = await state.get_data()
    animals_are_available = user.usd // data["animal_price"]
    await query.message.answer(
        text=await get_text_message(
            "enter_custom_quantity_animals", available=animals_are_available
        ),
        reply_markup=await rk_back(),
    )
    await state.set_state(UserState.rshop_enter_custom_qa_step)


@router.message(UserState.rshop_enter_custom_qa_step, GetTextButton("back"))
async def back_to_choice_quantity_rshop(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    data = await state.get_data()
    await message.answer(
        text=await get_text_message("backed"), reply_markup=await rk_zoomarket_menu()
    )
    animal = await session.scalar(
        select(Animal).where(Animal.code_name == data["animal"] + data["rarity"])
    )
    unity_idpk = int(user.current_unity.split(":")[-1]) if user.current_unity else None
    animal_income = await get_income_animal(
        session=session,
        animal=animal,
        unity_idpk=unity_idpk,
        info_about_items=user.info_about_items,
    )
    magic_count_animal = await magic_count_animal_for_kb(
        remain_seats=data["remain_seats"],
        balance=user.usd,
        price_per_one_animal=data["animal_price"],
    )
    msg = await answer_rarity_shop_offer(
        message=message,
        session=session,
        user=user,
        animal_group=data["animal"],
        animal=animal,
        animal_price=data["animal_price"],
        unity_idpk=unity_idpk,
        magic_count_animal=magic_count_animal,
    )
    await state.update_data(active_window=msg.message_id)
    await state.set_state(UserState.zoomarket_menu)


@router.message(UserState.rshop_enter_custom_qa_step)
async def get_custom_quantity_animals_rshop(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    quantity_animal = await find_integers(message.text)
    if not quantity_animal:
        await message.answer(text=await get_text_message("enter_digit"))
        return
    if quantity_animal < 1:
        await message.answer(text=await get_text_message("enter_digit"))
        return
    remain_seats = await get_remain_seats(session=session, user=user)
    if remain_seats < quantity_animal:
        await message.answer(await get_text_message("not_enough_seats"))
        return
    data = await state.get_data()
    finite_price = quantity_animal * data["animal_price"]
    if user.usd < finite_price:
        await message.answer(text=await get_text_message("not_enough_money"))
        return
    user.usd -= finite_price
    user.amount_expenses_usd += finite_price
    await add_animal(
        self=user,
        code_name_animal=data["animal"] + data["rarity"],
        quantity=quantity_animal,
        session=session,
    )
    await message.answer(
        text=await get_text_message("you_paid", fp=finite_price),
        reply_markup=await rk_zoomarket_menu(),
    )
    await session.commit()
    await state.set_state(UserState.zoomarket_menu)
