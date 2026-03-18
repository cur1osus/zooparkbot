import asyncio
import random

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, ReactionTypeEmoji, ReplyKeyboardRemove
from aiogram.utils.chat_action import ChatActionSender
from bot.callbacks import (
    RandomMerchantAnimalCallback,
    RandomMerchantBackCallback,
    RandomMerchantBackTarget,
    RandomMerchantOfferCallback,
    RandomMerchantQuantityCallback,
)
from bot.filters import GetTextButton
from bot.keyboards import (
    ik_choice_animal_rmerchant,
    ik_choice_quantity_animals_rmerchant,
    ik_merchant_menu,
    rk_back,
    rk_zoomarket_menu,
)
from bot.states import UserState
from db import Animal, RandomMerchant, User
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    add_animal,
    create_random_merchant,
    disable_not_main_window,
    find_integers,
    gen_price,
    gen_quantity_animals,
    get_animal_with_random_rarity,
    get_random_animal,
    get_remain_seats,
    get_text_message,
    get_value,
    magic_count_animal_for_kb,
)

flags = {"throttling_key": "default"}
router = Router()
petard_emoji_effect = "5046509860389126442"


@router.message(UserState.zoomarket_menu, GetTextButton("random_merchant"), flags=flags)
async def random_merchant_menu(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await disable_not_main_window(data=await state.get_data(), message=message)
    merchant: RandomMerchant = await session.scalar(
        select(RandomMerchant).where(RandomMerchant.id_user == user.id_user)
    )
    if not merchant:
        merchant: RandomMerchant = await create_random_merchant(
            session=session, user=user
        )
    animal_name = await session.scalar(
        select(Animal.name).where(Animal.code_name == merchant.code_name_animal)
    )
    msg = await message.answer(
        text=await get_text_message(
            "random_merchant_menu", n=merchant.name, usd=user.usd
        ),
        reply_markup=await ik_merchant_menu(
            quantity_animals=merchant.quantity_animals,
            name_animal=animal_name,
            discount=merchant.discount,
            price_with_discount=merchant.price_with_discount,
            price=merchant.price,
            first_offer_bought=merchant.first_offer_bought,
        ),
    )
    await state.set_data({})
    await state.update_data(active_window=msg.message_id, animal_name=animal_name)


async def check_funds_and_seats(
    query, user, merchant, session, price, quantity_animals
):
    if user.usd < price:
        await query.answer(await get_text_message("not_enough_money"), show_alert=True)
        return False
    remain_seats = await get_remain_seats(session=session, user=user)
    if remain_seats < quantity_animals:
        await query.answer(await get_text_message("not_enough_seats"), show_alert=True)
        return False
    return True


async def update_user_data(user, merchant, price, quantity_animals, session):
    user.usd -= price
    user.amount_expenses_usd += price
    await add_animal(
        self=user,
        code_name_animal=merchant.code_name_animal,
        quantity=quantity_animals,
        session=session,
    )


@router.callback_query(UserState.zoomarket_menu, RandomMerchantOfferCallback.filter())
async def buy_one_of_offer(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: RandomMerchantOfferCallback,
):
    offer = str(callback_data.offer)
    merchant = await session.scalar(
        select(RandomMerchant).where(RandomMerchant.id_user == user.id_user)
    )
    match offer:
        case "1":
            if not await check_funds_and_seats(
                query,
                user,
                merchant,
                session,
                merchant.price_with_discount,
                merchant.quantity_animals,
            ):
                return
            await update_user_data(
                user,
                merchant,
                merchant.price_with_discount,
                merchant.quantity_animals,
                session,
            )
            merchant.first_offer_bought = True
            await session.commit()
            await query.message.edit_reply_markup(
                reply_markup=await ik_merchant_menu(
                    price=merchant.price,
                    first_offer_bought=True,
                )
            )
            await query.answer(
                await get_text_message("offer_bought_successfully"), show_alert=True
            )
        case "2":
            if not await check_funds_and_seats(
                query,
                user,
                merchant,
                session,
                merchant.price,
                await get_value(session=session, value_name="MAX_QUANTITY_ANIMALS"),
            ):
                return
            await query.message.delete_reply_markup()
            user.usd -= merchant.price
            user.amount_expenses_usd += merchant.price
            quantity_animals = await gen_quantity_animals(session=session, user=user)

            await state.set_state(UserState.for_while_shell)
            while quantity_animals > 0:
                animal = await get_random_animal(
                    session=session, user_animals=user.animals
                )
                part_animals = random.randint(1, quantity_animals)
                quantity_animals -= part_animals
                await add_animal(
                    self=user,
                    code_name_animal=animal.code_name,
                    quantity=part_animals,
                    session=session,
                )
                async with ChatActionSender.typing(
                    bot=query.bot, chat_id=query.message.chat.id
                ):
                    await asyncio.sleep(2)
                    await query.message.answer(
                        await get_text_message(
                            "you_got_this_animal", an=animal.name, aq=part_animals
                        )
                    )
            merchant.price = await gen_price(
                session=session,
                animals=user.animals,
                user=user,
            )
            await session.commit()
            await state.set_state(UserState.zoomarket_menu)
            await query.message.answer(
                await get_text_message("you_lucky"),
                message_effect_id=petard_emoji_effect,
            )
        case "3":
            await query.message.edit_text(
                text=await get_text_message("merchant_choice_animal"),
                reply_markup=await ik_choice_animal_rmerchant(session=session),
            )


@router.callback_query(UserState.zoomarket_menu, RandomMerchantAnimalCallback.filter())
async def choice_animal_to_buy(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: RandomMerchantAnimalCallback,
):
    animal = callback_data.animal
    animal_price = await session.scalar(
        select(Animal.price).where(Animal.code_name == f"{animal}-")
    )
    remain_seats = await get_remain_seats(session=session, user=user)
    await state.update_data(
        code_name_animal=animal, animal_price=animal_price, remain_seats=remain_seats
    )
    magic_count_animal = await magic_count_animal_for_kb(
        remain_seats=remain_seats, balance=user.usd, price_per_one_animal=animal_price
    )
    await query.message.edit_text(
        text=await get_text_message("merchant_choise_quantity_animal"),
        reply_markup=await ik_choice_quantity_animals_rmerchant(
            session=session,
            animal_price=animal_price,
            magic_count_animal=magic_count_animal,
        ),
    )


@router.callback_query(
    UserState.zoomarket_menu, RandomMerchantQuantityCallback.filter()
)
async def choice_qa_to_buy(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: RandomMerchantQuantityCallback,
):
    data = await state.get_data()
    animal_price = data["animal_price"]
    quantity = callback_data.quantity
    finite_price = animal_price * quantity

    remain_seats = data["remain_seats"]
    if remain_seats < quantity:
        await query.answer(await get_text_message("not_enough_seats"), show_alert=True)
        return
    if user.usd < finite_price:
        await query.answer(await get_text_message("not_enough_money"), show_alert=True)
        return
    user.usd -= finite_price
    user.amount_expenses_usd += finite_price
    await query.message.delete_reply_markup()

    await state.set_state(UserState.for_while_shell)
    while quantity > 0:
        animal_obj: Animal = await get_animal_with_random_rarity(
            session=session, animal=data["code_name_animal"]
        )
        part_animals = random.randint(1, quantity)
        quantity -= part_animals
        async with ChatActionSender.typing(
            bot=query.bot, chat_id=query.message.chat.id
        ):
            await asyncio.sleep(2)
            await query.message.answer(
                await get_text_message(
                    "you_got_this_rarity_animal", an=animal_obj.name, aq=part_animals
                )
            )

        await add_animal(
            self=user,
            code_name_animal=animal_obj.code_name,
            quantity=part_animals,
            session=session,
        )
    await state.set_state(UserState.zoomarket_menu)

    await query.message.answer(text=await get_text_message("you_lucky"))
    await session.commit()


@router.callback_query(UserState.for_while_shell)
async def for_while_shell_callback(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await query.answer(text=await get_text_message("while_shell"), show_alert=False)


@router.message(UserState.for_while_shell)
async def random_merchant_menu_(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await message.react(reaction=[ReactionTypeEmoji(emoji="🗿")])


@router.callback_query(UserState.zoomarket_menu, RandomMerchantBackCallback.filter())
async def back_distributor(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
    callback_data: RandomMerchantBackCallback,
):
    back_to = callback_data.target
    match back_to:
        case RandomMerchantBackTarget.all_offers:
            merchant = await session.scalar(
                select(RandomMerchant).where(RandomMerchant.id_user == user.id_user)
            )
            data = await state.get_data()
            animal_name = data["animal_name"]
            await query.message.edit_text(
                text=await get_text_message(
                    "random_merchant_menu", n=merchant.name, usd=user.usd
                ),
                reply_markup=await ik_merchant_menu(
                    quantity_animals=merchant.quantity_animals,
                    name_animal=animal_name,
                    discount=merchant.discount,
                    price_with_discount=merchant.price_with_discount,
                    price=merchant.price,
                    first_offer_bought=merchant.first_offer_bought,
                ),
            )
        case RandomMerchantBackTarget.choice_animal:
            await query.message.edit_text(
                text=await get_text_message("merchant_choise_animal"),
                reply_markup=await ik_choice_animal_rmerchant(session=session),
            )


@router.callback_query(UserState.zoomarket_menu, F.data == "cqa")
async def custom_quantity_animals(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    await query.message.delete_reply_markup()
    data = await state.get_data()
    animal_price = data["animal_price"]
    animals_are_available = user.usd // animal_price
    await query.message.answer(
        text=await get_text_message(
            "enter_custom_quantity_animals", available=animals_are_available
        ),
        reply_markup=await rk_back(),
    )
    await state.set_state(UserState.rmerchant_enter_custom_qa_step)


@router.message(UserState.rmerchant_enter_custom_qa_step, GetTextButton("back"))
async def back_to_choice_quantity(
    message: Message,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    data = await state.get_data()
    await message.answer(
        text=await get_text_message("backed"), reply_markup=await rk_zoomarket_menu()
    )
    magic_count_animal = await magic_count_animal_for_kb(
        remain_seats=data["remain_seats"],
        balance=user.usd,
        price_per_one_animal=data["animal_price"],
    )
    msg = await message.answer(
        text=await get_text_message("merchant_choise_quantity_animal"),
        reply_markup=await ik_choice_quantity_animals_rmerchant(
            session=session,
            animal_price=data["animal_price"],
            magic_count_animal=magic_count_animal,
        ),
    )
    await state.update_data(active_window=msg.message_id)
    await state.set_state(UserState.zoomarket_menu)


@router.message(UserState.rmerchant_enter_custom_qa_step)
async def get_custom_quantity_animals(
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
    remain_seats = await get_remain_seats(session=session, user=user)
    if remain_seats < num:
        await message.answer(
            await get_text_message("not_enough_seats"), show_alert=True
        )
        return
    data = await state.get_data()
    finite_price = num * data["animal_price"]
    if user.usd < finite_price:
        await message.answer(text=await get_text_message("not_enough_money"))
        return
    user.usd -= finite_price
    user.amount_expenses_usd += finite_price
    quantity = num
    await message.answer(
        text=await get_text_message("you_paid", fp=finite_price),
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(UserState.for_while_shell)
    while quantity > 0:
        animal = await get_animal_with_random_rarity(
            session=session, animal=data["code_name_animal"]
        )
        part_animals = random.randint(1, quantity)
        quantity -= part_animals
        async with ChatActionSender.typing(bot=message.bot, chat_id=message.chat.id):
            await asyncio.sleep(2)
            await message.answer(
                await get_text_message(
                    "you_got_this_rarity_animal", an=animal.name, aq=part_animals
                )
            )
        await add_animal(
            self=user,
            code_name_animal=animal.code_name,
            quantity=part_animals,
            session=session,
        )
    await state.set_state(UserState.zoomarket_menu)

    await message.answer(
        await get_text_message("you_lucky"), reply_markup=await rk_zoomarket_menu()
    )
    await session.commit()
