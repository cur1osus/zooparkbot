import random

import game_variables
from db import Animal, Unity, User
from db.structured_state import (
    add_user_animals,
    get_user_animals_map,
    get_user_total_animals,
)
from fastjson import loads_or_default
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import tools

TOP_UNITY_ANIMAL_CACHE_KEY = "top_unity_animal_bonus"


async def get_all_animals(session: AsyncSession) -> list[Animal]:
    result = await session.scalars(select(Animal).where(Animal.code_name.contains("-")))
    return result.all()


async def get_price_animal(
    session: AsyncSession,
    animal_code_name: str,
    unity_idpk: int | None,
    info_about_items: str | dict,
    user: User | None = None,
) -> int:
    """
    Calculate purchase price for one unit of an animal.

    If `user` is provided, applies quantity-based price scaling:
    each 10 already owned of this species raises the price by
    ANIMAL_PRICE_SCALE_PER_10 percent (default 15%).

    Example with ANIMAL_PRICE_SCALE_PER_10=15:
      0 owned  → 1.00x base price
      10 owned → 1.15x
      20 owned → 1.32x  (1.15²)
      50 owned → 2.01x  (1.15⁵)
      100 owned → 4.05x (1.15¹⁰)
    """
    discount = (
        await _get_unity_data_for_price_animal(session=session, idpk_unity=unity_idpk)
        if unity_idpk
        else 0
    )
    price = await session.scalar(
        select(Animal.price).where(Animal.code_name == animal_code_name)
    )
    name_prop = f"{animal_code_name}:animal_sale"
    if v := tools.get_value_prop_from_iai(
        info_about_items=info_about_items, name_prop=name_prop
    ):
        price = price * (1 - v / 100)
    if discount:
        price *= 1 - (discount / 100)

    # Quantity-based price scaling (exponential cost to prevent farming one species)
    if user is not None:
        from db.structured_state import get_user_animals_map
        user_animals = await get_user_animals_map(session=session, user=user)
        quantity_owned = user_animals.get(animal_code_name, 0)
        if quantity_owned > 0:
            import math
            scale_pct = await tools.get_value(
                session=session, value_name="ANIMAL_PRICE_SCALE_PER_10"
            )
            base = 1 + scale_pct / 100
            # Cap exponent so result never exceeds 100x (avoids Decimal overflow)
            max_exp = math.log(100) / math.log(base) if base > 1 else float("inf")
            exponent = min(quantity_owned / 10, max_exp)
            scale_multiplier = min(100.0, base ** exponent)
            price = int(price * scale_multiplier)

    return int(price)


async def _get_unity_data_for_price_animal(session: AsyncSession, idpk_unity: int):
    unity = await session.get(Unity, idpk_unity)
    bonus = 0
    if unity.level == 2:
        bonus = await tools.get_value(
            session=session, value_name="BONUS_DISCOUNT_FOR_ANIMAL_2ND_LVL"
        )
    elif unity.level == 3:
        bonus = await tools.get_value(
            session=session, value_name="BONUS_DISCOUNT_FOR_ANIMAL_3RD_LVL"
        )

    # Check for clan project buff "Центр выкупа" (-10% price)
    from tools.unity_projects import get_active_clan_buff

    active_buff = await get_active_clan_buff(session=session, unity_idpk=idpk_unity)
    if active_buff and active_buff.get("type") == "shop_discount":
        # Multiplicative stacking: 0.9 discount.
        # If bonus was 5 (5%), result is 14.5 (14.5% discount)
        bonus = 100 - ((100 - bonus) * 0.9)

    return bonus


async def get_income_animal(
    session: AsyncSession,
    animal: Animal,
    unity_idpk: int,
    info_about_items: str,
):
    animal_income = animal.income
    name_prop = f"{animal.code_name}:animal_income"
    if v := tools.get_value_prop_from_iai(
        info_about_items=info_about_items, name_prop=name_prop
    ):
        animal_income = animal_income * (1 + v / 100)
    if unity_idpk:
        top_unity_data = session.info.get(TOP_UNITY_ANIMAL_CACHE_KEY)
        if top_unity_data is None:
            unity_idpk_top, animal_top = await tools.get_top_unity_by_animal(
                session=session
            )
            top_unity_data = (unity_idpk_top, next(iter(animal_top), None))
            session.info[TOP_UNITY_ANIMAL_CACHE_KEY] = top_unity_data
        unity_idpk_top, animal_code_name_top = top_unity_data
        if unity_idpk_top == unity_idpk and animal.code_name == animal_code_name_top:
            bonus = await tools.get_value(
                session=session, value_name="BONUS_FOR_AMOUNT_ANIMALS"
            )
            animal_income = animal_income * (1 + (bonus / 100))
    return int(animal_income)


async def get_dict_animals(
    self: User,
    session: AsyncSession | None = None,
) -> dict[str, int]:
    if session is not None:
        return await get_user_animals_map(session=session, user=self)
    return {}


async def get_numbers_animals(
    self: User,
    session: AsyncSession | None = None,
) -> list[int]:
    decoded_dict = await get_dict_animals(self=self, session=session)
    return list(decoded_dict.values())


async def add_animal(
    self: User,
    code_name_animal: str,
    quantity: int,
    session: AsyncSession,
) -> None:
    await add_user_animals(
        session=session,
        user=self,
        animal_code_name=code_name_animal,
        quantity=quantity,
    )
    # Auto-sync income to keep background jobs fast
    await tools.sync_user_income(session=session, user=self)


async def get_total_number_animals(
    self: User,
    session: AsyncSession,
) -> int:
    return await get_user_total_animals(session=session, user=self)


# async def _get_income_animal(
#     session: AsyncSession,
#     animal: Animal,
#     unity_idpk: int,
# ):
#     animal_income = animal.income
#     if unity_idpk:
#         unity_idpk_top, animal_top = await tools.get_top_unity_by_animal(
#             session=session
#         )
#         if (
#             unity_idpk_top == unity_idpk
#             and animal.code_name == list(animal_top.keys())[0]
#         ):
#             bonus = await tools.get_value(
#                 session=session, value_name="BONUS_FOR_AMOUNT_ANIMALS"
#             )
#             animal_income = animal_income * (1 + (bonus / 100))
#     return int(animal_income)


async def get_random_animal(
    session: AsyncSession,
    user_animals: str | dict[str, int] | None = None,
    user: User | None = None,
) -> Animal:
    dict_animals: dict[str, int] = {}
    if user is not None:
        dict_animals = await get_user_animals_map(session=session, user=user)
    elif isinstance(user_animals, dict):
        dict_animals = user_animals
    else:
        dict_animals = loads_or_default(user_animals, {})
    if not dict_animals:
        animal_names_to_choice = await tools.fetch_and_parse_str_value(
            session=session,
            value_name="START_ANIMALS_FOR_RMERCHANT",
            func_to_element=str,
        )
    else:
        animal_names_to_choice = [
            animal_name.split("_")[0] for animal_name in dict_animals
        ]
    animal_name = random.choice(animal_names_to_choice)
    rarity = random.choices(
        population=game_variables.rarities,
        weights=await tools.fetch_and_parse_str_value(
            session=session,
            value_name="WEIGHTS_FOR_RANDOM_MERCHANT",
            func_to_element=float,
        ),
    )
    animal = await session.scalar(
        select(Animal).where(Animal.code_name == animal_name + rarity[0])
    )
    return animal


async def get_animal_with_random_rarity(session: AsyncSession, animal: str) -> Animal:
    rarity = random.choices(
        population=game_variables.rarities,
        weights=await tools.fetch_and_parse_str_value(
            session=session,
            value_name="WEIGHTS_FOR_RANDOM_MERCHANT",
            func_to_element=float,
        ),
    )
    animal = await session.scalar(
        select(Animal).where(Animal.code_name == animal + rarity[0])
    )
    return animal


async def gen_quantity_animals(session: AsyncSession, user: User) -> int:
    MAX_QUANTITY_ANIMALS = await tools.get_value(
        session=session, value_name="MAX_QUANTITY_ANIMALS"
    )
    num = await tools.get_total_number_animals(user, session=session)
    if num == 0:
        MAX_QUANTITY_ANIMALS = 2
    quantity_animals = random.randint(1, MAX_QUANTITY_ANIMALS)
    return quantity_animals


async def get_average_price_animals(session: AsyncSession, animals_code_name: set[str]):
    result = await session.execute(
        select(Animal.price).where(Animal.code_name.in_(animals_code_name))
    )
    prices = [row[0] for row in result]
    return sum(prices) / len(prices)


async def magic_count_animal_for_kb(remain_seats, balance, price_per_one_animal):
    count_enough_animal = balance // price_per_one_animal
    count_enough_animal = min(count_enough_animal, remain_seats)
    return count_enough_animal
