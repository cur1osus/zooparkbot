import json

from db import Animal, Unity, User
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import tools


async def income_(session: AsyncSession, user: User):
    unity_idpk = int(user.current_unity.split(":")[-1]) if user.current_unity else None
    animals: dict = json.loads(user.animals)
    income = await income_from_animal(
        session=session,
        animals=animals,
        unity_idpk=unity_idpk,
        info_about_items=user.info_about_items,
    )
    if v := tools.get_value_prop_from_iai(
        info_about_items=user.info_about_items, name_prop="general_income"
    ):
        income = income * (1 + v / 100)
    if unity_idpk:
        unity_data = await get_unity_data_for_income(
            session=session, idpk_unity=unity_idpk
        )
        if unity_data["lvl"] in [1, 2, 3]:
            income *= 1 + (unity_data["bonus"] / 100)
    return int(income)


async def income_from_animal(
    session: AsyncSession, animals: dict, unity_idpk: int, info_about_items: str
):
    income = 0
    if not animals:
        return income

    animal_rows = await session.scalars(
        select(Animal).where(Animal.code_name.in_(list(animals.keys())))
    )
    animals_by_code = {animal.code_name: animal for animal in animal_rows.all()}

    for animal, quantity in animals.items():
        animal = animals_by_code.get(animal)
        if not animal:
            continue
        animal_income = await tools.get_income_animal(
            session=session,
            animal=animal,
            unity_idpk=unity_idpk,
            info_about_items=info_about_items,
        )
        income += animal_income * quantity
    return int(income)


async def get_unity_data_for_income(session: AsyncSession, idpk_unity: int):
    unity = await session.get(Unity, idpk_unity)
    data = {"lvl": unity.level}
    if unity.level in [1, 2]:
        data["bonus"] = await tools.get_value(
            session=session, value_name="BONUS_ADD_TO_INCOME_1ST_LVL"
        )
    elif unity.level == 3:
        data["bonus"] = await tools.get_value(
            session=session, value_name="BONUS_ADD_TO_INCOME_3RD_LVL"
        )
    return data
