from datetime import datetime

from db import Animal, Unity, User, Value
from db.structured_state import get_user_animals_map
from fastjson import loads_or_default
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import tools


async def income_(session: AsyncSession, user: User):
    unity_idpk = int(user.current_unity.split(":")[-1]) if user.current_unity else None
    animals = await get_user_animals_map(session=session, user=user)
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

        # Global clan project buff (e.g. +10% income from "Ветеринарная станция")
        if await _has_active_income_buff(session=session, unity_idpk=unity_idpk):
            income *= 1.10

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


async def _has_active_income_buff(session: AsyncSession, unity_idpk: int) -> bool:
    row = await session.scalar(
        select(Value).where(Value.name == f"CLAN_BUFF_{int(unity_idpk)}")
    )
    if not row or not row.value_str:
        return False

    try:
        payload = loads_or_default(row.value_str, {})
        if not isinstance(payload, dict):
            return False
        if payload.get("type") != "income_boost":
            return False

        ends_at_raw = payload.get("ends_at")
        if not ends_at_raw:
            return False

        return datetime.now() <= datetime.fromisoformat(str(ends_at_raw))
    except Exception:
        return False
