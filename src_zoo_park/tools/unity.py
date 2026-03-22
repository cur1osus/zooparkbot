import html
import re
from collections import defaultdict

from db import Unity, User
from db.structured_state import (
    count_unity_members,
    list_unity_member_ids,
)
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

import tools


async def shorten_whitespace_name_unity(name: str) -> str:
    return re.sub(r"\s+", " ", name).strip()


async def has_special_characters_name(name: str) -> str | None:
    # Паттерн для поиска специальных символов
    pattern = r"[^a-zA-Zа-яА-Я0-9\-\ ]"
    special_chars = re.findall(pattern, html.unescape(name))
    return "".join(special_chars) if special_chars else None


async def is_unique_name(session: AsyncSession, nickname: str) -> bool:
    count_unities = await session.scalar(
        select(func.count())
        .select_from(Unity)
        .where(func.lower(Unity.name) == nickname.lower())
    )
    return not count_unities


async def get_row_unity_for_kb(session: AsyncSession):
    row = await tools.get_value(session=session, value_name="ROW_UNITY_FOR_KB")
    return row


async def get_size_unity_for_kb(session: AsyncSession):
    size = await tools.get_value(session=session, value_name="SIZE_UNITY_FOR_KB")
    return size


async def get_unity_name_and_idpk(session: AsyncSession) -> list[tuple[str, int]]:
    r = await session.execute(select(Unity.name, Unity.idpk_user))
    return r.all()


async def count_page_unity(
    session: AsyncSession,
) -> int:
    size = await get_size_unity_for_kb(session=session)
    len_unity = await session.scalar(select(func.count()).select_from(Unity)) or 0
    remains = len_unity % size
    return len_unity // size + (1 if remains else 0)


async def check_condition_1st_lvl(session: AsyncSession, unity: Unity) -> bool:
    AMOUNT_MEMBERS_1ST_LVL = await tools.get_value(
        session=session, value_name="AMOUNT_MEMBERS_1ST_LVL"
    )
    return (
        await count_unity_members(session=session, unity=unity)
        >= AMOUNT_MEMBERS_1ST_LVL
    )


async def get_data_by_lvl_unity(session: AsyncSession, lvl: int, unity: Unity) -> dict:
    data = {"lvl": lvl}

    lvl_data = {
        0: {
            "values": [
                ("amount_members", "AMOUNT_MEMBERS_1ST_LVL"),
                ("f_amount_members", "-"),
            ],
            "next_lvl": 1,
        },
        1: {
            "values": [
                ("amount_income", "AMOUNT_INCOME_2ND_LVL"),
                ("f_current_income", "-"),
                ("amount_animals", "AMOUNT_ANIMALS_2ND_LVL"),
                ("f_members_not_have_amount_animals", "-"),
                ("bonus_add_to_income", "BONUS_ADD_TO_INCOME_1ST_LVL"),
            ],
            "next_lvl": 2,
        },
        2: {
            "values": [
                ("amount_income", "AMOUNT_INCOME_3RD_LVL"),
                ("f_current_income", "-"),
                ("amount_animals", "AMOUNT_ANIMALS_3RD_LVL"),
                ("f_members_not_have_amount_animals", "-"),
                ("amount_members", "AMOUNT_MEMBERS_3RD_LVL"),
                ("f_amount_members", "-"),
                ("bonus_add_to_income", "BONUS_ADD_TO_INCOME_1ST_LVL"),
                ("bonus_discount_for_animal", "BONUS_DISCOUNT_FOR_ANIMAL_2ND_LVL"),
            ],
            "next_lvl": 3,
        },
        3: {
            "values": [
                ("bonus_add_to_income", "BONUS_ADD_TO_INCOME_3RD_LVL"),
                ("bonus_discount_for_animal", "BONUS_DISCOUNT_FOR_ANIMAL_3RD_LVL"),
            ]
        },
    }

    if lvl in lvl_data:
        for key, value_name in lvl_data[lvl]["values"]:
            data[key] = await tools.get_value(session=session, value_name=value_name)
            if key == "f_current_income":
                data[key] = await tools.count_income_unity(session=session, unity=unity)
            elif key == "f_amount_members":
                data[key] = await count_unity_members(session=session, unity=unity)
            elif key == "f_members_not_have_amount_animals":
                data[key] = await get_members_not_have_amount_animals(
                    session=session,
                    idpk_unity=unity.idpk,
                    condition=data["amount_animals"],
                )
        if "next_lvl" in lvl_data[lvl]:
            data["next_lvl"] = lvl_data[lvl]["next_lvl"]
    return data


async def get_members_not_have_amount_animals(
    session: AsyncSession, idpk_unity: int, condition: int
) -> str:
    unity = await session.get(Unity, idpk_unity)
    members_idpk = await list_unity_member_ids(session=session, unity=unity)
    members_not_have_amount_animals = []
    for idpk in members_idpk:
        user = await session.get(User, idpk)
        animals = await tools.get_numbers_animals(self=user, session=session)
        is_have = all(i >= condition for i in animals) if animals else False
        if not is_have:
            members_not_have_amount_animals.append(user.nickname)
    if not members_not_have_amount_animals:
        return ""
    not_have = ", ".join(members_not_have_amount_animals)
    text = await tools.get_text_message(
        "pattern_not_have_amount_animals",
        not_have=not_have,
    )
    return text


async def get_row_unity_members(session: AsyncSession):
    row = await tools.get_value(session=session, value_name="ROW_UNITY_MEMBERS")
    return row


async def get_size_unity_members(session: AsyncSession):
    size = await tools.get_value(session=session, value_name="SIZE_UNITY_MEMBERS")
    return size


async def count_page_unity_members(session: AsyncSession, idpk_unity: int) -> int:
    size = await get_size_unity_members(session=session)
    unity = await session.get(Unity, idpk_unity)
    len_unity = await count_unity_members(session=session, unity=unity)
    remains = len_unity % size
    return len_unity // size + (1 if remains else 0)


async def get_members_name_and_idpk(
    session: AsyncSession, idpk_unity: int
) -> list[tuple[str, int]]:
    unity = await session.get(Unity, idpk_unity)
    members_idpk = await list_unity_member_ids(session=session, unity=unity)
    members = [await session.get(User, idpk) for idpk in members_idpk]
    members_name = [member.nickname for member in members]
    data = list(zip(members_name, members_idpk))
    return data


async def get_top_unity_by_animal(session: AsyncSession) -> tuple[int, dict]:
    table_for_compare = {}

    unites = await session.scalars(select(Unity))
    unites = unites.all()
    if not unites:
        return 0, {}

    user_ids = []
    for unity in unites:
        user_ids.extend(await list_unity_member_ids(session=session, unity=unity))
    if not user_ids:
        return 0, {}

    users = await session.execute(select(User).where(User.idpk.in_(user_ids)))
    users = {user.idpk: user for user in users.scalars().all()}

    for unity in unites:
        member_ids = await list_unity_member_ids(session=session, unity=unity)
        animals = defaultdict(int)
        for idpk in member_ids:
            user = users[int(idpk)]
            animals_user = await tools.get_dict_animals(self=user, session=session)
            for animal_name, num_animal in animals_user.items():
                animals[animal_name] += num_animal
        if not animals:
            continue
        max_animal = max(animals, key=animals.get)
        table_for_compare[unity.idpk] = {max_animal: animals[max_animal]}
    if not table_for_compare:
        return 0, {}

    top_unity = max(
        table_for_compare, key=lambda x: next(iter(table_for_compare[x].values()))
    )
    return int(top_unity), table_for_compare[top_unity]


async def get_unity_users(session: AsyncSession, unity: Unity) -> list[User]:
    users: list[User] = []
    for idpk in await list_unity_member_ids(session=session, unity=unity):
        user = await session.get(User, int(idpk))
        if user is not None:
            users.append(user)
    return users


async def check_condition_2nd_lvl(session: AsyncSession, unity: Unity) -> bool:
    AMOUNT_INCOME_2ND_LVL = int(
        await tools.get_value(session=session, value_name="AMOUNT_INCOME_2ND_LVL")
    )
    AMOUNT_ANIMALS_2ND_LVL = int(
        await tools.get_value(session=session, value_name="AMOUNT_ANIMALS_2ND_LVL")
    )
    users = await get_unity_users(session=session, unity=unity)
    total_income = sum(
        [await tools.income_(session=session, user=user) for user in users]
    )
    if total_income < AMOUNT_INCOME_2ND_LVL:
        return False

    for user in users:
        animal_counts = await tools.get_numbers_animals(self=user, session=session)
        if not animal_counts or any(
            num_animal < AMOUNT_ANIMALS_2ND_LVL for num_animal in animal_counts
        ):
            return False

    return True


async def check_condition_3rd_lvl(session: AsyncSession, unity: Unity) -> bool:
    AMOUNT_INCOME_3RD_LVL = int(
        await tools.get_value(session=session, value_name="AMOUNT_INCOME_3RD_LVL")
    )
    AMOUNT_ANIMALS_3RD_LVL = int(
        await tools.get_value(session=session, value_name="AMOUNT_ANIMALS_3RD_LVL")
    )
    AMOUNT_MEMBERS_3RD_LVL = int(
        await tools.get_value(session=session, value_name="AMOUNT_MEMBERS_3RD_LVL")
    )
    if await count_unity_members(session=session, unity=unity) < AMOUNT_MEMBERS_3RD_LVL:
        return False

    users = await get_unity_users(session=session, unity=unity)
    total_income = sum(
        [await tools.income_(session=session, user=user) for user in users]
    )
    if total_income < AMOUNT_INCOME_3RD_LVL:
        return False

    for user in users:
        animal_counts = await tools.get_numbers_animals(self=user, session=session)
        if not animal_counts or any(
            num_animal < AMOUNT_ANIMALS_3RD_LVL for num_animal in animal_counts
        ):
            return False

    return True


async def count_income_unity(session: AsyncSession, unity: Unity) -> int:
    total_income = 0
    users = [
        await session.get(User, int(idpk))
        for idpk in await list_unity_member_ids(session=session, unity=unity)
    ]
    total_income = sum(
        [await tools.income_(session=session, user=user) for user in users]
    )
    return total_income


async def fetch_unity(
    session: AsyncSession, idpk_unity: int | None
) -> Unity | tools.UnityPlug:
    return await session.get(Unity, idpk_unity) if idpk_unity else tools.UnityPlug()


def get_unity_idpk(current_unity: str | None):
    return current_unity.split(":")[-1] if current_unity else None
