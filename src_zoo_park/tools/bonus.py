import random
from dataclasses import dataclass

from db import Aviary, User
from game_variables import types_bonus
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import tools


async def referral_bonus(session: AsyncSession, referral: User):
    bonus = await tools.get_value(session=session, value_name="REFERRAL_BONUS")
    referral.usd += bonus
    await session.commit()
    return bonus


async def referrer_bonus(session: AsyncSession, referrer: User):
    bonus = await tools.get_value(session=session, value_name="REFERRER_BONUS")
    referrer.usd += bonus
    await session.commit()
    return bonus


async def bonus_for_sub_on_chat(session: AsyncSession, user: User):
    bonus = await tools.get_value(
        session=session, value_name="SUBSCRIPTION_BONUS_ON_CHAT"
    )
    user.usd += bonus
    await session.commit()
    return bonus


async def bonus_for_sub_on_channel(session: AsyncSession, user: User):
    bonus = await tools.get_value(
        session=session, value_name="SUBSCRIPTION_BONUS_ON_CHANNEL"
    )
    user.usd += bonus
    return bonus


async def handle_rub_bonus(user, session, **kwargs) -> int:
    income = await tools.income_(session=session, user=user)
    if income == 0:
        income = 12
    income_for_8_hour = income * 480
    rub_to_add = random.randint(income_for_8_hour // 3, income_for_8_hour)
    return rub_to_add


async def handle_paw_coins(user, session, **kwargs) -> int:
    paw_coins_to_add = random.randint(1, 50)
    return paw_coins_to_add


async def handle_usd_bonus(user, session, **kwargs) -> int:
    weights = await tools.fetch_and_parse_str_value(
        session=session, value_name="WEIGHTS_FOR_BONUS_USD", func_to_element=float
    )
    values_usd_bonus = await tools.fetch_and_parse_str_value(
        session=session, value_name="TYPES_USD_BONUS"
    )
    usd_to_add = random.choices(population=values_usd_bonus, weights=weights)[0]
    return usd_to_add


async def handle_aviary_bonus(user, session, **kwargs) -> tuple[dict, int]:
    types_aviaries = list(await session.scalars(select(Aviary)))
    aviary_to_add: Aviary = random.choice(types_aviaries)
    amount_to_add = random.randint(1, 5)
    return aviary_to_add.as_dict(), amount_to_add


async def handle_animal_bonus(
    user: User, session: AsyncSession, **kwargs
) -> tuple[dict, int]:
    animal = await tools.get_random_animal(session=session, user=user)
    THRESHOLD_MIN = 1
    THRESHOLD_MAX = min(20, kwargs["remain_seats"])
    amount_to_add = random.randint(
        THRESHOLD_MIN,
        THRESHOLD_MAX,
    )
    return animal.as_dict(), amount_to_add


# async def handle_item_bonus(user: User, session) -> dict:
#     items = list(
#         await session.scalars(select(Item).where(Item.currency != "paw_coins"))
#     )
#     item_to_add: Item = random.choice(items)
#     return item_to_add.as_dict()


@dataclass
class DataBonus:
    bonus_type: str
    result_func: any


async def get_bonus(session: AsyncSession, user: User) -> DataBonus:
    weights = await tools.fetch_and_parse_str_value(
        session=session, value_name="WEIGHTS_FOR_BONUS", func_to_element=float
    )
    bonus_type = random.choices(population=types_bonus, weights=weights)[0]
    args = {
        "user": user,
        "session": session,
    }
    if bonus_type == "animal":
        remain_seats = await tools.get_remain_seats(session=session, user=user)
        if remain_seats == 0:
            bonus_type = random.choices(population=types_bonus, weights=weights)[0]
        else:
            args["remain_seats"] = remain_seats
    # if bonus_type == "item":
    #     item = await handle_item_bonus(**args)
    #     if item["code_name"] in user.info_about_items:
    #         return DataBonus(
    #             bonus_type=item["currency"], result_func=item["price"] // 2
    #         )
    handlers = {
        "rub": handle_rub_bonus,
        "usd": handle_usd_bonus,
        "paw_coins": handle_paw_coins,
        "aviary": handle_aviary_bonus,
        "animal": handle_animal_bonus,
        # "item": handle_item_bonus,
    }
    return DataBonus(
        bonus_type=bonus_type,
        result_func=await handlers[bonus_type](**args),
    )


async def apply_bonus(session: AsyncSession, user: User, data_bonus: DataBonus):
    match data_bonus.bonus_type:
        case "rub":
            user.rub += data_bonus.result_func
        case "usd":
            user.usd += data_bonus.result_func
        case "aviary":
            await tools.add_aviary(
                session=session,
                self=user,
                code_name_aviary=data_bonus.result_func[0]["code_name"],
                quantity=data_bonus.result_func[1],
                is_buy=False,
            )
        case "animal":
            await tools.add_animal(
                self=user,
                code_name_animal=data_bonus.result_func[0]["code_name"],
                quantity=data_bonus.result_func[1],
                session=session,
            )
        # case "item":
        #     await tools.add_item(
        #         session=session,
        #         self=user,
        #         code_name_item=data_bonus.result_func["code_name"],
        #     )
        case "paw_coins":
            user.paw_coins += data_bonus.result_func
