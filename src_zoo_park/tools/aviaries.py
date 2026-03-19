from db import Aviary, User
from db.structured_state import get_user_aviaries_map, upsert_user_aviary
from fastjson import loads_or_default
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import tools


async def get_name_and_code_name(session: AsyncSession):
    aviaries = await session.scalars(select(Aviary))
    return [(aviary.name_with_size, aviary.code_name) for aviary in aviaries]


async def get_total_number_seats(
    session: AsyncSession,
    aviaries: str | dict,
) -> int:
    decoded_dict: dict = (
        aviaries if isinstance(aviaries, dict) else loads_or_default(aviaries, {})
    )
    all_seats = 0
    for key, value in decoded_dict.items():
        all_seats += (
            await session.scalar(select(Aviary.size).where(Aviary.code_name == key))
            * value["quantity"]
        )
    return all_seats


async def get_remain_seats(session: AsyncSession, user: User) -> int:
    aviary_map = await get_user_aviaries_map(session=session, user=user)
    all_seats = await get_total_number_seats(session=session, aviaries=aviary_map)
    amount_animals = await tools.get_total_number_animals(self=user, session=session)
    return all_seats - amount_animals


async def add_aviary(
    session: AsyncSession,
    self: User,
    code_name_aviary: str,
    quantity: int,
    is_buy: bool = True,
) -> None:
    INCREASE_FOR_AVIARY = await tools.get_value(
        session, value_name="INCREASE_FOR_AVIARY"
    )
    decoded_dict = await get_user_aviaries_map(session=session, user=self)

    if code_name_aviary in decoded_dict and is_buy:
        current_row = decoded_dict[code_name_aviary]
        new_price = int(current_row["price"] * (1 + int(INCREASE_FOR_AVIARY) / 100))
        await upsert_user_aviary(
            session=session,
            user=self,
            aviary_code_name=code_name_aviary,
            quantity_delta=quantity,
            buy_count_delta=1,
            current_price=new_price,
        )
        return
    if code_name_aviary in decoded_dict:
        await upsert_user_aviary(
            session=session,
            user=self,
            aviary_code_name=code_name_aviary,
            quantity_delta=quantity,
        )
        return
    price_aviary = await session.scalar(
        select(Aviary.price).where(Aviary.code_name == code_name_aviary)
    )
    await upsert_user_aviary(
        session=session,
        user=self,
        aviary_code_name=code_name_aviary,
        quantity_delta=quantity,
        buy_count_delta=1,
        current_price=int(price_aviary or 0),
    )


async def get_price_aviaries(
    session: AsyncSession,
    aviaries: str | dict,
    code_name_aviary: str,
    info_about_items: str,
) -> int:
    aviaries = (
        aviaries if isinstance(aviaries, dict) else loads_or_default(aviaries, {})
    )
    aviary_price = 0
    if aviaries.get(code_name_aviary):
        aviary_price = aviaries[code_name_aviary]["price"]
    else:
        aviary_price = await session.scalar(
            select(Aviary.price).where(Aviary.code_name == code_name_aviary)
        )
    if v := tools.get_value_prop_from_iai(
        info_about_items=info_about_items, name_prop="aviaries_sale"
    ):
        aviary_price = aviary_price * (1 - v / 100)
    return int(aviary_price)
