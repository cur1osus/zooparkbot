from db import User, Value
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

import tools


async def get_rate(session: AsyncSession, user: User):
    rate = await tools.get_value(
        session=session, value_name="RATE_RUB_USD", cache_=False
    )
    if v := tools.get_value_prop_from_iai(
        info_about_items=user.info_about_items, name_prop="exchange_bank"
    ):
        rate = rate * (1 - (v / 100))
    return int(rate)


async def update_bank_storage(session: AsyncSession, amount: int):
    old_storage = await tools.get_value(
        session=session, value_name="BANK_STORAGE", value_type="str", cache_=False
    )
    old_storage = float(old_storage)
    new_storage = amount + old_storage
    await session.execute(
        update(Value)
        .where(Value.name == "BANK_STORAGE")
        .values(value_str=str(new_storage))
    )


async def exchange(
    session: AsyncSession, user: User, amount: int, rate: int, all: bool = True
):
    BANK_PERCENT_FEE = await tools.get_value(
        session=session, value_name="BANK_PERCENT_FEE"
    )
    you_got, remains = divmod(amount, rate)
    you_change = you_got * rate
    bank_fee = int(you_got * (BANK_PERCENT_FEE / 100))
    bank_fee = bank_fee if bank_fee > 0 else 1 if you_got > 1 else 0
    if bank_fee:
        await update_bank_storage(session, bank_fee)
    you_got = you_got - bank_fee
    if all:
        user.rub = remains
    else:
        user.rub -= amount - remains
    return you_change, bank_fee if bank_fee > 0 else None, you_got


async def get_weight_rate_bank(session: AsyncSession) -> list[float]:
    weight_rate = await tools.get_value(
        session=session, value_name="WEIGHT_RATE_BANK", value_type="str"
    )
    try:
        values = [float(i.strip()) for i in weight_rate.split(",") if i.strip()]
    except ValueError:
        values = []
    if len(values) < 2:
        return [1.0, 1.0]
    return values[:2]


async def get_increase_rate_bank(
    session: AsyncSession,
) -> tuple[list[int], list[int]]:
    increase_rate_plus = await tools.get_value(
        session=session, value_name="INCREASE_PLUS_RATE_BANK", value_type="str"
    )
    increase_rate_minus = await tools.get_value(
        session=session, value_name="INCREASE_MINUS_RATE_BANK", value_type="str"
    )
    try:
        increase_rate_plus = [
            int(i.strip()) for i in increase_rate_plus.split(",") if i.strip()
        ]
    except ValueError:
        increase_rate_plus = []
    try:
        increase_rate_minus = [
            int(i.strip()) for i in increase_rate_minus.split(",") if i.strip()
        ]
    except ValueError:
        increase_rate_minus = []
    if not increase_rate_plus:
        increase_rate_plus = [1]
    if not increase_rate_minus:
        increase_rate_minus = [1]
    return increase_rate_plus, increase_rate_minus
