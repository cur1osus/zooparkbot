from cache import value_cache
from db import Value
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession


async def get_value(
    session: AsyncSession, value_name: str, value_type: str = "int", cache_: bool = True
):
    cache_key = f"{value_type}:{value_name}"
    if cache_ and cache_key in value_cache:
        return value_cache[cache_key]

    if value_type == "int":
        value = await session.scalar(
            select(Value.value_int).where(Value.name == value_name)
        )
        if value is None:
            await session.execute(
                insert(Value).values(name=value_name, value_int=1, value_str="-")
            )
            await session.commit()
            value = 1
    elif value_type == "str":
        value = await session.scalar(
            select(Value.value_str).where(Value.name == value_name)
        )
        if value is None:
            await session.execute(
                insert(Value).values(name=value_name, value_int=0, value_str="0")
            )
            await session.commit()
            value = "0"

    if cache_:
        value_cache[cache_key] = value

    return value
