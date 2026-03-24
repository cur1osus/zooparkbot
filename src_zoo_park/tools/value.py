from cache import value_cache
from db import Value
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession


_BULK_LOADED = False


async def preload_values(session: AsyncSession):
    """Bulk load all values into memory cache to avoid N+1 queries later."""
    global _BULK_LOADED
    rows = await session.scalars(select(Value))
    for row in rows.all():
        value_cache[f"int:{row.name}"] = int(row.value_int)
        value_cache[f"str:{row.name}"] = str(row.value_str)
    _BULK_LOADED = True


async def get_value(
    session: AsyncSession, value_name: str, value_type: str = "int", cache_: bool = True
):
    cache_key = f"{value_type}:{value_name}"
    
    # Fast path: in-memory cache
    if cache_ and cache_key in value_cache:
        return value_cache[cache_key]

    # Preload all if not yet done (one trip to DB instead of many)
    if not _BULK_LOADED:
        await preload_values(session)
        if cache_ and cache_key in value_cache:
            return value_cache[cache_key]

    # Slow path: individual fetch and upsert
    if value_type == "int":
        value = await session.scalar(
            select(Value.value_int).where(Value.name == value_name)
        )
        if value is None:
            # Upsert logic - only commit once
            new_val = Value(name=value_name, value_int=1, value_str="-")
            session.add(new_val)
            await session.flush()  # Use flush instead of commit to be part of caller's transaction
            value = 1
        else:
            value = int(value)
    elif value_type == "str":
        value = await session.scalar(
            select(Value.value_str).where(Value.name == value_name)
        )
        if value is None:
            new_val = Value(name=value_name, value_int=0, value_str="0")
            session.add(new_val)
            await session.flush()
            value = "0"
    else:
        value = None

    if cache_ and value is not None:
        value_cache[cache_key] = value

    return value
