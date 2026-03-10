import html
import re

from db import Item, User
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession


async def shorten_whitespace_nickname(nickname: str) -> str:
    """Убирает лишние пробелы и делает никнейм однострочным"""
    return re.sub(r"\s+", " ", nickname).strip()


async def has_special_characters_nickname(nickname: str) -> str | None:
    """Возвращает специальные символы, если они есть в никнейме"""
    # Паттерн для поиска специальных символов
    pattern = r"[^a-zA-Zа-яА-Я0-9\-\ ]"
    special_chars = re.findall(pattern, html.unescape(nickname))
    return "".join(special_chars) if special_chars else None


async def is_unique_nickname(session: AsyncSession, nickname: str) -> bool:
    """Проверяет, что никнейм пользователя уникален"""
    count_users = await session.scalar(
        select(func.count())
        .select_from(User)
        .where(func.lower(User.nickname) == nickname.lower())
    )
    return not count_users


async def view_nickname(session: AsyncSession, user: User):
    emojis = await session.scalars(
        select(Item.emoji).where(
            and_(Item.id_user == user.id_user, Item.is_active == True)  # noqa: E712
        )
    )
    emojis = "".join(emojis.all())
    return f"{user.nickname} [{emojis}]" if emojis else user.nickname
