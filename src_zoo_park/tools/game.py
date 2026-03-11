from decimal import Decimal

from aiogram import Bot
from db import Game, Gamer, User
from game_variables import ID_AUTOGENERATE_MINI_GAME, translated_currencies
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

import tools


async def get_current_amount_gamers(session: AsyncSession, id_game: str):
    gamers = await session.scalars(select(Gamer).where(Gamer.id_game == id_game))
    return len(gamers.all())


async def get_total_moves_game(session: AsyncSession, id_game: str):
    gamers = await session.scalars(select(Gamer).where(Gamer.id_game == id_game))
    return sum(gamer.moves for gamer in gamers)


async def get_user_where_max_score(session: AsyncSession, game: Game):
    gamers = await session.scalars(select(Gamer).where(Gamer.id_game == game.id_game))
    gamers_sorted = sorted(gamers, key=lambda gamer: gamer.score, reverse=True)
    return gamers_sorted[0].idpk_gamer if gamers_sorted else None


async def get_top_places_game(session: AsyncSession, id_game: str, num_places: int = 3):
    gamers = await session.scalars(select(Gamer).where(Gamer.id_game == id_game))
    gamers_sorted = sorted(gamers, key=lambda gamer: gamer.score, reverse=True)
    return gamers_sorted[:3] if gamers else None


async def get_nickname_game_owner(
    session: AsyncSession, idpk_game_owner: int, bot: Bot
):
    if idpk_game_owner == ID_AUTOGENERATE_MINI_GAME:
        return (await bot.get_my_name()).name
    game_owner = await session.get(User, idpk_game_owner)
    if not game_owner:
        return "Unknown"
    # NPCs should always be shown by their in-game nickname (e.g., ИИван), not bot/system identity.
    if int(game_owner.id_user) < 0:
        return game_owner.nickname
    if not game_owner.username:
        return game_owner.nickname
    nickname = tools.mention_html_by_username(
        username=game_owner.username, name=game_owner.nickname
    )
    return nickname


async def get_gamer(session: AsyncSession, idpk_gamer: int, id_game: str):
    gamer = await session.scalar(
        select(Gamer).where(
            and_(Gamer.id_game == id_game, Gamer.idpk_gamer == idpk_gamer)
        )
    )
    return gamer


async def gamer_have_active_game(session: AsyncSession, idpk_gamer: int):
    gamer = await session.scalar(
        select(Gamer).where(
            and_(Gamer.idpk_gamer == idpk_gamer, Gamer.game_end == False)  # noqa: E712
        )
    )
    return bool(gamer)


def format_award_game(award: Decimal, award_currency: str):
    award = tools.formatter.format_large_number(int(award))
    return f"{award}{translated_currencies[award_currency]}"


