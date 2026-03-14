import asyncio
import contextlib
import json
import logging
import random
from datetime import datetime, timedelta, timezone

from aiogram.utils.deep_linking import create_start_link
from bot.keyboards import ik_start_created_game, rk_main_menu
from config import CHAT_ID
from db import Game, Gamer, RandomMerchant, RequestToUnity, User, Value
from game_variables import (
    ID_AUTOGENERATE_MINI_GAME,
    MAX_AMOUNT_GAMERS,
    games,
    petard_emoji_effect,
)
from init_bot import bot
from init_db import _sessionmaker_for_func
from npc_agent.schedule import wake_all_npcs_now
from sqlalchemy import and_, delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    add_to_currency,
    factory_text_top_mini_game,
    fetch_and_parse_str_value,
    format_award_game,
    ft_place_winning_gamers,
    gen_key,
    get_current_amount_gamers,
    get_increase_rate_bank,
    get_nickname_game_owner,
    get_text_message,
    get_top_places_game,
    get_total_moves_game,
    get_value,
    get_weight_rate_bank,
    income_,
    referral_bonus,
    referrer_bonus,
    get_id_for_edit_message,
    factory_text_account_animals,
    formatter,
)

job_minute_lock = asyncio.Lock()


async def job_sec() -> None:
    await verification_referrals()
    # await test()
    # await add_bonus_to_users()
    # await ender_games(bot)


async def job_minute() -> None:
    second = datetime.now().second
    if second not in {0, 30, 50}:
        return

    if job_minute_lock.locked():
        return

    async with job_minute_lock:
        second = datetime.now().second

        if second == 30:
            async with _sessionmaker_for_func() as session:
                await updater_message_minigame(session=session)
            return

        if second == 50:
            async with _sessionmaker_for_func() as session:
                await ender_minigames(session=session)
            return

        if second != 0:
            return

        async with _sessionmaker_for_func() as session:
            await accrual_of_income(session=session)
            await update_rate_bank(session=session)
            await check_inaction(session=session)
            await deleter_request_to_unity(session=session)
            await session.commit()


async def verification_referrals():
    async with _sessionmaker_for_func() as session:
        users = await session.scalars(
            select(User).where(
                and_(User.id_referrer != None, User.referral_verification == False)
            )
        )
        users = users.all()
        QUANTITY_MOVES_TO_PASS = await get_value(
            session=session, value_name="QUANTITY_MOVES_TO_PASS"
        )
        QUANTITY_USD_TO_PASS = await get_value(
            session=session, value_name="QUANTITY_USD_TO_PASS"
        )
        for user in users:
            if user.moves < QUANTITY_MOVES_TO_PASS:
                continue
            if user.amount_expenses_usd < QUANTITY_USD_TO_PASS:
                continue
            referrer: User = await session.get(User, user.id_referrer)
            bonus = await referrer_bonus(session=session, referrer=referrer)
            await bot.send_message(
                chat_id=referrer.id_user,
                text=await get_text_message("you_got_bonus_referrer", bonus=bonus),
            )
            bonus = await referral_bonus(session=session, referral=user)
            await bot.send_message(
                chat_id=user.id_user,
                text=await get_text_message("you_got_bonus_referral", bonus=bonus),
            )
            user.referral_verification = True
        await session.commit()


async def reset_first_offer_bought() -> None:
    async with _sessionmaker_for_func() as session:
        await session.execute(delete(RandomMerchant))
        await wake_all_npcs_now(session=session, reason="merchant_reset")
        await session.commit()


async def add_bonus_to_users() -> None:
    async with _sessionmaker_for_func() as session:
        await session.execute(update(User).where(User.bonus == False).values(bonus=1))
        await wake_all_npcs_now(session=session, reason="daily_bonus_reset")
        await session.commit()


# async def reset_items_effect() -> None:
#     async with _sessionmaker_for_func() as session:
#         users = await session.scalars(select(User))
#         for user in users.all():
#             items: dict = json.loads(user.info_about_items)
#             reset_items = {
#                 k: {"is_activate": v["is_activate"]} for k, v in items.items()
#             }
#             user.info_about_items = json.dumps(reset_items)
#         await session.commit()


async def update_rate_bank(session: AsyncSession):
    weight_plus, weight_minus = await get_weight_rate_bank(session=session)
    increase_plus, increase_minus = await get_increase_rate_bank(session=session)
    current_rate = await get_value(
        session=session, value_name="RATE_RUB_USD", cache_=False
    )
    previous_rate = int(current_rate)
    sign = random.choices([1, -1], weights=[weight_plus, weight_minus])[0]
    if sign == 1:
        increase = random.choice(increase_plus)
        current_rate += increase
    elif sign == -1:
        increase = random.choice(increase_minus)
        current_rate -= increase
    MIN_RATE_RUB_USD = await get_value(session=session, value_name="MIN_RATE_RUB_USD")
    MAX_RATE_RUB_USD = await get_value(session=session, value_name="MAX_RATE_RUB_USD")
    current_rate = max(current_rate, MIN_RATE_RUB_USD)
    current_rate = min(current_rate, MAX_RATE_RUB_USD)
    await session.execute(
        update(Value).where(Value.name == "RATE_RUB_USD").values(value_int=current_rate)
    )

    # Keep lightweight rate history for NPC reasoning (last 24h max).
    history_raw = await get_value(
        session=session,
        value_name="RATE_RUB_USD_HISTORY_JSON",
        value_type="str",
        cache_=False,
    )
    try:
        history = json.loads(history_raw) if history_raw else []
        if not isinstance(history, list):
            history = []
    except Exception:
        history = []

    now_ts = int(datetime.now(timezone.utc).timestamp())
    history.append({"ts": now_ts, "rate": int(current_rate)})
    cutoff = now_ts - 24 * 3600
    history = [
        row
        for row in history
        if isinstance(row, dict)
        and int(row.get("ts", 0) or 0) >= cutoff
        and int(row.get("rate", 0) or 0) > 0
    ]
    history = history[-2000:]

    # value_str in DB is limited (String(4096)); store compact JSON and trim oldest
    # points until payload fits safely.
    max_payload_chars = 3900
    while True:
        payload = json.dumps(history, ensure_ascii=False, separators=(",", ":"))
        if len(payload) <= max_payload_chars or len(history) <= 1:
            break
        history = history[1:]

    await session.execute(
        update(Value)
        .where(Value.name == "RATE_RUB_USD_HISTORY_JSON")
        .values(value_str=payload)
    )

    return {
        "previous": int(previous_rate),
        "current": int(current_rate),
        "delta": int(current_rate) - int(previous_rate),
    }


async def accrual_of_income(
    session: AsyncSession,
):
    users = await session.scalars(select(User))
    users = users.all()
    for user in users:
        user.rub += await income_(session=session, user=user)


async def deleter_request_to_unity(session: AsyncSession):
    await session.execute(
        delete(RequestToUnity).where(RequestToUnity.date_request_end < datetime.now())
    )


async def create_game_for_chat():
    async with _sessionmaker_for_func() as session:
        members = await bot.get_chat_member_count(chat_id=CHAT_ID)
        award = await get_value(
            session=session, value_name="BANK_STORAGE", cache_=False, value_type="str"
        )
        award = int(float(award))
        if award == 0:
            return
        SEC_TO_EXPIRE_GAME = await get_value(
            session=session, value_name="SEC_TO_EXPIRE_GAME"
        )
        game = Game(
            id_game=f"game_{gen_key(length=12)}",
            idpk_user=ID_AUTOGENERATE_MINI_GAME,
            type_game=random.choice(list(games.keys())),
            amount_gamers=min(members // 2, MAX_AMOUNT_GAMERS),
            amount_award=award,
            currency_award="usd",
            end_date=datetime.now() + timedelta(seconds=SEC_TO_EXPIRE_GAME),
            amount_moves=random.randint(6, 12),
            source_chat_id=CHAT_ID,
        )
        session.add(game)
        await session.execute(
            update(Value).where(Value.name == "BANK_STORAGE").values(value_str=0)
        )
        award_text = format_award_game(award=award, award_currency="usd")
        msg = await bot.send_message(
            chat_id=CHAT_ID,
            text=await get_text_message(
                "info_game",
                nickname=(await bot.get_my_name()).name,
                game_type=game.type_game,
                amount_gamers=game.amount_gamers,
                amount_moves=game.amount_moves,
                award=award_text,
            ),
            disable_web_page_preview=True,
            reply_markup=await ik_start_created_game(
                link=await create_start_link(bot=bot, payload=game.id_game),
                total_gamers=game.amount_gamers,
                current_gamers=0,
            ),
        )
        game.id_mess = msg.message_id
        game.activate = True
        await wake_all_npcs_now(session=session, reason="chat_game_auto_created")
        await session.commit()


def _game_roll_max(game_type: str) -> int:
    # Telegram dice ranges differ by emoji. Keep conservative caps.
    if game_type in {"⚽️", "🏀"}:
        return 5
    return 6


async def autoplay_npc_gamers(session: AsyncSession, game: Game) -> int:
    npcs = (
        await session.execute(
            select(Gamer, User)
            .join(User, User.idpk == Gamer.idpk_gamer)
            .where(
                Gamer.id_game == game.id_game,
                Gamer.moves > 0,
                Gamer.game_end == False,  # noqa: E712
                User.id_user < 0,
            )
        )
    ).all()

    played = 0
    max_roll = _game_roll_max(game.type_game)
    for gamer, _user in npcs:
        remaining = int(gamer.moves or 0)
        if remaining <= 0:
            continue
        gamer.score += sum(random.randint(1, max_roll) for _ in range(remaining))
        gamer.moves = 0
        gamer.game_end = True
        played += remaining
    return played


async def ender_minigames(session: AsyncSession):
    all_games = await session.scalars(
        select(Game).where(and_(Game.end == False, Game.last_update_mess == True))
    )
    for game in all_games:
        await autoplay_npc_gamers(session=session, game=game)
        game.end = True
        await session.execute(
            update(Gamer).where(Gamer.id_game == game.id_game).values(game_end=True)
        )
        await add_award_and_send_message(session=session, game=game)
    await session.commit()


async def updater_message_minigame(session: AsyncSession):
    all_games = (
        await session.scalars(select(Game).where(Game.last_update_mess == False))
    ).all()
    now = datetime.now()
    for game in all_games:
        await autoplay_npc_gamers(session=session, game=game)
        if (
            await get_current_amount_gamers(session=session, id_game=game.id_game)
            != game.amount_gamers
            and game.end_date > now
        ):
            continue
        if (
            await get_total_moves_game(session=session, id_game=game.id_game) != 0
            and game.end_date > now
        ):
            continue
        game.last_update_mess = True
        await edit_text_game_in_chat(session, game)
    await session.commit()


async def add_award_and_send_message(session: AsyncSession, game: Game):
    winning_gamers: list[Gamer] = await get_top_places_game(
        session=session, id_game=game.id_game
    )
    if not winning_gamers:
        return

    percentage_of_award_by_place: list[int] = await fetch_and_parse_str_value(
        session=session, value_name="PERCENT_PLACES_AWARD"
    )
    winning_users: list[User] = [
        await session.get(User, gamer.idpk_gamer) for gamer in winning_gamers
    ]
    for place_in_top, user in enumerate(winning_users, start=1):
        if (
            len(winning_gamers) == place_in_top
            and len(percentage_of_award_by_place) > 1
        ):
            award = int(game.amount_award) * (sum(percentage_of_award_by_place) / 100)
        else:
            percentage_of_award = percentage_of_award_by_place.pop(0)
            award = int(game.amount_award) * (percentage_of_award / 100)
        await add_to_currency(
            self=user,
            currency=game.currency_award,
            amount=award,
        )
        award_text = int(award)
        try:
            await bot.send_message(
                chat_id=user.id_user,
                text=await get_text_message(
                    "game_winer_message",
                    award=award_text,
                ),
                message_effect_id=petard_emoji_effect,
                reply_markup=await rk_main_menu(user_id=user.id_user),
            )
        except Exception:
            logging.exception(
                "Failed to send game_winer_message", extra={"id_user": user.id_user}
            )


async def edit_text_game_in_chat(session: AsyncSession, game: Game):
    winning_gamers: list[Gamer] = await get_top_places_game(
        session=session, id_game=game.id_game
    )
    nickname = await get_nickname_game_owner(
        session=session, idpk_game_owner=game.idpk_user, bot=bot
    )
    additional_message_parameters = get_id_for_edit_message(game.id_mess)
    award = format_award_game(game.amount_award, game.currency_award)
    text_for_top_mini_game = await factory_text_top_mini_game(
        session=session, id_game=game.id_game
    )
    if not winning_gamers:
        with contextlib.suppress(Exception):
            await bot.edit_message_text(
                text=await get_text_message(
                    "game_end_without_winning_gamers",
                    t=text_for_top_mini_game,
                    nickname=nickname,
                    game_type=game.type_game,
                    award=award,
                ),
                reply_markup=None,
                disable_web_page_preview=True,
                **additional_message_parameters,
            )
        return

    text_winning_gamers = await ft_place_winning_gamers(session, winning_gamers)
    with contextlib.suppress(Exception):
        await bot.edit_message_text(
            text=await get_text_message(
                "game_end_with_winning_gamers",
                t=text_for_top_mini_game,
                nickname=nickname,
                game_type=game.type_game,
                amount_gamers=game.amount_gamers,
                amount_moves=game.amount_moves,
                award=award,
                winning_gamers=text_winning_gamers,
            ),
            reply_markup=None,
            disable_web_page_preview=True,
            **additional_message_parameters,
        )


async def check_inaction(session: AsyncSession):
    return
