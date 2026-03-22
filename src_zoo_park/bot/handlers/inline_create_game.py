import random
from datetime import datetime, timedelta

from aiogram import F, Router
from aiogram.types import (
    ChosenInlineResult,
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
)
from aiogram.utils.deep_linking import create_start_link
from bot.keyboards import (
    ik_start_created_game,
)
from db import Game, User
from game_variables import games, translated_currencies
from npc_agent.schedule import wake_all_npcs_now
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    add_to_amount_expenses_currency,
    add_to_currency,
    find_integers,
    gen_key,
    get_currency,
    get_text_message,
    get_value,
    mention_html_by_username,
)

router = Router()
MAX_AMOUNT_GAMERS = 80


async def create_inline_query_result(
    title_key: str,
    description_key: str,
    message_text_key: str,
    photo_url: str = None,
):
    return InlineQueryResultArticle(
        id=str(random.randint(1, 100000)),
        title=await get_text_message(title_key),
        description=await get_text_message(description_key),
        thumbnail_url=photo_url,
        input_message_content=InputTextMessageContent(
            message_text=await get_text_message(message_text_key)
        ),
    )


@router.inline_query(F.query.split()[0].in_(games.keys()))
async def inline_game_three_pm(
    inline_query: InlineQuery,
    session: AsyncSession,
    user: User | None,
):
    if not user:
        r = await create_inline_query_result(
            title_key="attention",
            description_key="press_start_to_play",
            message_text_key="press_start_to_play",
        )
        await inline_query.answer(results=[r], cache_time=0)
        return

    split_query = inline_query.query.split()
    amount_params = len(split_query)
    attention_photo = "https://avatars.yandex.net/get-music-content/2433207/64c60238.a.12011003-1/m1000x1000?webp=false"
    if amount_params == 1:
        r = await create_inline_query_result(
            title_key="point_1",
            description_key="enter_amount_gamers",
            message_text_key="error_enter_amount_gamers",
            photo_url="https://www.clipartmax.com/png/full/219-2197586_organization-training-business-learning-technology-no-1-icon-png.png",
        )
        return await inline_query.answer(results=[r], cache_time=0)

    if not split_query[1].isdigit() or int(split_query[1]) < 2:
        description_key = (
            "amount_gamers_too_small"
            if split_query[1].isdigit()
            else "enter_amount_gamers"
        )
        message_text_key = (
            "error_amount_gamers_too_small"
            if split_query[1].isdigit()
            else "error_enter_amount_gamers"
        )
        r = await create_inline_query_result(
            title_key="attention",
            description_key=description_key,
            message_text_key=message_text_key,
            photo_url=attention_photo,
        )
        return await inline_query.answer(results=[r], cache_time=0)

    gamers = int(split_query[1])
    if gamers > MAX_AMOUNT_GAMERS:
        r = await create_inline_query_result(
            title_key="attention",
            description_key="amount_gamers_too_big",
            message_text_key="error_amount_gamers_too_big",
            photo_url=attention_photo,
        )
        return await inline_query.answer(results=[r], cache_time=0)

    if amount_params == 2:
        r = await create_inline_query_result(
            title_key="point_2",
            description_key="enter_prise_for_win",
            message_text_key="error_enter_prise_for_win",
            photo_url="https://cdn-ru.bitrix24.ru/b9251457/landing/7b7/7b7c7b41a9e0c33f1867fea82a8d5c08/Resurs_20_s_1x.png",
        )
        return await inline_query.answer(results=[r], cache_time=0)

    if (
        not await find_integers(split_query[2])
        or await find_integers(split_query[2]) < 1
    ):
        description_key = (
            "enter_prise_small"
            if await find_integers(split_query[2])
            else "enter_prise_for_win"
        )
        message_text_key = (
            "error_enter_prise_small"
            if await find_integers(split_query[2])
            else "error_enter_prise_for_win"
        )
        r = await create_inline_query_result(
            title_key="attention",
            description_key=description_key,
            message_text_key=message_text_key,
            photo_url=attention_photo,
        )
        return await inline_query.answer(results=[r], cache_time=0)

    if amount_params == 3:
        r = await create_inline_query_result(
            title_key="point_3_",
            description_key="enter_currency_to_price",
            message_text_key="error_enter_currency_to_price",
            photo_url="https://upload.wikimedia.org/wikipedia/commons/thumb/0/01/Paris_transit_icons_-_Métro_3.svg/1200px-Paris_transit_icons_-_Métro_3.svg.png",
        )
        return await inline_query.answer(results=[r], cache_time=0)

    if split_query[3] not in ["rub", "usd"]:
        r = await create_inline_query_result(
            title_key="attention",
            description_key="enter_currency_to_price",
            message_text_key="error_enter_currency_to_price",
            photo_url=attention_photo,
        )
        return await inline_query.answer(results=[r], cache_time=0)

    award = await find_integers(split_query[2])
    currency = await get_currency(self=user, currency=split_query[3])
    if currency < award:
        r = InlineQueryResultArticle(
            id=str(random.randint(1, 100000)),
            title=await get_text_message("attention"),
            description=await get_text_message(
                "not_money_to_create_game", money=currency
            ),
            thumbnail_url=attention_photo,
            input_message_content=InputTextMessageContent(
                message_text=await get_text_message("error_not_money_to_create_game")
            ),
        )
        return await inline_query.answer(results=[r], cache_time=0)

    SEC_TO_EXPIRE_GAME = await get_value(
        session=session, value_name="SEC_TO_EXPIRE_GAME"
    )
    game = Game(
        id_game=f"game_{gen_key(length=12)}",
        idpk_user=user.idpk,
        type_game=split_query[0],
        amount_gamers=gamers,
        amount_award=award,
        currency_award=split_query[3],
        end_date=datetime.now() + timedelta(seconds=SEC_TO_EXPIRE_GAME),
        amount_moves=random.randint(1, 10),
        source_chat_id=-1 if getattr(inline_query, "chat_type", "") in ("sender", "private") else 0,
    )
    session.add(game)
    await session.commit()
    nickname = (
        mention_html_by_username(username=user.username, name=user.nickname)
        if user.nickname
        else user.nickname
    )
    c = translated_currencies[game.currency_award]
    award = f"{game.amount_award:,d}{c}"
    r = InlineQueryResultArticle(
        id=f"{game.idpk}:game",
        title=await get_text_message("point_4"),
        description=await get_text_message("create_game"),
        thumbnail_url="https://chrleader.com/wp-content/uploads/2019/05/press-start.jpg",
        input_message_content=InputTextMessageContent(
            message_text=await get_text_message(
                "info_game",
                nickname=nickname,
                game_type=game.type_game,
                amount_gamers=game.amount_gamers,
                amount_moves=game.amount_moves,
                award=award,
            ),
            disable_web_page_preview=True,
        ),
        reply_markup=await ik_start_created_game(
            link=await create_start_link(bot=inline_query.bot, payload=game.id_game),
            total_gamers=game.amount_gamers,
            current_gamers=0,
        ),
    )

    await inline_query.answer(results=[r], cache_time=0)


@router.chosen_inline_result(F.result_id.split(":")[-1] == "game")
async def game_activate(chosen_result: ChosenInlineResult, session: AsyncSession):
    idpk_game, _ = chosen_result.result_id.split(":")
    game = await session.get(Game, int(idpk_game))
    if not game:
        return

    user = await session.get(User, game.idpk_user)
    if not user:
        return

    await add_to_currency(
        self=user,
        currency=game.currency_award,
        amount=-game.amount_award,
    )
    await add_to_amount_expenses_currency(
        self=user,
        currency=game.currency_award,
        amount=game.amount_award,
    )

    game.activate = True
    game.id_mess = chosen_result.inline_message_id

    await session.flush()
    await wake_all_npcs_now(session=session, reason="chat_game_created")
    await session.execute(delete(Game).where(Game.activate == False))
    await session.commit()
