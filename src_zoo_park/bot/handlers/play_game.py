import asyncio
import contextlib

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    CallbackQuery,
)
from aiogram.utils.deep_linking import create_start_link
from bot.keyboards import (
    ik_button_play,
    ik_start_created_game,
    rk_main_menu,
)
from bot.states import UserState
from config import CHAT_ID
from db import Game, Gamer, User
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    factory_text_top_mini_game,
    get_current_amount_gamers,
    get_text_message,
    get_value_prop_from_iai,
    get_id_for_edit_message,
)

router = Router()
petard_emoji_effect = "5046509860389126442"


async def handle_game_end(
    query: CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    user: User,
):
    # link = create_telegram_link(chat_username, str(message_id)) if chat_username else ""
    await query.message.answer(
        text=await get_text_message("game_end"),
        reply_markup=await rk_main_menu(user_id=user.id_user),
        disable_web_page_preview=False,
    )
    await state.clear()
    await state.set_state(UserState.main_menu)


@router.callback_query(UserState.game, F.data == "dice")
async def play_game(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    data = await state.get_data()
    game = await session.get(Game, data["idpk_game"])
    await query.message.delete_reply_markup()
    msg = await query.message.answer_dice(emoji=game.type_game)
    value_dice = msg.dice.value
    await asyncio.sleep(4)

    gamer = await session.scalar(
        select(Gamer).where(
            and_(Gamer.idpk_gamer == user.idpk, Gamer.id_game == game.id_game)
        )
    )
    gamer.score += value_dice
    gamer.moves -= 1

    if gamer.moves == 0:
        await query.message.answer(
            text=await get_text_message("you_got", value_dice=value_dice)
        )
        if v := get_value_prop_from_iai(
            info_about_items=user.info_about_items, name_prop="last_chance"
        ):
            gamer.score += v
            await query.message.answer(
                text=await get_text_message("you_got_bonus_item", value=v)
            )
        await query.message.answer(
            text=await get_text_message(
                "play_game",
                score=gamer.score,
            )
        )
        await handle_game_end(
            query=query,
            state=state,
            session=session,
            user=user,
        )
    else:
        await query.message.answer(
            text=await get_text_message("you_got", value_dice=value_dice)
        )
        await query.message.answer(
            text=await get_text_message(
                "play_game",
                score=gamer.score,
            ),
            reply_markup=await ik_button_play(
                game_type=game.type_game,
                total_moves=game.amount_moves,
                remain_moves=gamer.moves,
            ),
        )

    additional_message_parameters = get_id_for_edit_message(id_message=game.id_mess)
    text_for_top_mini_game = await factory_text_top_mini_game(
        session=session, id_game=game.id_game
    )
    with contextlib.suppress(Exception):
        await query.message.bot.edit_message_text(
            text=await get_text_message(
                "game_start",
                t=text_for_top_mini_game,
                nickname=data["nickname"],
                game_type=data["type_game"],
                amount_gamers=data["amount_gamers"],
                amount_moves=data["amount_moves"],
                award=data["award"],
            ),
            reply_markup=await ik_start_created_game(
                link=await create_start_link(bot=query.bot, payload=game.id_game),
                total_gamers=data["amount_gamers"],
                current_gamers=await get_current_amount_gamers(
                    session=session, id_game=game.id_game
                ),
            ),
            disable_web_page_preview=True,
            **additional_message_parameters,
        )
    await session.commit()


@router.callback_query(UserState.game, F.data == "dice_autopilot")
async def play_game_autopilot(
    query: CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    user: User,
):
    data = await state.get_data()
    await query.message.delete_reply_markup()
    game = await session.get(Game, data["idpk_game"])
    gamer_moves = await session.scalar(
        select(Gamer.moves).where(
            and_(Gamer.idpk_gamer == user.idpk, Gamer.id_game == game.id_game)
        )
    )
    while gamer_moves > 0:
        game = await session.get(Game, data["idpk_game"])
        gamer = await session.scalar(
            select(Gamer).where(
                and_(Gamer.idpk_gamer == user.idpk, Gamer.id_game == game.id_game)
            )
        )
        msg = await query.message.answer_dice(
            emoji=game.type_game, disable_notification=True
        )
        value_dice = msg.dice.value
        gamer.score += value_dice
        gamer.moves -= 1
        gamer_moves = gamer.moves
        await session.commit()
        await asyncio.sleep(4)
        await query.message.answer(
            text=await get_text_message("you_got", value_dice=value_dice),
            disable_notification=True,
        )
        await asyncio.sleep(data["delay_autopilot"])
        mess_data = (
            {"chat_id": CHAT_ID, "message_id": game.id_mess}
            if game.id_mess.isdigit()
            else {"inline_message_id": game.id_mess}
        )
        with contextlib.suppress(Exception):
            t = await factory_text_top_mini_game(session=session, game=game)
            await query.message.bot.edit_message_text(
                text=await get_text_message(
                    "game_start",
                    t=t,
                    nickname=data["nickname"],
                    game_type=data["type_game"],
                    amount_gamers=data["amount_gamers"],
                    amount_moves=data["amount_moves"],
                    award=data["award"],
                ),
                reply_markup=await ik_start_created_game(
                    link=await create_start_link(bot=query.bot, payload=game.id_game),
                    total_gamers=data["amount_gamers"],
                    current_gamers=await get_current_amount_gamers(
                        session=session, game=game
                    ),
                ),
                disable_web_page_preview=True,
                **mess_data,
            )
    await query.message.answer(
        text=await get_text_message(
            "play_game",
            score=gamer.score,
        )
    )
    await handle_game_end(
        query=query,
        state=state,
        session=session,
        user=user,
    )
    await session.commit()
