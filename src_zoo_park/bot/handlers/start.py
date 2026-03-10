import contextlib
from datetime import datetime

from aiogram import F, Router
from aiogram.filters import CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, ReplyKeyboardRemove
from aiogram.utils.deep_linking import create_start_link
from bot.keyboards import ik_button_play, ik_start_created_game, rk_main_menu
from bot.states import UserState
from config import ADMIN_ID, CHAT_ID
from db import Game, Gamer, User
from game_variables import translated_currencies
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    factory_text_top_mini_game,
    formatter,
    gamer_have_active_game,
    get_current_amount_gamers,
    get_gamer,
    get_nickname_game_owner,
    get_text_message,
    get_value,
    get_value_prop_from_iai,
    has_special_characters_nickname,
    is_unique_nickname,
    mention_html,
    shorten_whitespace_nickname,
    validate_command_arg,
    format_award_game,
    get_id_for_edit_message,
)

router = Router()


@router.message(CommandStart(deep_link=True), F.text.contains("game_"))
async def command_start_game(
    message: Message,
    command: CommandObject,
    session: AsyncSession,
    state: FSMContext,
    user: User | None,
):
    if not user:
        data_user = {
            "id_user": message.from_user.id,
            "username": message.from_user.username,
        }
        await state.set_state(UserState.start_reg_step)
        await state.update_data(data_user=data_user)
        await message.answer(text=await get_text_message("enter_nickname"))
        return
    user.username = message.from_user.username
    id_game = command.args
    game = await session.scalar(select(Game).where(Game.id_game == id_game))
    if not game:
        await message.answer(text=await get_text_message("game_not_found"))
        return
    if game.end:
        await message.answer(text=await get_text_message("game_expired"))
        return
    if await get_gamer(session=session, id_game=id_game, idpk_gamer=user.idpk):
        await message.answer(text=await get_text_message("game_already_started"))
        return
    if await gamer_have_active_game(session=session, idpk_gamer=user.idpk):
        await message.answer(text=await get_text_message("finished_active_game"))
        return
    if game.amount_gamers == await get_current_amount_gamers(
        session=session, id_game=game.id_game
    ):
        await message.answer(text=await get_text_message("game_full"))
        return
    gamer = Gamer(
        id_game=id_game,
        idpk_gamer=user.idpk,
        moves=game.amount_moves,
    )
    if v := get_value_prop_from_iai(
        info_about_items=user.info_about_items, name_prop="extra_moves"
    ):
        gamer.moves += v
    session.add(gamer)
    await session.commit()
    nickname = await get_nickname_game_owner(
        session=session, idpk_game_owner=game.idpk_user, bot=message.bot
    )
    award_text = format_award_game(game.amount_award, game.currency_award)
    additional_message_parameters = get_id_for_edit_message(id_message=game.id_mess)
    text_for_top_mini_game = await factory_text_top_mini_game(
        session=session, id_game=game.id_game
    )
    with contextlib.suppress(Exception):
        await message.bot.edit_message_text(
            text=await get_text_message(
                "game_start",
                t=text_for_top_mini_game,
                nickname=nickname,
                game_type=game.type_game,
                amount_gamers=game.amount_gamers,
                amount_moves=game.amount_moves,
                award=award_text,
            ),
            reply_markup=await ik_start_created_game(
                link=await create_start_link(bot=message.bot, payload=game.id_game),
                total_gamers=game.amount_gamers,
                current_gamers=await get_current_amount_gamers(
                    session=session, id_game=game.id_game
                ),
            ),
            disable_web_page_preview=True,
            **additional_message_parameters,
        )
    await state.set_state(UserState.game)
    await state.update_data(
        idpk_game=game.idpk,
        nickname=nickname,
        award=award_text,
        type_game=game.type_game,
        amount_gamers=game.amount_gamers,
        amount_moves=game.amount_moves,
    )
    await message.answer(
        text=await get_text_message("begin_game"), reply_markup=ReplyKeyboardRemove()
    )
    keyboard_data = {
        "game_type": game.type_game,
        "total_moves": game.amount_moves,
        "remain_moves": gamer.moves,
    }
    await message.answer(
        text=await get_text_message("play_game", score=gamer.score),
        reply_markup=await ik_button_play(**keyboard_data),
    )


@router.message(CommandStart(deep_link=True))
async def command_start_with_deep_link(
    message: Message,
    command: CommandObject,
    session: AsyncSession,
    state: FSMContext,
    user: User | None,
):
    if user:
        await message.answer(
            text=await get_text_message("main_menu"),
            reply_markup=await rk_main_menu(user_id=message.from_user.id),
        )
        await state.set_state(UserState.main_menu)
        return
    valid_arg = validate_command_arg(command.args)
    if not valid_arg:
        await message.answer(
            text=await get_text_message("invalid_deep_link"),
            reply_markup=None,
        )
        return
    referrer = await session.get(User, valid_arg)
    if not referrer:
        await message.answer(
            text=await get_text_message("referrer_not_exist"),
            reply_markup=None,
        )
        return
    data_user = {
        "id_user": message.from_user.id,
        "username": message.from_user.username,
        "id_referrer": referrer.idpk,
    }
    await state.set_state(UserState.start_reg_step)
    await state.update_data(data_user=data_user)
    await message.answer(text=await get_text_message("welcome_message"))
    await message.answer(text=await get_text_message("enter_nickname"))


@router.message(CommandStart())
async def command_start(
    message: Message,
    state: FSMContext,
    session: AsyncSession,
    user: User | None,
) -> None:
    if user:
        await message.answer(
            text=await get_text_message("main_menu"),
            reply_markup=await rk_main_menu(user_id=message.from_user.id),
        )
        await state.set_state(UserState.main_menu)
        return
    data_user = {
        "id_user": message.from_user.id,
        "username": message.from_user.username,
    }
    await state.set_state(UserState.start_reg_step)
    await state.update_data(data_user=data_user)
    await message.answer(text=await get_text_message("welcome_message"))
    await message.answer(text=await get_text_message("enter_nickname"))


@router.message(UserState.start_reg_step)
async def getting_nickname(
    message: Message, state: FSMContext, session: AsyncSession
) -> None:
    LIMIT_LENGTH_MAX = await get_value(
        session=session, value_name="NICKNAME_LENGTH_MAX"
    )
    LIMIT_LENGTH_MIN = await get_value(
        session=session, value_name="NICKNAME_LENGTH_MIN"
    )
    if len(message.text) > LIMIT_LENGTH_MAX:
        await message.answer(text=await get_text_message("nickname_too_long"))
        return
    if len(message.text) < LIMIT_LENGTH_MIN:
        await message.answer(text=await get_text_message("nickname_too_short"))
        return
    if chars := await has_special_characters_nickname(message.text):
        await message.answer(
            text=await get_text_message("nickname_has_special_characters", chars=chars)
        )
        return
    if not await is_unique_nickname(session=session, nickname=message.text):
        await message.answer(text=await get_text_message("nickname_not_unique"))
        return
    data = await state.get_data()
    data_user = data["data_user"]
    data_user["nickname"] = await shorten_whitespace_nickname(message.text)
    data_user["date_reg"] = datetime.now()
    data_user["usd"] = await get_value(session=session, value_name="START_USD")
    session.add(User(**data_user))
    await session.commit()
    await message.answer(
        text=await get_text_message("main_menu"),
        reply_markup=await rk_main_menu(user_id=message.from_user.id),
    )
    await state.set_state(UserState.main_menu)
    await message.bot.send_message(
        chat_id=ADMIN_ID,
        text=await get_text_message(
            "new_user",
            user=mention_html(message.from_user.id, name=message.from_user.full_name),
        ),
    )
