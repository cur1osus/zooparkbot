import random

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import (
    CallbackQuery,
    ChosenInlineResult,
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
    LinkPreviewOptions,
)
from bot.callbacks import TransferActivateCallback
from bot.filters import FindIntegers
from bot.keyboards import ik_get_money, ik_get_money_one_piece
from db import TransferMoney, User
from game_variables import translated_currencies
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from tools import (
    add_to_amount_expenses_currency,
    add_to_currency,
    add_user_to_used,
    find_integers,
    gen_key,
    get_currency,
    get_text_message,
    in_used,
)

router = Router()


@router.inline_query(FindIntegers())
async def inline_send_money(
    inline_query: InlineQuery,
    session: AsyncSession,
    user: User | None,
):
    if not user:
        r = InlineQueryResultArticle(
            id="money_user_not_found",
            title=await get_text_message("attention"),
            description=await get_text_message("press_start_to_play"),
            input_message_content=InputTextMessageContent(
                message_text=await get_text_message("press_start_to_play")
            ),
        )
        await inline_query.answer(results=[r], cache_time=0)
        return

    split_query = inline_query.query.split()
    num = await find_integers(split_query[0])
    amount_params = len(split_query)
    if amount_params == 1:
        r = InlineQueryResultArticle(
            id=str(random.randint(1, 100000)),
            title=await get_text_message("point_1"),
            description=await get_text_message("enter_currency_to_send"),
            thumbnail_url="https://www.clipartmax.com/png/full/219-2197586_organization-training-business-learning-technology-no-1-icon-png.png",
            input_message_content=InputTextMessageContent(
                message_text=await get_text_message("error_enter_currency_to_send")
            ),
        )
        return await inline_query.answer(results=[r], cache_time=0)
    if split_query[1] not in ["rub", "usd"]:
        r = InlineQueryResultArticle(
            id=str(random.randint(1, 100000)),
            title=await get_text_message("attention"),
            description=await get_text_message("enter_usd_or_rub"),
            thumbnail_url="https://avatars.yandex.net/get-music-content/2433207/64c60238.a.12011003-1/m1000x1000?webp=false",
            input_message_content=InputTextMessageContent(
                message_text=await get_text_message("error_enter_usd_or_rub")
            ),
        )
        return await inline_query.answer(results=[r], cache_time=0)
    currency = await get_currency(self=user, currency=split_query[1])
    if currency < num:
        r = InlineQueryResultArticle(
            id=str(random.randint(1, 100000)),
            title=await get_text_message("attention"),
            description=await get_text_message(
                "not_enough_money_to_send", money=currency
            ),
            thumbnail_url="https://avatars.yandex.net/get-music-content/2433207/64c60238.a.12011003-1/m1000x1000?webp=false",
            input_message_content=InputTextMessageContent(
                message_text=await get_text_message("error_not_enough_money_to_send")
            ),
        )
        return await inline_query.answer(results=[r], cache_time=0)
    if amount_params == 2:
        r = InlineQueryResultArticle(
            id=str(random.randint(1, 100000)),
            title=await get_text_message("point_2"),
            description=await get_text_message("enter_split_to_send"),
            thumbnail_url="https://cdn-ru.bitrix24.ru/b9251457/landing/7b7/7b7c7b41a9e0c33f1867fea82a8d5c08/Resurs_20_s_1x.png",
            input_message_content=InputTextMessageContent(
                message_text=await get_text_message("error_enter_split_to_send")
            ),
        )
        return await inline_query.answer(results=[r], cache_time=0)
    pieces = await find_integers(split_query[2])
    if not pieces:
        r = InlineQueryResultArticle(
            id=str(random.randint(1, 100000)),
            title=await get_text_message("attention"),
            description=await get_text_message("enter_correct_split_to_send"),
            thumbnail_url="https://avatars.yandex.net/get-music-content/2433207/64c60238.a.12011003-1/m1000x1000?webp=false",
            input_message_content=InputTextMessageContent(
                message_text=await get_text_message("error_enter_correct_split_to_send")
            ),
        )
        return await inline_query.answer(results=[r], cache_time=0)
    if pieces == 0:
        r = InlineQueryResultArticle(
            id=str(random.randint(1, 100000)),
            title=await get_text_message("attention"),
            description=await get_text_message("amount_pieces_too_small"),
            thumbnail_url="https://avatars.yandex.net/get-music-content/2433207/64c60238.a.12011003-1/m1000x1000?webp=false",
            input_message_content=InputTextMessageContent(
                message_text=await get_text_message("error_amount_pieces_too_small")
            ),
        )
        return await inline_query.answer(results=[r], cache_time=0)
    if pieces > num or pieces > 500:
        r = InlineQueryResultArticle(
            id=str(random.randint(1, 100000)),
            title=await get_text_message("attention"),
            description=await get_text_message("amount_pieces_too_big"),
            thumbnail_url="https://avatars.yandex.net/get-music-content/2433207/64c60238.a.12011003-1/m1000x1000?webp=false",
            input_message_content=InputTextMessageContent(
                message_text=await get_text_message("error_amount_pieces_too_big")
            ),
        )
        return await inline_query.answer(results=[r], cache_time=0)
    one_piece = num // pieces
    key = gen_key(length=10)
    tr = TransferMoney(
        id_transfer=key,
        idpk_user=user.idpk,
        currency=split_query[1],
        one_piece_sum=one_piece,
        pieces=pieces,
    )
    session.add(tr)
    await session.commit()
    photo_url = (
        "https://imgimg.co/files/images/meshok-s-dengami/meshok-s-dengami-9.webp"
    )
    invisible_char = "&#8203;"
    d = {
        "sum": num,
        "invisible": f'<a href="{photo_url}">{invisible_char}</a>',
        "nickname": user.nickname,
        "ps": pieces,
    }
    c = translated_currencies[split_query[1]]
    d["currency"] = c
    one_piece = f"{one_piece:,d}{c}"
    text = (
        await get_text_message("info_about_send_money", **d)
        if pieces > 1
        else await get_text_message(
            "info_about_send_money_one_p",
            nickname=d["nickname"],
            sum=d["sum"],
            invisible=d["invisible"],
            currency=d["currency"],
        )
    )
    keyboard = (
        await ik_get_money(one_piece=one_piece, remain_pieces=pieces, idpk_tr=tr.idpk)
        if pieces > 1
        else await ik_get_money_one_piece(idpk_tr=tr.idpk)
    )
    r = InlineQueryResultArticle(
        id=f"{tr.idpk}:activate_tr",
        title=await get_text_message("point_3"),
        description=await get_text_message("for_send_money_tap_on_bttn"),
        thumbnail_url="https://blog.4d.com/wp-content/uploads/2017/08/UX2.png",
        input_message_content=InputTextMessageContent(
            message_text=text,
            link_preview_options=LinkPreviewOptions(show_above_text=True),
            disable_web_page_preview=False,
        ),
        reply_markup=keyboard,
    )
    await inline_query.answer(results=[r], cache_time=0)


@router.chosen_inline_result(F.result_id.split(":")[-1] == "activate_tr")
async def pagination_demo(chosen_result: ChosenInlineResult, session: AsyncSession):
    idpk_tr = int(chosen_result.result_id.split(":")[0])
    tr = await session.get(TransferMoney, idpk_tr)
    if not tr:
        return

    user = await session.scalar(
        select(User).where(User.id_user == chosen_result.from_user.id)
    )
    if not user:
        return

    await add_to_currency(
        self=user,
        currency=tr.currency,
        amount=-tr.pieces * tr.one_piece_sum,
    )
    await add_to_amount_expenses_currency(
        self=user,
        currency=tr.currency,
        amount=tr.pieces * tr.one_piece_sum,
    )
    tr.status = True
    # Save inline message id so backend claimers (NPC) can refresh keyboard state.
    if chosen_result.inline_message_id:
        tr.id_mess = str(chosen_result.inline_message_id)
    await session.flush()
    await session.execute(delete(TransferMoney).where(TransferMoney.status == False))
    await session.commit()


@router.callback_query(TransferActivateCallback.filter())
async def activate_tr(
    query: CallbackQuery,
    session: AsyncSession,
    user: User,
    callback_data: TransferActivateCallback,
):
    if not user:
        await query.answer(
            text=await get_text_message("error_user_not_found"), show_alert=True
        )
        return
    idpk_tr = callback_data.transfer_idpk
    tr = await session.get(TransferMoney, idpk_tr)
    if not tr:
        try:
            await query.bot.edit_message_reply_markup(
                inline_message_id=query.inline_message_id, reply_markup=None
            )
        except TelegramBadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                raise
        return
    if tr.pieces == 0:
        await query.answer(text=await get_text_message("error_tr_end"), show_alert=True)
        try:
            await query.bot.edit_message_reply_markup(
                inline_message_id=query.inline_message_id, reply_markup=None
            )
        except TelegramBadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                raise
        return
    if await in_used(session=session, idpk_tr=tr.idpk, idpk_user=user.idpk):
        await query.answer(
            text=await get_text_message("error_tr_already_used"), show_alert=True
        )
        return
    if tr.idpk_user == user.idpk:
        await query.answer(
            text=await get_text_message("error_tr_self"), show_alert=True
        )
        return
    await add_user_to_used(session=session, idpk_tr=tr.idpk, idpk_user=user.idpk)
    await add_to_currency(self=user, currency=tr.currency, amount=tr.one_piece_sum)
    tr.pieces -= 1
    await session.commit()
    c = translated_currencies[tr.currency]
    await query.bot.send_message(
        chat_id=user.id_user,
        text=await get_text_message(
            "you_get_money",
            money=tr.one_piece_sum,
            currency=c,
        ),
    )
    keyboard = (
        await ik_get_money(
            one_piece=f"{tr.one_piece_sum:,d}{c}",
            remain_pieces=tr.pieces,
            idpk_tr=tr.idpk,
        )
        if tr.pieces > 0
        else None
    )
    try:
        await query.bot.edit_message_reply_markup(
            inline_message_id=query.inline_message_id, reply_markup=keyboard
        )
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            raise
