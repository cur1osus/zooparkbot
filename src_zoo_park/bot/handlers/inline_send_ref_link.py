from aiogram import F, Router
from aiogram.types import (
    ChosenInlineResult,
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
)
from aiogram.utils.deep_linking import create_start_link
from bot.keyboards import ik_start_game
from db import User
from sqlalchemy.ext.asyncio import AsyncSession
from tools import get_text_message

router = Router()


@router.inline_query()
async def share_referral_link(
    inline_query: InlineQuery,
    session: AsyncSession,
    user: User | None,
):
    if not user:
        r = InlineQueryResultArticle(
            id="ref_not_found",
            title=await get_text_message("attention"),
            description=await get_text_message("press_start_to_play"),
            input_message_content=InputTextMessageContent(
                message_text=await get_text_message("press_start_to_play")
            ),
        )
        await inline_query.answer(results=[r], cache_time=0)
        return

    link = await create_start_link(bot=inline_query.bot, payload=user.idpk)
    r = InlineQueryResultArticle(
        id="ref",
        title=await get_text_message("title_inline_mess"),
        description=await get_text_message("description_inline_mess"),
        thumbnail_url="https://wepik.com/api/vector/9c37cdfd-1eed-4e23-a779-094960429bc4/preview",
        input_message_content=InputTextMessageContent(
            message_text=await get_text_message("promo_for_referrals")
        ),
        reply_markup=await ik_start_game(link=link),
    )
    await inline_query.answer(results=[r], cache_time=0)


@router.chosen_inline_result(F.result_id == "ref")
async def ref(
    chosen_result: ChosenInlineResult,
):
    return
