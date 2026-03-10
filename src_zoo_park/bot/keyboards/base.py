import base64

import tools
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from bot.callbacks import InlineRateUpdateCallback


async def ik_back(custom_callback_data: str = "back"):
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button("back"), callback_data=custom_callback_data
    )
    return builder.as_markup()


async def ik_start_game(link: str):
    builder = InlineKeyboardBuilder()
    builder.button(text=await tools.get_text_button("start_zoopark_by_link"), url=link)
    return builder.as_markup()


async def ik_start_created_game(link: str, current_gamers: int, total_gamers: int):
    builder = InlineKeyboardBuilder()
    builder.button(
        text=await tools.get_text_button(
            "start_game_by_link",
            current_gamers=current_gamers,
            total_gamers=total_gamers,
        ),
        url=link,
    )
    return builder.as_markup()


async def rk_back():
    builder = ReplyKeyboardBuilder()
    builder.button(text=await tools.get_text_button("back"))
    return builder.as_markup(resize_keyboard=True)


async def rk_cancel():
    builder = ReplyKeyboardBuilder()
    builder.button(text=await tools.get_text_button("cancel"))
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)


async def ik_update_inline_rate(inline_message_id: str):
    builder = InlineKeyboardBuilder()
    if not inline_message_id:
        builder.button(
            text=await tools.get_text_button("loading_inline_rate"),
            callback_data="loading_inline_rate",
        )
    else:
        token = base64.urlsafe_b64encode(inline_message_id.encode("utf-8")).decode(
            "ascii"
        )
        builder.button(
            text=await tools.get_text_button("update_inline_rate"),
            callback_data=InlineRateUpdateCallback(token=token),
        )
    return builder.as_markup()
