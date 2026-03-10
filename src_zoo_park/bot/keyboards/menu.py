import tools
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from config import ADMIN_ID


ADMIN_PANEL_BUTTON = "Админ"


async def rk_main_menu(user_id: int | None = None):
    builder = ReplyKeyboardBuilder()
    builder.button(text=await tools.get_text_button("zoomarket"))
    builder.button(text=await tools.get_text_button("bank"))
    builder.button(text=await tools.get_text_button("unity"))
    builder.button(text=await tools.get_text_button("account"))
    builder.button(text=await tools.get_text_button("top"))
    builder.button(text=await tools.get_text_button("bonus"))
    if user_id == ADMIN_ID:
        builder.button(text=ADMIN_PANEL_BUTTON)

    if user_id == ADMIN_ID:
        builder.adjust(3, 2, 1, 1)
    else:
        builder.adjust(3, 2, 1)
    return builder.as_markup(resize_keyboard=True)


async def rk_zoomarket_menu():
    builder = ReplyKeyboardBuilder()
    builder.button(text=await tools.get_text_button("random_merchant"))
    builder.button(text=await tools.get_text_button("rarity_shop"))
    # builder.button(text=await tools.get_text_button("workshop_items"))
    builder.button(text=await tools.get_text_button("forge_items"))
    builder.button(text=await tools.get_text_button("aviaries"))
    builder.button(text=await tools.get_text_button("back"))
    builder.adjust(2, 2, 1)
    return builder.as_markup(resize_keyboard=True)


async def rk_unity_menu():
    builder = ReplyKeyboardBuilder()
    builder.button(text=await tools.get_text_button("level"))
    builder.button(text=await tools.get_text_button("unity_members"))
    builder.button(text=await tools.get_text_button("top_unity"))
    builder.button(text=await tools.get_text_button("exit_from_unity"))
    builder.button(text=await tools.get_text_button("back"))

    builder.adjust(3, 2)
    return builder.as_markup(resize_keyboard=True)
