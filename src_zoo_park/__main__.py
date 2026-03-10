import asyncio

import aioschedule
import tools
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.types import BotCommand
from bot.handlers import setup_message_routers
from bot.middlewares import (
    CheckGame,
    CheckUnity,
    CheckUser,
    DBSessionMiddleware,
    RegMove,
    ThrottlingMiddleware,
)
from db import Base
from init_bot import bot
from init_db import _engine, _sessionmaker
from init_db_redis import redis
from jobs import (
    add_bonus_to_users,
    create_game_for_chat,
    job_minute,
    reset_first_offer_bought,
    verification_referrals,
)
from npc_agent import run_npc_players_turn
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession


async def on_startup(_engine: AsyncEngine) -> None:
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def on_shutdown(session: AsyncSession) -> None:
    await session.close_all()


async def scheduler() -> None:
    # aioschedule.every(1).seconds.do(job_sec)
    aioschedule.every(1).seconds.do(job_minute)
    aioschedule.every(15).minutes.do(run_npc_players_turn)
    aioschedule.every().day.at("10:30").do(reset_first_offer_bought)
    aioschedule.every().day.at("11:00").do(add_bonus_to_users)
    aioschedule.every().day.at("13:00").do(create_game_for_chat)
    aioschedule.every().day.at("16:30").do(create_game_for_chat)
    aioschedule.every().day.at("20:00").do(create_game_for_chat)
    aioschedule.every().day.at("21:00").do(verification_referrals)
    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(1)


async def set_default_commands(bot: Bot):
    await bot.set_my_commands(
        [
            BotCommand(
                command="start",
                description=await tools.get_text_message("command_start_description"),
            ),
            BotCommand(
                command="calculator",
                description=await tools.get_text_message(
                    "command_calculator_description"
                ),
            ),
            BotCommand(
                command="support",
                description=await tools.get_text_message("command_support_description"),
            ),
            BotCommand(
                command="donate",
                description=await tools.get_text_message("command_donate_description"),
            ),
            BotCommand(
                command="faq",
                description=await tools.get_text_message("command_faq_description"),
            ),
        ]
    )


async def main() -> None:
    dp = Dispatcher(_engine=_engine, storage=RedisStorage(redis=redis))

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    dp.message.middleware(ThrottlingMiddleware())

    dp.message.middleware(DBSessionMiddleware(session_pool=_sessionmaker))
    dp.callback_query.middleware(DBSessionMiddleware(session_pool=_sessionmaker))
    dp.inline_query.middleware(DBSessionMiddleware(session_pool=_sessionmaker))
    dp.update.middleware(DBSessionMiddleware(session_pool=_sessionmaker))

    dp.message.middleware(CheckUser())
    dp.callback_query.middleware(CheckUser())
    dp.inline_query.middleware(CheckUser())

    dp.message.middleware(CheckUnity())
    dp.callback_query.middleware(CheckUnity())

    dp.message.middleware(CheckGame())
    dp.callback_query.middleware(CheckGame())

    dp.message.middleware(RegMove())

    message_routers = setup_message_routers()
    asyncio.create_task(scheduler())
    dp.include_router(message_routers)
    await set_default_commands(bot)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
