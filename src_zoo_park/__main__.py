import asyncio
from datetime import datetime
import traceback

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


async def on_shutdown(*args, **kwargs) -> None:
    await _engine.dispose()


async def scheduler() -> None:
    last_runs: dict[str, str] = {}

    async def run_task(name: str, coro, stamp: str | None = None) -> None:
        try:
            await coro
        except Exception:
            traceback.print_exc()
        else:
            last_runs[name] = stamp or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    while True:
        now = datetime.now()
        await run_task(name="job_minute", coro=job_minute())

        if now.second % 5 == 0:
            npc_tick_key = now.strftime("%Y-%m-%d %H:%M:%S")
            if last_runs.get("npc") != npc_tick_key:
                await run_task(
                    name="npc",
                    coro=run_npc_players_turn(),
                    stamp=npc_tick_key,
                )

        if now.second < 10:
            minute_key = now.strftime("%Y-%m-%d %H:%M")
            clock = now.strftime("%H:%M")

            daily_jobs = {
                "10:30": ("reset_first_offer_bought", reset_first_offer_bought),
                "11:00": ("add_bonus_to_users", add_bonus_to_users),
                "13:00": ("create_game_for_chat_1", create_game_for_chat),
                "16:30": ("create_game_for_chat_2", create_game_for_chat),
                "20:00": ("create_game_for_chat_3", create_game_for_chat),
                "21:00": ("verification_referrals", verification_referrals),
            }
            if clock in daily_jobs:
                task_name, task_func = daily_jobs[clock]
                if last_runs.get(task_name) != minute_key:
                    await run_task(name=task_name, coro=task_func(), stamp=minute_key)

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

    await on_startup(_engine)

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
