import asyncio
import random
import contextlib
import html
import json
import math
from datetime import datetime
from typing import Any

from config import CHAT_ID
from db import User
from init_bot import bot
from init_db import _sessionmaker_for_func
from init_db_redis import redis
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from tools.value import get_value

from .client import NpcDecisionClient
from .logs import log_npc_decision
from .memory import build_npc_snapshot, remember_npc_turn
from .schedule import (
    clear_npc_event_wake,
    clamp_npc_sleep_seconds,
    default_npc_sleep_seconds,
    get_npc_wake_trigger,
    schedule_next_npc_wake,
)
from .settings import settings

from .state_builder import (
    build_observation,
    validate_action,
    apply_action_guardrails,
    should_stop_npc_cycle,
    safe_int,
    compute_smart_sleep_seconds,
    register_npc_move,
    ensure_random_merchant_for_user,
)
from .action_dispatcher import execute_action

_NPC_LOCKS: dict[int, asyncio.Lock] = {}


def get_npc_lock(user_idpk: int) -> asyncio.Lock:
    """Return a per-NPC asyncio.Lock so each NPC cycles independently (#9)."""
    if user_idpk not in _NPC_LOCKS:
        _NPC_LOCKS[user_idpk] = asyncio.Lock()
    return _NPC_LOCKS[user_idpk]


def npc_chat_cooldown_key(user_idpk: int) -> str:
    return f"npc_chat_comment:{user_idpk}"


def estimate_usd_eta_seconds(
    usd: int,
    rub: int,
    rate_rub_usd: int,
    income_per_minute_rub: int,
    target_usd: int,
) -> int | None:
    if target_usd <= int(usd):
        return 0
    rate = max(1, int(rate_rub_usd or 1))
    effective_usd = float(int(usd)) + float(int(rub)) / rate
    usd_gap = float(target_usd) - effective_usd
    if usd_gap <= 0:
        return 0
    if int(income_per_minute_rub) <= 0:
        return None
    gap_rub = usd_gap * rate
    minutes = math.ceil(gap_rub / max(1, int(income_per_minute_rub)))
    return max(60, int(minutes) * 60)


def score_animal_market_option(option: dict[str, Any]) -> float:
    price = max(1, int(option.get("price_usd", 0) or 0))
    income = max(0, int(option.get("income_rub", 0) or 0))
    payback = option.get("payback_minutes")
    rarity_weight = {
        "_rare": 1.0,
        "_epic": 1.08,
        "_mythical": 1.18,
        "_leg": 1.3,
    }.get(str(option.get("rarity", "_rare")), 1.0)
    roi = (income / price) * 1000 if price else 0.0
    payback_bonus = 0.0
    if payback is not None:
        payback_bonus = max(0.0, 220.0 - float(payback))
    affordable_bonus = min(5, int(option.get("affordable_quantity", 0) or 0)) * 8.0
    return round(roi * rarity_weight + payback_bonus + affordable_bonus, 2)


def build_rival_pressure(observation: dict[str, Any]) -> list[dict[str, Any]]:
    player_idpk = int(observation.get("player", {}).get("idpk", 0) or 0)
    category_weights = {
        "top_income": 1.6,
        "top_money": 1.2,
        "top_animals": 1.4,
        "top_referrals": 0.9,
    }
    rivals: dict[int, dict[str, Any]] = {}
    standings = observation.get("standings", {})
    for category, weight in category_weights.items():
        for row in standings.get(category, []) or []:
            rival_idpk = int(row.get("idpk", 0) or 0)
            if not rival_idpk or rival_idpk == player_idpk:
                continue
            payload = rivals.setdefault(
                rival_idpk,
                {
                    "idpk": rival_idpk,
                    "nickname": row.get("nickname"),
                    "pressure": 0.0,
                    "reasons": [],
                },
            )
            payload["pressure"] += max(0.0, (6 - int(row.get("rank", 6) or 6))) * weight
            payload["nickname"] = payload.get("nickname") or row.get("nickname")
            payload["reasons"].append(category.replace("top_", ""))
    ordered = sorted(
        rivals.values(),
        key=lambda row: (row["pressure"], row.get("nickname") or ""),
        reverse=True,
    )
    return [
        {
            **row,
            "pressure": round(float(row.get("pressure", 0.0)), 2),
            "reasons": list(dict.fromkeys(row.get("reasons", [])))[:3],
        }
        for row in ordered[: settings.top_candidates_limit]
    ]


async def run_npc_players_turn() -> None:
    if not settings.enabled or not settings.api_key:
        return

    import logging

    client = NpcDecisionClient(settings=settings)
    # Fetch all NPC users outside any per-NPC lock
    async with _sessionmaker_for_func() as session:
        npc_users = await get_npc_users(session=session)
        if not npc_users:
            npc_users = [await ensure_default_npc_user(session=session)]

    for npc_user in npc_users:
        npc_lock = get_npc_lock(npc_user.idpk)  # per-NPC lock (#9)
        if npc_lock.locked():
            continue
        async with npc_lock:
            # Wake trigger checks
            async with _sessionmaker_for_func() as session:
                wake_trigger = await get_npc_wake_trigger(
                    session=session, user=npc_user
                )
                if not wake_trigger["due"]:
                    continue
                await ensure_random_merchant_for_user(session=session, user=npc_user)
                await session.commit()

            last_action = None
            last_result = None
            last_before_snapshot = None
            last_after_snapshot = None
            last_observation = None
            llm_error_count = 0  # for exponential backoff (#4)

            for decision_index in range(1, settings.max_actions_per_cycle + 1):
                # Phase 1: Snapshot and Observation
                async with _sessionmaker_for_func() as session:
                    # refresh user obj
                    npc_user_refreshed = await session.get(User, npc_user.idpk)
                    if not npc_user_refreshed:
                        break
                    before_snapshot = await build_npc_snapshot(
                        session=session, user=npc_user_refreshed
                    )
                    observation = await build_observation(
                        session=session,
                        user=npc_user_refreshed,
                        wake_context=wake_trigger,
                    )

                # Phase 2: Action decision via LLM WITHOUT blocking DB session
                try:
                    decision = await client.choose_action(observation=observation)
                    llm_error_count = 0  # reset streak on success
                except Exception as exc:
                    logging.exception(
                        f"LLM Error during action decision for {npc_user.nickname}"
                    )
                    llm_error_count += 1
                    # Exponential backoff with ±10% jitter (#4)
                    base_delay = default_npc_sleep_seconds(
                        user=npc_user, salt="llm_error"
                    )
                    retry_delay = min(int(base_delay * (2**llm_error_count)), 300)
                    retry_delay += int(retry_delay * random.uniform(-0.1, 0.1))
                    decision = {
                        "action": "wait",
                        "params": {},
                        "reason": f"llm_error:{str(exc)[:250]}",
                        "sleep_seconds": retry_delay,
                    }

                action = apply_action_guardrails(
                    action=validate_action(decision=decision),
                    observation=observation,
                )

                # Phase 3: Execute in DB and Commit
                async with _sessionmaker_for_func() as session:
                    npc_user_refreshed = await session.get(User, npc_user.idpk)
                    try:
                        result = await execute_action(
                            session=session,
                            user=npc_user_refreshed,
                            action=action,
                            observation=observation,
                            client=client,
                        )
                    except Exception as exc:
                        logging.exception(
                            f"Execution Error during action for {npc_user_refreshed.nickname}"
                        )
                        result = {
                            "status": "error",
                            "summary": f"execution_error:{type(exc).__name__}",
                        }
                    after_snapshot = await build_npc_snapshot(
                        session=session, user=npc_user_refreshed
                    )
                    last_action = action
                    last_result = result
                    last_before_snapshot = before_snapshot
                    last_after_snapshot = after_snapshot
                    last_observation = observation
                    print(
                        "NPC_DECISION",
                        json.dumps(
                            {
                                "time": datetime.now().isoformat(),
                                "npc": npc_user_refreshed.nickname,
                                "step": decision_index,
                                "action": action,
                                "result": result,
                            },
                            ensure_ascii=False,
                        ),
                        flush=True,
                    )
                    await register_npc_move(
                        session=session,
                        user=npc_user_refreshed,
                        action=action,
                        result=result,
                        wake_trigger=wake_trigger,
                    )
                    await remember_npc_turn(
                        session=session,
                        user=npc_user_refreshed,
                        observation=observation,
                        before_snapshot=before_snapshot,
                        after_snapshot=after_snapshot,
                        action=action,
                        result=result,
                        wake_trigger=wake_trigger,
                        decision_index=decision_index,
                        client=client,
                    )
                    await session.commit()

                try:
                    await log_npc_decision(
                        log_path=settings.log_path,
                        payload={
                            "time": datetime.now().isoformat(),
                            "npc": {
                                "id_user": npc_user.id_user,
                                "nickname": npc_user.nickname,
                            },
                            "step": decision_index,
                            "action": action,
                            "result": result,
                            "snapshot_before": before_snapshot,
                            "snapshot_after": after_snapshot,
                            "observation": observation,
                        },
                    )
                except Exception:
                    pass

                if should_stop_npc_cycle(action=action, result=result):
                    break

            # Re-fetch user to commit sleep schedule
            async with _sessionmaker_for_func() as session:
                npc_user_refreshed = await session.get(User, npc_user.idpk)
                if not npc_user_refreshed:
                    continue
                planned_sleep_seconds = resolve_npc_sleep_seconds(
                    user=npc_user_refreshed,
                    wake_trigger=wake_trigger,
                    action=last_action,
                    result=last_result,
                    observation=last_observation,
                )
                await schedule_next_npc_wake(
                    session=session,
                    user=npc_user_refreshed,
                    sleep_seconds=planned_sleep_seconds,
                    source=str(wake_trigger["source"]),
                    reason=build_npc_wake_reason(
                        action=last_action, result=last_result
                    ),
                )
                if wake_trigger["source"] == "event":
                    await clear_npc_event_wake(npc_user_refreshed.idpk)
                await session.commit()

            if last_action and last_result and last_observation and last_after_snapshot:
                with contextlib.suppress(Exception):
                    await maybe_send_npc_chat_comment(
                        client=client,
                        user=npc_user,
                        observation=last_observation,
                        before_snapshot=last_before_snapshot or {},
                        after_snapshot=last_after_snapshot,
                        action=last_action,
                        result=last_result,
                        planned_sleep_seconds=planned_sleep_seconds,
                    )


async def get_npc_users(session: AsyncSession) -> list[User]:
    result = await session.scalars(
        select(User).where(or_(User.id_user < 0, User.username.like("npc_%")))
    )
    return list(result.all())


async def ensure_default_npc_user(session: AsyncSession) -> User:
    npc_user = await session.scalar(select(User).where(User.id_user == settings.npc_id))
    if npc_user:
        return npc_user

    start_usd = await get_value(session=session, value_name="START_USD")
    npc_user = User(
        id_user=settings.npc_id,
        username=settings.npc_username,
        nickname=settings.npc_nickname,
        date_reg=datetime.now(),
        usd=start_usd,
        history_moves="{}",
        animals="{}",
        info_about_items="{}",
        aviaries="{}",
    )
    session.add(npc_user)
    await session.commit()
    return npc_user


def build_npc_wake_reason(
    action: dict[str, Any] | None,
    result: dict[str, Any] | None,
) -> str:
    if not action:
        return "cycle_complete"
    action_name = str(action.get("action", "wait"))
    result_summary = ""
    if result:
        result_summary = str(result.get("summary", ""))[:180]
    if result_summary:
        return f"{action_name}:{result_summary}"[:255]
    return action_name[:255]


async def maybe_send_npc_chat_comment(
    client: NpcDecisionClient,
    user: User,
    observation: dict[str, Any],
    before_snapshot: dict[str, Any],
    after_snapshot: dict[str, Any],
    action: dict[str, Any],
    result: dict[str, Any],
    planned_sleep_seconds: int,
) -> None:
    cooldown_seconds = settings.chat_min_interval_seconds
    if cooldown_seconds <= 0:
        return

    cooldown_set = await redis.set(
        npc_chat_cooldown_key(user.idpk),
        str(datetime.now().timestamp()),
        ex=cooldown_seconds,
        nx=True,
    )
    if not cooldown_set:
        return

    standings = observation.get("standings", {})
    self_standings = standings.get("self", {})
    top_income = (standings.get("top_income") or [{}])[0]
    top_money = (standings.get("top_money") or [{}])[0]
    top_animals = (standings.get("top_animals") or [{}])[0]
    top_referrals = (standings.get("top_referrals") or [{}])[0]
    planner = observation.get("planner", {})
    summary = observation.get("strategy_signals", {}).get("summary", {})
    profile = observation.get("memory", {}).get("profile", {})
    income_rank = self_standings.get("income_rank")
    chat_mode = "taunt"
    if result.get("status") != "ok":
        chat_mode = "complaint"
    elif income_rank == 1:
        chat_mode = "world_domination"  # #8: rank-based mood
    elif isinstance(income_rank, int) and income_rank <= 3:
        chat_mode = "podium_pressure"  # #8
    elif int(after_snapshot.get("income_per_minute_rub", 0) or 0) > int(
        before_snapshot.get("income_per_minute_rub", 0) or 0
    ):
        chat_mode = "victory_lap"
    elif action.get("action") in {
        "join_best_unity",
        "recruit_top_player",
        "review_unity_request",
    }:
        chat_mode = "social"
    elif action.get("action") == "wait":
        chat_mode = "plotting"

    payload = {
        "npc": {
            "nickname": user.nickname,
            "usd": int(user.usd),
            "rub": int(user.rub),
            "paw_coins": int(user.paw_coins),
            "moves": int(user.moves),
            "public_voice": profile.get("public_voice"),
            "humor_style": profile.get("humor_style"),
            "rivalry_style": profile.get("rivalry_style"),
        },
        "action": {
            "name": action.get("action", "wait"),
            "reason": str(action.get("reason", ""))[:220],
            "sleep_seconds": int(action.get("sleep_seconds") or planned_sleep_seconds),
        },
        "result": {
            "status": result.get("status", "unknown"),
            "summary": str(result.get("summary", ""))[:220],
        },
        "progress": {
            "income_rank": self_standings.get("income_rank"),
            "money_rank": self_standings.get("money_rank"),
            "animals_rank": self_standings.get("animals_rank"),
            "referrals_rank": self_standings.get("referrals_rank"),
            "leader_income": top_income,
            "leader_money": top_money,
            "leader_animals": top_animals,
            "leader_referrals": top_referrals,
            "top_rivals": summary.get("top_rivals", []),
        },
        "economy": {
            "before": {
                "usd": int(before_snapshot.get("usd", 0) or 0),
                "rub": int(before_snapshot.get("rub", 0) or 0),
                "income_per_minute_rub": int(
                    before_snapshot.get("income_per_minute_rub", 0) or 0
                ),
                "total_animals": int(before_snapshot.get("total_animals", 0) or 0),
            },
            "after": {
                "usd": int(after_snapshot.get("usd", 0) or 0),
                "rub": int(after_snapshot.get("rub", 0) or 0),
                "income_per_minute_rub": int(
                    after_snapshot.get("income_per_minute_rub", 0) or 0
                ),
                "total_animals": int(after_snapshot.get("total_animals", 0) or 0),
            },
        },
        "tone": {
            "persona": "AI alone against the whole zoo",
            "goal": "sound funny, strategic, and slightly dramatic",
            "mode": chat_mode,
        },
        "plan": {
            "phase": planner.get("phase"),
            "primary_goal": planner.get("primary_goal"),
            "next_unlock": planner.get("next_unlock"),
            "next_steps": planner.get("recommended_actions", [])[:3],
        },
    }
    signature = f"{user.nickname}: "
    max_message_length = max(0, settings.chat_max_length - len(signature))
    message = (await client.generate_chat_comment(payload=payload))[
        :max_message_length
    ].strip()
    if not message:
        return

    await bot.send_message(
        chat_id=CHAT_ID,
        text=html.escape(f"{signature}{message}", quote=False),
        disable_notification=True,
    )


def resolve_npc_sleep_seconds(
    user: User,
    wake_trigger: dict[str, Any],
    action: dict[str, Any] | None,
    result: dict[str, Any] | None,
    observation: dict[str, Any] | None = None,
) -> int:
    def _jitter(seconds: int) -> int:  # ±10% random jitter (#7)
        return clamp_npc_sleep_seconds(
            int(seconds + seconds * random.uniform(-0.1, 0.1))
        )

    default_sleep = default_npc_sleep_seconds(
        user=user,
        salt=f"{wake_trigger.get('source', 'scheduled')}:{wake_trigger.get('reason', '')}",
    )
    if action and action.get("action") == "wait":
        default_sleep = clamp_npc_sleep_seconds(default_sleep * 2)
    if result and result.get("status") == "error":
        default_sleep = settings.min_sleep_seconds
    elif wake_trigger.get("source") == "event":
        default_sleep = min(default_sleep, settings.step_seconds)
    default_sleep = compute_smart_sleep_seconds(
        observation=observation,
        wake_trigger=wake_trigger,
        action=action,
        result=result,
        default_sleep=default_sleep,
    )
    if not action:
        return _jitter(default_sleep)
    sleep_value = action.get("sleep_seconds")
    if sleep_value is None:
        return _jitter(default_sleep)
    proposed_sleep = clamp_npc_sleep_seconds(
        safe_int(sleep_value, default=default_sleep)
    )
    if action.get("action") == "wait":
        return _jitter(min(proposed_sleep, default_sleep))
    return _jitter(proposed_sleep)
