import asyncio
import random
import contextlib
import html
import json
import math
import time
from datetime import datetime
from typing import Any

from config import CHAT_ID
from db import User
from fastjson import dumps as fast_dumps, loads_or_default
from init_bot import bot
from init_db import _sessionmaker_for_func
from init_db_redis import redis
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from tools.value import get_value
from text_utils import (
    fit_db_field,
    preview_error,
    preview_with_prefix,
    semantic_preview,
)

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


def npc_llm_degraded_key(user_idpk: int) -> str:
    return f"npc_llm_degraded:{user_idpk}"


def npc_llm_error_streak_key(user_idpk: int) -> str:
    return f"npc_llm_error_streak:{user_idpk}"


def npc_v2_memory_key(user_idpk: int) -> str:
    return f"npc_v2_memory:{user_idpk}"


async def load_npc_v2_memory(user_idpk: int) -> dict[str, Any]:
    raw = await redis.get(npc_v2_memory_key(user_idpk))
    if not raw:
        return {"recent_outcomes": [], "tool_scores": {}}
    try:
        payload = loads_or_default(raw, {})
    except Exception:
        return {"recent_outcomes": [], "tool_scores": {}}

    recent = payload.get("recent", []) or []
    recent_outcomes = []
    for row in recent[-4:]:
        if not isinstance(row, dict):
            continue
        recent_outcomes.append(
            {
                "action": row.get("action"),
                "status": row.get("status"),
                "delta_usd": int(row.get("delta_usd", 0) or 0),
                "delta_income": int(row.get("delta_income", 0) or 0),
                "ts": int(row.get("ts", 0) or 0),
            }
        )

    tool_scores = payload.get("tool_scores", {}) or {}
    compact_scores: dict[str, Any] = {}
    for action_name, stats in tool_scores.items():
        if not isinstance(stats, dict):
            continue
        compact_scores[str(action_name)] = {
            "ok": int(stats.get("ok", 0) or 0),
            "err": int(stats.get("err", 0) or 0),
        }

    return {"recent_outcomes": recent_outcomes, "tool_scores": compact_scores}


async def update_npc_v2_memory(
    user_idpk: int,
    action: dict[str, Any],
    result: dict[str, Any],
    before_snapshot: dict[str, Any],
    after_snapshot: dict[str, Any],
) -> None:
    ttl_seconds = 12 * 3600
    now_ts = int(time.time())

    raw = await redis.get(npc_v2_memory_key(user_idpk))
    try:
        payload = loads_or_default(raw, {}) if raw else {}
    except Exception:
        payload = {}

    recent = payload.get("recent", []) or []
    tool_scores = payload.get("tool_scores", {}) or {}

    action_name = str(action.get("action", "wait"))
    status = str(result.get("status", "unknown"))
    delta_usd = int(after_snapshot.get("usd", 0) or 0) - int(
        before_snapshot.get("usd", 0) or 0
    )
    delta_income = int(after_snapshot.get("income_per_minute_rub", 0) or 0) - int(
        before_snapshot.get("income_per_minute_rub", 0) or 0
    )

    recent.append(
        {
            "ts": now_ts,
            "action": action_name,
            "status": status,
            "delta_usd": delta_usd,
            "delta_income": delta_income,
        }
    )
    recent = [
        row for row in recent if int(row.get("ts", 0) or 0) >= now_ts - ttl_seconds
    ]
    recent = recent[-12:]

    stats = tool_scores.get(action_name, {"ok": 0, "err": 0})
    if status == "ok":
        stats["ok"] = int(stats.get("ok", 0) or 0) + 1
    elif status in {"error", "failed"}:
        stats["err"] = int(stats.get("err", 0) or 0) + 1
    tool_scores[action_name] = stats

    payload = {
        "updated_at": now_ts,
        "recent": recent,
        "tool_scores": tool_scores,
    }
    await redis.set(npc_v2_memory_key(user_idpk), fast_dumps(payload), ex=ttl_seconds)


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


def _classify_llm_error(error_text: str) -> str:
    t = str(error_text or "").lower()
    if "http_401" in t or "http_403" in t:
        return "auth"
    if "http_429" in t or "rate limit" in t:
        return "rate_limit"
    if "http_5" in t or "timeout" in t:
        return "transient"
    if "http_404" in t:
        return "endpoint"
    return "other"


def _fallback_action_without_llm(
    observation: dict[str, Any], retry_delay: int
) -> dict[str, Any]:
    allowed = [
        row
        for row in (observation.get("allowed_actions", []) or [])
        if isinstance(row, dict)
    ]

    # Capacity lock mode: prefer opening seats first, then liquidity.
    need_seats = bool(
        (observation.get("strategy_signals", {}).get("summary", {}) or {}).get(
            "need_seats"
        )
    )
    if need_seats:
        seat_lock_priority = [
            "buy_aviary",
            "exchange_bank",
            "invest_for_income",
            "wait",
        ]
        for action_name in seat_lock_priority:
            for row in allowed:
                if str(row.get("action", "")).strip() == action_name:
                    return {
                        "action": action_name,
                        "params": row.get("params", {}) or {},
                        "reason": "llm_degraded_fallback_policy:seat_lock",
                        "sleep_seconds": retry_delay,
                    }

    priority = [
        "claim_daily_bonus",
        "review_unity_request",
        "buy_aviary",
        "buy_rarity_animal",
        "exchange_bank",
        "invest_for_income",
        "claim_chat_transfer",
        "wait",
    ]
    for action_name in priority:
        for row in allowed:
            if str(row.get("action", "")).strip() == action_name:
                return {
                    "action": action_name,
                    "params": row.get("params", {}) or {},
                    "reason": "llm_degraded_fallback_policy",
                    "sleep_seconds": retry_delay,
                }
    return {
        "action": "wait",
        "params": {},
        "reason": "llm_degraded_no_legal_actions",
        "sleep_seconds": retry_delay,
    }


async def run_npc_players_turn() -> None:
    # CLI mode doesn't require api_key; HTTP mode does.
    if not settings.enabled:
        return
    if settings.transport != "cli" and not settings.api_key:
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
            reflection_feedback: dict[str, Any] | None = None

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
                        execution_feedback=reflection_feedback,
                    )
                    if int(npc_user_refreshed.id_user) in {-1001, -1002}:
                        observation["v2_memory"] = await load_npc_v2_memory(
                            npc_user_refreshed.idpk
                        )

                # Phase 2: Action decision via LLM WITHOUT blocking DB session
                try:
                    decision = await client.choose_action(observation=observation)
                    llm_error_count = 0  # reset streak on success
                    await redis.delete(npc_llm_error_streak_key(npc_user.idpk))
                    if await redis.delete(npc_llm_degraded_key(npc_user.idpk)):
                        logging.warning(
                            "NPC %s: LLM connection recovered, decision loop back to normal.",
                            npc_user.nickname,
                        )
                except Exception as exc:
                    logging.exception(
                        f"LLM Error during action decision for {npc_user.nickname}"
                    )

                    # On any primary LLM error, try secondary provider/model once.
                    err_text = str(exc)
                    fallback_model_ok = False
                    if (
                        settings.fallback_model
                        and settings.fallback_api_key
                        and settings.fallback_base_url
                    ):
                        try:
                            decision = await client.choose_action_with_provider(
                                observation=observation,
                                model_override=settings.fallback_model,
                                base_url_override=settings.fallback_base_url,
                                api_key_override=settings.fallback_api_key,
                            )
                            decision["reason"] = preview_with_prefix(
                                f"fallback_model:{settings.fallback_model}",
                                decision.get("reason", ""),
                                max_chars=280,
                            )
                            llm_error_count = 0
                            fallback_model_ok = True
                            await redis.delete(npc_llm_error_streak_key(npc_user.idpk))
                        except Exception as fallback_exc:
                            logging.exception(
                                f"Fallback model error for {npc_user.nickname}"
                            )
                            exc = fallback_exc
                            err_text = str(exc)
                            kind = _classify_llm_error(err_text)
                            if kind in {"rate_limit", "transient"}:
                                try:
                                    await asyncio.sleep(2)
                                    decision = await client.choose_action_with_provider(
                                        observation=observation,
                                        model_override=settings.fallback_model,
                                        base_url_override=settings.fallback_base_url,
                                        api_key_override=settings.fallback_api_key,
                                    )
                                    decision["reason"] = preview_with_prefix(
                                        f"fallback_retry:{settings.fallback_model}",
                                        decision.get("reason", ""),
                                        max_chars=280,
                                    )
                                    llm_error_count = 0
                                    fallback_model_ok = True
                                    await redis.delete(
                                        npc_llm_error_streak_key(npc_user.idpk)
                                    )
                                except Exception as fallback_retry_exc:
                                    exc = fallback_retry_exc
                                    err_text = str(exc)

                    if not fallback_model_ok:
                        llm_error_count += 1
                        streak = int(
                            await redis.incr(npc_llm_error_streak_key(npc_user.idpk))
                            or 1
                        )
                        await redis.expire(
                            npc_llm_error_streak_key(npc_user.idpk), 3600
                        )
                        # Exponential backoff with ±10% jitter
                        base_delay = default_npc_sleep_seconds(
                            user=npc_user, salt="llm_error"
                        )
                        retry_delay = min(
                            int(base_delay * (2 ** min(streak, 6))), 4 * 3600
                        )
                        retry_delay += int(retry_delay * random.uniform(-0.1, 0.1))

                        # Degraded mode alert (one message per 30 min max)
                        degraded_mark = await redis.set(
                            npc_llm_degraded_key(npc_user.idpk),
                            datetime.now().isoformat(),
                            ex=1800,
                            nx=True,
                        )
                        if degraded_mark:
                            logging.warning(
                                "NPC %s: LLM degraded (%s). Fallback policy enabled.",
                                npc_user.nickname,
                                type(exc).__name__,
                            )

                        decision = _fallback_action_without_llm(
                            observation=observation,
                            retry_delay=retry_delay,
                        )
                        decision["reason"] = preview_with_prefix(
                            f"llm_error:{preview_error(err_text, max_chars=180)}",
                            decision["reason"],
                            max_chars=280,
                        )

                if decision.get("action") not in {"wait"} and "llm_error" not in str(
                    decision.get("reason", "")
                ):
                    with contextlib.suppress(Exception):
                        is_two_step_npc = int(
                            (observation.get("player") or {}).get("id_user", 0) or 0
                        ) in {-1001, -1002}
                        if is_two_step_npc:
                            eval_result = await client.optimize_decision(
                                decision=decision, observation=observation
                            )
                            correction_prefix = "optimizer_correction"
                        else:
                            eval_result = await client.evaluate_decision(
                                decision=decision, observation=observation
                            )
                            correction_prefix = "critic_correction"

                        if not eval_result.get("is_valid") and isinstance(
                            eval_result.get("correction"), dict
                        ):
                            corrected = dict(eval_result["correction"])
                            corrected["reason"] = (
                                f"{correction_prefix}:{eval_result.get('reason', '')} | {corrected.get('reason', '')}"
                            )
                            decision.update(corrected)

                action = validate_action(decision=decision)

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
                    if int(npc_user_refreshed.id_user) in {-1001, -1002}:
                        await update_npc_v2_memory(
                            user_idpk=npc_user_refreshed.idpk,
                            action=action,
                            result=result,
                            before_snapshot=before_snapshot,
                            after_snapshot=after_snapshot,
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
                            "decision_trace": {
                                "phase_1_observation": {
                                    "wake_context": observation.get("wake_context"),
                                    "planner": observation.get("planner"),
                                    "action_contract": observation.get(
                                        "action_contract"
                                    ),
                                },
                                "phase_2_decision": decision,
                                "phase_2_action": action,
                                "phase_3_execution": result,
                                "phase_3_delta": {
                                    "usd": int(after_snapshot.get("usd", 0) or 0)
                                    - int(before_snapshot.get("usd", 0) or 0),
                                    "rub": int(after_snapshot.get("rub", 0) or 0)
                                    - int(before_snapshot.get("rub", 0) or 0),
                                    "income_per_minute_rub": int(
                                        after_snapshot.get("income_per_minute_rub", 0)
                                        or 0
                                    )
                                    - int(
                                        before_snapshot.get("income_per_minute_rub", 0)
                                        or 0
                                    ),
                                    "animals": int(
                                        after_snapshot.get("total_animals", 0) or 0
                                    )
                                    - int(before_snapshot.get("total_animals", 0) or 0),
                                },
                                "quality_metrics": {
                                    "decision_execution_match": str(
                                        decision.get("action", "")
                                    ).strip()
                                    == str(action.get("action", "")).strip(),
                                    "rerouted": str(decision.get("action", "")).strip()
                                    != str(action.get("action", "")).strip(),
                                    "status_ok": str(result.get("status", ""))
                                    .strip()
                                    .lower()
                                    == "ok",
                                },
                            },
                        },
                    )
                except Exception:
                    pass

                status = str(result.get("status", "")).strip().lower()
                if status != "ok":
                    reflection_feedback = {
                        "failed_action": str(
                            result.get("failed_action")
                            or action.get("action")
                            or "wait"
                        ),
                        "error_code": str(
                            result.get("error_code")
                            or result.get("summary")
                            or "action_unavailable"
                        ),
                        "error_message": str(
                            result.get("error_message")
                            or result.get("summary")
                            or "action unavailable"
                        ),
                        "allowed_actions": list(result.get("allowed_actions") or []),
                        "retryable": bool(result.get("retryable", False)),
                        "cooldown_sec": int(result.get("cooldown_sec", 0) or 0),
                        "suggested_alternatives": list(
                            result.get("suggested_alternatives") or []
                        ),
                        "resource_deficit": result.get("resource_deficit"),
                    }
                else:
                    reflection_feedback = None

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
                    is_proactive = False
                    if wake_trigger and wake_trigger.get("source") == "scheduled":
                        player_obs = last_observation.get("player", {})
                        mood = player_obs.get("current_mood", "neutral")
                        affinity = player_obs.get("affinity_score", 50)
                        if (
                            mood in {"energetic", "positive", "chatty", "focused"}
                            or affinity > 70
                        ):
                            is_proactive = True

                    await maybe_send_npc_chat_comment(
                        client=client,
                        user=npc_user,
                        observation=last_observation,
                        before_snapshot=last_before_snapshot or {},
                        after_snapshot=last_after_snapshot,
                        action=last_action,
                        result=last_result,
                        planned_sleep_seconds=planned_sleep_seconds,
                        is_proactive=is_proactive,
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
        info_about_items="{}",
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
        result_summary = semantic_preview(
            result.get("summary", ""),
            max_segments=2,
            max_words=24,
            max_chars=180,
            placeholder="...",
        )
    if result_summary:
        return fit_db_field(
            f"{action_name}:{result_summary}",
            max_len=255,
            default="cycle_complete",
        )
    return fit_db_field(action_name, max_len=255, default="cycle_complete")


async def maybe_send_npc_chat_comment(
    client: NpcDecisionClient,
    user: User,
    observation: dict[str, Any],
    before_snapshot: dict[str, Any],
    after_snapshot: dict[str, Any],
    action: dict[str, Any],
    result: dict[str, Any],
    planned_sleep_seconds: int,
    is_proactive: bool = False,
) -> None:
    if not settings.chat_enabled:
        return

    import datetime as dt
    import pytz

    moscow_tz = pytz.timezone("Europe/Moscow")
    moscow_time = dt.datetime.now(moscow_tz)

    # Do not chat between 23:00 and 09:00 Moscow time
    if moscow_time.hour >= 23 or moscow_time.hour < 9:
        return

    cooldown_seconds = settings.chat_min_interval_seconds
    if is_proactive:
        cooldown_seconds = max(10, cooldown_seconds // 3)

    if cooldown_seconds <= 0 and not is_proactive:
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
        },
        "action": {
            "name": action.get("action", "wait"),
            "reason": semantic_preview(
                action.get("reason", ""),
                max_segments=2,
                max_words=30,
                max_chars=220,
                placeholder="...",
            ),
            "sleep_seconds": int(action.get("sleep_seconds") or planned_sleep_seconds),
        },
        "result": {
            "status": result.get("status", "unknown"),
            "summary": semantic_preview(
                result.get("summary", ""),
                max_segments=2,
                max_words=30,
                max_chars=220,
                placeholder="...",
            ),
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
    message = (await client.generate_chat_comment(payload=payload)).strip()
    if not message:
        return

    await bot.send_message(
        chat_id=CHAT_ID,
        text=html.escape(f"{signature}{message}", quote=False),
        disable_notification=True,
    )

    async with _sessionmaker_for_func() as session:
        from .memory import (
            ensure_npc_profile_memory,
            _json_loads,
            _rehydrate_profile_payload,
        )

        profile_row = await ensure_npc_profile_memory(session, user)
        profile_data = _rehydrate_profile_payload(
            user=user,
            payload=_json_loads(profile_row.payload),
        )
        recent = profile_data.get("recent_sent_chats", [])
        if not isinstance(recent, list):
            recent = []
        recent.insert(0, message)
        profile_data["recent_sent_chats"] = recent[:3]
        profile_row.payload = fast_dumps(profile_data)
        await session.commit()


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
