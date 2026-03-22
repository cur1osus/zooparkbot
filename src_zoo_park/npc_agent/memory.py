import hashlib
from datetime import datetime
from typing import TYPE_CHECKING, Any

from db import Item, NpcMemory, RequestToUnity, Unity, User
from db.structured_state import count_unity_members, get_user_aviaries_map
from fastjson import dumps as fast_dumps, loads as fast_loads
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from tools.animals import get_total_number_animals
from tools.aviaries import get_remain_seats, get_total_number_seats
from tools.income import income_
from tools.unity import get_unity_idpk
from tools.value import get_value
from text_utils import fit_db_field, semantic_preview

from .settings import settings

if TYPE_CHECKING:
    from .client import NpcDecisionClient


PROFILE_KIND = "profile"
EVENT_KIND = "event"
REFLECTION_KIND = "reflection"
GOAL_KIND = "goal"
RELATIONSHIP_KIND = "relationship"
FACT_KIND = "fact"

TACTIC_NAMES = (
    "economy_growth",
    "liquidity_control",
    "capacity_expansion",
    "unity_leverage",
    "item_engine",
    "leaderboard_pressure",
    "opportunistic_waiting",
)

ACTION_TACTIC_MAP = {
    "invest_for_income": ["economy_growth"],
    "buy_rarity_animal": ["economy_growth", "leaderboard_pressure"],
    "invest_for_top_animals": ["leaderboard_pressure", "economy_growth"],
    "exchange_bank": ["liquidity_control"],
    "claim_daily_bonus": ["liquidity_control", "opportunistic_waiting"],
    "wait": ["opportunistic_waiting", "liquidity_control"],
    "buy_aviary": ["capacity_expansion"],
    "create_unity": ["unity_leverage"],
    "join_best_unity": ["unity_leverage"],
    "recruit_top_player": ["unity_leverage", "leaderboard_pressure"],
    "review_unity_request": ["unity_leverage"],
    "upgrade_unity_level": ["unity_leverage"],
    "exit_from_unity": ["unity_leverage"],
    "send_chat_transfer": ["unity_leverage", "leaderboard_pressure"],
    "claim_chat_transfer": ["opportunistic_waiting", "leaderboard_pressure"],
    "create_chat_game": ["unity_leverage", "leaderboard_pressure"],
    "join_chat_game": ["leaderboard_pressure", "opportunistic_waiting"],
    "create_item": ["item_engine"],
    "optimize_items": ["item_engine"],
    "upgrade_item": ["item_engine"],
    "merge_items": ["item_engine"],
}


def _now() -> datetime:
    return datetime.now()


def _json_dumps(payload: dict[str, Any] | list[Any]) -> str:
    return fast_dumps(payload)


def _json_loads(payload: str | None) -> dict[str, Any]:
    if not payload:
        return {}
    try:
        value = fast_loads(payload)
    except Exception:
        return {}
    if isinstance(value, dict):
        return value
    return {"value": value}


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, int(value)))


def _count_bool(items: list[dict[str, Any]], key: str, expected: Any) -> int:
    return len([item for item in items if item.get(key) == expected])


def _pick_strings(value: Any, limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    result = []
    for item in value:
        text = str(item).strip()
        if not text:
            continue
        result.append(
            semantic_preview(text, max_segments=2, max_words=24, max_chars=200)
        )
        if len(result) >= limit:
            break
    return result


def _pick_dicts(value: Any, limit: int) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    result: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        result.append(item)
        if len(result) >= limit:
            break
    return result


def _next_income_milestone(current_income: int) -> int:
    if current_income < 100:
        return 100
    if current_income < 500:
        step = 50
    elif current_income < 2000:
        step = 100
    elif current_income < 10000:
        step = 500
    else:
        step = 1000
    return ((current_income + step - 1) // step) * step


def _ratio(current: int, target: int) -> float:
    if target <= 0:
        return 1.0
    return max(0.0, min(1.0, current / target))


def compute_goal_priority_score(goal: dict, context: dict) -> float:
    """
    Вычисляет приоритет цели на основе срочности, ROI и блокировок.
    Возвращает score для сортировки целей (выше = приоритетнее).
    """
    progress_ratio = float(goal.get("progress", {}).get("ratio", 0) or 0)
    priority = int(goal.get("priority", 500) or 500)
    horizon = str(goal.get("horizon", "medium")).strip().lower()
    
    # Блокировки — высший приоритет
    is_blocker = context.get("is_hard_blocker", False)
    if is_blocker:
        return 1000.0
    
    # Срочные цели получают бонус
    horizon_bonus = {"short": 150, "medium": 80, "long": 0}.get(horizon, 0)
    
    # Недостигнутые цели с высоким прогрессом приоритетнее (эффект завершения)
    completion_bonus = (1 - progress_ratio) * 100
    
    # Бонус за критически низкий прогресс
    urgency_bonus = 0
    if progress_ratio < 0.2:
        urgency_bonus = 80
    elif progress_ratio < 0.5:
        urgency_bonus = 40
    
    return float(priority) + float(horizon_bonus) + float(completion_bonus) + float(urgency_bonus)


def _topic_suffix() -> str:
    return _now().strftime("%Y%m%d%H%M%S%f")


def _tactic_shift_entry(
    tactic: str,
    delta: int,
    reason: str,
    source: str,
) -> dict[str, Any]:
    return {
        "tactic": tactic,
        "delta": int(delta),
        "reason": semantic_preview(reason, max_segments=1, max_words=18, max_chars=180),
        "source": fit_db_field(source, max_len=32, default="event"),
        "time": _now().isoformat(),
    }


def _limit_entries(items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if len(items) <= limit:
        return items
    return items[-limit:]


def _semantic_text_window(
    value: Any,
    *,
    max_segments: int = 2,
    max_words: int = 24,
    max_chars: int = 180,
) -> str:
    return semantic_preview(
        value,
        max_segments=max_segments,
        max_words=max_words,
        max_chars=max_chars,
    )


def _build_initial_tactic_scores() -> dict[str, int]:
    return {name: 400 for name in TACTIC_NAMES}


def _sanitize_tactic_scores_raw(existing_scores: Any) -> dict[str, int]:
    initial_scores = _build_initial_tactic_scores()
    payload = existing_scores if isinstance(existing_scores, dict) else {}
    return {
        name: _clamp(
            int(payload.get(name, initial_scores[name]) or initial_scores[name]),
            80,
            1_000_000,
        )
        for name in TACTIC_NAMES
    }


def _normalize_tactic_scores(
    tactic_scores_raw: dict[str, int],
    scale_max: int = 1000,
) -> dict[str, int]:
    safe_scale = max(100, int(scale_max or 1000))
    max_raw = max(1, max(int(v or 0) for v in tactic_scores_raw.values()))
    return {
        name: max(1, int(round((int(value or 0) / max_raw) * safe_scale)))
        for name, value in tactic_scores_raw.items()
    }


def _derive_active_tactics(tactic_scores: dict[str, int]) -> list[str]:
    ordered = sorted(
        tactic_scores.items(),
        key=lambda item: item[1],
        reverse=True,
    )
    return [name for name, _ in ordered[:3]]


def _rehydrate_profile_payload(
    user: User, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    tactic_scores_raw = _sanitize_tactic_scores_raw(
        source.get("tactic_scores_raw", source.get("tactic_scores"))
    )
    tactic_scores = _normalize_tactic_scores(tactic_scores_raw, scale_max=1000)
    adaptation_signals = source.get("adaptation_signals")
    if not isinstance(adaptation_signals, dict):
        adaptation_signals = {}
    adaptation_signals = {
        "last_updated_at": adaptation_signals.get("last_updated_at"),
        "last_event_action": adaptation_signals.get("last_event_action"),
        "last_event_result": adaptation_signals.get("last_event_result"),
        "success_streak": int(adaptation_signals.get("success_streak", 0) or 0),
        "failure_streak": int(adaptation_signals.get("failure_streak", 0) or 0),
        "recent_success_rate": float(
            adaptation_signals.get("recent_success_rate", 0.0) or 0.0
        ),
        "recent_trait_shifts": [],
        "recent_tactic_shifts": _limit_entries(
            [
                item
                for item in adaptation_signals.get("recent_tactic_shifts", [])
                if isinstance(item, dict)
            ],
            16,
        ),
        "last_reflection_summary": semantic_preview(
            adaptation_signals.get("last_reflection_summary", ""),
            max_segments=2,
            max_words=30,
            max_chars=240,
        ),
    }
    action_stats = source.get("action_stats")
    if not isinstance(action_stats, dict):
        action_stats = {}
    active_tactics = []
    for item in source.get("active_tactics", []):
        tactic = str(item).strip()
        if tactic not in TACTIC_NAMES or tactic in active_tactics:
            continue
        active_tactics.append(tactic)
        if len(active_tactics) >= 3:
            break
    if not active_tactics:
        active_tactics = _derive_active_tactics(tactic_scores_raw)
    return {
        "identity": {
            "npc_id_user": int(user.id_user),
            "nickname": user.nickname,
        },
        "current_mood": fit_db_field(
            source.get("current_mood", "neutral"),
            max_len=32,
            default="neutral",
        ),
        "active_strategic_goal": fit_db_field(
            source.get("active_strategic_goal", "compound_income_growth"),
            max_len=255,
            default="compound_income_growth",
        ),
        "affinity_score": _clamp(int(source.get("affinity_score", 50) or 50), 1, 100),
        "recent_sent_chats": source.get("recent_sent_chats", [])[:3],
        "tactic_scores_raw": tactic_scores_raw,
        "tactic_scores": tactic_scores,
        "active_tactics": active_tactics,
        "adaptation_signals": adaptation_signals,
        "action_stats": action_stats,
    }


async def _get_memory_row(
    session: AsyncSession,
    user_idpk: int,
    kind: str,
    topic: str,
) -> NpcMemory | None:
    return await session.scalar(
        select(NpcMemory).where(
            NpcMemory.idpk_user == user_idpk,
            NpcMemory.kind == kind,
            NpcMemory.topic == topic,
        )
    )


async def _upsert_memory_row(
    session: AsyncSession,
    user_idpk: int,
    kind: str,
    topic: str,
    payload: dict[str, Any],
    importance: int,
    confidence: int,
    status: str = "active",
) -> NpcMemory:
    row = await _get_memory_row(
        session=session,
        user_idpk=user_idpk,
        kind=kind,
        topic=topic,
    )
    now = _now()
    if row:
        row.payload = _json_dumps(payload)
        row.importance = _clamp(importance, 0, 1000)
        row.confidence = _clamp(confidence, 0, 1000)
        row.status = fit_db_field(status, max_len=32, default="active")
        row.updated_at = now
        return row
    row = NpcMemory(
        idpk_user=user_idpk,
        kind=fit_db_field(kind, max_len=32, default=PROFILE_KIND),
        topic=fit_db_field(topic, max_len=128, default="topic"),
        payload=_json_dumps(payload),
        importance=_clamp(importance, 0, 1000),
        confidence=_clamp(confidence, 0, 1000),
        status=fit_db_field(status, max_len=32, default="active"),
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    await session.flush()
    return row


async def _append_memory_row(
    session: AsyncSession,
    user_idpk: int,
    kind: str,
    topic: str,
    payload: dict[str, Any],
    importance: int,
    confidence: int,
    status: str = "active",
) -> NpcMemory:
    now = _now()
    row = NpcMemory(
        idpk_user=user_idpk,
        kind=fit_db_field(kind, max_len=32, default=EVENT_KIND),
        topic=fit_db_field(topic, max_len=128, default="topic"),
        payload=_json_dumps(payload),
        importance=_clamp(importance, 0, 1000),
        confidence=_clamp(confidence, 0, 1000),
        status=fit_db_field(status, max_len=32, default="active"),
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    await session.flush()
    return row


def build_npc_profile_payload(user: User) -> dict[str, Any]:
    return _rehydrate_profile_payload(user=user)


async def ensure_npc_profile_memory(session: AsyncSession, user: User) -> NpcMemory:
    row = await _get_memory_row(
        session=session,
        user_idpk=user.idpk,
        kind=PROFILE_KIND,
        topic="core",
    )
    payload = _rehydrate_profile_payload(
        user=user,
        payload=_json_loads(row.payload) if row else None,
    )

    # Avoid aggressive database updates every turn if profile hasn't changed.
    # Only update occasionally or on first creation.
    if row:
        # Check if we really need to save back
        last_update_ts = row.updated_at.timestamp() if row.updated_at else 0
        now_ts = _now().timestamp()

        # Only rewrite the profile memory row if it's older than 6 hours
        if now_ts - last_update_ts > 6 * 3600:
            row.payload = _json_dumps(payload)
            row.importance = 950
            row.confidence = 950
            row.status = "active"
            row.updated_at = _now()

        return row

    return await _upsert_memory_row(
        session=session,
        user_idpk=user.idpk,
        kind=PROFILE_KIND,
        topic="core",
        payload=payload,
        importance=950,
        confidence=950,
    )


async def build_npc_snapshot(session: AsyncSession, user: User) -> dict[str, Any]:
    income_value = await income_(session=session, user=user)
    aviary_map = await get_user_aviaries_map(session=session, user=user)
    total_seats = await get_total_number_seats(session=session, aviaries=aviary_map)
    remain_seats = await get_remain_seats(session=session, user=user)
    total_animals = await get_total_number_animals(self=user, session=session)
    items_count = int(
        await session.scalar(
            select(func.count()).select_from(Item).where(Item.id_user == user.id_user)
        )
        or 0
    )
    active_items = int(
        await session.scalar(
            select(func.count())
            .select_from(Item)
            .where(Item.id_user == user.id_user, Item.is_active == True)  # noqa: E712
        )
        or 0
    )
    current_unity_idpk = int(get_unity_idpk(user.current_unity) or 0) or None
    current_unity = (
        await session.get(Unity, current_unity_idpk) if current_unity_idpk else None
    )
    pending_requests_count = 0
    if current_unity and current_unity.idpk_user == user.idpk:
        pending_requests_count = int(
            await session.scalar(
                select(func.count())
                .select_from(RequestToUnity)
                .where(RequestToUnity.idpk_unity_owner == user.idpk)
            )
            or 0
        )
    return {
        "usd": int(user.usd),
        "rub": int(user.rub),
        "paw_coins": int(user.paw_coins),
        "income_per_minute_rub": int(income_value),
        "total_animals": int(total_animals),
        "total_seats": int(total_seats),
        "remain_seats": int(remain_seats),
        "daily_bonus_available": int(user.bonus),
        "items_owned": items_count,
        "active_items": active_items,
        "current_unity": user.current_unity,
        "current_unity_idpk": current_unity_idpk,
        "unity_level": int(current_unity.level) if current_unity else 0,
        "unity_members": (
            await count_unity_members(session=session, unity=current_unity)
            if current_unity
            else 0
        ),
        "pending_unity_requests": pending_requests_count,
        "moves_logged": int(user.moves),
    }


def extract_snapshot_from_observation(observation: dict[str, Any]) -> dict[str, Any]:
    player = observation.get("player", {})
    zoo = observation.get("zoo", {})
    items = observation.get("items", {})
    unity = observation.get("unity", {})
    current_unity = unity.get("current") or {}
    return {
        "usd": int(player.get("usd", 0)),
        "rub": int(player.get("rub", 0)),
        "paw_coins": int(player.get("paw_coins", 0)),
        "income_per_minute_rub": int(player.get("income_per_minute_rub", 0)),
        "total_animals": sum(int(v) for v in (zoo.get("animals") or {}).values()),
        "total_seats": int(zoo.get("total_seats", 0)),
        "remain_seats": int(zoo.get("remain_seats", 0)),
        "daily_bonus_available": int(player.get("daily_bonus_available", 0)),
        "items_owned": int(items.get("owned_count", 0)),
        "active_items": int(items.get("active_count", 0)),
        "current_unity": player.get("current_unity"),
        "current_unity_idpk": current_unity.get("idpk"),
        "unity_level": int(current_unity.get("level", 0) or 0),
        "unity_members": int(current_unity.get("members", 0) or 0),
        "pending_unity_requests": int(
            current_unity.get("pending_requests_count", 0) or 0
        ),
        "moves_logged": int(player.get("moves_logged", 0)),
        "income_rank": observation.get("strategy_signals", {})
        .get("summary", {})
        .get("income_rank"),
        "money_rank": observation.get("strategy_signals", {})
        .get("summary", {})
        .get("money_rank"),
        "animals_rank": observation.get("strategy_signals", {})
        .get("summary", {})
        .get("animals_rank"),
        "create_item_price_usd": int(items.get("create_price_usd", 0) or 0),
    }


async def refresh_npc_goals(
    session: AsyncSession,
    user: User,
    snapshot: dict[str, Any],
    observation: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    price_create_unity = int(
        await get_value(session=session, value_name="PRICE_FOR_CREATE_UNITY")
    )
    create_item_price = int(
        snapshot.get("create_item_price_usd")
        or observation.get("items", {}).get("create_price_usd", 0)
        if observation
        else 0
    )
    active_payloads: list[dict[str, Any]] = []

    current_income = int(snapshot.get("income_per_minute_rub", 0))
    usd_balance = int(snapshot.get("usd", 0) or 0)
    remain_seats = int(snapshot.get("remain_seats", 0) or 0)
    total_seats = int(snapshot.get("total_seats", 0) or 0)
    income_rank = snapshot.get("income_rank")
    target_income = max(
        _next_income_milestone(current_income + max(25, current_income // 4)),
        current_income + 50,
    )
    income_gap = max(0, target_income - current_income)
    active_payloads.append(
        {
            "topic": "economy_growth",
            "title": "Grow recurring income",
            "summary": "Raise passive income to unlock stronger compounding decisions.",
            "target": {
                "metric": "income_per_minute_rub",
                "value": target_income,
            },
            "progress": {
                "current": current_income,
                "target": target_income,
                "ratio": round(_ratio(current_income, target_income), 3),
            },
            "priority": _clamp(
                620 + min(220, income_gap * 2) + (40 if current_income < 150 else 0),
                0,
                1000,
            ),
            "horizon": "medium",
            "recommended_actions": ["invest_for_income", "buy_rarity_animal"],
            "success_signal": f"Reach {target_income} RUB/min.",
        }
    )

    if remain_seats <= max(1, total_seats // 5):
        target_free_seats = max(2, int(snapshot.get("total_animals", 0)) // 4)
        seat_gap = max(0, target_free_seats - remain_seats)
        active_payloads.append(
            {
                "topic": "capacity_balance",
                "title": "Restore seat capacity",
                "summary": "Keep enough free seats so profitable animal buys are never blocked.",
                "target": {
                    "metric": "remain_seats",
                    "value": target_free_seats,
                },
                "progress": {
                    "current": int(snapshot.get("remain_seats", 0)),
                    "target": target_free_seats,
                    "ratio": round(
                        _ratio(
                            int(snapshot.get("remain_seats", 0)),
                            target_free_seats,
                        ),
                        3,
                    ),
                },
                "priority": _clamp(760 + min(220, seat_gap * 90), 0, 1000),
                "horizon": "short",
                "recommended_actions": ["buy_aviary"],
                "success_signal": f"Hold at least {target_free_seats} free seats.",
            }
        )

    summary = (
        (observation or {}).get("strategy_signals", {}).get("summary", {})
        if observation
        else {}
    )
    next_unlock = summary.get("next_unlock") or {}
    next_unlock_target = int(next_unlock.get("target_usd", 0) or 0)
    best_income_option = summary.get("best_income_option") or {}
    best_income_price = int(best_income_option.get("price_usd", 0) or 0)
    immediate_engine_target = max(next_unlock_target, best_income_price)

    # Dynamic liquidity target: stay near current execution horizon, don't anchor on item cost.
    # Floor keeps some flexibility; cap prevents unrealistic long-term hoarding pressure.
    reserve_target = int(max(350, immediate_engine_target * 1.2, price_create_unity))
    reserve_target = min(reserve_target, 12000)
    reserve_gap = max(0, reserve_target - usd_balance)

    active_payloads.append(
        {
            "topic": "liquidity_buffer",
            "title": "Maintain strategic liquidity",
            "summary": "Keep enough USD to execute the next unlock line without stalling compounding.",
            "target": {"metric": "usd", "value": reserve_target},
            "progress": {
                "current": usd_balance,
                "target": reserve_target,
                "ratio": round(_ratio(usd_balance, reserve_target), 3),
            },
            "priority": _clamp(
                480 + min(260, reserve_gap // 12) + (60 if usd_balance < 250 else 0),
                0,
                1000,
            ),
            "horizon": "short",
            "recommended_actions": ["wait", "exchange_bank", "claim_daily_bonus"],
            "success_signal": f"Keep at least {reserve_target} USD liquid.",
        }
    )

    if snapshot.get("current_unity"):
        if str(snapshot.get("current_unity", "")).startswith("owner:"):
            target_members = max(3, min(8, int(snapshot.get("unity_members", 1)) + 2))
            active_payloads.append(
                {
                    "topic": "unity_leadership",
                    "title": "Strengthen unity leadership",
                    "summary": "Convert social leverage into better members and higher clan income.",
                    "target": {
                        "metric": "unity_members",
                        "value": target_members,
                    },
                    "progress": {
                        "current": int(snapshot.get("unity_members", 1)),
                        "target": target_members,
                        "ratio": round(
                            _ratio(
                                int(snapshot.get("unity_members", 1)),
                                target_members,
                            ),
                            3,
                        ),
                    },
                    "priority": _clamp(
                        580
                        + min(
                            220,
                            max(
                                0,
                                target_members - int(snapshot.get("unity_members", 1)),
                            )
                            * 90,
                        )
                        + min(
                            120,
                            int(snapshot.get("pending_unity_requests", 0) or 0) * 40,
                        ),
                        0,
                        1000,
                    ),
                    "horizon": "medium",
                    "recommended_actions": [
                        "review_unity_request",
                        "recruit_top_player",
                        "upgrade_unity_level",
                    ],
                    "success_signal": f"Reach {target_members} unity members.",
                }
            )
        else:
            active_payloads.append(
                {
                    "topic": "unity_contribution",
                    "title": "Increase value inside current unity",
                    "summary": "Grow strong enough to become a meaningful contributor inside the clan.",
                    "target": {
                        "metric": "income_per_minute_rub",
                        "value": max(150, _next_income_milestone(current_income + 100)),
                    },
                    "progress": {
                        "current": current_income,
                        "target": max(
                            150, _next_income_milestone(current_income + 100)
                        ),
                        "ratio": round(
                            _ratio(
                                current_income,
                                max(150, _next_income_milestone(current_income + 100)),
                            ),
                            3,
                        ),
                    },
                    "priority": _clamp(
                        470
                        + min(
                            180,
                            max(
                                0,
                                max(150, _next_income_milestone(current_income + 100))
                                - current_income,
                            )
                            * 2,
                        ),
                        0,
                        1000,
                    ),
                    "horizon": "medium",
                    "recommended_actions": ["invest_for_income", "optimize_items"],
                    "success_signal": "Raise contribution to clan income.",
                }
            )
    else:
        can_create_unity = usd_balance >= price_create_unity
        build_own_unity = can_create_unity and current_income >= 180
        recommended_actions = (
            ["create_unity", "join_best_unity"]
            if build_own_unity
            else ["join_best_unity"]
        )
        active_payloads.append(
            {
                "topic": "unity_position",
                "title": "Secure a stronger social position",
                "summary": "Stop playing alone when a clan can accelerate growth and recruitment.",
                "target": {
                    "metric": "current_unity",
                    "value": "member_or_owner",
                },
                "progress": {
                    "current": 0,
                    "target": 1,
                    "ratio": 0.0,
                },
                "priority": _clamp(
                    520
                    + (120 if can_create_unity else 0)
                    + (
                        80
                        if int(snapshot.get("income_per_minute_rub", 0) or 0) >= 180
                        else 0
                    ),
                    0,
                    1000,
                ),
                "horizon": "medium",
                "recommended_actions": recommended_actions,
                "success_signal": "Join or create a useful unity.",
            }
        )

    if int(snapshot.get("items_owned", 0)) < 3 or int(
        snapshot.get("active_items", 0)
    ) < min(3, int(snapshot.get("items_owned", 0) or 0)):
        active_payloads.append(
            {
                "topic": "item_program",
                "title": "Improve item quality",
                "summary": "Build a better passive modifier stack through creation, activation, and upgrades.",
                "target": {
                    "metric": "active_items",
                    "value": 3,
                },
                "progress": {
                    "current": int(snapshot.get("active_items", 0)),
                    "target": 3,
                    "ratio": round(_ratio(int(snapshot.get("active_items", 0)), 3), 3),
                },
                "priority": _clamp(
                    360
                    + min(
                        220, max(0, 3 - int(snapshot.get("active_items", 0) or 0)) * 90
                    )
                    + (
                        60
                        if create_item_price and usd_balance >= create_item_price
                        else 0
                    ),
                    0,
                    1000,
                ),
                "horizon": "medium",
                "recommended_actions": [
                    "create_item",
                    "optimize_items",
                    "upgrade_item",
                    "merge_items",
                ],
                "success_signal": "Maintain three strong active items.",
            }
        )

    if isinstance(income_rank, int) and income_rank > 3:
        active_payloads.append(
            {
                "topic": "leaderboard_push",
                "title": "Climb the income leaderboard",
                "summary": "Use compounding and timing to break into a higher leaderboard tier.",
                "target": {
                    "metric": "income_rank",
                    "value": max(1, income_rank - 2),
                },
                "progress": {
                    "current": income_rank,
                    "target": max(1, income_rank - 2),
                    "ratio": round(
                        1.0 - min(1.0, income_rank / max(1, income_rank + 2)), 3
                    ),
                },
                "priority": _clamp(
                    500 + min(260, max(0, income_rank - 3) * 55),
                    0,
                    1000,
                ),
                "horizon": "long",
                "recommended_actions": ["invest_for_income", "buy_rarity_animal"],
                "success_signal": "Climb at least two places in income rank.",
            }
        )

    active_topics = set()
    
    # Build context for priority computation
    priority_context = {
        "is_hard_blocker": int(snapshot.get("remain_seats", 0) or 0) <= 0,
        "usd_balance": int(snapshot.get("usd", 0) or 0),
        "income_per_minute": int(snapshot.get("income_per_minute_rub", 0) or 0),
    }
    
    # Sort payloads by computed priority score instead of raw priority
    sorted_payloads = sorted(
        active_payloads,
        key=lambda item: compute_goal_priority_score(item, priority_context),
        reverse=True,
    )
    
    for payload in sorted_payloads:
        active_topics.add(payload["topic"])
        await _upsert_memory_row(
            session=session,
            user_idpk=user.idpk,
            kind=GOAL_KIND,
            topic=payload["topic"],
            payload=payload,
            importance=int(payload["priority"]),
            confidence=880,
        )

    old_goals = await session.scalars(
        select(NpcMemory).where(
            NpcMemory.idpk_user == user.idpk,
            NpcMemory.kind == GOAL_KIND,
            NpcMemory.status == "active",
        )
    )
    for row in old_goals.all():
        if row.topic in active_topics:
            continue
        row.status = "archived"
        row.updated_at = _now()

    rows = await session.scalars(
        select(NpcMemory)
        .where(
            NpcMemory.idpk_user == user.idpk,
            NpcMemory.kind == GOAL_KIND,
            NpcMemory.status == "active",
        )
        .order_by(NpcMemory.importance.desc(), NpcMemory.updated_at.desc())
    )
    return [
        _json_loads(row.payload) for row in rows.all()[: settings.memory_goal_limit]
    ]


def _event_subjects(
    action: dict[str, Any],
    result: dict[str, Any],
    wake_trigger: dict[str, Any],
    observation: dict[str, Any],
) -> list[dict[str, Any]]:
    params = action.get("params", {}) or {}
    action_name = str(action.get("action", "wait"))
    result_status = str(result.get("status", ""))
    subjects: list[dict[str, Any]] = []
    wake_reason = str(wake_trigger.get("reason", ""))

    if action_name == "join_best_unity":
        owner_idpk = int(params.get("owner_idpk") or 0)
        candidate = None
        for row in observation.get("unity", {}).get("candidates", []):
            if owner_idpk and int(row.get("owner_idpk", 0)) == owner_idpk:
                candidate = row
                break
        if not candidate and observation.get("unity", {}).get("candidates"):
            candidate = observation["unity"]["candidates"][0]
        if candidate:
            subjects.append(
                {
                    "kind": "unity_owner",
                    "topic": f"user:{candidate['owner_idpk']}",
                    "subject_idpk": int(candidate["owner_idpk"]),
                    "display_name": candidate.get("owner_nickname")
                    or candidate.get("name"),
                    "event": "joined_unity"
                    if result_status == "ok"
                    else "unity_attempt",
                    "delta_trust": 35 if result_status == "ok" else 5,
                    "delta_affinity": 25 if result_status == "ok" else 0,
                }
            )
    elif action_name == "recruit_top_player":
        target_idpk = int(params.get("idpk_user") or 0)
        candidate = None
        for row in observation.get("unity", {}).get("recruit_targets", []):
            if target_idpk and int(row.get("idpk", 0)) == target_idpk:
                candidate = row
                break
        if candidate:
            subjects.append(
                {
                    "kind": "user",
                    "topic": f"user:{candidate['idpk']}",
                    "subject_idpk": int(candidate["idpk"]),
                    "display_name": candidate.get("nickname"),
                    "event": "invite_sent",
                    "delta_trust": 8,
                    "delta_affinity": 12,
                }
            )
    elif action_name == "review_unity_request":
        target_idpk = int(params.get("idpk_user") or 0)
        decision = str(params.get("decision", "accept"))
        subjects.append(
            {
                "kind": "user",
                "topic": f"user:{target_idpk}",
                "subject_idpk": target_idpk,
                "display_name": None,
                "event": f"request_{decision}",
                "delta_trust": 25 if decision == "accept" else -10,
                "delta_affinity": 20 if decision == "accept" else -5,
            }
        )

    if wake_reason.startswith("unity_request:"):
        applicant_idpk = int(wake_reason.split(":")[-1] or 0)
        subjects.append(
            {
                "kind": "user",
                "topic": f"user:{applicant_idpk}",
                "subject_idpk": applicant_idpk,
                "display_name": None,
                "event": "incoming_request",
                "delta_trust": 18,
                "delta_affinity": 10,
            }
        )
    elif wake_reason.startswith("npc_invite_accepted:"):
        target_idpk = int(wake_reason.split(":")[-1] or 0)
        subjects.append(
            {
                "kind": "user",
                "topic": f"user:{target_idpk}",
                "subject_idpk": target_idpk,
                "display_name": None,
                "event": "invite_accepted",
                "delta_trust": 45,
                "delta_affinity": 35,
            }
        )
    elif wake_reason.startswith("npc_invite_rejected:"):
        target_idpk = int(wake_reason.split(":")[-1] or 0)
        subjects.append(
            {
                "kind": "user",
                "topic": f"user:{target_idpk}",
                "subject_idpk": target_idpk,
                "display_name": None,
                "event": "invite_rejected",
                "delta_trust": -20,
                "delta_affinity": -12,
            }
        )
    elif wake_reason.startswith("unity_member_left:"):
        target_idpk = int(wake_reason.split(":")[-1] or 0)
        subjects.append(
            {
                "kind": "user",
                "topic": f"user:{target_idpk}",
                "subject_idpk": target_idpk,
                "display_name": None,
                "event": "member_left",
                "delta_trust": -30,
                "delta_affinity": -20,
            }
        )
    return subjects


async def _update_relationship_memories(
    session: AsyncSession,
    user: User,
    action: dict[str, Any],
    result: dict[str, Any],
    wake_trigger: dict[str, Any],
    observation: dict[str, Any],
) -> None:
    for subject in _event_subjects(action, result, wake_trigger, observation):
        row = await _get_memory_row(
            session=session,
            user_idpk=user.idpk,
            kind=RELATIONSHIP_KIND,
            topic=subject["topic"],
        )
        payload = _json_loads(row.payload if row else "{}")
        payload.setdefault("subject_type", subject.get("kind", "user"))
        payload.setdefault("subject_idpk", int(subject.get("subject_idpk", 0)))
        payload.setdefault("display_name", subject.get("display_name"))
        payload.setdefault("trust", 500)
        payload.setdefault("affinity", 500)
        payload.setdefault("interactions", 0)
        payload.setdefault("accepted_invites", 0)
        payload.setdefault("rejected_invites", 0)
        payload.setdefault("incoming_requests", 0)
        payload.setdefault("members_left", 0)
        payload["interactions"] = int(payload.get("interactions", 0)) + 1
        payload["display_name"] = subject.get("display_name") or payload.get(
            "display_name"
        )
        payload["trust"] = _clamp(
            int(payload.get("trust", 500)) + int(subject.get("delta_trust", 0)),
            0,
            1000,
        )
        payload["affinity"] = _clamp(
            int(payload.get("affinity", 500)) + int(subject.get("delta_affinity", 0)),
            0,
            1000,
        )
        event_name = str(subject.get("event", "interaction"))
        payload["last_event"] = event_name
        payload["last_event_at"] = _now().isoformat()
        if event_name == "invite_accepted":
            payload["accepted_invites"] = int(payload.get("accepted_invites", 0)) + 1
            payload["status"] = "ally"
        elif event_name == "invite_rejected":
            payload["rejected_invites"] = int(payload.get("rejected_invites", 0)) + 1
            payload["status"] = "resistant"
        elif event_name == "incoming_request":
            payload["incoming_requests"] = int(payload.get("incoming_requests", 0)) + 1
            payload["status"] = "interested"
        elif event_name == "member_left":
            payload["members_left"] = int(payload.get("members_left", 0)) + 1
            payload["status"] = "unstable"
        elif event_name == "joined_unity":
            payload["status"] = "leader"
        elif event_name == "request_accept":
            payload["status"] = "member"
        importance = _clamp(
            (int(payload.get("trust", 500)) + int(payload.get("affinity", 500))) // 2,
            0,
            1000,
        )
        await _upsert_memory_row(
            session=session,
            user_idpk=user.idpk,
            kind=RELATIONSHIP_KIND,
            topic=subject["topic"],
            payload=payload,
            importance=importance,
            confidence=800,
        )


def _action_tactics(action_name: str) -> list[str]:
    return ACTION_TACTIC_MAP.get(action_name, ["economy_growth"])


def _validated_reflection_tactics(reflection_payload: dict[str, Any]) -> list[str]:
    tactics = []
    for item in reflection_payload.get("tactical_focus", []):
        tactic = str(item).strip()
        if tactic not in TACTIC_NAMES or tactic in tactics:
            continue
        tactics.append(tactic)
    return tactics


def _compute_success_score_from_outcome(
    action_name: str,
    result: dict[str, Any],
    delta: dict[str, Any],
) -> float:
    """
    Вычисляет score успешности действия на основе результата и изменений.
    Возвращает значение от -100 до +100.
    """
    status = str(result.get("status", "")).strip().lower()
    summary = str(result.get("summary", "")).strip().lower()
    
    # Базовый score по статусу
    if status == "ok":
        base_score = 50.0
    elif status == "skipped":
        # Skipped — нейтральный результат, не неудача
        if summary in {"no_bonus", "merchant_offer_used", "item_not_found"}:
            base_score = 10.0
        else:
            base_score = -10.0
    else:
        # Error или failed
        base_score = -50.0
    
    # Бонусы за конкретные метрики
    delta_income = int(delta.get("income_per_minute_rub", 0) or 0)
    delta_usd = int(delta.get("usd", 0) or 0)
    delta_animals = int(delta.get("animals", 0) or 0)
    delta_seats = int(delta.get("seats", 0) or 0)
    
    # Рост дохода — всегда хорошо
    if delta_income > 0:
        base_score += min(40.0, delta_income * 0.5)
    elif delta_income < 0:
        base_score -= min(30.0, abs(delta_income) * 0.3)
    
    # Рост USD (если не за счет продажи животных)
    if action_name not in {"sell_item", "exchange_bank"} and delta_usd > 0:
        base_score += min(20.0, delta_usd * 0.05)
    
    # Рост количества животных
    if delta_animals > 0:
        base_score += min(15.0, delta_animals * 2.0)
    
    # Рост мест (авиарии)
    if delta_seats > 0:
        base_score += min(20.0, delta_seats * 3.0)
    
    # Специфичные бонусы для действий
    if action_name == "claim_daily_bonus" and status == "ok":
        base_score += 15.0
    
    if action_name == "review_unity_request" and status == "ok":
        base_score += 10.0
    
    # Штраф за повторяющиеся ошибки
    if "already" in summary or "duplicate" in summary:
        base_score -= 20.0
    
    return max(-100.0, min(100.0, base_score))


def _derive_event_tactic_adjustments(
    current_event: dict[str, Any],
    after_snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    action_name = str(current_event.get("action", {}).get("name", "wait"))
    result = current_event.get("result", {}) or {}
    delta = current_event.get("delta", {})
    tactics = _action_tactics(action_name)
    updates: list[dict[str, Any]] = []
    
    # Вычисляем success score для действия
    success_score = _compute_success_score_from_outcome(
        action_name=action_name,
        result=result,
        delta=delta,
    )
    
    # Базовое изменение тактики на основе успеха
    if success_score > 30:
        base_shift = 12  # Сильный успех
    elif success_score > 0:
        base_shift = 6  # Умеренный успех
    elif success_score > -20:
        base_shift = 0  # Нейтрально
    else:
        base_shift = -8  # Неудача
    
    for tactic in tactics:
        delta_value = base_shift
        
        # Дополнительные бонусы для специфичных тактик
        if tactic == "economy_growth":
            delta_value += min(12, max(0, int(delta.get("income_per_minute_rub", 0)) // 20))
        elif tactic == "capacity_expansion":
            if int(delta.get("seats", 0)) > 0:
                delta_value += 8
            if int(after_snapshot.get("remain_seats", 0)) <= 0:
                delta_value += settings.memory_tactic_step_limit // 2  # Seat pressure
        elif tactic == "unity_leverage":
            if int(delta.get("unity_members", 0)) > 0:
                delta_value += 10
        elif tactic == "liquidity_control":
            if int(after_snapshot.get("usd", 0)) < 300:
                delta_value += 6  # Need more liquidity
            elif int(delta.get("usd", 0)) > 100:
                delta_value += 4  # Good liquidity management
        elif tactic == "leaderboard_pressure":
            if int(delta.get("animals", 0)) > 0:
                delta_value += 5
        
        updates.append(
            {
                "tactic": tactic,
                "delta": _clamp(
                    delta_value,
                    -settings.memory_tactic_step_limit,
                    settings.memory_tactic_step_limit,
                ),
                "reason": f"event_{action_name}",
                "source": "event",
            }
        )
    
    # Принудительное усиление тактик при блокировках
    if int(after_snapshot.get("remain_seats", 0)) <= 0:
        updates.append(
            {
                "tactic": "capacity_expansion",
                "delta": settings.memory_tactic_step_limit,
                "reason": "seat_pressure",
                "source": "state",
            }
        )
    
    return updates


def _is_neutral_action_outcome(action_name: str, result: dict[str, Any]) -> bool:
    status = str(result.get("status", "")).strip().lower()
    summary = str(result.get("summary", "")).strip().lower()

    if action_name == "recruit_top_player":
        # These outcomes are selection/state artifacts, not strategic failures.
        if status == "ok" and summary.startswith("recruit_invite"):
            return True
        if summary in {"invite_already_sent", "recruit_target_unavailable"}:
            return True
        if summary.startswith("reject_unity_request"):
            return True

    return False


def _update_action_stats(
    profile: dict[str, Any], current_event: dict[str, Any]
) -> None:
    action_name = str(current_event.get("action", {}).get("name", "wait"))
    result = current_event.get("result", {}) or {}
    stats = profile.setdefault("action_stats", {})
    if not isinstance(stats, dict):
        stats = {}
        profile["action_stats"] = stats
    action_stats = stats.setdefault(action_name, {})
    action_stats["attempts"] = int(action_stats.get("attempts", 0)) + 1

    if _is_neutral_action_outcome(action_name, result):
        action_stats["neutral"] = int(action_stats.get("neutral", 0)) + 1
    elif result.get("status") == "ok":
        action_stats["successes"] = int(action_stats.get("successes", 0)) + 1
    else:
        action_stats["failures"] = int(action_stats.get("failures", 0)) + 1

    action_stats["net_usd_delta"] = int(action_stats.get("net_usd_delta", 0)) + int(
        current_event.get("delta", {}).get("usd", 0)
    )
    action_stats["net_income_delta"] = int(
        action_stats.get("net_income_delta", 0)
    ) + int(current_event.get("delta", {}).get("income_per_minute_rub", 0))
    action_stats["net_animals_delta"] = int(
        action_stats.get("net_animals_delta", 0)
    ) + int(current_event.get("delta", {}).get("animals", 0))
    action_stats["last_used_at"] = current_event.get("time")
    action_stats["last_result"] = current_event.get("result", {}).get("status")


async def evolve_npc_profile(
    session: AsyncSession,
    user: User,
    current_event: dict[str, Any],
    after_snapshot: dict[str, Any],
    reflection_payload: dict[str, Any] | None = None,
) -> None:
    row = await ensure_npc_profile_memory(session=session, user=user)
    profile = _rehydrate_profile_payload(user=user, payload=_json_loads(row.payload))
    tactic_scores = _sanitize_tactic_scores_raw(
        profile.get("tactic_scores_raw", profile.get("tactic_scores"))
    )
    adaptation_signals = profile["adaptation_signals"]

    tactic_updates = _derive_event_tactic_adjustments(
        current_event=current_event,
        after_snapshot=after_snapshot,
    )
    if reflection_payload:
        tactic_updates.extend(
            {
                "tactic": tactic,
                "delta": max(2, settings.memory_tactic_step_limit // 2),
                "reason": "reflection_focus",
                "source": "reflection_model",
            }
            for tactic in _validated_reflection_tactics(reflection_payload)
        )

    tactic_shift_log = list(adaptation_signals.get("recent_tactic_shifts", []))
    for update in tactic_updates:
        tactic = str(update.get("tactic", ""))
        if tactic not in TACTIC_NAMES:
            continue
        delta_value = _clamp(
            int(update.get("delta", 0) or 0),
            -settings.memory_tactic_step_limit,
            settings.memory_tactic_step_limit,
        )
        if delta_value == 0:
            continue
        tactic_scores[tactic] = _clamp(
            int(tactic_scores.get(tactic, 400)) + delta_value,
            80,
            1_000_000,
        )
        tactic_shift_log.append(
            _tactic_shift_entry(
                tactic=tactic,
                delta=delta_value,
                reason=str(update.get("reason", "tactic_adjustment")),
                source=str(update.get("source", "event")),
            )
        )

    _update_action_stats(profile=profile, current_event=current_event)
    profile["tactic_scores_raw"] = _sanitize_tactic_scores_raw(tactic_scores)
    profile["tactic_scores"] = _normalize_tactic_scores(
        profile["tactic_scores_raw"], scale_max=1000
    )
    profile["active_tactics"] = _derive_active_tactics(profile["tactic_scores_raw"])

    result_status = str(current_event.get("result", {}).get("status", ""))
    if result_status == "ok":
        adaptation_signals["success_streak"] = (
            int(adaptation_signals.get("success_streak", 0)) + 1
        )
        adaptation_signals["failure_streak"] = 0
    else:
        adaptation_signals["failure_streak"] = (
            int(adaptation_signals.get("failure_streak", 0)) + 1
        )
        adaptation_signals["success_streak"] = 0
    adaptation_signals["last_updated_at"] = _now().isoformat()
    adaptation_signals["last_event_action"] = current_event.get("action", {}).get(
        "name"
    )
    adaptation_signals["last_event_result"] = result_status
    adaptation_signals["recent_success_rate"] = round(
        _build_progress_summary([current_event]).get("success_rate", 0.0),
        3,
    )
    adaptation_signals["recent_trait_shifts"] = []
    adaptation_signals["recent_tactic_shifts"] = _limit_entries(tactic_shift_log, 16)
    if reflection_payload:
        new_goal = reflection_payload.get("active_strategic_goal")
        if new_goal:
            profile["active_strategic_goal"] = fit_db_field(str(new_goal), max_len=255)

        adaptation_signals["last_reflection_summary"] = semantic_preview(
            reflection_payload.get("summary", ""),
            max_segments=2,
            max_words=30,
            max_chars=240,
        )
    profile["adaptation_signals"] = adaptation_signals

    row.payload = _json_dumps(profile)
    row.updated_at = _now()


def _build_event_payload(
    user: User,
    observation: dict[str, Any],
    before_snapshot: dict[str, Any],
    after_snapshot: dict[str, Any],
    action: dict[str, Any],
    result: dict[str, Any],
    wake_trigger: dict[str, Any],
    decision_index: int,
) -> dict[str, Any]:
    action_name = str(action.get("action", "wait"))
    delta = {
        "usd": int(after_snapshot.get("usd", 0)) - int(before_snapshot.get("usd", 0)),
        "rub": int(after_snapshot.get("rub", 0)) - int(before_snapshot.get("rub", 0)),
        "income_per_minute_rub": int(after_snapshot.get("income_per_minute_rub", 0))
        - int(before_snapshot.get("income_per_minute_rub", 0)),
        "animals": int(after_snapshot.get("total_animals", 0))
        - int(before_snapshot.get("total_animals", 0)),
        "seats": int(after_snapshot.get("total_seats", 0))
        - int(before_snapshot.get("total_seats", 0)),
        "remain_seats": int(after_snapshot.get("remain_seats", 0))
        - int(before_snapshot.get("remain_seats", 0)),
        "items_owned": int(after_snapshot.get("items_owned", 0))
        - int(before_snapshot.get("items_owned", 0)),
        "unity_members": int(after_snapshot.get("unity_members", 0))
        - int(before_snapshot.get("unity_members", 0)),
    }
    importance = 450
    if result.get("status") == "error":
        importance += 180
    if result.get("status") == "ok":
        importance += 60
    importance += min(160, abs(delta["income_per_minute_rub"]) * 3)
    importance += min(140, abs(delta["usd"]) // 5)
    importance += min(120, abs(delta["animals"]) * 25)
    importance += min(80, abs(delta["unity_members"]) * 40)
    if action_name in {
        "create_unity",
        "join_best_unity",
        "recruit_top_player",
        "review_unity_request",
    }:
        importance += 110
    if wake_trigger.get("source") == "event":
        importance += 45
    importance = _clamp(importance, 0, 1000)
    return {
        "type": "action_result",
        "time": _now().isoformat(),
        "npc": {
            "id_user": int(user.id_user),
            "nickname": user.nickname,
        },
        "decision_index": int(decision_index),
        "wake_context": {
            "source": wake_trigger.get("source"),
            "reason": wake_trigger.get("reason"),
        },
        "action": {
            "name": action_name,
            "params": action.get("params", {}),
            "reason": action.get("reason", ""),
            "sleep_seconds": action.get("sleep_seconds"),
        },
        "result": result,
        "before": before_snapshot,
        "after": after_snapshot,
        "delta": delta,
        "current_focus": [
            goal.get("topic")
            for goal in observation.get("memory", {}).get("active_goals", [])
        ][: settings.memory_goal_limit],
        "strategy_summary": observation.get("strategy_signals", {}).get("summary", {}),
        "planner": observation.get("planner", {}),
        "behavior_guidance": observation.get("memory", {}).get("behavior_guidance", {}),
        "importance": importance,
    }


def _deterministic_reflection(
    events: list[dict[str, Any]],
    active_goals: list[dict[str, Any]],
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    success_actions: dict[str, int] = {}
    failed_actions: dict[str, int] = {}
    usd_delta_total = 0
    income_delta_total = 0
    animal_delta_total = 0
    for event in events:
        action_name = str(event.get("action", {}).get("name", "wait"))
        if event.get("result", {}).get("status") == "ok":
            success_actions[action_name] = success_actions.get(action_name, 0) + 1
        else:
            failed_actions[action_name] = failed_actions.get(action_name, 0) + 1
        delta = event.get("delta", {})
        usd_delta_total += int(delta.get("usd", 0))
        income_delta_total += int(delta.get("income_per_minute_rub", 0))
        animal_delta_total += int(delta.get("animals", 0))

    best_action = (
        max(success_actions, key=lambda name: success_actions[name])
        if success_actions
        else "wait"
    )
    worst_action = (
        max(failed_actions, key=lambda name: failed_actions[name])
        if failed_actions
        else None
    )
    summary_parts = []
    if income_delta_total > 0:
        summary_parts.append(
            f"Income trend is up by {income_delta_total} RUB/min recently"
        )
    elif income_delta_total < 0:
        summary_parts.append(
            f"Income trend slipped by {abs(income_delta_total)} RUB/min"
        )
    if usd_delta_total < 0:
        summary_parts.append(f"USD spending pace is {-usd_delta_total}")
    if animal_delta_total > 0:
        summary_parts.append(f"zoo size grew by {animal_delta_total} animals")
    if not summary_parts:
        summary_parts.append("Recent turns were mostly positional and informational")

    lessons = []
    if best_action != "wait":
        lessons.append(
            f"Lean more on {best_action} when the board state matches its setup."
        )
    if int(snapshot.get("remain_seats", 0)) <= 0:
        lessons.append(
            "Seat pressure is blocking growth; aviary expansion should stay near the front of the queue."
        )
    if not snapshot.get("current_unity"):
        lessons.append(
            "Social leverage is still underused while playing outside a unity."
        )
    if int(snapshot.get("usd", 0)) < 250:
        lessons.append(
            "Liquidity is thin; protect USD for the next meaningful opportunity."
        )

    opportunities = []
    if int(snapshot.get("daily_bonus_available", 0)) > 0:
        opportunities.append("A daily bonus is still available.")
    if int(snapshot.get("pending_unity_requests", 0)) > 0:
        opportunities.append(
            "There are pending unity requests waiting for a review decision."
        )
    if int(snapshot.get("remain_seats", 0)) > 0:
        opportunities.append(
            "Free seats exist, so profitable animal purchases are available."
        )
    if not opportunities:
        opportunities.append(
            "The next edge likely comes from sequencing, not from a free giveaway."
        )

    risks = []
    if usd_delta_total < 0 and income_delta_total <= 0:
        risks.append(
            "Recent spending is not yet translating into stronger recurring income."
        )
    if worst_action:
        risks.append(
            f"Repeated {worst_action} failures can waste turns and confidence."
        )
    if int(snapshot.get("usd", 0)) < max(250, int(snapshot.get("items_owned", 0)) * 75):
        risks.append("Cash reserve may be too thin for recovery after a bad sequence.")

    priorities = [goal.get("topic") for goal in active_goals[:3] if goal.get("topic")]
    return {
        "summary": semantic_preview(
            "; ".join(summary_parts),
            max_segments=4,
            max_words=80,
            max_chars=500,
        ),
        "lessons": lessons[:4],
        "opportunities": opportunities[:4],
        "risks": risks[:4],
        "priority_topics": priorities,
    }


async def _maybe_create_reflection(
    session: AsyncSession,
    user: User,
    client: "NpcDecisionClient | None",
    current_event: dict[str, Any],
    after_snapshot: dict[str, Any],
) -> dict[str, Any] | None:
    last_reflection = await session.scalar(
        select(NpcMemory)
        .where(
            NpcMemory.idpk_user == user.idpk,
            NpcMemory.kind == REFLECTION_KIND,
            NpcMemory.status == "active",
        )
        .order_by(NpcMemory.created_at.desc())
    )
    min_interval = int(settings.memory_reflection_min_interval_seconds or 0)
    if last_reflection is not None and min_interval > 0:
        elapsed = (_now() - last_reflection.created_at).total_seconds()
        if elapsed < min_interval:
            return None

    recent_events_rows = await session.scalars(
        select(NpcMemory)
        .where(
            NpcMemory.idpk_user == user.idpk,
            NpcMemory.kind == EVENT_KIND,
            NpcMemory.status == "active",
        )
        .order_by(NpcMemory.created_at.desc())
    )
    recent_events = []
    events_since_last_reflection = 0
    for row in recent_events_rows.all():
        payload = _json_loads(row.payload)
        recent_events.append(payload)
        if last_reflection is None or row.created_at > last_reflection.created_at:
            events_since_last_reflection += 1
        if len(recent_events) >= settings.memory_reflection_event_window:
            break

    if (
        events_since_last_reflection < settings.memory_reflection_every_events
        and int(current_event.get("importance", 0))
        < settings.memory_reflection_min_importance
    ):
        return None

    profile_row = await ensure_npc_profile_memory(session=session, user=user)
    profile = _rehydrate_profile_payload(
        user=user, payload=_json_loads(profile_row.payload)
    )
    goal_rows = await session.scalars(
        select(NpcMemory)
        .where(
            NpcMemory.idpk_user == user.idpk,
            NpcMemory.kind == GOAL_KIND,
            NpcMemory.status == "active",
        )
        .order_by(NpcMemory.importance.desc(), NpcMemory.updated_at.desc())
    )
    active_goals = [
        _json_loads(row.payload)
        for row in goal_rows.all()[: settings.memory_goal_limit]
    ]
    reflection_payload = _deterministic_reflection(
        events=list(reversed(recent_events)),
        active_goals=active_goals,
        snapshot=after_snapshot,
    )
    llm_reflection: dict[str, Any] = {}
    if settings.memory_use_llm_reflection and client:
        try:
            llm_reflection = await client.reflect_on_memory(
                payload={
                    "profile": profile,
                    "recent_events": list(reversed(recent_events)),
                    "active_goals": active_goals,
                    "current_state": after_snapshot,
                }
            )
        except Exception:
            llm_reflection = {}
    if llm_reflection:
        reflection_payload["summary"] = semantic_preview(
            llm_reflection.get("summary") or reflection_payload["summary"],
            max_segments=4,
            max_words=80,
            max_chars=500,
        )
        reflection_payload["lessons"] = (
            _pick_strings(
                llm_reflection.get("lessons"),
                4,
            )
            or reflection_payload["lessons"]
        )
        reflection_payload["opportunities"] = (
            _pick_strings(
                llm_reflection.get("opportunities"),
                4,
            )
            or reflection_payload["opportunities"]
        )
        reflection_payload["risks"] = (
            _pick_strings(
                llm_reflection.get("risks"),
                4,
            )
            or reflection_payload["risks"]
        )
        reflection_payload["goal_adjustments"] = _pick_dicts(
            llm_reflection.get("goal_adjustments"),
            4,
        )
        reflection_payload["tactical_focus"] = _pick_strings(
            llm_reflection.get("tactical_focus"),
            4,
        )
        semantic_facts = _pick_strings(
            llm_reflection.get("semantic_facts"),
            4,
        )
        for fact in semantic_facts:
            if fact:
                await _append_memory_row(
                    session=session,
                    user_idpk=user.idpk,
                    kind=FACT_KIND,
                    topic=f"fact:{hashlib.md5(fact.encode('utf-8')).hexdigest()[:8]}",
                    payload={
                        "fact": fact,
                        "source_reflection": semantic_preview(
                            reflection_payload.get("summary", ""),
                            max_segments=1,
                            max_words=8,
                            max_chars=50,
                        ),
                    },
                    importance=500,
                    confidence=800,
                )
    reflection_payload["covered_event_count"] = len(recent_events)
    reflection_payload["generated_at"] = _now().isoformat()
    importance = max(
        int(current_event.get("importance", 0)),
        650 + len(reflection_payload.get("lessons", [])) * 40,
    )
    await _append_memory_row(
        session=session,
        user_idpk=user.idpk,
        kind=REFLECTION_KIND,
        topic=f"reflection:{_topic_suffix()}",
        payload=reflection_payload,
        importance=_clamp(importance, 0, 1000),
        confidence=820,
    )
    return reflection_payload


async def trim_npc_memory(session: AsyncSession, user: User) -> None:
    active_events = await session.scalars(
        select(NpcMemory)
        .where(
            NpcMemory.idpk_user == user.idpk,
            NpcMemory.kind == EVENT_KIND,
        )
        .order_by(NpcMemory.created_at.desc())
    )
    for index, row in enumerate(active_events.all(), start=1):
        if index <= settings.memory_max_active_events:
            continue
        await session.delete(row)

    active_reflections = await session.scalars(
        select(NpcMemory)
        .where(
            NpcMemory.idpk_user == user.idpk,
            NpcMemory.kind == REFLECTION_KIND,
            NpcMemory.status == "active",
        )
        .order_by(NpcMemory.created_at.desc())
    )
    for index, row in enumerate(active_reflections.all(), start=1):
        if index <= settings.memory_reflections_limit * 4:
            continue
        row.status = "archived"
        row.updated_at = _now()


async def remember_npc_turn(
    session: AsyncSession,
    user: User,
    observation: dict[str, Any],
    before_snapshot: dict[str, Any],
    after_snapshot: dict[str, Any],
    action: dict[str, Any],
    result: dict[str, Any],
    wake_trigger: dict[str, Any],
    decision_index: int,
    client: "NpcDecisionClient | None" = None,
) -> None:
    current_event = _build_event_payload(
        user=user,
        observation=observation,
        before_snapshot=before_snapshot,
        after_snapshot=after_snapshot,
        action=action,
        result=result,
        wake_trigger=wake_trigger,
        decision_index=decision_index,
    )
    await _append_memory_row(
        session=session,
        user_idpk=user.idpk,
        kind=EVENT_KIND,
        topic=f"event:{_topic_suffix()}",
        payload=current_event,
        importance=int(current_event["importance"]),
        confidence=900,
    )
    await _update_relationship_memories(
        session=session,
        user=user,
        action=action,
        result=result,
        wake_trigger=wake_trigger,
        observation=observation,
    )
    await refresh_npc_goals(
        session=session,
        user=user,
        snapshot=after_snapshot,
        observation=observation,
    )
    reflection_payload = await _maybe_create_reflection(
        session=session,
        user=user,
        client=client,
        current_event=current_event,
        after_snapshot=after_snapshot,
    )
    await evolve_npc_profile(
        session=session,
        user=user,
        current_event=current_event,
        after_snapshot=after_snapshot,
        reflection_payload=reflection_payload,
    )
    await trim_npc_memory(session=session, user=user)


def _build_progress_summary(recent_events: list[dict[str, Any]]) -> dict[str, Any]:
    if not recent_events:
        return {
            "success_rate": 0.0,
            "usd_delta_total": 0,
            "income_delta_total": 0,
            "animals_delta_total": 0,
            "most_successful_action": None,
            "most_failed_action": None,
        }
    successes = [
        event
        for event in recent_events
        if event.get("result", {}).get("status") == "ok"
    ]
    usd_delta_total = sum(
        int(event.get("delta", {}).get("usd", 0)) for event in recent_events
    )
    income_delta_total = sum(
        int(event.get("delta", {}).get("income_per_minute_rub", 0))
        for event in recent_events
    )
    animals_delta_total = sum(
        int(event.get("delta", {}).get("animals", 0)) for event in recent_events
    )
    success_actions: dict[str, int] = {}
    failed_actions: dict[str, int] = {}
    for event in recent_events:
        action_name = str(event.get("action", {}).get("name", "wait"))
        if event.get("result", {}).get("status") == "ok":
            success_actions[action_name] = success_actions.get(action_name, 0) + 1
        else:
            failed_actions[action_name] = failed_actions.get(action_name, 0) + 1
    return {
        "success_rate": round(len(successes) / max(1, len(recent_events)), 3),
        "usd_delta_total": usd_delta_total,
        "income_delta_total": income_delta_total,
        "animals_delta_total": animals_delta_total,
        "most_successful_action": max(
            success_actions, key=lambda name: success_actions[name]
        )
        if success_actions
        else None,
        "most_failed_action": max(failed_actions, key=lambda name: failed_actions[name])
        if failed_actions
        else None,
    }


def _event_summary_for_context(payload: dict[str, Any]) -> dict[str, Any]:
    action = payload.get("action", {})
    result = payload.get("result", {})
    delta = payload.get("delta", {})
    planner = payload.get("planner", {})
    next_unlock = planner.get("next_unlock") or {}
    return {
        "time": payload.get("time"),
        "action": {
            "name": action.get("name"),
            "reason": _semantic_text_window(
                action.get("reason", ""),
                max_segments=2,
                max_words=18,
                max_chars=140,
            ),
            "sleep_seconds": action.get("sleep_seconds"),
        },
        "result": {
            "status": result.get("status"),
            "summary": _semantic_text_window(
                result.get("summary", ""),
                max_segments=2,
                max_words=18,
                max_chars=140,
            ),
            "bank_fee": result.get("bank_fee"),
        },
        "delta": {
            "usd": int(delta.get("usd", 0) or 0),
            "rub": int(delta.get("rub", 0) or 0),
            "income_per_minute_rub": int(delta.get("income_per_minute_rub", 0) or 0),
            "animals": int(delta.get("animals", 0) or 0),
            "seats": int(delta.get("seats", 0) or 0),
            "remain_seats": int(delta.get("remain_seats", 0) or 0),
        },
        "wake_context": {
            "source": (payload.get("wake_context") or {}).get("source"),
            "reason": _semantic_text_window(
                (payload.get("wake_context") or {}).get("reason", ""),
                max_segments=1,
                max_words=10,
                max_chars=80,
            ),
        },
        "current_focus": list(payload.get("current_focus", []))[:3],
        "planner": {
            "phase": planner.get("phase"),
            "primary_goal": planner.get("primary_goal"),
            "next_unlock": {
                "kind": next_unlock.get("kind"),
                "label": next_unlock.get("label"),
                "eta_seconds": next_unlock.get("eta_seconds"),
            },
        },
        "importance": int(payload.get("importance", 0) or 0),
    }


def _goal_summary_for_context(payload: dict[str, Any]) -> dict[str, Any]:
    progress = payload.get("progress", {})
    return {
        "title": payload.get("title"),
        "topic": payload.get("topic"),
        "priority": payload.get("priority"),
        "horizon": payload.get("horizon"),
        "progress": {
            "current": progress.get("current"),
            "target": progress.get("target"),
            "ratio": progress.get("ratio"),
        },
        "recommended_actions": list(payload.get("recommended_actions", []))[:3],
        "success_signal": _semantic_text_window(
            payload.get("success_signal", ""),
            max_segments=2,
            max_words=14,
            max_chars=120,
        ),
    }


def _reflection_summary_for_context(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": payload.get("generated_at"),
        "summary": _semantic_text_window(
            payload.get("summary", ""),
            max_segments=2,
            max_words=24,
            max_chars=180,
        ),
        "lessons": [
            _semantic_text_window(item, max_segments=1, max_words=14, max_chars=110)
            for item in list(payload.get("lessons", []))[:2]
        ],
        "opportunities": [
            _semantic_text_window(item, max_segments=1, max_words=14, max_chars=110)
            for item in list(payload.get("opportunities", []))[:2]
        ],
        "risks": [
            _semantic_text_window(item, max_segments=1, max_words=14, max_chars=110)
            for item in list(payload.get("risks", []))[:2]
        ],
    }


def _relationship_summary_for_context(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "subject_idpk": payload.get("subject_idpk"),
        "display_name": payload.get("display_name"),
        "status": payload.get("status"),
        "trust": payload.get("trust"),
        "affinity": payload.get("affinity"),
        "last_event": _semantic_text_window(
            payload.get("last_event", ""),
            max_segments=1,
            max_words=12,
            max_chars=100,
        ),
        "last_event_at": payload.get("last_event_at"),
        "interactions": payload.get("interactions"),
    }


def _profile_summary_for_context(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "current_mood": fit_db_field(
            payload.get("current_mood", "neutral"),
            max_len=32,
            default="neutral",
        ),
        "affinity_score": _clamp(int(payload.get("affinity_score", 50) or 50), 1, 100),
    }


def _build_behavior_guidance(
    active_goals: list[dict[str, Any]],
    progress_summary: dict[str, Any],
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    suggested_actions: list[str] = []
    for goal in active_goals:
        for action_name in goal.get("recommended_actions", []) or []:
            action_name = str(action_name).strip()
            if not action_name or action_name in suggested_actions:
                continue
            suggested_actions.append(action_name)
            if len(suggested_actions) >= 6:
                break
        if len(suggested_actions) >= 6:
            break

    playbook = []
    if progress_summary.get("income_delta_total", 0) > 0:
        playbook.append("Recent income growth validates compounding plays.")
    if int((snapshot or {}).get("remain_seats", 0) or 0) <= 0:
        playbook.append(
            "Seat pressure is active; capacity unlocks deserve immediate attention."
        )
    if int((snapshot or {}).get("usd", 0) or 0) < 250:
        playbook.append("Keep enough USD liquid to avoid stalling the next unlock.")

    return {
        "suggested_actions": suggested_actions[:6],
        "playbook": playbook[:5],
    }


def _select_relationships_for_context(
    relationships: list[dict[str, Any]],
    observation: dict[str, Any],
) -> list[dict[str, Any]]:
    relevant_ids: set[int] = set()
    unity = observation.get("unity", {})
    current = unity.get("current") or {}
    if current.get("owner_idpk"):
        relevant_ids.add(int(current["owner_idpk"]))
    for request in current.get("pending_requests", []):
        relevant_ids.add(int(request.get("idpk_user", 0)))
    for row in unity.get("recruit_targets", []):
        relevant_ids.add(int(row.get("idpk", 0)))
    for row in unity.get("candidates", []):
        relevant_ids.add(int(row.get("owner_idpk", 0)))

    scored = []
    for payload in relationships:
        subject_idpk = int(payload.get("subject_idpk", 0) or 0)
        score = int(payload.get("trust", 500)) + int(payload.get("affinity", 500))
        if subject_idpk and subject_idpk in relevant_ids:
            score += 400
        scored.append((score, payload))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [payload for _, payload in scored[: settings.memory_relationship_limit]]


async def build_npc_memory_context(
    session: AsyncSession,
    user: User,
    observation: dict[str, Any],
) -> dict[str, Any]:
    profile_row = await ensure_npc_profile_memory(session=session, user=user)
    snapshot = extract_snapshot_from_observation(observation)
    active_goals = await refresh_npc_goals(
        session=session,
        user=user,
        snapshot=snapshot,
        observation=observation,
    )
    recent_event_rows = await session.scalars(
        select(NpcMemory)
        .where(
            NpcMemory.idpk_user == user.idpk,
            NpcMemory.kind == EVENT_KIND,
            NpcMemory.status == "active",
        )
        .order_by(NpcMemory.created_at.desc())
    )
    recent_events = [
        _json_loads(row.payload)
        for row in recent_event_rows.all()[: settings.memory_recent_events_limit]
    ]
    recent_events_for_context = [
        _event_summary_for_context(payload) for payload in recent_events
    ]
    reflection_rows = await session.scalars(
        select(NpcMemory)
        .where(
            NpcMemory.idpk_user == user.idpk,
            NpcMemory.kind == REFLECTION_KIND,
            NpcMemory.status == "active",
        )
        .order_by(NpcMemory.updated_at.desc(), NpcMemory.importance.desc())
    )
    reflections = [
        _json_loads(row.payload)
        for row in reflection_rows.all()[: settings.memory_reflections_limit]
    ]
    relationship_rows = await session.scalars(
        select(NpcMemory)
        .where(
            NpcMemory.idpk_user == user.idpk,
            NpcMemory.kind == RELATIONSHIP_KIND,
            NpcMemory.status == "active",
        )
        .order_by(NpcMemory.importance.desc(), NpcMemory.updated_at.desc())
    )
    relationships = [_json_loads(row.payload) for row in relationship_rows.all()]
    selected_relationships = _select_relationships_for_context(
        relationships=relationships,
        observation=observation,
    )
    goals_for_context = [
        _goal_summary_for_context(payload)
        for payload in active_goals[: settings.memory_goal_limit]
    ]
    reflections_for_context = [
        _reflection_summary_for_context(payload) for payload in reflections
    ]
    relationships_for_context = [
        _relationship_summary_for_context(payload) for payload in selected_relationships
    ]
    fact_rows = await session.scalars(
        select(NpcMemory)
        .where(
            NpcMemory.idpk_user == user.idpk,
            NpcMemory.kind == FACT_KIND,
            NpcMemory.status == "active",
        )
        .order_by(NpcMemory.created_at.desc())
    )
    semantic_facts = []
    incoming_signals = []
    for row in fact_rows.all()[: settings.memory_goal_limit * 2]:
        fact_payload = _json_loads(row.payload)
        fact_text = fact_payload.get("fact")

        if row.topic and row.topic.startswith("incoming_signal:"):
            incoming_signals.append(
                {
                    "from_id": row.topic.split(":")[1],
                    "message": fact_text,
                    "created_at": str(row.created_at),
                }
            )
        elif fact_text:
            semantic_facts.append(fact_text)

    lessons = []
    for reflection in reflections:
        for lesson in reflection.get("lessons", []):
            text = str(lesson).strip()
            if not text or text in lessons:
                continue
            lessons.append(text)
            if len(lessons) >= settings.memory_reflections_limit * 2:
                break
        if len(lessons) >= settings.memory_reflections_limit * 2:
            break
    open_loops = []
    for relationship in selected_relationships:
        status = str(relationship.get("status", ""))
        if status in {"interested", "resistant", "unstable"}:
            open_loops.append(
                {
                    "subject_idpk": relationship.get("subject_idpk"),
                    "display_name": relationship.get("display_name"),
                    "status": status,
                    "last_event": relationship.get("last_event"),
                }
            )
    profile = _rehydrate_profile_payload(
        user=user, payload=_json_loads(profile_row.payload)
    )
    profile_for_context = _profile_summary_for_context(profile)
    progress_summary = _build_progress_summary(recent_events)
    behavior_guidance = _build_behavior_guidance(
        active_goals=active_goals,
        progress_summary=progress_summary,
        snapshot=snapshot,
    )
    return {
        "profile": profile_for_context,
        "active_strategic_goal": profile.get("active_strategic_goal", "compound_income_growth"),
        "active_goals": goals_for_context,
        "recent_events": recent_events_for_context,
        "reflections": reflections_for_context,
        "lessons": [
            _semantic_text_window(item, max_segments=1, max_words=14, max_chars=110)
            for item in lessons[:4]
        ],
        "relationships": relationships_for_context,
        "progress_summary": progress_summary,
        "behavior_guidance": behavior_guidance,
        "open_loops": open_loops[: settings.memory_relationship_limit],
        "semantic_facts": semantic_facts[: settings.memory_goal_limit],
        "incoming_signals": incoming_signals,
    }
