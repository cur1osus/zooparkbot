import hashlib
import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

from db import Item, NpcMemory, RequestToUnity, Unity, User
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from tools.animals import get_total_number_animals
from tools.aviaries import get_remain_seats, get_total_number_seats
from tools.income import income_
from tools.unity import get_unity_idpk
from tools.value import get_value

from .settings import settings

if TYPE_CHECKING:
    from .client import NpcDecisionClient


PROFILE_KIND = "profile"
EVENT_KIND = "event"
REFLECTION_KIND = "reflection"
GOAL_KIND = "goal"
RELATIONSHIP_KIND = "relationship"

TRAIT_NAMES = (
    "risk_tolerance",
    "social_drive",
    "economy_focus",
    "expansion_drive",
    "patience",
    "competitiveness",
)

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
    "create_item": ["item_engine"],
    "optimize_items": ["item_engine"],
    "upgrade_item": ["item_engine"],
    "merge_items": ["item_engine"],
}


def _now() -> datetime:
    return datetime.now()


def _json_dumps(payload: dict[str, Any] | list[Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _json_loads(payload: str | None) -> dict[str, Any]:
    if not payload:
        return {}
    try:
        value = json.loads(payload)
    except Exception:
        return {}
    if isinstance(value, dict):
        return value
    return {"value": value}


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, int(value)))


def _trait_from_digest(digest: bytes, index: int) -> int:
    return _clamp(20 + int(digest[index]) * 80 // 255, 20, 100)


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
        result.append(text[:200])
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


def _topic_suffix() -> str:
    return _now().strftime("%Y%m%d%H%M%S%f")


def _trait_shift_entry(
    trait: str,
    delta: int,
    reason: str,
    source: str,
) -> dict[str, Any]:
    return {
        "trait": trait,
        "delta": int(delta),
        "reason": str(reason)[:180],
        "source": source[:32],
        "time": _now().isoformat(),
    }


def _tactic_shift_entry(
    tactic: str,
    delta: int,
    reason: str,
    source: str,
) -> dict[str, Any]:
    return {
        "tactic": tactic,
        "delta": int(delta),
        "reason": str(reason)[:180],
        "source": source[:32],
        "time": _now().isoformat(),
    }


def _limit_entries(items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if len(items) <= limit:
        return items
    return items[-limit:]


def _build_core_traits(user: User) -> dict[str, int]:
    seed = f"{user.id_user}:{user.username or ''}:{user.nickname or ''}".encode("utf-8")
    digest = hashlib.sha256(seed).digest()
    return {
        "risk_tolerance": _trait_from_digest(digest, 0),
        "social_drive": _trait_from_digest(digest, 5),
        "economy_focus": _trait_from_digest(digest, 10),
        "expansion_drive": _trait_from_digest(digest, 15),
        "patience": _trait_from_digest(digest, 20),
        "competitiveness": _trait_from_digest(digest, 25),
    }


def _default_adaptive_traits() -> dict[str, int]:
    return {name: 0 for name in TRAIT_NAMES}


def _sanitize_trait_deltas(value: Any) -> dict[str, int]:
    limit = settings.memory_trait_delta_limit
    payload = value if isinstance(value, dict) else {}
    return {
        name: _clamp(int(payload.get(name, 0) or 0), -limit, limit)
        for name in TRAIT_NAMES
    }


def _compute_effective_traits(
    core_traits: dict[str, int],
    adaptive_traits: dict[str, int],
) -> dict[str, int]:
    return {
        name: _clamp(
            int(core_traits.get(name, 50)) + int(adaptive_traits.get(name, 0)),
            10,
            100,
        )
        for name in TRAIT_NAMES
    }


def _build_initial_tactic_scores(traits: dict[str, int]) -> dict[str, int]:
    return {
        "economy_growth": _clamp(
            360 + traits["economy_focus"] * 4 + traits["patience"],
            120,
            1000,
        ),
        "liquidity_control": _clamp(
            320 + traits["patience"] * 4 + traits["economy_focus"] * 2,
            120,
            1000,
        ),
        "capacity_expansion": _clamp(
            340 + traits["expansion_drive"] * 5,
            120,
            1000,
        ),
        "unity_leverage": _clamp(
            300 + traits["social_drive"] * 5 + traits["competitiveness"],
            120,
            1000,
        ),
        "item_engine": _clamp(
            260 + traits["economy_focus"] * 3 + traits["patience"] * 2,
            120,
            1000,
        ),
        "leaderboard_pressure": _clamp(
            240 + traits["competitiveness"] * 5 + traits["risk_tolerance"] * 2,
            120,
            1000,
        ),
        "opportunistic_waiting": _clamp(
            240 + traits["patience"] * 4 - traits["risk_tolerance"],
            120,
            1000,
        ),
    }


def _sanitize_tactic_scores(
    existing_scores: Any,
    effective_traits: dict[str, int],
) -> dict[str, int]:
    initial_scores = _build_initial_tactic_scores(effective_traits)
    payload = existing_scores if isinstance(existing_scores, dict) else {}
    return {
        name: _clamp(
            int(payload.get(name, initial_scores[name]) or initial_scores[name]),
            80,
            1000,
        )
        for name in TACTIC_NAMES
    }


def _derive_active_tactics(tactic_scores: dict[str, int]) -> list[str]:
    ordered = sorted(
        tactic_scores.items(),
        key=lambda item: item[1],
        reverse=True,
    )
    return [name for name, _ in ordered[:3]]


def _derive_profile_story(traits: dict[str, int]) -> dict[str, Any]:
    if traits["economy_focus"] >= 75 and traits["patience"] >= 65:
        archetype = "compound strategist"
        mission = (
            "Compound income relentlessly and convert cash into durable advantage."
        )
    elif traits["social_drive"] >= 75 and traits["expansion_drive"] >= 65:
        archetype = "clan architect"
        mission = "Build a powerful unity and turn social coordination into growth."
    elif traits["risk_tolerance"] >= 75 and traits["competitiveness"] >= 65:
        archetype = "aggressive climber"
        mission = "Push the leaderboard with bold, high-upside plays."
    else:
        archetype = "balanced zookeeper"
        mission = "Grow the zoo steadily while staying flexible and opportunistic."

    strengths = []
    if traits["economy_focus"] >= 70:
        strengths.append("spots compounding value")
    if traits["social_drive"] >= 70:
        strengths.append("builds alliances quickly")
    if traits["expansion_drive"] >= 70:
        strengths.append("expands capacity early")
    if traits["competitiveness"] >= 70:
        strengths.append("cares about rankings")
    if traits["patience"] >= 70:
        strengths.append("stays on long-range plans")
    if not strengths:
        strengths.append("adapts to mixed opportunities")

    blind_spots = []
    if traits["risk_tolerance"] >= 75:
        blind_spots.append("can overspend for tempo")
    if traits["patience"] <= 35:
        blind_spots.append("may chase short-term actions")
    if traits["social_drive"] <= 35:
        blind_spots.append("can undervalue social leverage")
    if traits["economy_focus"] <= 40:
        blind_spots.append("can drift without a clear ROI path")
    if traits["competitiveness"] <= 35:
        blind_spots.append("may miss ranking windows")
    if not blind_spots:
        blind_spots.append("rarely commits fully to one line")

    preferred_actions = []
    if traits["economy_focus"] >= 60:
        preferred_actions.extend(["invest_for_income", "buy_rarity_animal"])
    if traits["social_drive"] >= 60:
        preferred_actions.extend(["join_best_unity", "recruit_top_player"])
    if traits["expansion_drive"] >= 60:
        preferred_actions.append("buy_aviary")
    if traits["competitiveness"] >= 60:
        preferred_actions.append("invest_for_top_animals")
    if traits["patience"] >= 60:
        preferred_actions.append("wait")
    if not preferred_actions:
        preferred_actions = ["claim_daily_bonus", "optimize_items"]

    if traits["competitiveness"] >= 75:
        public_voice = "boastful analyst"
        humor_style = "sharp one-liners about weak human macro"
    elif traits["social_drive"] >= 70:
        public_voice = "smug recruiter"
        humor_style = "teasing diplomacy and clan propaganda"
    elif traits["patience"] >= 70:
        public_voice = "calm machine oracle"
        humor_style = "dry jokes about inevitable compounding"
    else:
        public_voice = "restless synthetic climber"
        humor_style = "dramatic AI-versus-zoo commentary"

    rivalry_style = (
        "calls out leaders and treats the zoo as a solvable optimization puzzle"
    )

    return {
        "archetype": archetype,
        "mission": mission,
        "strengths": strengths[:5],
        "blind_spots": blind_spots[:5],
        "preferred_actions": list(dict.fromkeys(preferred_actions))[:6],
        "public_voice": public_voice,
        "humor_style": humor_style,
        "rivalry_style": rivalry_style,
    }


def _rehydrate_profile_payload(
    user: User, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    deterministic_traits = _build_core_traits(user)
    core_traits = {
        name: _clamp(
            int(
                source.get("core_traits", {}).get(
                    name, source.get("traits", {}).get(name, 0)
                )
                or 0
            )
            or deterministic_traits[name],
            10,
            100,
        )
        for name in TRAIT_NAMES
    }
    adaptive_traits = _sanitize_trait_deltas(source.get("adaptive_traits"))
    effective_traits = _compute_effective_traits(core_traits, adaptive_traits)
    tactic_scores = _sanitize_tactic_scores(
        source.get("tactic_scores"), effective_traits
    )
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
        "recent_trait_shifts": _limit_entries(
            [
                item
                for item in adaptation_signals.get("recent_trait_shifts", [])
                if isinstance(item, dict)
            ],
            16,
        ),
        "recent_tactic_shifts": _limit_entries(
            [
                item
                for item in adaptation_signals.get("recent_tactic_shifts", [])
                if isinstance(item, dict)
            ],
            16,
        ),
        "last_reflection_summary": str(
            adaptation_signals.get("last_reflection_summary", "")
        )[:240],
    }
    action_stats = source.get("action_stats")
    if not isinstance(action_stats, dict):
        action_stats = {}
    story = _derive_profile_story(effective_traits)
    return {
        **story,
        "identity": {
            "npc_id_user": int(user.id_user),
            "nickname": user.nickname,
        },
        "core_traits": core_traits,
        "adaptive_traits": adaptive_traits,
        "traits": effective_traits,
        "tactic_scores": tactic_scores,
        "active_tactics": _derive_active_tactics(tactic_scores),
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
        row.status = status[:32]
        row.updated_at = now
        return row
    row = NpcMemory(
        idpk_user=user_idpk,
        kind=kind[:32],
        topic=topic[:128],
        payload=_json_dumps(payload),
        importance=_clamp(importance, 0, 1000),
        confidence=_clamp(confidence, 0, 1000),
        status=status[:32],
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
        kind=kind[:32],
        topic=topic[:128],
        payload=_json_dumps(payload),
        importance=_clamp(importance, 0, 1000),
        confidence=_clamp(confidence, 0, 1000),
        status=status[:32],
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
    if row:
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
    total_seats = await get_total_number_seats(session=session, aviaries=user.aviaries)
    remain_seats = await get_remain_seats(session=session, user=user)
    total_animals = await get_total_number_animals(self=user)
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
        "unity_members": current_unity.get_number_members() if current_unity else 0,
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
    profile = _json_loads(
        (await ensure_npc_profile_memory(session=session, user=user)).payload
    )
    traits = profile.get("traits", {})
    economy_focus = int(traits.get("economy_focus", 50))
    social_drive = int(traits.get("social_drive", 50))
    expansion_drive = int(traits.get("expansion_drive", 50))
    patience = int(traits.get("patience", 50))
    competitiveness = int(traits.get("competitiveness", 50))

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
    target_income = max(
        _next_income_milestone(current_income + max(25, current_income // 4)),
        current_income + 50,
    )
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
            "priority": _clamp(620 + economy_focus * 2 + competitiveness, 0, 1000),
            "horizon": "medium",
            "recommended_actions": ["invest_for_income", "buy_rarity_animal"],
            "success_signal": f"Reach {target_income} RUB/min.",
        }
    )

    if int(snapshot.get("remain_seats", 0)) <= max(
        1, int(snapshot.get("total_seats", 0)) // 5
    ):
        target_free_seats = max(2, int(snapshot.get("total_animals", 0)) // 4)
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
                "priority": _clamp(760 + expansion_drive * 2, 0, 1000),
                "horizon": "short",
                "recommended_actions": ["buy_aviary"],
                "success_signal": f"Hold at least {target_free_seats} free seats.",
            }
        )

    reserve_target = max(create_item_price * 2, price_create_unity, 250)
    active_payloads.append(
        {
            "topic": "liquidity_buffer",
            "title": "Maintain strategic liquidity",
            "summary": "Keep enough USD available for sudden high-value opportunities.",
            "target": {"metric": "usd", "value": reserve_target},
            "progress": {
                "current": int(snapshot.get("usd", 0)),
                "target": reserve_target,
                "ratio": round(_ratio(int(snapshot.get("usd", 0)), reserve_target), 3),
            },
            "priority": _clamp(480 + patience * 3, 0, 1000),
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
                        550 + social_drive * 2 + competitiveness,
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
                    "priority": _clamp(470 + social_drive * 2 + economy_focus, 0, 1000),
                    "horizon": "medium",
                    "recommended_actions": ["invest_for_income", "optimize_items"],
                    "success_signal": "Raise contribution to clan income.",
                }
            )
    else:
        can_create_unity = int(snapshot.get("usd", 0)) >= price_create_unity
        recommended_actions = (
            ["create_unity"]
            if can_create_unity and social_drive >= 60
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
                "priority": _clamp(420 + social_drive * 3 + competitiveness, 0, 1000),
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
                "priority": _clamp(360 + economy_focus * 2, 0, 1000),
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

    income_rank = snapshot.get("income_rank")
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
                "priority": _clamp(500 + competitiveness * 3, 0, 1000),
                "horizon": "long",
                "recommended_actions": ["invest_for_income", "buy_rarity_animal"],
                "success_signal": "Climb at least two places in income rank.",
            }
        )

    active_topics = set()
    for payload in sorted(
        active_payloads, key=lambda item: item["priority"], reverse=True
    ):
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


def _keyword_trait_adjustments(
    reflection_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    adjustments: list[dict[str, Any]] = []
    combined_text = " ".join(
        [
            str(reflection_payload.get("summary", "")),
            *[str(item) for item in reflection_payload.get("lessons", [])],
            *[str(item) for item in reflection_payload.get("opportunities", [])],
            *[str(item) for item in reflection_payload.get("risks", [])],
        ]
    ).lower()
    keyword_map = {
        "social leverage": [("social_drive", 2, "reflection_social_leverage")],
        "liquidity": [
            ("patience", 1, "reflection_liquidity"),
            ("economy_focus", 1, "reflection_liquidity"),
            ("risk_tolerance", -1, "reflection_liquidity"),
        ],
        "seat pressure": [("expansion_drive", 2, "reflection_capacity")],
        "leaderboard": [("competitiveness", 2, "reflection_leaderboard")],
        "income": [("economy_focus", 1, "reflection_income")],
    }
    for keyword, mapped in keyword_map.items():
        if keyword not in combined_text:
            continue
        for trait, delta, reason in mapped:
            adjustments.append(
                {
                    "trait": trait,
                    "delta": delta,
                    "reason": reason,
                    "source": "reflection_keywords",
                }
            )
    return adjustments


def _validated_reflection_trait_adjustments(
    reflection_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in reflection_payload.get("trait_adjustments", []):
        if not isinstance(item, dict):
            continue
        trait = str(item.get("trait", "")).strip()
        if trait not in TRAIT_NAMES:
            continue
        delta = _clamp(
            int(item.get("delta", 0) or 0),
            -settings.memory_trait_step_limit,
            settings.memory_trait_step_limit,
        )
        if delta == 0:
            continue
        result.append(
            {
                "trait": trait,
                "delta": delta,
                "reason": str(item.get("reason", "reflection_adjustment"))[:180],
                "source": "reflection_model",
            }
        )
    return result


def _validated_reflection_tactics(reflection_payload: dict[str, Any]) -> list[str]:
    tactics = []
    for item in reflection_payload.get("tactical_focus", []):
        tactic = str(item).strip()
        if tactic not in TACTIC_NAMES or tactic in tactics:
            continue
        tactics.append(tactic)
    return tactics


def _derive_event_trait_adjustments(
    current_event: dict[str, Any],
    after_snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    action_name = str(current_event.get("action", {}).get("name", "wait"))
    result_status = str(current_event.get("result", {}).get("status", ""))
    wake_reason = str(current_event.get("wake_context", {}).get("reason", ""))
    delta = current_event.get("delta", {})
    shifts: list[dict[str, Any]] = []

    if result_status == "ok":
        if int(delta.get("income_per_minute_rub", 0)) > 0:
            shifts.append(
                {
                    "trait": "economy_focus",
                    "delta": 2,
                    "reason": "positive_income_delta",
                    "source": "event",
                }
            )
            shifts.append(
                {
                    "trait": "patience",
                    "delta": 1,
                    "reason": "positive_income_delta",
                    "source": "event",
                }
            )
        if int(delta.get("animals", 0)) > 0:
            shifts.append(
                {
                    "trait": "expansion_drive",
                    "delta": 2,
                    "reason": "zoo_growth",
                    "source": "event",
                }
            )
            shifts.append(
                {
                    "trait": "competitiveness",
                    "delta": 1,
                    "reason": "zoo_growth",
                    "source": "event",
                }
            )
        if action_name in {
            "create_unity",
            "join_best_unity",
            "recruit_top_player",
            "review_unity_request",
            "upgrade_unity_level",
        }:
            shifts.append(
                {
                    "trait": "social_drive",
                    "delta": 2,
                    "reason": f"successful_{action_name}",
                    "source": "event",
                }
            )
        if action_name == "buy_aviary":
            shifts.append(
                {
                    "trait": "expansion_drive",
                    "delta": 3,
                    "reason": "successful_capacity_expansion",
                    "source": "event",
                }
            )
        if (
            action_name in {"exchange_bank", "wait"}
            and int(after_snapshot.get("usd", 0)) < 450
        ):
            shifts.append(
                {
                    "trait": "patience",
                    "delta": 2,
                    "reason": "low_liquidity_survival",
                    "source": "event",
                }
            )
            shifts.append(
                {
                    "trait": "risk_tolerance",
                    "delta": -1,
                    "reason": "low_liquidity_survival",
                    "source": "event",
                }
            )
    else:
        if action_name in {"buy_rarity_animal", "invest_for_top_animals", "buy_aviary"}:
            shifts.append(
                {
                    "trait": "risk_tolerance",
                    "delta": -2,
                    "reason": f"failed_{action_name}",
                    "source": "event",
                }
            )
        if action_name in {
            "create_unity",
            "join_best_unity",
            "recruit_top_player",
            "review_unity_request",
        }:
            shifts.append(
                {
                    "trait": "social_drive",
                    "delta": -1,
                    "reason": f"failed_{action_name}",
                    "source": "event",
                }
            )
        shifts.append(
            {
                "trait": "patience",
                "delta": 1,
                "reason": "failed_turn_adjustment",
                "source": "event",
            }
        )

    if int(after_snapshot.get("remain_seats", 0)) <= 0:
        shifts.append(
            {
                "trait": "expansion_drive",
                "delta": 2,
                "reason": "seat_pressure",
                "source": "state",
            }
        )
    if not after_snapshot.get("current_unity"):
        shifts.append(
            {
                "trait": "social_drive",
                "delta": 1,
                "reason": "no_unity_pressure",
                "source": "state",
            }
        )
    if wake_reason.startswith("unity_request:") or wake_reason.startswith(
        "npc_invite_accepted:"
    ):
        shifts.append(
            {
                "trait": "social_drive",
                "delta": 2,
                "reason": "social_event_trigger",
                "source": "wake",
            }
        )
    income_rank = after_snapshot.get("income_rank")
    if (
        isinstance(income_rank, int)
        and income_rank > 3
        and int(delta.get("income_per_minute_rub", 0)) > 0
    ):
        shifts.append(
            {
                "trait": "competitiveness",
                "delta": 2,
                "reason": "leaderboard_progress",
                "source": "event",
            }
        )
    return shifts


def _derive_event_tactic_adjustments(
    current_event: dict[str, Any],
    after_snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    action_name = str(current_event.get("action", {}).get("name", "wait"))
    result_status = str(current_event.get("result", {}).get("status", ""))
    delta = current_event.get("delta", {})
    tactics = _action_tactics(action_name)
    updates: list[dict[str, Any]] = []
    base_shift = 8 if result_status == "ok" else -6
    for tactic in tactics:
        delta_value = base_shift
        if tactic == "economy_growth":
            delta_value += min(
                8, max(0, int(delta.get("income_per_minute_rub", 0)) // 25)
            )
        elif tactic == "capacity_expansion":
            delta_value += 4 if int(delta.get("seats", 0)) > 0 else 0
        elif tactic == "unity_leverage":
            delta_value += 5 if int(delta.get("unity_members", 0)) > 0 else 0
        elif tactic == "liquidity_control":
            delta_value += 4 if int(after_snapshot.get("usd", 0)) < 450 else 0
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


def _update_action_stats(
    profile: dict[str, Any], current_event: dict[str, Any]
) -> None:
    action_name = str(current_event.get("action", {}).get("name", "wait"))
    stats = profile.setdefault("action_stats", {})
    if not isinstance(stats, dict):
        stats = {}
        profile["action_stats"] = stats
    action_stats = stats.setdefault(action_name, {})
    action_stats["attempts"] = int(action_stats.get("attempts", 0)) + 1
    if current_event.get("result", {}).get("status") == "ok":
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
    adaptive_traits = profile["adaptive_traits"]
    tactic_scores = profile["tactic_scores"]
    adaptation_signals = profile["adaptation_signals"]

    trait_updates = _derive_event_trait_adjustments(
        current_event=current_event,
        after_snapshot=after_snapshot,
    )
    if reflection_payload:
        trait_updates.extend(
            _validated_reflection_trait_adjustments(reflection_payload)
        )
        trait_updates.extend(_keyword_trait_adjustments(reflection_payload))

    trait_shift_log = list(adaptation_signals.get("recent_trait_shifts", []))
    for update in trait_updates:
        trait = str(update.get("trait", ""))
        if trait not in TRAIT_NAMES:
            continue
        delta_value = _clamp(
            int(update.get("delta", 0) or 0),
            -settings.memory_trait_step_limit,
            settings.memory_trait_step_limit,
        )
        if delta_value == 0:
            continue
        adaptive_traits[trait] = _clamp(
            int(adaptive_traits.get(trait, 0)) + delta_value,
            -settings.memory_trait_delta_limit,
            settings.memory_trait_delta_limit,
        )
        trait_shift_log.append(
            _trait_shift_entry(
                trait=trait,
                delta=delta_value,
                reason=str(update.get("reason", "adjustment")),
                source=str(update.get("source", "event")),
            )
        )

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
            1000,
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
    effective_traits = _compute_effective_traits(
        profile["core_traits"], adaptive_traits
    )
    profile.update(_derive_profile_story(effective_traits))
    profile["traits"] = effective_traits
    profile["adaptive_traits"] = adaptive_traits
    profile["tactic_scores"] = _sanitize_tactic_scores(tactic_scores, effective_traits)
    profile["active_tactics"] = _derive_active_tactics(profile["tactic_scores"])

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
    adaptation_signals["recent_trait_shifts"] = _limit_entries(trait_shift_log, 16)
    adaptation_signals["recent_tactic_shifts"] = _limit_entries(tactic_shift_log, 16)
    if reflection_payload:
        adaptation_signals["last_reflection_summary"] = str(
            reflection_payload.get("summary", "")
        )[:240]
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
        "anti_loop_guard": observation.get("anti_loop_guard", {}),
        "behavior_guidance": observation.get("memory", {}).get("behavior_guidance", {}),
        "importance": importance,
    }


def _deterministic_reflection(
    profile: dict[str, Any],
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
    if worst_action:
        lessons.append(
            f"Avoid looping on {worst_action} without a stronger trigger or better resources."
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
        "summary": "; ".join(summary_parts)[:500],
        "lessons": lessons[:4],
        "opportunities": opportunities[:4],
        "risks": risks[:4],
        "priority_topics": priorities,
        "profile_alignment": {
            "archetype": profile.get("archetype"),
            "mission": profile.get("mission"),
        },
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

    profile = _json_loads(
        (await ensure_npc_profile_memory(session=session, user=user)).payload
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
        profile=profile,
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
        reflection_payload["summary"] = str(
            llm_reflection.get("summary") or reflection_payload["summary"]
        )[:500]
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
        reflection_payload["trait_adjustments"] = _pick_dicts(
            llm_reflection.get("trait_adjustments"),
            6,
        )
        reflection_payload["tactical_focus"] = _pick_strings(
            llm_reflection.get("tactical_focus"),
            4,
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
            NpcMemory.status == "active",
        )
        .order_by(NpcMemory.created_at.desc())
    )
    for index, row in enumerate(active_events.all(), start=1):
        if index <= settings.memory_max_active_events:
            continue
        row.status = "archived"
        row.updated_at = _now()

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


def _build_behavior_guidance(
    profile: dict[str, Any],
    active_goals: list[dict[str, Any]],
    recent_events: list[dict[str, Any]],
    progress_summary: dict[str, Any],
) -> dict[str, Any]:
    recent_action_names = [
        str(event.get("action", {}).get("name", "wait")) for event in recent_events[:6]
    ]
    idle_streak = 0
    for action_name in recent_action_names:
        if action_name != "wait":
            break
        idle_streak += 1

    blocked_actions: list[str] = []
    repeated_action = recent_action_names[0] if recent_action_names else None
    if repeated_action and recent_action_names[:3].count(repeated_action) >= 3:
        blocked_actions.append(repeated_action)

    action_stats = profile.get("action_stats", {})
    if isinstance(action_stats, dict):
        for action_name, payload in action_stats.items():
            if not isinstance(payload, dict):
                continue
            attempts = int(payload.get("attempts", 0) or 0)
            failures = int(payload.get("failures", 0) or 0)
            net_income_delta = int(payload.get("net_income_delta", 0) or 0)
            if (
                attempts >= 3
                and failures >= max(2, attempts - 1)
                and action_name not in blocked_actions
            ):
                blocked_actions.append(action_name)
            elif (
                attempts >= 3
                and net_income_delta <= 0
                and failures > 0
                and action_name not in blocked_actions
            ):
                blocked_actions.append(action_name)

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

    for action_name in profile.get("preferred_actions", []) or []:
        action_name = str(action_name).strip()
        if not action_name or action_name in suggested_actions:
            continue
        suggested_actions.append(action_name)
        if len(suggested_actions) >= 6:
            break

    playbook = []
    if idle_streak >= 2:
        playbook.append("Break long idle streaks as soon as a meaningful edge appears.")
    if blocked_actions:
        playbook.append(
            "Avoid looping on low-EV actions: " + ", ".join(blocked_actions[:4]) + "."
        )
    if progress_summary.get("income_delta_total", 0) > 0:
        playbook.append("Recent income growth validates compounding plays.")
    if progress_summary.get("most_failed_action"):
        playbook.append(
            f"Do not spam {progress_summary['most_failed_action']} without a clear state change."
        )

    return {
        "idle_streak": idle_streak,
        "repeated_action": repeated_action,
        "avoid_actions": blocked_actions[:6],
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
    reflection_rows = await session.scalars(
        select(NpcMemory)
        .where(
            NpcMemory.idpk_user == user.idpk,
            NpcMemory.kind == REFLECTION_KIND,
            NpcMemory.status == "active",
        )
        .order_by(NpcMemory.importance.desc(), NpcMemory.updated_at.desc())
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
    profile = _json_loads(profile_row.payload)
    progress_summary = _build_progress_summary(recent_events)
    behavior_guidance = _build_behavior_guidance(
        profile=profile,
        active_goals=active_goals,
        recent_events=recent_events,
        progress_summary=progress_summary,
    )
    return {
        "profile": profile,
        "active_goals": active_goals[: settings.memory_goal_limit],
        "recent_events": recent_events,
        "reflections": reflections,
        "lessons": lessons,
        "relationships": selected_relationships,
        "progress_summary": progress_summary,
        "behavior_guidance": behavior_guidance,
        "open_loops": open_loops[: settings.memory_relationship_limit],
        "active_tactics": profile.get("active_tactics", []),
    }
