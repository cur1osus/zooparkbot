from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import datetime
from itertools import combinations
from typing import TYPE_CHECKING, Any

from config import CHAT_ID
from db import (
    Animal,
    Aviary,
    Game,
    Gamer,
    Item,
    RandomMerchant,
    RequestToUnity,
    TransferMoney,
    Unity,
    User,
)
from db.structured_state import (
    append_npc_turn_history,
    count_unity_members,
    get_user_animals_map,
    get_user_aviaries_map,
    list_recent_npc_history_payloads,
    list_transfer_claim_user_ids,
)
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from text_utils import semantic_preview
from tools.animals import get_all_animals, get_price_animal, get_total_number_animals
from tools.aviaries import get_price_aviaries, get_remain_seats, get_total_number_seats
from tools.bank import get_rate
from tools.income import income_
from game_variables import prop_quantity_by_rarity
from init_db_redis import redis
from tools.items import (
    CREATE_ITEM_PAW_PRICE,
    calculate_percent_to_enhance,
    gen_price_to_create_item,
    get_value_prop_from_iai,
    synchronize_info_about_items,
)
from tools.random_merchant import create_random_merchant
from tools.referrals import get_referrals_count_map
from tools.unity import (
    check_condition_1st_lvl,
    check_condition_2nd_lvl,
    check_condition_3rd_lvl,
    count_income_unity,
    get_data_by_lvl_unity,
    get_unity_idpk,
)
from tools.value import get_value
from tools.unity_projects import (
    get_or_create_project,
    get_project_reward_preview,
    get_user_chests,
)

from .memory import build_npc_memory_context
from .schedule import clamp_npc_sleep_seconds
from .settings import settings

if TYPE_CHECKING:
    from .client import NpcDecisionClient


_STANDINGS_CACHE_KEY = "npc:standings:v2"
_STANDINGS_TTL = 90


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
    income = max(1, int(income_per_minute_rub))
    minutes = (int(gap_rub) + income - 1) // income
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


def _standings_with_self(cached: dict[str, Any], user_idpk: int) -> dict[str, Any]:
    """Re-attach user-specific rank fields from the shared cached payload."""
    rk = str(user_idpk)
    return {
        "self": {
            "income_rank": cached["income_rank_map"].get(rk),
            "money_rank": cached["money_rank_map"].get(rk),
            "animals_rank": cached["animals_rank_map"].get(rk),
            "referrals_rank": cached["referrals_rank_map"].get(rk),
        },
        "top_income": cached["top_income"],
        "top_money": cached["top_money"],
        "top_animals": cached["top_animals"],
        "top_referrals": cached["top_referrals"],
    }


async def build_standings(session: AsyncSession, user: User) -> dict[str, Any]:  # #1 #2
    # Try Redis cache first (#2)
    try:
        cached_raw = await redis.get(_STANDINGS_CACHE_KEY)
        if cached_raw:
            return _standings_with_self(json.loads(cached_raw), user.idpk)
    except Exception:
        pass

    users = await session.scalars(select(User))
    users = list(users.all())
    if not users:
        return {
            "self": {},
            "top_income": [],
            "top_money": [],
            "top_animals": [],
            "top_referrals": [],
        }

    referrals_count = await get_referrals_count_map(
        session=session,
        idpk_users=[member.idpk for member in users],
    )

    # Parallel computation of income and animals for all users (#1)
    income_results = await asyncio.gather(
        *[income_(session=session, user=m) for m in users]
    )
    animals_results = await asyncio.gather(
        *[get_total_number_animals(self=m, session=session) for m in users]
    )

    incomes = [(users[i], int(income_results[i])) for i in range(len(users))]
    money = [(m, int(m.usd)) for m in users]
    animals = [(users[i], int(animals_results[i])) for i in range(len(users))]
    referrals = [(m, int(referrals_count.get(m.idpk, 0))) for m in users]

    def sort_desc(rows: list[tuple[User, int]]) -> list[tuple[User, int]]:
        return sorted(rows, key=lambda row: row[1], reverse=True)

    income_sorted = sort_desc(incomes)
    money_sorted = sort_desc(money)
    animals_sorted = sort_desc(animals)
    referrals_sorted = sort_desc(referrals)

    income_rank_map = {
        str(m.idpk): rank for rank, (m, _) in enumerate(income_sorted, 1)
    }
    money_rank_map = {str(m.idpk): rank for rank, (m, _) in enumerate(money_sorted, 1)}
    animals_rank_map = {
        str(m.idpk): rank for rank, (m, _) in enumerate(animals_sorted, 1)
    }
    referrals_rank_map = {
        str(m.idpk): rank for rank, (m, _) in enumerate(referrals_sorted, 1)
    }

    cache_payload: dict[str, Any] = {
        "top_income": serialize_rank_rows(income_sorted),
        "top_money": serialize_rank_rows(money_sorted),
        "top_animals": serialize_rank_rows(animals_sorted),
        "top_referrals": serialize_rank_rows(referrals_sorted),
        "income_rank_map": income_rank_map,
        "money_rank_map": money_rank_map,
        "animals_rank_map": animals_rank_map,
        "referrals_rank_map": referrals_rank_map,
    }
    try:
        await redis.set(
            _STANDINGS_CACHE_KEY,
            json.dumps(cache_payload, ensure_ascii=False),
            ex=_STANDINGS_TTL,
        )
    except Exception:
        pass
    return _standings_with_self(cache_payload, user.idpk)


def get_rank_for_user(rows: list[tuple[User, int]], idpk_user: int) -> int | None:
    for index, (member, _) in enumerate(rows, start=1):
        if member.idpk == idpk_user:
            return index
    return None


def serialize_rank_rows(rows: list[tuple[User, int]]) -> list[dict[str, Any]]:
    return [
        {
            "rank": index,
            "idpk": member.idpk,
            "nickname": member.nickname,
            "value": int(value),
        }
        for index, (member, value) in enumerate(
            rows[: settings.top_candidates_limit],
            start=1,
        )
    ]


def npc_unity_invite_key(owner_idpk: int, target_idpk: int) -> str:
    return f"npc_unity_invite:{owner_idpk}:{target_idpk}"


async def build_recruit_targets(
    session: AsyncSession, user: User
) -> list[dict[str, Any]]:
    # Filter at SQL level — only real users without a unity, limit early.
    pool = await session.scalars(
        select(User)
        .where(
            User.idpk != user.idpk,
            User.current_unity.is_(None),
            User.id_user > 0,
        )
        .limit(settings.top_candidates_limit * 8)
    )
    members = list(pool.all())

    # Exclude users with already pending requests to this unity owner.
    pending_rows = await session.scalars(
        select(RequestToUnity.idpk_user).where(
            RequestToUnity.idpk_unity_owner == user.idpk
        )
    )
    pending_request_targets = {int(v) for v in pending_rows.all()}

    owner_income = int(await income_(session=session, user=user))

    candidates = []
    for member in members:
        member_idpk = int(member.idpk)

        # Skip if invite already sent recently (Redis TTL guard).
        if await redis.get(npc_unity_invite_key(user.idpk, member_idpk)):
            continue

        # Skip if there is already a pending request from this player.
        if member_idpk in pending_request_targets:
            continue

        candidate_income = int(await income_(session=session, user=member))
        candidate_animals = int(
            await get_total_number_animals(self=member, session=session)
        )

        # Recruit priority: income first, animals as tie-breaker.
        # Keep score as an explicit value so execution can enforce a quality bar.
        candidate_score = float(candidate_income) + float(candidate_animals) * 0.25

        candidates.append(
            {
                "idpk": member_idpk,
                "id_user": int(member.id_user),
                "nickname": member.nickname,
                "income": candidate_income,
                "animals": candidate_animals,
                "usd": int(member.usd),
                "score": round(candidate_score, 2),
            }
        )

    candidates.sort(
        key=lambda row: (
            float(row.get("score", 0) or 0),
            int(row.get("animals", 0) or 0),
        ),
        reverse=True,
    )

    if not candidates:
        return []

    best_score = float(candidates[0].get("score", 0) or 0)
    min_score = best_score * settings.recruit_min_score_ratio_vs_best
    min_income = max(
        settings.recruit_min_income_abs,
        int(owner_income * settings.recruit_min_income_ratio_vs_owner),
    )

    filtered_candidates = [
        row
        for row in candidates
        if int(row.get("income", 0) or 0) >= min_income
        and float(row.get("score", 0) or 0) >= min_score
    ]
    return filtered_candidates[: settings.top_candidates_limit]


async def ensure_random_merchant_for_user(
    session: AsyncSession, user: User
) -> RandomMerchant:
    merchant = await session.scalar(
        select(RandomMerchant).where(RandomMerchant.id_user == user.id_user)
    )
    if merchant:
        return merchant
    return await create_random_merchant(session=session, user=user)


async def build_chat_games_state(
    session: AsyncSession, user: User
) -> list[dict[str, Any]]:
    games = await session.scalars(
        select(Game).where(Game.activate == True, Game.end == False)  # noqa: E712
    )
    payload: list[dict[str, Any]] = []
    now = datetime.now()
    for game in games.all():
        game_chat_id = int(getattr(game, "source_chat_id", 0) or 0)
        if game_chat_id != 0 and game_chat_id != int(CHAT_ID):
            continue
        if game.end_date and game.end_date < now:
            continue
        current_players = int(
            await session.scalar(
                select(func.count())
                .select_from(Gamer)
                .where(Gamer.id_game == game.id_game)
            )
            or 0
        )
        payload.append(
            {
                "idpk": int(game.idpk),
                "id_game": game.id_game,
                "owner_idpk": int(game.idpk_user),
                "source_chat_id": int(getattr(game, "source_chat_id", 0) or 0),
                "type_game": game.type_game,
                "amount_gamers": int(game.amount_gamers),
                "amount_moves": int(game.amount_moves),
                "amount_award": int(game.amount_award),
                "currency_award": game.currency_award,
                "current_gamers": current_players,
                "free_slots": max(0, int(game.amount_gamers) - current_players),
                "seconds_left": max(
                    0,
                    int((game.end_date - now).total_seconds()) if game.end_date else 0,
                ),
            }
        )
    payload.sort(key=lambda row: (row["free_slots"] <= 0, row["seconds_left"]))
    return payload[: settings.top_candidates_limit]


async def build_chat_transfers_state(
    session: AsyncSession, user: User
) -> list[dict[str, Any]]:
    transfers = await session.scalars(
        select(TransferMoney).where(
            TransferMoney.status == True, TransferMoney.pieces > 0
        )  # noqa: E712
    )
    payload: list[dict[str, Any]] = []
    for tr in transfers.all():
        # Only official chat transfers or globally created user transfers.
        tr_chat_id = int(getattr(tr, "source_chat_id", 0) or 0)
        if tr_chat_id != 0 and tr_chat_id != int(CHAT_ID):
            continue
        if not getattr(tr, "id_mess", None):
            continue
        if int(tr.idpk_user) == int(user.idpk):
            continue
        claimed_user_ids = await list_transfer_claim_user_ids(
            session=session,
            transfer_idpk=int(tr.idpk),
        )
        if int(user.idpk) in claimed_user_ids:
            continue
        payload.append(
            {
                "idpk": int(tr.idpk),
                "idpk_user": int(tr.idpk_user),
                "currency": tr.currency,
                "one_piece_sum": int(tr.one_piece_sum),
                "pieces": int(tr.pieces),
                "total_left": int(tr.one_piece_sum) * int(tr.pieces),
            }
        )
    payload.sort(
        key=lambda row: (row["total_left"], row["one_piece_sum"]), reverse=True
    )
    return payload[: settings.top_candidates_limit]


async def build_unity_state(session: AsyncSession, user: User) -> dict[str, Any]:
    current_unity_idpk = get_unity_idpk(user.current_unity)
    current = None
    recruit_targets = []
    if current_unity_idpk:
        unity = await session.get(Unity, int(current_unity_idpk))
        if unity:
            pending_requests = []
            if unity.idpk_user == user.idpk:
                requests = await session.scalars(
                    select(RequestToUnity).where(
                        RequestToUnity.idpk_unity_owner == user.idpk
                    )
                )
                for request in requests.all():
                    applicant = await session.get(User, request.idpk_user)
                    if not applicant:
                        continue
                    pending_requests.append(
                        {
                            "idpk_user": int(applicant.idpk),
                            "id_user": int(applicant.id_user),
                            "nickname": applicant.nickname,
                            "usd": int(applicant.usd),
                            "rub": int(applicant.rub),
                            "animals": int(
                                await get_total_number_animals(
                                    self=applicant, session=session
                                )
                            ),
                            "income": int(
                                await income_(session=session, user=applicant)
                            ),
                            "has_unity": bool(applicant.current_unity),
                            "expires_at": request.date_request_end.isoformat(),
                        }
                    )
                pending_requests.sort(
                    key=lambda row: (row["income"], row["animals"], row["usd"]),
                    reverse=True,
                )
            upgrade_requirements = await get_unity_upgrade_requirements(
                session=session,
                unity=unity,
            )
            current = {
                "idpk": unity.idpk,
                "name": unity.name,
                "level": int(unity.level),
                "members": int(await count_unity_members(session=session, unity=unity)),
                "owner_idpk": int(unity.idpk_user),
                "income": int(await count_income_unity(session=session, unity=unity)),
                "can_upgrade": bool(upgrade_requirements.get("can_upgrade", False)),
                "upgrade_requirements": upgrade_requirements,
                "is_owner": unity.idpk_user == user.idpk,
                "pending_requests": pending_requests[: settings.top_candidates_limit],
                "pending_requests_count": len(pending_requests),
            }
            if unity.idpk_user == user.idpk:
                recruit_targets = await build_recruit_targets(
                    session=session, user=user
                )

    unities = await session.scalars(select(Unity))
    unities = list(unities.all())
    candidates = []
    for unity in unities:
        if current and unity.idpk == current["idpk"]:
            continue
        owner = await session.get(User, unity.idpk_user)
        candidates.append(
            {
                "idpk": int(unity.idpk),
                "name": unity.name,
                "level": int(unity.level),
                "members": int(await count_unity_members(session=session, unity=unity)),
                "owner_idpk": int(unity.idpk_user),
                "owner_nickname": owner.nickname if owner else None,
                "income": int(await count_income_unity(session=session, unity=unity)),
                "owner_is_npc": bool(owner and owner.id_user < 0),
            }
        )
    candidates.sort(key=lambda row: row["income"], reverse=True)
    return {
        "current": current,
        "candidates": candidates[: settings.top_candidates_limit],
        "recruit_targets": recruit_targets,
    }


async def can_upgrade_unity(session: AsyncSession, unity: Unity) -> bool:
    match unity.level:
        case 0:
            return await check_condition_1st_lvl(session=session, unity=unity)
        case 1:
            return await check_condition_2nd_lvl(session=session, unity=unity)
        case 2:
            return await check_condition_3rd_lvl(session=session, unity=unity)
    return False


async def get_unity_upgrade_requirements(
    session: AsyncSession, unity: Unity
) -> dict[str, Any]:
    level = int(unity.level)
    if level >= 3:
        return {
            "can_upgrade": False,
            "next_level": None,
            "blockers": ["unity_max_level"],
            "metrics": {},
        }

    data = await get_data_by_lvl_unity(session=session, lvl=level, unity=unity)
    blockers: list[str] = []
    metrics: dict[str, Any] = {}

    if "amount_members" in data:
        required = int(data.get("amount_members", 0) or 0)
        current = int(data.get("f_amount_members", 0) or 0)
        metrics["members"] = {"current": current, "required": required}
        if current < required:
            blockers.append(f"need_members:{current}/{required}")

    if "amount_income" in data:
        required = int(data.get("amount_income", 0) or 0)
        current = int(data.get("f_current_income", 0) or 0)
        metrics["income_per_minute_rub"] = {"current": current, "required": required}
        if current < required:
            blockers.append(f"need_income:{current}/{required}")

    if "amount_animals" in data:
        required = int(data.get("amount_animals", 0) or 0)
        not_have = str(data.get("f_members_not_have_amount_animals", "") or "").strip()
        metrics["animals_per_member"] = {
            "required": required,
            "members_missing": not_have,
        }
        if not_have:
            blockers.append("need_animals_per_member")

    return {
        "can_upgrade": len(blockers) == 0,
        "next_level": int(data.get("next_lvl", level + 1) or (level + 1)),
        "blockers": blockers,
        "metrics": metrics,
    }


async def build_item_opportunities(session: AsyncSession, user: User) -> dict[str, Any]:
    items = await session.scalars(select(Item).where(Item.id_user == user.id_user))
    items = list(items.all())
    max_lvl_item = await get_value(session=session, value_name="MAX_LVL_ITEM")
    usd_to_up_item = await get_value(session=session, value_name="USD_TO_UP_ITEM")
    usd_to_merge_items = await get_value(
        session=session, value_name="USD_TO_MERGE_ITEMS"
    )

    upgrade_candidates = []
    for item in items:
        if item.lvl >= max_lvl_item:
            continue
        cost = int(usd_to_up_item) * (int(item.lvl) + 1)
        success_percent = await calculate_percent_to_enhance(
            session=session,
            current_item_lvl=item.lvl,
        )
        upgrade_candidates.append(
            {
                "id_item": item.id_item,
                "name": item.name_with_emoji,
                "lvl": int(item.lvl),
                "cost_usd": int(cost),
                "success_percent": int(success_percent),
                "score": round(item_score(item), 2),
                "is_active": item.is_active,
            }
        )

    merge_candidates = []
    allowed_props = prop_quantity_by_rarity["mythical"]
    for item_1, item_2 in combinations(items, 2):
        count_props_1 = len(json.loads(item_1.properties))
        count_props_2 = len(json.loads(item_2.properties))
        if count_props_1 > allowed_props or count_props_2 > allowed_props:
            continue
        q_props = count_props_1 + count_props_2
        lvl_sum = max(1, int(item_1.lvl) + int(item_2.lvl))
        cost = int(usd_to_merge_items) * (q_props + lvl_sum)
        merge_candidates.append(
            {
                "id_item_1": item_1.id_item,
                "id_item_2": item_2.id_item,
                "name_1": item_1.name_with_emoji,
                "name_2": item_2.name_with_emoji,
                "cost_usd": int(cost),
                "combined_score": round(item_score(item_1) + item_score(item_2), 2),
                "active_pair": bool(item_1.is_active or item_2.is_active),
            }
        )

    upgrade_candidates.sort(
        key=lambda row: (row["success_percent"], row["score"]), reverse=True
    )
    merge_candidates.sort(key=lambda row: row["combined_score"], reverse=True)
    return {
        "upgrade_candidates": upgrade_candidates[: settings.top_candidates_limit],
        "merge_candidates": merge_candidates[: settings.top_candidates_limit],
    }


def _effective_usd(player: dict[str, Any], bank: dict[str, Any]) -> float:
    rate = max(1, int(bank.get("rate_rub_usd", 1) or 1))
    return (
        float(int(player.get("usd", 0) or 0))
        + float(int(player.get("rub", 0) or 0)) / rate
    )


def _actions_since_last(rows: list[dict[str, Any]], target_actions: set[str]) -> int:
    count = 0
    for row in reversed(rows):
        action_name = str(row.get("action", "")).strip()
        if action_name in target_actions:
            return count
        count += 1
    return count


def _build_rate_history_snapshot(
    history_raw: str, now_ts: int | None = None
) -> dict[str, Any]:
    try:
        rows = json.loads(history_raw) if history_raw else []
        if not isinstance(rows, list):
            rows = []
    except Exception:
        rows = []

    points: list[dict[str, int]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ts = int(row.get("ts", 0) or 0)
        rate = int(row.get("rate", 0) or 0)
        if ts <= 0 or rate <= 0:
            continue
        points.append({"ts": ts, "rate": rate})

    if not points:
        return {"points_1h": [], "summary_1h": {}}

    points.sort(key=lambda x: x["ts"])
    now_ts = now_ts or int(datetime.now().timestamp())
    one_hour_ago = now_ts - 3600
    one_hour_points = [p for p in points if p["ts"] >= one_hour_ago]
    if not one_hour_points:
        one_hour_points = points[-1:]

    rates = [int(p["rate"]) for p in one_hour_points]
    first_rate = int(rates[0])
    last_rate = int(rates[-1])
    summary = {
        "samples": len(one_hour_points),
        "first": first_rate,
        "last": last_rate,
        "min": min(rates),
        "max": max(rates),
        "delta": last_rate - first_rate,
        "trend": "up"
        if last_rate > first_rate
        else "down"
        if last_rate < first_rate
        else "flat",
    }
    return {
        "points_1h": one_hour_points[-60:],
        "summary_1h": summary,
    }


async def build_momentum_signal(
    session: AsyncSession,
    user: User,
    current_income: int,
) -> dict[str, Any]:
    rows = await list_recent_npc_history_payloads(session=session, user=user, limit=12)
    recent_rows = rows[-3:]
    last_3_actions = [
        str(row.get("action", "wait")).strip() or "wait" for row in recent_rows[-3:]
    ]

    income_anchor = None
    usd_anchor = None
    for row in recent_rows:
        after_state = row.get("after_state") or {}
        if (
            income_anchor is None
            and after_state.get("income_per_minute_rub") is not None
        ):
            income_anchor = int(after_state.get("income_per_minute_rub", 0) or 0)
        if usd_anchor is None and after_state.get("usd") is not None:
            usd_anchor = int(after_state.get("usd", 0) or 0)

    if income_anchor is None:
        income_trend = "insufficient history"
    else:
        income_delta = int(current_income) - income_anchor
        income_trend = f"{income_delta:+d} RUB/min over last {max(1, len(recent_rows))} logged moves"

    if usd_anchor is None:
        usd_trend = "insufficient history"
    else:
        usd_delta = int(user.usd) - usd_anchor
        usd_trend = (
            f"{usd_delta:+d} USD over last {max(1, len(recent_rows))} logged moves"
        )

    return {
        "income_trend": income_trend,
        "usd_trend": usd_trend,
        "actions_since_last_aviary": _actions_since_last(rows, {"buy_aviary"}),
        "actions_since_last_item": _actions_since_last(
            rows,
            {
                "create_item",
                "activate_item",
                "deactivate_item",
                "sell_item",
                "upgrade_item",
                "merge_items",
                "optimize_items",
            },
        ),
        "last_3_actions": last_3_actions,
    }


def _append_unique_action(
    actions: list[dict[str, Any]],
    action_name: str,
    params: dict[str, Any] | None = None,
) -> None:
    payload = {"action": action_name, "params": params or {}}
    key = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    existing = {
        json.dumps(item, ensure_ascii=False, sort_keys=True)
        for item in actions
        if isinstance(item, dict)
    }
    if key not in existing:
        actions.append(payload)


def _iter_animal_variants(observation: dict[str, Any]) -> list[dict[str, Any]]:
    variants: list[dict[str, Any]] = []
    for animal in observation.get("animal_market", []):
        for variant in animal.get("variants", []):
            variants.append({"animal": animal.get("animal"), **variant})
    return variants


def _find_animal_variant(
    observation: dict[str, Any], animal_name: str, rarity: str
) -> dict[str, Any] | None:
    for variant in _iter_animal_variants(observation):
        if variant.get("animal") == animal_name and variant.get("rarity") == rarity:
            return variant
    return None


def _find_aviary_option(
    observation: dict[str, Any], code_name_aviary: str
) -> dict[str, Any] | None:
    for row in observation.get("aviary_market", []):
        if row.get("code_name") == code_name_aviary:
            return row
    return None


async def build_allowed_actions(
    session: AsyncSession,
    user: User,
    observation: dict[str, Any],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    player = observation.get("player", {})
    zoo = observation.get("zoo", {})
    bank = observation.get("bank", {})
    merchant = observation.get("merchant", {})
    items = observation.get("items", {})
    item_opportunities = observation.get("item_opportunities", {})
    unity = observation.get("unity", {})
    current_unity = unity.get("current") or {}

    usd = int(player.get("usd", 0) or 0)
    rub = int(player.get("rub", 0) or 0)
    rate = max(1, int(bank.get("rate_rub_usd", 1) or 1))
    remain_seats = int(zoo.get("remain_seats", 0) or 0)
    owned_items = list(items.get("items", []))
    active_items = [item for item in owned_items if item.get("is_active")]
    inactive_items = [item for item in owned_items if not item.get("is_active")]
    affordable_aviaries = [
        row
        for row in observation.get("aviary_market", [])
        if int(row.get("affordable_quantity", 0) or 0) > 0
    ]
    affordable_aviaries.sort(key=lambda row: int(row.get("price_usd", 0) or 0))
    affordable_variants = [
        row
        for row in _iter_animal_variants(observation)
        if int(row.get("affordable_quantity", 0) or 0) > 0
    ]
    affordable_variants.sort(
        key=lambda row: (
            row.get("payback_minutes") is None,
            float(row.get("payback_minutes") or 10**9),
            -int(row.get("income_rub", 0) or 0),
        )
    )
    targeted_candidates: list[str] = []
    for row in affordable_variants:
        animal_name = str(row.get("animal", "")).strip()
        if animal_name and animal_name not in targeted_candidates:
            targeted_candidates.append(animal_name)

    max_quantity_animals = int(
        await get_value(session=session, value_name="MAX_QUANTITY_ANIMALS")
    )
    price_for_create_unity = int(
        await get_value(session=session, value_name="PRICE_FOR_CREATE_UNITY")
    )

    _append_unique_action(actions, "wait", {})

    if int(player.get("daily_bonus_available", 0) or 0) > 0:
        _append_unique_action(actions, "claim_daily_bonus", {"rerolls": 0})

    # Let the LLM decide exchange timing based on recent rate history in observation.bank.
    # Keep both tactical and full conversion options available.
    if rub >= rate:
        # Prefer full conversion by default; tactical one-rate exchange stays as a secondary option.
        _append_unique_action(actions, "exchange_bank", {"mode": "all"})
        _append_unique_action(
            actions, "exchange_bank", {"mode": "amount", "amount": rate}
        )

    can_create_item_now = bool(
        usd >= int(items.get("create_price_usd", 0) or 0)
        or int(player.get("paw_coins", 0) or 0) >= CREATE_ITEM_PAW_PRICE
    )
    if affordable_aviaries or affordable_variants or can_create_item_now:
        _append_unique_action(actions, "invest_for_income", {})
    if remain_seats > 0 and (targeted_candidates or affordable_variants):
        _append_unique_action(actions, "invest_for_top_animals", {})

    for row in affordable_aviaries[:3]:
        aviary_size = max(1, int(row.get("size", 1) or 1))
        affordable_quantity = max(1, int(row.get("affordable_quantity", 1) or 1))
        # Let the model choose meaningful multi-buy batches instead of hardcoded x1.
        quantity_options = {1, min(2, affordable_quantity), min(5, affordable_quantity)}
        if remain_seats <= 0:
            # Capacity-lock: suggest enough aviaries to open a playable seat buffer.
            target_new_seats = max(aviary_size, 6)
            needed_qty = max(1, (target_new_seats + aviary_size - 1) // aviary_size)
            quantity_options.add(min(needed_qty, affordable_quantity))

        for qty in sorted(q for q in quantity_options if q >= 1):
            _append_unique_action(
                actions,
                "buy_aviary",
                {"code_name_aviary": row.get("code_name"), "quantity": int(qty)},
            )

    for row in affordable_variants[:4]:
        affordable_quantity = int(row.get("affordable_quantity", 0) or 0)
        # No hard cap: suggest full currently available quantity.
        suggested_quantity = max(1, min(affordable_quantity, remain_seats))
        _append_unique_action(
            actions,
            "buy_rarity_animal",
            {
                "animal": row.get("animal"),
                "rarity": row.get("rarity"),
                "quantity": suggested_quantity,
            },
        )

    if (
        not merchant.get("first_offer_bought")
        and remain_seats >= int(merchant.get("quantity_animals", 0) or 0)
        and usd >= int(merchant.get("price_with_discount", 0) or 0)
    ):
        _append_unique_action(actions, "buy_merchant_discount_offer", {})

    if remain_seats >= max_quantity_animals and usd >= int(
        merchant.get("random_offer_price", 0) or 0
    ):
        _append_unique_action(actions, "buy_merchant_random_offer", {})

    if remain_seats > 0:
        for animal_name in targeted_candidates[:3]:
            _append_unique_action(
                actions,
                "buy_merchant_targeted_offer",
                {"animal": animal_name, "quantity": 1},
            )

    if can_create_item_now:
        _append_unique_action(actions, "create_item", {})

    if owned_items:
        _append_unique_action(actions, "optimize_items", {})

    if inactive_items and len(active_items) < 3:
        for item in sorted(
            inactive_items, key=lambda row: int(row.get("lvl", 0) or 0), reverse=True
        )[:3]:
            _append_unique_action(
                actions,
                "activate_item",
                {"id_item": item.get("id_item")},
            )

    for item in active_items[:3]:
        _append_unique_action(
            actions,
            "deactivate_item",
            {"id_item": item.get("id_item")},
        )

    for item in sorted(
        owned_items, key=lambda row: int(row.get("lvl", 0) or 0), reverse=True
    )[:3]:
        _append_unique_action(actions, "sell_item", {"id_item": item.get("id_item")})

    affordable_upgrades = [
        row
        for row in item_opportunities.get("upgrade_candidates", [])
        if usd >= int(row.get("cost_usd", 0) or 0)
    ]
    for row in affordable_upgrades[:3]:
        _append_unique_action(actions, "upgrade_item", {"id_item": row.get("id_item")})

    affordable_merges = [
        row
        for row in item_opportunities.get("merge_candidates", [])
        if usd >= int(row.get("cost_usd", 0) or 0)
    ]
    for row in affordable_merges[:2]:
        _append_unique_action(
            actions,
            "merge_items",
            {
                "id_item_1": row.get("id_item_1"),
                "id_item_2": row.get("id_item_2"),
            },
        )

    if not player.get("current_unity"):
        if usd >= price_for_create_unity:
            _append_unique_action(actions, "create_unity", {})
        for row in (unity.get("candidates") or [])[:2]:
            _append_unique_action(
                actions,
                "join_best_unity",
                {"owner_idpk": int(row.get("owner_idpk", 0) or 0)},
            )
    else:
        if current_unity.get("is_owner"):
            for row in (unity.get("recruit_targets") or [])[:2]:
                _append_unique_action(
                    actions,
                    "recruit_top_player",
                    {"idpk_user": int(row.get("idpk", 0) or 0)},
                )
            if current_unity.get("can_upgrade"):
                _append_unique_action(actions, "upgrade_unity_level", {})
            pending_requests = current_unity.get("pending_requests") or []
            # Clan project tooling for NPC
            with contextlib.suppress(Exception):
                unity_idpk = int(current_unity.get("idpk", 0) or 0)
                if unity_idpk:
                    unity_obj = await session.get(Unity, unity_idpk)
                    if unity_obj is not None:
                        project = await get_or_create_project(
                            session=session, unity=unity_obj
                        )
                        if str(project.get("status", "active")) == "active":
                            if rub >= 50_000:
                                _append_unique_action(
                                    actions,
                                    "contribute_clan_project",
                                    {"rub": min(rub, 200_000), "usd": 0},
                                )
                            if usd >= 5_000:
                                _append_unique_action(
                                    actions,
                                    "contribute_clan_project",
                                    {"rub": 0, "usd": min(usd, 20_000)},
                                )

            with contextlib.suppress(Exception):
                ch = await get_user_chests(session=session, user_idpk=user.idpk)
                if int(ch.get("epic", 0) or 0) > 0:
                    _append_unique_action(
                        actions, "open_clan_chest", {"chest_type": "epic"}
                    )
                if int(ch.get("rare", 0) or 0) > 0:
                    _append_unique_action(
                        actions, "open_clan_chest", {"chest_type": "rare"}
                    )
                if int(ch.get("common", 0) or 0) > 0:
                    _append_unique_action(
                        actions, "open_clan_chest", {"chest_type": "common"}
                    )
            for row in pending_requests[:2]:
                applicant_id = int(row.get("idpk_user", 0) or 0)
                _append_unique_action(
                    actions,
                    "review_unity_request",
                    {"idpk_user": applicant_id, "decision": "accept"},
                )
                _append_unique_action(
                    actions,
                    "review_unity_request",
                    {"idpk_user": applicant_id, "decision": "reject"},
                )

    # Chat-only transfer/game actions: allowed only with strong surplus and no recent spam.
    recent_actions = [
        str(item).strip()
        for item in (observation.get("momentum", {}).get("last_3_actions") or [])
    ]
    recent_chat_action = any(
        action_name in {"send_chat_transfer", "create_chat_game", "join_chat_game"}
        for action_name in recent_actions
    )
    effective_usd = float(usd) + float(rub) / max(1, rate)
    has_strong_surplus = effective_usd >= 6000 and usd >= 1200
    economy_not_blocked = remain_seats > 0

    if has_strong_surplus and economy_not_blocked and not recent_chat_action:
        _append_unique_action(
            actions,
            "send_chat_transfer",
            {"currency": "usd", "amount": min(usd, 300), "pieces": 5},
        )

    # Claim chat transfers (best-effort). We keep this enabled for +EV pickup behavior.
    for tr in (observation.get("chat_transfers") or [])[:2]:
        _append_unique_action(
            actions,
            "claim_chat_transfer",
            {"idpk_tr": int(tr.get("idpk", 0) or 0)},
        )

    if (
        has_strong_surplus
        and usd >= 1800
        and economy_not_blocked
        and not recent_chat_action
    ):
        _append_unique_action(
            actions,
            "create_chat_game",
            {
                "game_type": "🎲",
                "amount_gamers": 5,
                "amount_award": min(usd // 4, 500),
                "currency": "usd",
            },
        )

    # Joining existing chat games is low-cost and can be +EV even during capacity lock.
    if not recent_chat_action:
        for row in (observation.get("chat_games") or [])[:3]:
            # Join official chat games or user-created global games.
            game_chat_id = int(row.get("source_chat_id", 0) or 0)
            if game_chat_id != 0 and game_chat_id != int(CHAT_ID):
                continue
            if int(row.get("owner_idpk", 0) or 0) == int(player.get("idpk", 0) or 0):
                continue
            free_slots = int(row.get("free_slots", 0) or 0)
            if free_slots <= 0:
                continue
            award = int(row.get("amount_award", 0) or 0)
            if award <= 0:
                continue
            _append_unique_action(
                actions,
                "join_chat_game",
                {"id_game": row.get("id_game")},
            )

    return actions


def _score_allowed_action(
    action_entry: dict[str, Any], observation: dict[str, Any]
) -> tuple[int, str]:
    action_name = str(action_entry.get("action", "wait"))
    params = action_entry.get("params", {}) or {}
    summary = observation.get("strategy_signals", {}).get("summary", {})
    player = observation.get("player", {})
    bank = observation.get("bank", {})
    merchant = observation.get("merchant", {})
    items = observation.get("items", {})
    unity = observation.get("unity", {})
    current_unity = unity.get("current") or {}

    if action_name == "claim_daily_bonus":
        return 93, "free value available immediately"
    if action_name == "exchange_bank":
        rate = max(1, int(bank.get("rate_rub_usd", 1) or 1))
        rub = int(player.get("rub", 0) or 0)
        return 82, f"converts about {rub // rate} USD-equivalent from idle RUB"
    if action_name == "buy_aviary":
        option = _find_aviary_option(
            observation, str(params.get("code_name_aviary", ""))
        )
        if not option:
            return 78, "adds zoo capacity"
        base_score = 95 if summary.get("need_seats") else 81
        return (
            base_score,
            f"unlocks {int(option.get('size', 0) or 0)} seats for {int(option.get('price_usd', 0) or 0)} USD",
        )
    if action_name == "buy_rarity_animal":
        variant = _find_animal_variant(
            observation,
            str(params.get("animal", "")),
            str(params.get("rarity", "")),
        )
        if not variant:
            return 80, "adds more animal income"
        payback = variant.get("payback_minutes")
        payback_note = (
            f"payback {float(payback):.1f} min"
            if payback is not None
            else "income upgrade"
        )
        return (
            90,
            f"{payback_note}, cost {int(variant.get('price_usd', 0) or 0)} USD, +{int(variant.get('income_rub', 0) or 0)} RUB/min",
        )
    if action_name == "buy_merchant_discount_offer":
        return (
            84,
            f"merchant discount buys {int(merchant.get('quantity_animals', 0) or 0)} animals for {int(merchant.get('price_with_discount', 0) or 0)} USD",
        )
    if action_name == "buy_merchant_random_offer":
        return (
            76,
            f"merchant random bundle costs {int(merchant.get('random_offer_price', 0) or 0)} USD",
        )
    if action_name == "buy_merchant_targeted_offer":
        return 74, f"targets {params.get('animal')} directly through the merchant"
    if action_name == "create_item":
        return (
            77,
            f"item engine costs {int(items.get('create_price_usd', 0) or 0)} USD or paw coins",
        )
    if action_name == "optimize_items":
        return 68, "rebalances active items with no direct currency cost"
    if action_name == "activate_item":
        return 71, "activates an idle item slot"
    if action_name == "deactivate_item":
        return 54, "frees an active slot for a better item"
    if action_name == "sell_item":
        return 48, "liquidates an item for immediate cash"
    if action_name == "upgrade_item":
        for row in observation.get("item_opportunities", {}).get(
            "upgrade_candidates", []
        ):
            if row.get("id_item") == params.get("id_item"):
                return (
                    72,
                    f"upgrade costs {int(row.get('cost_usd', 0) or 0)} USD at {int(row.get('success_percent', 0) or 0)}% success",
                )
        return 70, "improves an existing item"
    if action_name == "merge_items":
        for row in observation.get("item_opportunities", {}).get(
            "merge_candidates", []
        ):
            if row.get("id_item_1") == params.get("id_item_1") and row.get(
                "id_item_2"
            ) == params.get("id_item_2"):
                return (
                    69,
                    f"merge costs {int(row.get('cost_usd', 0) or 0)} USD with combined score {float(row.get('combined_score', 0) or 0):.1f}",
                )
        return 67, "merges two items into one stronger roll"
    if action_name == "create_unity":
        return 66, "opens a new social shell when solo play is too limiting"
    if action_name == "join_best_unity":
        for row in unity.get("candidates", []):
            if int(row.get("owner_idpk", 0) or 0) == int(
                params.get("owner_idpk", 0) or 0
            ):
                return (
                    74,
                    f"joins {row.get('name')} with {int(row.get('income', 0) or 0)} RUB/min unity income",
                )
        return 72, "joins a stronger unity for social leverage"
    if action_name == "recruit_top_player":
        for row in unity.get("recruit_targets", []):
            if int(row.get("idpk", 0) or 0) == int(params.get("idpk_user", 0) or 0):
                return (
                    79,
                    f"invites {row.get('nickname')} with {int(row.get('income', 0) or 0)} income",
                )
        return 77, "recruits a strong free player"
    if action_name == "upgrade_unity_level":
        return (
            83,
            f"current unity can level up from {int(current_unity.get('level', 0) or 0)}",
        )
    if action_name == "contribute_clan_project":
        clan_project = observation.get("clan_project", {}) or {}
        success_rewards = clan_project.get("reward_success", {}) or {}
        current_rewards = clan_project.get("reward_current", {}) or {}
        return (
            75,
            "helps close both RUB and USD goals; "
            f"full rewards c:{int(success_rewards.get('common', 0) or 0)} "
            f"r:{int(success_rewards.get('rare', 0) or 0)} "
            f"e:{int(success_rewards.get('epic', 0) or 0)}, "
            f"current fallback c:{int(current_rewards.get('common', 0) or 0)} "
            f"r:{int(current_rewards.get('rare', 0) or 0)} "
            f"e:{int(current_rewards.get('epic', 0) or 0)}",
        )
    if action_name == "open_clan_chest":
        return 73, "converts stored chest rewards into immediate RUB/USD"
    if action_name == "exit_from_unity":
        return 45, "leaves current unity to switch social strategy"
    if action_name == "send_chat_transfer":
        if bool(
            (observation.get("strategy_signals", {}).get("summary", {}) or {}).get(
                "need_seats"
            )
        ):
            return 64, "capacity-locked: chat transfer can create short-term upside"
        return 26, "optional social spend; valid only when economy has surplus"
    if action_name == "claim_chat_transfer":
        return 72, "free immediate upside by claiming an available chat transfer"
    if action_name == "create_chat_game":
        if bool(
            (observation.get("strategy_signals", {}).get("summary", {}) or {}).get(
                "need_seats"
            )
        ):
            return (
                62,
                "capacity-locked: game creation can unlock upside while waiting for seats",
            )
        return 28, "optional social spend; valid only with stable surplus"
    if action_name == "join_chat_game":
        if bool(
            (observation.get("strategy_signals", {}).get("summary", {}) or {}).get(
                "need_seats"
            )
        ):
            return (
                68,
                "free upside while capacity-locked; can improve cash without spending",
            )
        return 24, "optional social action after core economy is stable"
    if action_name == "review_unity_request":
        decision = str(params.get("decision", "accept"))
        return (
            94 if decision == "accept" else 63,
            f"{decision}s a pending unity request immediately",
        )
    if action_name == "invest_for_income":
        return 73, "delegates to the best immediate compounding investment"
    if action_name == "invest_for_top_animals":
        return 70, "pushes raw animal count when direct buys are available"
    if action_name == "wait":
        need_seats = bool((summary or {}).get("need_seats"))
        if need_seats:
            return (
                20,
                "idle fallback only when no productive seat-lock action is available",
            )
        return 32, "idle fallback when no better move is available"
    return 0, ""


def build_decision_brief(observation: dict[str, Any]) -> dict[str, Any]:
    player = observation.get("player", {})
    bank = observation.get("bank", {})
    zoo = observation.get("zoo", {})
    items = observation.get("items", {})
    unity = observation.get("unity", {})
    summary = observation.get("strategy_signals", {}).get("summary", {})
    effective_usd = _effective_usd(player, bank)

    if int(zoo.get("remain_seats", 0) or 0) <= 0:
        bottleneck = "Seat capacity is at zero, so all animal growth is blocked until capacity opens."
    elif int(items.get("owned_count", 0) or 0) > 0 and int(
        items.get("active_count", 0) or 0
    ) < min(3, int(items.get("owned_count", 0) or 0)):
        bottleneck = "Some owned items are idle, so passive modifiers are underused."
    elif not player.get("current_unity") and (unity.get("candidates") or []):
        bottleneck = (
            "The NPC is solo, so unity leverage and recruiting pressure are both idle."
        )
    elif summary.get("next_unlock"):
        next_unlock = summary.get("next_unlock") or {}
        target_usd = float(int(next_unlock.get("target_usd", 0) or 0))
        missing_usd = max(0, int(target_usd - effective_usd))
        bottleneck = f"Cash is short by about {missing_usd} USD for the next unlock {next_unlock.get('label')}."
    else:
        bottleneck = "No dominant blocker detected, so the best immediate compounding action should win."

    top_affordable_actions = []
    for action_entry in observation.get("allowed_actions", []):
        if action_entry.get("action") == "wait":
            continue
        score, note = _score_allowed_action(
            action_entry=action_entry, observation=observation
        )
        if score <= 0:
            continue
        top_affordable_actions.append(
            {
                "action": action_entry.get("action"),
                "params": action_entry.get("params", {}) or {},
                "score": score,
                "note": note,
            }
        )
    top_affordable_actions.sort(
        key=lambda row: (int(row.get("score", 0) or 0), str(row.get("action", ""))),
        reverse=True,
    )

    next_unaffordable_candidates: list[dict[str, Any]] = []
    best_income_option = summary.get("best_income_option") or {}
    if (
        best_income_option
        and int(best_income_option.get("affordable_quantity", 0) or 0) <= 0
    ):
        target_usd = int(best_income_option.get("price_usd", 0) or 0)
        next_unaffordable_candidates.append(
            {
                "action": "buy_rarity_animal",
                "animal": best_income_option.get("animal"),
                "rarity": best_income_option.get("rarity"),
                "missing_usd": max(0, int(target_usd - effective_usd)),
                "eta_minutes": None
                if best_income_option.get("eta_seconds") is None
                else int(best_income_option.get("eta_seconds", 0) or 0) // 60,
            }
        )
    best_aviary_option = summary.get("best_aviary_option") or {}
    if (
        best_aviary_option
        and int(best_aviary_option.get("affordable_quantity", 0) or 0) <= 0
    ):
        target_usd = int(best_aviary_option.get("price_usd", 0) or 0)
        next_unaffordable_candidates.append(
            {
                "action": "buy_aviary",
                "code_name_aviary": best_aviary_option.get("code_name"),
                "missing_usd": max(0, int(target_usd - effective_usd)),
                "eta_minutes": None
                if best_aviary_option.get("eta_seconds") is None
                else int(best_aviary_option.get("eta_seconds", 0) or 0) // 60,
            }
        )
    create_item_price = int(items.get("create_price_usd", 0) or 0)
    if create_item_price > 0 and not summary.get("can_create_item_now"):
        create_item_eta = estimate_usd_eta_seconds(
            usd=int(player.get("usd", 0) or 0),
            rub=int(player.get("rub", 0) or 0),
            rate_rub_usd=int(bank.get("rate_rub_usd", 1) or 1),
            income_per_minute_rub=int(player.get("income_per_minute_rub", 0) or 0),
            target_usd=create_item_price,
        )
        next_unaffordable_candidates.append(
            {
                "action": "create_item",
                "missing_usd": max(0, int(create_item_price - effective_usd)),
                "eta_minutes": None
                if create_item_eta is None
                else int(create_item_eta) // 60,
            }
        )

    next_unaffordable = None
    if next_unaffordable_candidates:
        next_unaffordable_candidates.sort(
            key=lambda row: (
                row.get("eta_minutes") is None,
                int(row.get("eta_minutes", 10**9) or 10**9),
                int(row.get("missing_usd", 0) or 0),
            )
        )
        next_unaffordable = next_unaffordable_candidates[0]

    clan_project = observation.get("clan_project", {}) or {}
    clan_context = None
    if clan_project:
        status = str(clan_project.get("status", "active"))
        member_count = int(clan_project.get("member_count", 1) or 1)
        pr_rub = int(clan_project.get("progress_rub", 0) or 0)
        tg_rub = int(clan_project.get("target_rub", 0) or 0)
        pr_usd = int(clan_project.get("progress_usd", 0) or 0)
        tg_usd = int(clan_project.get("target_usd", 0) or 0)
        success_rewards = clan_project.get("reward_success", {}) or {}
        current_rewards = clan_project.get("reward_current", {}) or {}
        clan_context = (
            "Clan project context: project has a 3-day deadline. "
            f"Rewards scale with clan size ({member_count} members). "
            "Project closes only when both RUB and USD goals are reached. "
            "Contributors receive chest rewards; if deadline is missed, rewards are still split only among contributors. "
            f"Current project {clan_project.get('name', 'Заповедник')} L{int(clan_project.get('level', 1) or 1)} "
            f"status={status}, progress RUB {pr_rub}/{tg_rub}, USD {pr_usd}/{tg_usd}. "
            f"Full rewards: c:{int(success_rewards.get('common', 0) or 0)} "
            f"r:{int(success_rewards.get('rare', 0) or 0)} "
            f"e:{int(success_rewards.get('epic', 0) or 0)}. "
            f"If deadline hit now: c:{int(current_rewards.get('common', 0) or 0)} "
            f"r:{int(current_rewards.get('rare', 0) or 0)} "
            f"e:{int(current_rewards.get('epic', 0) or 0)}. "
            "MVP gets +1 epic chest."
        )

    return {
        "bottleneck": bottleneck,
        "top_affordable_actions": top_affordable_actions[:4],
        "next_unaffordable": next_unaffordable,
        "clan_project_context": clan_context,
    }


def build_strategy_signals(observation: dict[str, Any]) -> dict[str, Any]:
    remain_seats = int(observation["zoo"]["remain_seats"])
    rate = int(observation["bank"]["rate_rub_usd"])
    usd = int(observation["player"]["usd"])
    rub = int(observation["player"]["rub"])
    income_per_minute_rub = int(observation["player"]["income_per_minute_rub"])

    income_options = []
    for animal in observation["animal_market"]:
        for variant in animal["variants"]:
            candidate = {
                "animal": animal["animal"],
                **variant,
            }
            candidate["score"] = score_animal_market_option(candidate)
            candidate["eta_seconds"] = estimate_usd_eta_seconds(
                usd=usd,
                rub=rub,
                rate_rub_usd=rate,
                income_per_minute_rub=income_per_minute_rub,
                target_usd=int(candidate.get("price_usd", 0) or 0),
            )
            income_options.append(candidate)
    income_options.sort(
        key=lambda row: (
            int(row.get("affordable_quantity", 0) or 0) > 0,
            float(row.get("score", 0.0)),
            -(row.get("payback_minutes") or 999999),
        ),
        reverse=True,
    )
    best_income_option = income_options[0] if income_options else None

    aviary_options = []
    for row in observation.get("aviary_market", []):
        cost_per_seat = round(int(row["price_usd"]) / max(1, int(row["size"])), 2)
        aviary_options.append(
            {
                **row,
                "cost_per_seat": cost_per_seat,
                "eta_seconds": estimate_usd_eta_seconds(
                    usd=usd,
                    rub=rub,
                    rate_rub_usd=rate,
                    income_per_minute_rub=income_per_minute_rub,
                    target_usd=int(row.get("price_usd", 0) or 0),
                ),
            }
        )
    aviary_options.sort(key=lambda row: (row["cost_per_seat"], row["price_usd"]))
    cheapest_aviary = aviary_options[0] if aviary_options else None

    create_item_price = int(
        observation.get("items", {}).get("create_price_usd", 0) or 0
    )
    create_item_eta = estimate_usd_eta_seconds(
        usd=usd,
        rub=rub,
        rate_rub_usd=rate,
        income_per_minute_rub=income_per_minute_rub,
        target_usd=create_item_price,
    )
    next_unlock_candidates = []
    if best_income_option:
        next_unlock_candidates.append(
            {
                "kind": "animal",
                "label": f"{best_income_option['animal']}{best_income_option['rarity']}",
                "target_usd": int(best_income_option["price_usd"]),
                "eta_seconds": best_income_option.get("eta_seconds"),
            }
        )
    if cheapest_aviary:
        next_unlock_candidates.append(
            {
                "kind": "aviary",
                "label": cheapest_aviary["code_name"],
                "target_usd": int(cheapest_aviary["price_usd"]),
                "eta_seconds": cheapest_aviary.get("eta_seconds"),
            }
        )
    if create_item_price > 0:
        next_unlock_candidates.append(
            {
                "kind": "item",
                "label": "create_item",
                "target_usd": create_item_price,
                "eta_seconds": create_item_eta,
            }
        )
    next_unlock_candidates = [
        row for row in next_unlock_candidates if row.get("eta_seconds") is not None
    ]
    next_unlock_candidates.sort(
        key=lambda row: (
            int(row.get("eta_seconds", 10**9)),
            int(row.get("target_usd", 0)),
        )
    )

    standings = observation["standings"]["self"]
    unity_current = observation.get("unity", {}).get("current") or {}
    pending_requests = unity_current.get("pending_requests", []) or []
    memory_relationships = observation.get("memory", {}).get("relationships", []) or []

    nemesis_ids = {
        int(r.get("subject_idpk", 0))
        for r in memory_relationships
        if int(r.get("trust", 500)) < 300
    }
    favorite_ids = {
        int(r.get("subject_idpk", 0))
        for r in memory_relationships
        if int(r.get("trust", 500)) > 700
    }

    social_target = None
    if pending_requests:
        first_req = pending_requests[0]
        req_id = int(first_req.get("idpk_user", 0))
        social_target = {
            "mode": "review_request",
            "target": first_req,
            "is_nemesis": req_id in nemesis_ids,
            "is_favorite": req_id in favorite_ids,
        }
    elif observation.get("unity", {}).get("recruit_targets") or []:
        for target in observation["unity"]["recruit_targets"]:
            tid = int(target.get("idpk", 0))
            if tid not in nemesis_ids:
                social_target = {
                    "mode": "recruit",
                    "target": target,
                    "is_nemesis": False,
                    "is_favorite": tid in favorite_ids,
                }
                break
        if not social_target:
            social_target = {
                "mode": "recruit",
                "target": observation["unity"]["recruit_targets"][0],
                "is_nemesis": True,
                "is_favorite": False,
            }
    elif observation.get("unity", {}).get("candidates") or []:
        for target in observation["unity"]["candidates"]:
            tid = int(target.get("owner_idpk", 0))
            if tid not in nemesis_ids:
                social_target = {
                    "mode": "join",
                    "target": target,
                    "is_nemesis": False,
                    "is_favorite": tid in favorite_ids,
                }
                break
        if not social_target:
            social_target = {
                "mode": "join",
                "target": observation["unity"]["candidates"][0],
                "is_nemesis": True,
                "is_favorite": False,
            }

    return {
        "summary": {
            "need_seats": remain_seats <= 0,
            "has_bonus": observation["player"]["daily_bonus_available"] > 0,
            "best_income_option": best_income_option,
            "best_aviary_option": cheapest_aviary,
            "top_income_options": income_options[: settings.top_candidates_limit],
            "top_rivals": build_rival_pressure(observation=observation),
            "next_unlock": next_unlock_candidates[0]
            if next_unlock_candidates
            else None,
            "social_target": social_target,
            "pending_social_actions": len(pending_requests),
            "can_create_item_now": bool(
                usd >= create_item_price
                or int(observation.get("player", {}).get("paw_coins", 0) or 0)
                >= CREATE_ITEM_PAW_PRICE
            ),
            "income_rank": standings.get("income_rank"),
            "money_rank": standings.get("money_rank"),
            "animals_rank": standings.get("animals_rank"),
            "referrals_rank": standings.get("referrals_rank"),
        }
    }


def _derive_cycle_goal(observation: dict[str, Any]) -> str:
    summary = observation.get("strategy_signals", {}).get("summary", {}) or {}
    player = observation.get("player", {}) or {}
    unity = observation.get("unity", {}) or {}

    if int(player.get("daily_bonus_available", 0) or 0) > 0:
        return "liquidity"
    if int((unity.get("current") or {}).get("pending_requests_count", 0) or 0) > 0:
        return "unity"
    if bool(summary.get("need_seats")):
        return "income"
    social_target = summary.get("social_target") or {}
    if social_target.get("mode") in {"join", "recruit", "review_request"}:
        return "unity"
    top_income = summary.get("top_income_options") or []
    if top_income:
        return "income"
    return "liquidity"


def _goal_weight_for_action(action_name: str, cycle_goal: str) -> float:
    goal_map: dict[str, set[str]] = {
        "income": {
            "buy_aviary",
            "buy_rarity_animal",
            "invest_for_income",
            "exchange_bank",
            "create_item",
            "optimize_items",
            "claim_daily_bonus",
            "claim_chat_transfer",
        },
        "animals": {
            "buy_rarity_animal",
            "buy_merchant_discount_offer",
            "buy_merchant_random_offer",
            "buy_merchant_targeted_offer",
            "invest_for_top_animals",
        },
        "unity": {
            "review_unity_request",
            "join_best_unity",
            "recruit_top_player",
            "upgrade_unity_level",
            "create_unity",
            "exit_from_unity",
        },
        "liquidity": {
            "claim_daily_bonus",
            "exchange_bank",
            "claim_chat_transfer",
            "wait",
        },
    }
    if action_name in goal_map.get(cycle_goal, set()):
        return 1.15
    if action_name == "wait":
        return 0.9
    return 0.82


def _estimate_action_ev(
    action_entry: dict[str, Any], observation: dict[str, Any]
) -> dict[str, Any]:
    action_name = str(action_entry.get("action", "wait"))
    params = action_entry.get("params", {}) or {}
    summary = observation.get("strategy_signals", {}).get("summary", {}) or {}
    best_income = summary.get("best_income_option") or {}

    ev_score = 0.0
    payback_minutes: float | None = None
    fail_risk = 0.1

    if action_name == "buy_rarity_animal":
        variant = _find_animal_variant(
            observation,
            str(params.get("animal", "")),
            str(params.get("rarity", "")),
        )
        if variant:
            payback = variant.get("payback_minutes")
            payback_minutes = float(payback) if payback is not None else None
            income = float(int(variant.get("income_rub", 0) or 0))
            price = float(max(1, int(variant.get("price_usd", 0) or 1)))
            ev_score = (income / price) * 100.0
            fail_risk = (
                0.05 if int(variant.get("affordable_quantity", 0) or 0) > 0 else 0.85
            )
    elif action_name == "buy_aviary":
        option = _find_aviary_option(
            observation, str(params.get("code_name_aviary", ""))
        )
        if option:
            seats = float(max(1, int(option.get("size", 1) or 1)))
            price = float(max(1, int(option.get("price_usd", 0) or 1)))
            ev_score = (seats / price) * 160.0
            fail_risk = (
                0.1 if int(option.get("affordable_quantity", 0) or 0) > 0 else 0.8
            )
    elif action_name == "exchange_bank":
        bank = observation.get("bank", {}) or {}
        rate = float(int(bank.get("rate_rub_usd", 0) or 0))
        min_rate = float(int(bank.get("min_rate_rub_usd", 0) or 0))
        max_rate = float(int(bank.get("max_rate_rub_usd", 0) or 0))
        if max_rate > min_rate and rate > 0:
            # Lower rate is better for RUB->USD exchange.
            cheapness = (max_rate - rate) / (max_rate - min_rate)
            cheapness = max(0.0, min(1.0, cheapness))
            ev_score = 22.0 + cheapness * 28.0
        else:
            ev_score = 28.0
        if bool(summary.get("next_unlock")):
            ev_score += 6.0
        fail_risk = 0.15
    elif action_name in {
        "claim_daily_bonus",
        "claim_chat_transfer",
        "review_unity_request",
    }:
        ev_score = 72.0
        fail_risk = 0.03
    elif action_name in {
        "recruit_top_player",
        "join_best_unity",
        "upgrade_unity_level",
    }:
        ev_score = 52.0
        fail_risk = 0.3
    elif action_name == "invest_for_income":
        ev_score = 64.0 if best_income else 35.0
        fail_risk = 0.22
    elif action_name == "wait":
        ev_score = 8.0
        fail_risk = 0.0
    else:
        ev_score = 34.0
        fail_risk = 0.35

    return {
        "ev_score": round(float(ev_score), 2),
        "payback_minutes": None
        if payback_minutes is None
        else round(payback_minutes, 2),
        "fail_risk": round(float(fail_risk), 2),
    }


def _build_phase_a_candidates(
    observation: dict[str, Any], cycle_goal: str
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for entry in observation.get("allowed_actions", []) or []:
        if not isinstance(entry, dict):
            continue
        action_name = str(entry.get("action", "wait")).strip() or "wait"
        base_score, note = _score_allowed_action(entry, observation)
        ev = _estimate_action_ev(entry, observation)
        goal_weight = _goal_weight_for_action(action_name, cycle_goal)
        combined = (
            float(base_score) * goal_weight
            + float(ev.get("ev_score", 0.0)) * 0.35
            - float(ev.get("fail_risk", 0.0)) * 18.0
        )
        if action_name != "wait" and combined < 42.0:
            continue
        candidates.append(
            {
                "action": action_name,
                "params": entry.get("params", {}) or {},
                "base_score": int(base_score),
                "goal_weight": round(goal_weight, 2),
                "ev": ev,
                "combined_score": round(combined, 2),
                "note": note,
            }
        )

    candidates.sort(
        key=lambda row: (
            float(row.get("combined_score", 0.0) or 0.0),
            float(((row.get("ev") or {}).get("ev_score", 0.0) or 0.0)),
            str(row.get("action", "")),
        ),
        reverse=True,
    )
    return candidates[:3]


def build_npc_plan(observation: dict[str, Any]) -> dict[str, Any]:
    summary = observation.get("strategy_signals", {}).get("summary", {})
    decision_brief = observation.get("decision_brief", {})
    behavior = observation.get("memory", {}).get("behavior_guidance", {})
    active_goals = observation.get("memory", {}).get("active_goals", [])
    cycle_goal = _derive_cycle_goal(observation)
    phase_a_candidates = _build_phase_a_candidates(observation, cycle_goal=cycle_goal)
    recommended_actions: list[dict[str, Any]] = []

    def add_step(
        action: str,
        reason: str,
        params: dict[str, Any] | None = None,
        eta_seconds: int | None = 0,
    ) -> None:
        action = str(action).strip()
        if not action:
            return
        if any(step["action"] == action for step in recommended_actions):
            return
        recommended_actions.append(
            {
                "action": action,
                "params": params or {},
                "reason": semantic_preview(
                    reason,
                    max_segments=2,
                    max_words=24,
                    max_chars=180,
                    placeholder="...",
                ),
                "eta_seconds": eta_seconds,
            }
        )

    if summary.get("need_seats") and summary.get("best_aviary_option"):
        aviary = summary["best_aviary_option"]
        add_step(
            "buy_aviary",
            "Seat pressure is blocking all animal growth; unlock capacity first.",
            params={"code_name_aviary": aviary["code_name"], "quantity": 1},
            eta_seconds=aviary.get("eta_seconds"),
        )
        if int(observation.get("player", {}).get("rub", 0) or 0) >= int(
            observation.get("bank", {}).get("rate_rub_usd", 1) or 1
        ):
            add_step(
                "exchange_bank",
                "Convert idle RUB to accelerate the first capacity unlock.",
                params={"mode": "all"},
                eta_seconds=0,
            )

    for row in decision_brief.get("top_affordable_actions", [])[:3]:
        add_step(
            str(row.get("action", "wait")),
            str(row.get("note", "High-value affordable action.")),
            params=row.get("params", {}) or {},
            eta_seconds=0,
        )

    social_target = summary.get("social_target") or {}
    if summary.get("pending_social_actions"):
        target = social_target.get("target") or {}
        add_step(
            "review_unity_request",
            "A pending social decision is live right now and can change clan strength immediately.",
            params={
                "idpk_user": int(target.get("idpk_user", 0) or 0),
                "decision": "accept",
            },
            eta_seconds=0,
        )
    if summary.get("has_bonus"):
        add_step(
            "claim_daily_bonus",
            "Free value is on the table; collect it before planning expensive lines.",
            eta_seconds=0,
        )
    if summary.get("need_seats") and summary.get("best_aviary_option"):
        aviary = summary["best_aviary_option"]
        add_step(
            "buy_aviary",
            "Seat pressure is blocking the next profitable animal purchase.",
            params={"code_name_aviary": aviary["code_name"], "quantity": 1},
            eta_seconds=aviary.get("eta_seconds"),
        )
    if (
        summary.get("best_income_option")
        and int(observation.get("zoo", {}).get("remain_seats", 0) or 0) > 0
    ):
        option = summary["best_income_option"]
        add_step(
            "buy_rarity_animal",
            "Best ROI animal line is the clearest compounding upgrade.",
            params={
                "animal": option["animal"],
                "rarity": option["rarity"],
                "quantity": 1,
            },
            eta_seconds=option.get("eta_seconds"),
        )
    if int(observation.get("player", {}).get("rub", 0) or 0) >= int(
        observation.get("bank", {}).get("rate_rub_usd", 1) or 1
    ):
        add_step(
            "exchange_bank",
            "Bank RUB can accelerate the next unlock instead of idling in cash drag.",
            params={"mode": "all"},
            eta_seconds=0,
        )
    if summary.get("can_create_item_now"):
        add_step(
            "create_item",
            "Item engine is live and can improve passive modifiers right now.",
            eta_seconds=0,
        )
    if social_target.get("mode") == "join":
        target = social_target.get("target") or {}
        add_step(
            "join_best_unity",
            "A stronger social shell can compound faster than solo grind.",
            params={"owner_idpk": int(target.get("owner_idpk", 0) or 0)},
            eta_seconds=0,
        )
    elif social_target.get("mode") == "recruit":
        target = social_target.get("target") or {}
        add_step(
            "recruit_top_player",
            "Best recruit target is available and social leverage is part of the plan.",
            params={"idpk_user": int(target.get("idpk", 0) or 0)},
            eta_seconds=0,
        )

    next_unlock = summary.get("next_unlock") or {}
    primary_goal = active_goals[0].get("title") if active_goals else None
    phase = "compound_income"
    if summary.get("pending_social_actions"):
        phase = "social_response"
    elif summary.get("need_seats"):
        phase = "capacity_repair"
    elif social_target.get("mode") in {"join", "recruit"}:
        phase = "social_positioning"
    elif summary.get("has_bonus"):
        phase = "free_value_capture"
    elif summary.get("can_create_item_now") and "create_item" in (
        behavior.get("suggested_actions") or []
    ):
        phase = "item_engine"

    return {
        "phase": phase,
        "cycle_goal": cycle_goal,
        "primary_goal": primary_goal,
        "next_unlock": next_unlock,
        "capacity_unlock_mode": bool(summary.get("need_seats")),
        "recommended_actions": recommended_actions[:5],
        "phase_a_candidates": phase_a_candidates,
        "phase_b_policy": {
            "prefer_phase_a": True,
            "min_combined_score": 42,
        },
    }


def find_cheapest_affordable_animal(observation: dict[str, Any]) -> str | None:
    best_animal = None
    best_price = None
    for animal in observation["animal_market"]:
        variants = animal.get("variants", [])
        if not variants:
            continue
        price = min(variant["price_usd"] for variant in variants)
        if best_price is None or price < best_price:
            best_price = price
            best_animal = animal["animal"]
    return best_animal


async def generate_npc_unity_name(session: AsyncSession, user: User) -> str:
    limit_length_max = await get_value(
        session=session, value_name="NAME_UNITY_LENGTH_MAX"
    )
    base_name = f"{settings.npc_unity_prefix} {user.nickname}".strip()
    base_name = sanitize_unity_name(base_name, int(limit_length_max))
    if not base_name:
        base_name = sanitize_unity_name(
            settings.npc_unity_prefix, int(limit_length_max)
        )
    candidate = base_name
    suffix = 1
    while await session.scalar(select(Unity).where(Unity.name == candidate)):
        suffix += 1
        candidate = f"{base_name[: max(1, int(limit_length_max) - len(str(suffix)) - 1)]}-{suffix}"
    return candidate


def sanitize_unity_name(name: str, max_length: int) -> str:
    clean_name = " ".join(name.replace("\n", " ").replace("\r", " ").split())
    clean_name = clean_name.strip("\"'` ")
    return clean_name[:max_length].strip()


async def generate_npc_unity_name_via_llm(
    session: AsyncSession,
    user: User,
    observation: dict[str, Any],
    client: NpcDecisionClient | None,
) -> str:
    limit_length_max = int(
        await get_value(session=session, value_name="NAME_UNITY_LENGTH_MAX")
    )
    if client:
        try:
            generated_name = await client.generate_unity_name(
                context={
                    "npc_nickname": user.nickname,
                    "money_rank": observation.get("standings", {})
                    .get("self", {})
                    .get("money_rank"),
                    "income_rank": observation.get("standings", {})
                    .get("self", {})
                    .get("income_rank"),
                    "top_animals": observation.get("standings", {}).get(
                        "top_animals", []
                    ),
                    "theme": "zoo economy AI clan",
                    "max_length": limit_length_max,
                }
            )
            generated_name = sanitize_unity_name(generated_name, limit_length_max)
            if generated_name:
                candidate = generated_name
                suffix = 1
                while await session.scalar(
                    select(Unity).where(Unity.name == candidate)
                ):
                    suffix += 1
                    candidate = f"{generated_name[: max(1, limit_length_max - len(str(suffix)) - 1)]}-{suffix}"
                return candidate
        except Exception:
            pass
    return await generate_npc_unity_name(session=session, user=user)


async def build_observation(
    session: AsyncSession,
    user: User,
    wake_context: dict[str, Any] | None = None,
    execution_feedback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    merchant = await ensure_random_merchant_for_user(session=session, user=user)
    rate = await get_rate(session=session, user=user)
    current_income = await income_(session=session, user=user)
    animals_state = await get_user_animals_map(session=session, user=user)
    aviaries_state = await get_user_aviaries_map(session=session, user=user)
    total_seats = await get_total_number_seats(session=session, aviaries=aviaries_state)
    remain_seats = await get_remain_seats(session=session, user=user)
    standings = await build_standings(session=session, user=user)
    unity = await build_unity_state(session=session, user=user)
    chat_games = await build_chat_games_state(session=session, user=user)
    chat_transfers = await build_chat_transfers_state(session=session, user=user)
    item_opportunities = await build_item_opportunities(session=session, user=user)
    animal_market = await build_animal_market(
        session=session,
        user=user,
        remain_seats=remain_seats,
        rate_rub_usd=int(rate),
        income_per_minute_rub=int(current_income),
    )
    aviary_market = await build_aviary_market(session=session, user=user)
    items = await build_item_state(session=session, user=user)
    momentum = await build_momentum_signal(
        session=session,
        user=user,
        current_income=int(current_income),
    )
    bank_storage = await get_value(
        session=session,
        value_name="BANK_STORAGE",
        value_type="str",
        cache_=False,
    )
    bank_percent_fee = await get_value(
        session=session,
        value_name="BANK_PERCENT_FEE",
    )
    min_rate_rub_usd = await get_value(
        session=session,
        value_name="MIN_RATE_RUB_USD",
    )
    max_rate_rub_usd = await get_value(
        session=session,
        value_name="MAX_RATE_RUB_USD",
    )
    rate_history_raw = await get_value(
        session=session,
        value_name="RATE_RUB_USD_HISTORY_JSON",
        value_type="str",
        cache_=False,
    )
    rate_history = _build_rate_history_snapshot(rate_history_raw)
    clan_project_summary = {}
    with contextlib.suppress(Exception):
        unity_idpk = int(get_unity_idpk(user.current_unity) or 0)
        if unity_idpk:
            unity_obj = await session.get(Unity, unity_idpk)
            if unity_obj is not None:
                project = await get_or_create_project(session=session, unity=unity_obj)
                pr = project.get("progress", {}) or {}
                tg = project.get("target", {}) or {}
                reward_preview = get_project_reward_preview(project)
                clan_project_summary = {
                    "name": str(project.get("name", "Заповедник")),
                    "status": str(project.get("status", "active")),
                    "level": int(project.get("level", 1) or 1),
                    "member_count": int(project.get("member_count", 1) or 1),
                    "ends_at": str(project.get("ends_at", "")),
                    "progress_rub": int(pr.get("rub", 0) or 0),
                    "target_rub": int(tg.get("rub", 0) or 0),
                    "progress_usd": int(pr.get("usd", 0) or 0),
                    "target_usd": int(tg.get("usd", 0) or 0),
                    "reward_success": reward_preview.get("success", {}),
                    "reward_current": reward_preview.get("current", {}),
                    "mvp_epic_bonus": int(reward_preview.get("mvp_epic_bonus", 1) or 1),
                }

    observation = {
        "schema_version": 5,
        "current_time": datetime.now().isoformat(),
        "wake_context": {
            "source": (wake_context or {}).get("source", "scheduled"),
            "reason": (wake_context or {}).get("reason", "planned_wake"),
            "scheduled_at": (wake_context or {}).get("scheduled_at"),
            "constraints": {
                "min_sleep_seconds": settings.min_sleep_seconds,
                "max_sleep_seconds": settings.max_sleep_seconds,
                "default_sleep_seconds": settings.step_seconds,
            },
        },
        "execution_feedback": execution_feedback or {},
        "player": {
            "idpk": user.idpk,
            "id_user": user.id_user,
            "nickname": user.nickname,
            "usd": int(user.usd),
            "rub": int(user.rub),
            "paw_coins": int(user.paw_coins),
            "income_per_minute_rub": int(current_income),
            "moves_logged": int(user.moves),
            "daily_bonus_available": int(user.bonus),
            "bonus_reroll_attempts": int(
                get_value_prop_from_iai(
                    info_about_items=user.info_about_items,
                    name_prop="bonus_changer",
                )
                or 0
            ),
            "amount_expenses_usd": int(user.amount_expenses_usd),
            "current_unity": user.current_unity,
            "unity_idpk": get_unity_idpk(user.current_unity),
        },
        "zoo": {
            "animals": animals_state,
            "aviaries": aviaries_state,
            "total_seats": int(total_seats),
            "remain_seats": int(remain_seats),
        },
        "bank": {
            "rate_rub_usd": int(rate),
            "min_rate_rub_usd": int(min_rate_rub_usd),
            "max_rate_rub_usd": int(max_rate_rub_usd),
            "percent_fee": int(bank_percent_fee),
            "bank_storage": str(bank_storage),
            "rate_history_1h": rate_history.get("points_1h", []),
            "rate_history_1h_summary": rate_history.get("summary_1h", {}),
        },
        "merchant": {
            "name": merchant.name,
            "first_offer_bought": merchant.first_offer_bought,
            "code_name_animal": merchant.code_name_animal,
            "quantity_animals": int(merchant.quantity_animals),
            "discount": int(merchant.discount),
            "price_with_discount": int(merchant.price_with_discount),
            "random_offer_price": int(merchant.price),
        },
        "items": items,
        "item_opportunities": item_opportunities,
        "unity": unity,
        "clan_project": clan_project_summary,
        "chat_games": chat_games,
        "chat_transfers": chat_transfers,
        "standings": standings,
        "animal_market": animal_market,
        "aviary_market": aviary_market,
        "momentum": momentum,
    }
    observation["allowed_actions"] = await build_allowed_actions(
        session=session,
        user=user,
        observation=observation,
    )
    observation["strategy_signals"] = build_strategy_signals(observation=observation)
    observation["decision_brief"] = build_decision_brief(observation=observation)
    observation["memory"] = await build_npc_memory_context(
        session=session,
        user=user,
        observation=observation,
    )
    observation["planner"] = build_npc_plan(observation=observation)
    observation["action_contract"] = build_action_contract(observation=observation)
    observation["strategy_signals"]["goal_focus"] = [
        goal.get("topic") for goal in observation["memory"].get("active_goals", [])
    ][: settings.memory_goal_limit]
    observation["player"]["current_mood"] = (
        observation["memory"].get("profile", {}).get("current_mood", "neutral")
    )
    observation["player"]["affinity_score"] = (
        observation["memory"].get("profile", {}).get("affinity_score", 50)
    )
    return observation


async def build_animal_market(
    session: AsyncSession,
    user: User,
    remain_seats: int,
    rate_rub_usd: int,
    income_per_minute_rub: int,
) -> list[dict[str, Any]]:
    animals = await get_all_animals(session=session)
    unity_idpk = int(get_unity_idpk(user.current_unity) or 0) or None
    animals_state = await get_user_animals_map(session=session, user=user)
    market = []
    for animal in animals[: settings.max_observation_animals]:
        base_code = animal.code_name.strip("-")
        variants = []
        for rarity in ["_rare", "_epic", "_mythical", "_leg"]:
            code_name = f"{base_code}{rarity}"
            animal_variant = await session.scalar(
                select(Animal).where(Animal.code_name == code_name)
            )
            if not animal_variant:
                continue
            price = await get_price_animal(
                session=session,
                animal_code_name=code_name,
                unity_idpk=unity_idpk,
                info_about_items=user.info_about_items,
            )
            income_value = await income_for_animal_option(
                session=session,
                user=user,
                animal_obj=animal_variant,
                unity_idpk=unity_idpk,
            )
            affordable_quantity = min(
                remain_seats,
                int(user.usd) // price if price else 0,
            )
            variant_payload = {
                "rarity": rarity,
                "code_name": code_name,
                "price_usd": int(price),
                "income_rub": int(income_value),
                "payback_minutes": round(price / income_value, 2)
                if income_value
                else None,
                "owned": int(animals_state.get(code_name, 0)),
                "affordable_quantity": int(max(0, affordable_quantity)),
                "eta_seconds": estimate_usd_eta_seconds(
                    usd=int(user.usd),
                    rub=int(user.rub),
                    rate_rub_usd=rate_rub_usd,
                    income_per_minute_rub=income_per_minute_rub,
                    target_usd=int(price),
                ),
            }
            if int(variant_payload.get("affordable_quantity", 0) or 0) > 0 or (
                variant_payload.get("eta_seconds") is not None
                and int(variant_payload.get("eta_seconds", 0) or 0) <= 7200
            ):
                variants.append(variant_payload)
        if variants:
            market.append(
                {
                    "animal": base_code,
                    "variants": variants,
                }
            )
    return market


async def income_for_animal_option(
    session: AsyncSession,
    user: User,
    animal_obj: Animal,
    unity_idpk: int | None,
) -> int:
    from tools.animals import get_income_animal

    return await get_income_animal(
        session=session,
        animal=animal_obj,
        unity_idpk=unity_idpk,
        info_about_items=user.info_about_items,
    )


async def build_aviary_market(
    session: AsyncSession, user: User
) -> list[dict[str, Any]]:
    aviaries = await session.scalars(select(Aviary))
    aviaries_state = await get_user_aviaries_map(session=session, user=user)
    market = []
    for aviary in aviaries.all():
        price = await get_price_aviaries(
            session=session,
            aviaries=aviaries_state,
            code_name_aviary=aviary.code_name,
            info_about_items=user.info_about_items,
        )
        market.append(
            {
                "code_name": aviary.code_name,
                "name": aviary.name,
                "size": int(aviary.size),
                "price_usd": int(price),
                "affordable_quantity": int(int(user.usd) // price if price else 0),
            }
        )
    return market


async def build_item_state(session: AsyncSession, user: User) -> dict[str, Any]:
    items = await session.scalars(select(Item).where(Item.id_user == user.id_user))
    items = list(items.all())
    create_price = await gen_price_to_create_item(session=session, id_user=user.id_user)
    return {
        "create_price_usd": int(create_price),
        "create_price_paw": CREATE_ITEM_PAW_PRICE,
        "can_afford_create_with_paw": int(user.paw_coins) >= CREATE_ITEM_PAW_PRICE,
        "owned_count": len(items),
        "active_count": len([item for item in items if item.is_active]),
        "items": [
            {
                "id_item": item.id_item,
                "name": item.name,
                "emoji": item.emoji,
                "lvl": int(item.lvl),
                "rarity": item.rarity,
                "is_active": item.is_active,
                "properties": json.loads(item.properties),
            }
            for item in items
        ],
    }


def validate_action(decision: dict[str, Any]) -> dict[str, Any]:
    action = str(decision.get("action", "wait")).strip() or "wait"
    params = decision.get("params")
    if not isinstance(params, dict):
        params = {}

    reason = semantic_preview(
        decision.get("reason", ""),
        max_segments=3,
        max_words=40,
        max_chars=300,
        placeholder="...",
    )
    sleep_seconds = decision.get("sleep_seconds")

    # If model quota is exhausted (HTTP 403), force a long cooldown fallback.
    if action == "wait" and (
        "llm_error:http_403" in reason or "access_terminated_error" in reason
    ):
        sleep_seconds = 4 * 60 * 60

    return {
        "action": action,
        "params": params,
        "reason": reason,
        "sleep_seconds": sleep_seconds,
    }


def _matches_allowed_params(
    proposed_params: dict[str, Any], allowed_params: dict[str, Any]
) -> bool:
    if not allowed_params:
        return True
    for key, allowed_value in allowed_params.items():
        if key not in proposed_params:
            return False
        proposed_value = proposed_params.get(key)
        if isinstance(allowed_value, str):
            if allowed_value.startswith("<") and allowed_value.endswith(">"):
                continue
            if allowed_value in {"optional_string", "string"}:
                continue
            if "|" in allowed_value:
                if str(proposed_value) not in allowed_value.split("|"):
                    return False
                continue
        if proposed_value != allowed_value:
            return False
    return True


def build_action_contract(observation: dict[str, Any]) -> dict[str, Any]:
    summary = observation.get("strategy_signals", {}).get("summary", {}) or {}
    remain_seats = int(observation.get("zoo", {}).get("remain_seats", 0) or 0)
    must_do: list[str] = []
    must_not_do: list[str] = []

    if remain_seats <= 0 or bool(summary.get("need_seats")):
        must_not_do.extend(
            [
                "buy_rarity_animal",
                "invest_for_top_animals",
                "buy_merchant_discount_offer",
                "buy_merchant_random_offer",
                "buy_merchant_targeted_offer",
            ]
        )

    return {
        "must_do": list(dict.fromkeys(must_do)),
        "must_not_do": list(dict.fromkeys(must_not_do)),
        "blocked_until": {},
        "hard_constraints": {
            "remain_seats": remain_seats,
            "need_seats": bool(summary.get("need_seats")),
        },
    }


def should_stop_npc_cycle(action: dict[str, Any], result: dict[str, Any]) -> bool:
    if action["action"] == "wait":
        return True

    status = str(result.get("status", "")).strip().lower()
    # Keep the cycle running on "skipped" so the model can re-think with
    # execution_feedback and choose another action in the same wake.
    if status in {"error", "failed"}:
        return True
    return False


def safe_int(value: Any, default: int = 0, min_value: int | None = None) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError):
        result = default
    if min_value is not None:
        result = max(min_value, result)
    return result


def compute_smart_sleep_seconds(
    observation: dict[str, Any] | None,
    wake_trigger: dict[str, Any],
    action: dict[str, Any] | None,
    result: dict[str, Any] | None,
    default_sleep: int,
) -> int:
    smart_sleep = int(default_sleep)
    status = str((result or {}).get("status", "")).strip().lower()
    if status == "error":
        return min(
            settings.max_sleep_seconds,
            max(settings.step_seconds, settings.min_sleep_seconds * 3),
        )
    if status in {"failed", "skipped"}:
        smart_sleep = min(
            settings.max_sleep_seconds, max(settings.step_seconds, smart_sleep * 2)
        )
    if wake_trigger.get("source") == "event":
        smart_sleep = min(smart_sleep, settings.step_seconds)
    if not observation:
        return clamp_npc_sleep_seconds(smart_sleep)

    summary = observation.get("strategy_signals", {}).get("summary", {})
    planner = observation.get("planner", {})
    player = observation.get("player", {})
    unity_current = observation.get("unity", {}).get("current") or {}

    if int(player.get("daily_bonus_available", 0) or 0) > 0:
        smart_sleep = min(smart_sleep, settings.step_seconds)
    if int(unity_current.get("pending_requests_count", 0) or 0) > 0:
        smart_sleep = min(smart_sleep, settings.step_seconds)

    next_unlock = planner.get("next_unlock") or summary.get("next_unlock") or {}
    eta_seconds = next_unlock.get("eta_seconds")
    if eta_seconds is not None:
        eta_seconds = int(eta_seconds)
        if eta_seconds <= 0:
            smart_sleep = settings.min_sleep_seconds
        elif eta_seconds <= 15 * 60:
            smart_sleep = min(smart_sleep, max(settings.min_sleep_seconds, eta_seconds))
        elif eta_seconds <= 45 * 60:
            smart_sleep = min(smart_sleep, max(settings.step_seconds, eta_seconds // 2))

    if action and action.get("action") == "wait" and summary.get("best_income_option"):
        best_eta = summary["best_income_option"].get("eta_seconds")
        if best_eta is not None and int(best_eta) <= 20 * 60:
            smart_sleep = min(
                smart_sleep, max(settings.min_sleep_seconds, int(best_eta))
            )

    phase_a = (
        (planner.get("phase_a_candidates") or []) if isinstance(planner, dict) else []
    )
    if phase_a:
        top = phase_a[0] if isinstance(phase_a[0], dict) else {}
        top_score = float(top.get("combined_score", 0.0) or 0.0)
        top_ev = float(((top.get("ev") or {}).get("ev_score", 0.0) or 0.0))
        if top_score >= 80 or top_ev >= 60:
            smart_sleep = min(smart_sleep, settings.min_sleep_seconds)
        elif top_score >= 60:
            smart_sleep = min(smart_sleep, settings.step_seconds)

    if status in {"failed", "skipped"}:
        smart_sleep = max(settings.step_seconds, int(smart_sleep))

    return clamp_npc_sleep_seconds(smart_sleep)


async def get_user_item(
    session: AsyncSession,
    user: User,
    id_item: str,
) -> Item | None:
    if not id_item:
        return None
    return await session.scalar(
        select(Item).where(Item.id_item == id_item, Item.id_user == user.id_user)
    )


async def get_active_user_items(session: AsyncSession, user: User) -> list[Item]:
    items = await session.scalars(
        select(Item).where(Item.id_user == user.id_user, Item.is_active == True)  # noqa: E712
    )
    return list(items.all())


async def optimize_items_for_user(session: AsyncSession, user: User) -> str:
    items = await session.scalars(select(Item).where(Item.id_user == user.id_user))
    items = list(items.all())
    if not items:
        user.info_about_items = "{}"
        return "no_items"

    ranked_items = sorted(items, key=item_score, reverse=True)
    active_ids = {item.id_item for item in ranked_items[:3]}
    active_items = []
    for item in items:
        item.is_active = item.id_item in active_ids
        if item.is_active:
            active_items.append(item)
    user.info_about_items = await synchronize_info_about_items(items=active_items)
    return ",".join(sorted(active_ids))


def item_score(item: Item) -> float:
    props = json.loads(item.properties)
    weights = {
        "general_income": 5.0,
        "animal_income": 4.0,
        "animal_sale": 3.5,
        "aviaries_sale": 3.0,
        "exchange_bank": 2.5,
        "bonus_changer": 1.2,
        "extra_moves": 0.8,
        "last_chance": 0.5,
    }
    score = float(item.lvl) * 0.5
    for key, value in props.items():
        clean_key = key.split(":")[-1]
        score += float(value) * weights.get(clean_key, 1.0)
    return score


async def register_npc_move(
    session: AsyncSession,
    user: User,
    action: dict[str, Any],
    result: dict[str, Any],
    wake_trigger: dict[str, Any] | None = None,
) -> None:
    current_income = int(await income_(session=session, user=user))
    total_animals = int(await get_total_number_animals(self=user, session=session))
    payload = {
        "npc": user.nickname,
        "action": action["action"],
        "params": action["params"],
        "reason": action.get("reason", ""),
        "sleep_seconds": action.get("sleep_seconds"),
        "wake_source": (wake_trigger or {}).get("source"),
        "wake_reason": (wake_trigger or {}).get("reason"),
        "result": result,
        "after_state": {
            "usd": int(user.usd),
            "rub": int(user.rub),
            "paw_coins": int(user.paw_coins),
            "income_per_minute_rub": current_income,
            "total_animals": total_animals,
            "current_unity": user.current_unity,
        },
    }
    limit_on_write_moves = await get_value(
        session=session,
        value_name="LIMIT_ON_WRITE_MOVES",
    )
    await append_npc_turn_history(
        session=session,
        user=user,
        payload=payload,
        limit=int(limit_on_write_moves),
    )
    user.moves += 1
