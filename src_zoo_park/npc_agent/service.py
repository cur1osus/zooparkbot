import asyncio
import json
from datetime import datetime, timedelta
from itertools import combinations
from typing import Any

from db import Animal, Aviary, Item, RandomMerchant, RequestToUnity, Unity, User
from game_variables import prop_quantity_by_rarity
from init_bot import bot
from init_db import _sessionmaker_for_func
from init_db_redis import redis
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from bot.keyboards import ik_npc_unity_invitation
from tools.animals import (
    add_animal,
    get_all_animals,
    get_price_animal,
    get_total_number_animals,
)
from tools.aviaries import (
    add_aviary,
    get_price_aviaries,
    get_remain_seats,
    get_total_number_seats,
)
from tools.bank import exchange, get_rate
from tools.bonus import apply_bonus, get_bonus
from tools.income import income_
from tools.items import (
    add_item_to_db,
    able_to_enhance,
    calculate_percent_to_enhance,
    create_item,
    gen_price_to_create_item,
    get_value_prop_from_iai,
    merge_items,
    random_up_property_item,
    synchronize_info_about_items,
    update_prop_iai,
)
from tools.random_merchant import create_random_merchant, gen_price
from tools.referrals import get_referrals_count_map
from tools.unity import (
    check_condition_1st_lvl,
    check_condition_2nd_lvl,
    check_condition_3rd_lvl,
    count_income_unity,
    get_unity_idpk,
)
from tools.value import get_value

from .client import NpcDecisionClient
from .logs import log_npc_decision
from .settings import settings

NPC_LOCK = asyncio.Lock()


async def run_npc_players_turn() -> None:
    if not settings.enabled or not settings.api_key:
        return
    if NPC_LOCK.locked():
        return

    async with NPC_LOCK:
        async with _sessionmaker_for_func() as session:
            npc_users = await get_npc_users(session=session)
            if not npc_users:
                npc_users = [await ensure_default_npc_user(session=session)]

            client = NpcDecisionClient(settings=settings)
            for npc_user in npc_users:
                if not await npc_ready_for_step(session=session, user=npc_user):
                    continue
                await ensure_random_merchant_for_user(session=session, user=npc_user)
                for decision_index in range(1, settings.max_actions_per_cycle + 1):
                    observation = await build_observation(
                        session=session, user=npc_user
                    )
                    try:
                        decision = await client.choose_action(observation=observation)
                    except Exception as exc:
                        decision = {
                            "action": "wait",
                            "params": {},
                            "reason": f"llm_error:{str(exc)[:250]}",
                        }
                    action = validate_action(decision=decision)
                    try:
                        result = await execute_action(
                            session=session,
                            user=npc_user,
                            action=action,
                            observation=observation,
                        )
                    except Exception as exc:
                        result = {
                            "status": "error",
                            "summary": f"execution_error:{type(exc).__name__}",
                        }
                    print(
                        "NPC_DECISION",
                        json.dumps(
                            {
                                "time": datetime.now().isoformat(),
                                "npc": npc_user.nickname,
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
                        user=npc_user,
                        action=action,
                        result=result,
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
                                "observation": observation,
                            },
                        )
                    except Exception:
                        pass

                    if should_stop_npc_cycle(action=action, result=result):
                        break


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


async def npc_ready_for_step(session: AsyncSession, user: User) -> bool:
    history = json.loads(user.history_moves)
    if not history:
        return True
    last_event = max(
        history,
        key=lambda value: datetime.strptime(value, "%d.%m.%Y %H:%M:%S.%f"),
    )
    elapsed = datetime.now() - datetime.strptime(last_event, "%d.%m.%Y %H:%M:%S.%f")
    return elapsed.total_seconds() >= settings.step_seconds


async def ensure_random_merchant_for_user(
    session: AsyncSession, user: User
) -> RandomMerchant:
    merchant = await session.scalar(
        select(RandomMerchant).where(RandomMerchant.id_user == user.id_user)
    )
    if merchant:
        return merchant
    return await create_random_merchant(session=session, user=user)


async def build_standings(session: AsyncSession, user: User) -> dict[str, Any]:
    users = await session.scalars(select(User))
    users = list(users.all())
    if not users:
        return {"self": {}, "top_income": [], "top_money": [], "top_animals": []}

    referrals_count = await get_referrals_count_map(
        session=session,
        idpk_users=[member.idpk for member in users],
    )
    incomes = []
    money = []
    animals = []
    referrals = []
    for member in users:
        incomes.append((member, int(await income_(session=session, user=member))))
        money.append((member, int(member.usd)))
        animals.append((member, int(await get_total_number_animals(self=member))))
        referrals.append((member, int(referrals_count.get(member.idpk, 0))))

    def sort_desc(rows: list[tuple[User, int]]) -> list[tuple[User, int]]:
        return sorted(rows, key=lambda row: row[1], reverse=True)

    income_sorted = sort_desc(incomes)
    money_sorted = sort_desc(money)
    animals_sorted = sort_desc(animals)
    referrals_sorted = sort_desc(referrals)
    return {
        "self": {
            "income_rank": get_rank_for_user(income_sorted, user.idpk),
            "money_rank": get_rank_for_user(money_sorted, user.idpk),
            "animals_rank": get_rank_for_user(animals_sorted, user.idpk),
            "referrals_rank": get_rank_for_user(referrals_sorted, user.idpk),
        },
        "top_income": serialize_rank_rows(income_sorted),
        "top_money": serialize_rank_rows(money_sorted),
        "top_animals": serialize_rank_rows(animals_sorted),
        "top_referrals": serialize_rank_rows(referrals_sorted),
    }


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
    users = await session.scalars(select(User))
    candidates = []
    for member in users.all():
        if member.idpk == user.idpk or member.current_unity or member.id_user < 0:
            continue
        candidate_income = int(await income_(session=session, user=member))
        candidate_animals = int(await get_total_number_animals(self=member))
        candidates.append(
            {
                "idpk": int(member.idpk),
                "id_user": int(member.id_user),
                "nickname": member.nickname,
                "income": candidate_income,
                "animals": candidate_animals,
                "usd": int(member.usd),
                "score": candidate_income * 3 + candidate_animals * 2 + int(member.usd),
            }
        )
    candidates.sort(key=lambda row: row["score"], reverse=True)
    return candidates[: settings.top_candidates_limit]


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
                                await get_total_number_animals(self=applicant)
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
            current = {
                "idpk": unity.idpk,
                "name": unity.name,
                "level": int(unity.level),
                "members": int(unity.get_number_members()),
                "owner_idpk": int(unity.idpk_user),
                "income": int(await count_income_unity(session=session, unity=unity)),
                "can_upgrade": await can_upgrade_unity(session=session, unity=unity),
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
                "members": int(unity.get_number_members()),
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


def build_strategy_signals(observation: dict[str, Any]) -> dict[str, Any]:
    remain_seats = observation["zoo"]["remain_seats"]
    best_income_option = None
    for animal in observation["animal_market"]:
        for variant in animal["variants"]:
            if variant["affordable_quantity"] <= 0:
                continue
            if best_income_option is None:
                best_income_option = {
                    "animal": animal["animal"],
                    **variant,
                }
                continue
            if variant["payback_minutes"] is None:
                continue
            current_best = best_income_option.get("payback_minutes")
            if current_best is None or variant["payback_minutes"] < current_best:
                best_income_option = {
                    "animal": animal["animal"],
                    **variant,
                }

    cheapest_aviary = None
    if observation["aviary_market"]:
        cheapest_aviary = min(
            observation["aviary_market"],
            key=lambda row: row["price_usd"] / max(1, row["size"]),
        )

    standings = observation["standings"]["self"]
    return {
        "summary": {
            "need_seats": remain_seats <= 0,
            "has_bonus": observation["player"]["daily_bonus_available"] > 0,
            "best_income_option": best_income_option,
            "best_aviary_option": cheapest_aviary,
            "income_rank": standings.get("income_rank"),
            "money_rank": standings.get("money_rank"),
            "animals_rank": standings.get("animals_rank"),
        }
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
    base_name = f"{settings.npc_unity_prefix} {user.nickname}".strip()
    limit_length_max = await get_value(
        session=session, value_name="NAME_UNITY_LENGTH_MAX"
    )
    base_name = base_name[: int(limit_length_max)].strip()
    if not base_name:
        base_name = settings.npc_unity_prefix[: int(limit_length_max)].strip()
    candidate = base_name
    suffix = 1
    while await session.scalar(select(Unity).where(Unity.name == candidate)):
        suffix += 1
        candidate = f"{base_name[: max(1, int(limit_length_max) - len(str(suffix)) - 1)]}-{suffix}"
    return candidate


async def build_observation(session: AsyncSession, user: User) -> dict[str, Any]:
    merchant = await ensure_random_merchant_for_user(session=session, user=user)
    (
        rate,
        current_income,
        total_seats,
        remain_seats,
        standings,
        unity,
        item_opportunities,
    ) = await asyncio.gather(
        get_rate(session=session, user=user),
        income_(session=session, user=user),
        get_total_number_seats(session=session, aviaries=user.aviaries),
        get_remain_seats(session=session, user=user),
        build_standings(session=session, user=user),
        build_unity_state(session=session, user=user),
        build_item_opportunities(session=session, user=user),
    )
    animal_market = await build_animal_market(
        session=session,
        user=user,
        remain_seats=remain_seats,
    )
    aviary_market = await build_aviary_market(session=session, user=user)
    items = await build_item_state(session=session, user=user)
    bank_storage = await get_value(
        session=session,
        value_name="BANK_STORAGE",
        value_type="str",
        cache_=False,
    )
    observation = {
        "schema_version": 2,
        "current_time": datetime.now().isoformat(),
        "player": {
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
            "animals": json.loads(user.animals),
            "aviaries": json.loads(user.aviaries),
            "total_seats": int(total_seats),
            "remain_seats": int(remain_seats),
        },
        "bank": {
            "rate_rub_usd": int(rate),
            "bank_storage": str(bank_storage),
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
        "standings": standings,
        "animal_market": animal_market,
        "aviary_market": aviary_market,
        "allowed_actions": [
            {"action": "wait", "params": {}},
            {
                "action": "claim_daily_bonus",
                "params": {"rerolls": 0},
            },
            {"action": "invest_for_income", "params": {}},
            {"action": "invest_for_top_animals", "params": {}},
            {
                "action": "exchange_bank",
                "params": {"mode": "all"},
            },
            {
                "action": "exchange_bank",
                "params": {"mode": "amount", "amount": rate},
            },
            {
                "action": "buy_aviary",
                "params": {"code_name_aviary": "<from aviary_market>", "quantity": 1},
            },
            {
                "action": "buy_rarity_animal",
                "params": {
                    "animal": "<from animal_market>",
                    "rarity": "_rare|_epic|_mythical|_leg",
                    "quantity": 1,
                },
            },
            {"action": "buy_merchant_discount_offer", "params": {}},
            {"action": "buy_merchant_random_offer", "params": {}},
            {
                "action": "buy_merchant_targeted_offer",
                "params": {"animal": "<from animal_market>", "quantity": 1},
            },
            {"action": "create_item", "params": {}},
            {"action": "optimize_items", "params": {}},
            {
                "action": "activate_item",
                "params": {"id_item": "<from items.items>"},
            },
            {
                "action": "deactivate_item",
                "params": {"id_item": "<from items.items>"},
            },
            {
                "action": "sell_item",
                "params": {"id_item": "<from items.items>"},
            },
            {
                "action": "upgrade_item",
                "params": {"id_item": "<from item_opportunities.upgrade_candidates>"},
            },
            {
                "action": "merge_items",
                "params": {
                    "id_item_1": "<from item_opportunities.merge_candidates>",
                    "id_item_2": "<from item_opportunities.merge_candidates>",
                },
            },
            {"action": "create_unity", "params": {"name": "optional_string"}},
            {
                "action": "join_best_unity",
                "params": {"owner_idpk": "<from unity.candidates>"},
            },
            {
                "action": "recruit_top_player",
                "params": {"idpk_user": "<from unity.recruit_targets>"},
            },
            {"action": "upgrade_unity_level", "params": {}},
            {
                "action": "review_unity_request",
                "params": {
                    "idpk_user": "<from unity.current.pending_requests>",
                    "decision": "accept|reject",
                },
            },
        ],
    }
    observation["strategy_signals"] = build_strategy_signals(observation=observation)
    return observation


async def build_animal_market(
    session: AsyncSession,
    user: User,
    remain_seats: int,
) -> list[dict[str, Any]]:
    animals = await get_all_animals(session=session)
    unity_idpk = int(get_unity_idpk(user.current_unity) or 0) or None
    animals_state = json.loads(user.animals)
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
            variants.append(
                {
                    "rarity": rarity,
                    "code_name": code_name,
                    "price_usd": int(price),
                    "income_rub": int(income_value),
                    "payback_minutes": round(price / income_value, 2)
                    if income_value
                    else None,
                    "owned": int(animals_state.get(code_name, 0)),
                    "affordable_quantity": int(max(0, affordable_quantity)),
                }
            )
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
    market = []
    for aviary in aviaries.all():
        price = await get_price_aviaries(
            session=session,
            aviaries=user.aviaries,
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
    return {
        "action": action,
        "params": params,
        "reason": str(decision.get("reason", ""))[:300],
    }


def should_stop_npc_cycle(action: dict[str, Any], result: dict[str, Any]) -> bool:
    if action["action"] == "wait":
        return True
    if result.get("status") != "ok":
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


async def execute_action(
    session: AsyncSession,
    user: User,
    action: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    action_name = action["action"]
    params = action["params"]

    handlers = {
        "wait": execute_wait,
        "claim_daily_bonus": execute_claim_daily_bonus,
        "invest_for_income": execute_invest_for_income,
        "invest_for_top_animals": execute_invest_for_top_animals,
        "exchange_bank": execute_exchange_bank,
        "buy_aviary": execute_buy_aviary,
        "buy_rarity_animal": execute_buy_rarity_animal,
        "buy_merchant_discount_offer": execute_buy_merchant_discount_offer,
        "buy_merchant_random_offer": execute_buy_merchant_random_offer,
        "buy_merchant_targeted_offer": execute_buy_merchant_targeted_offer,
        "create_item": execute_create_item,
        "activate_item": execute_activate_item,
        "deactivate_item": execute_deactivate_item,
        "sell_item": execute_sell_item,
        "optimize_items": execute_optimize_items,
        "upgrade_item": execute_upgrade_item,
        "merge_items": execute_merge_items,
        "create_unity": execute_create_unity,
        "join_best_unity": execute_join_best_unity,
        "recruit_top_player": execute_recruit_top_player,
        "upgrade_unity_level": execute_upgrade_unity_level,
        "review_unity_request": execute_review_unity_request,
    }
    handler = handlers.get(action_name, execute_wait)
    return await handler(
        session=session,
        user=user,
        params=params,
        observation=observation,
    )


async def execute_wait(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    return {"status": "ok", "summary": "wait"}


async def execute_claim_daily_bonus(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    if not user.bonus:
        return {"status": "skipped", "summary": "no_bonus"}
    rerolls = safe_int(params.get("rerolls", 0), default=0, min_value=0)
    max_rerolls = int(
        get_value_prop_from_iai(
            info_about_items=user.info_about_items,
            name_prop="bonus_changer",
        )
        or 0
    )
    rerolls = min(rerolls, max_rerolls)
    data_bonus = await get_bonus(session=session, user=user)
    while rerolls > 0:
        data_bonus = await get_bonus(session=session, user=user)
        rerolls -= 1
    user.bonus -= 1
    await apply_bonus(session=session, user=user, data_bonus=data_bonus)
    return {"status": "ok", "summary": f"bonus:{data_bonus.bonus_type}"}


async def execute_invest_for_income(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    signal = observation["strategy_signals"]["summary"]
    if signal["need_seats"] and signal["best_aviary_option"]:
        return await execute_buy_aviary(
            session=session,
            user=user,
            params={
                "code_name_aviary": signal["best_aviary_option"]["code_name"],
                "quantity": 1,
            },
            observation=observation,
        )
    best_income_option = signal.get("best_income_option")
    if best_income_option:
        return await execute_buy_rarity_animal(
            session=session,
            user=user,
            params={
                "animal": best_income_option["animal"],
                "rarity": best_income_option["rarity"],
                "quantity": 1,
            },
            observation=observation,
        )
    if int(user.usd) >= observation["items"]["create_price_usd"]:
        return await execute_create_item(
            session=session,
            user=user,
            params={},
            observation=observation,
        )
    return {"status": "skipped", "summary": "no_income_investment_found"}


async def execute_invest_for_top_animals(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    if observation["merchant"]["first_offer_bought"] is False:
        merchant_quantity = int(observation["merchant"]["quantity_animals"])
        if merchant_quantity > 1:
            result = await execute_buy_merchant_discount_offer(
                session=session,
                user=user,
                params={},
                observation=observation,
            )
            if result["status"] == "ok":
                return result

    if observation["zoo"]["remain_seats"] <= 0:
        cheapest_aviary = observation["strategy_signals"]["summary"].get(
            "best_aviary_option"
        )
        if cheapest_aviary:
            return await execute_buy_aviary(
                session=session,
                user=user,
                params={
                    "code_name_aviary": cheapest_aviary["code_name"],
                    "quantity": 1,
                },
                observation=observation,
            )

    cheapest_target = find_cheapest_affordable_animal(observation=observation)
    if cheapest_target:
        return await execute_buy_merchant_targeted_offer(
            session=session,
            user=user,
            params={
                "animal": cheapest_target,
                "quantity": max(1, observation["zoo"]["remain_seats"]),
            },
            observation=observation,
        )
    return {"status": "skipped", "summary": "no_top_animals_investment_found"}


async def execute_exchange_bank(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    rate = await get_rate(session=session, user=user)
    if int(user.rub) < rate:
        return {"status": "skipped", "summary": "not_enough_rub"}

    mode = params.get("mode", "all")
    if mode == "amount":
        amount = safe_int(params.get("amount", 0), default=0)
        if amount < rate:
            return {"status": "skipped", "summary": "amount_too_small"}
        amount = min(amount, int(user.rub))
        you_change, bank_fee, you_got = await exchange(
            session=session,
            user=user,
            amount=amount,
            rate=rate,
            all=False,
        )
    else:
        you_change, bank_fee, you_got = await exchange(
            session=session,
            user=user,
            amount=int(user.rub),
            rate=rate,
            all=True,
        )
    user.usd += you_got
    return {
        "status": "ok",
        "summary": f"exchange:{you_change}->{you_got}",
        "bank_fee": bank_fee,
    }


async def execute_buy_aviary(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    code_name_aviary = str(params.get("code_name_aviary", "")).strip()
    quantity = safe_int(params.get("quantity", 1), default=1, min_value=1)
    if not code_name_aviary:
        return {"status": "skipped", "summary": "aviary_missing"}

    aviary_price = await get_price_aviaries(
        session=session,
        aviaries=user.aviaries,
        code_name_aviary=code_name_aviary,
        info_about_items=user.info_about_items,
    )
    finite_price = aviary_price * quantity
    if int(user.usd) < finite_price:
        return {"status": "skipped", "summary": "not_enough_usd"}

    user.usd -= finite_price
    user.amount_expenses_usd += finite_price
    await add_aviary(
        session=session,
        self=user,
        code_name_aviary=code_name_aviary,
        quantity=quantity,
    )
    return {
        "status": "ok",
        "summary": f"buy_aviary:{code_name_aviary}x{quantity}",
    }


async def execute_buy_rarity_animal(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    animal = str(params.get("animal", "")).strip()
    rarity = str(params.get("rarity", "")).strip()
    quantity = safe_int(params.get("quantity", 1), default=1, min_value=1)
    if rarity not in {"_rare", "_epic", "_mythical", "_leg"} or not animal:
        return {"status": "skipped", "summary": "bad_animal_params"}

    remain_seats = await get_remain_seats(session=session, user=user)
    if remain_seats < quantity:
        return {"status": "skipped", "summary": "not_enough_seats"}

    unity_idpk = int(get_unity_idpk(user.current_unity) or 0) or None
    code_name = f"{animal}{rarity}"
    animal_price = await get_price_animal(
        session=session,
        animal_code_name=code_name,
        unity_idpk=unity_idpk,
        info_about_items=user.info_about_items,
    )
    finite_price = animal_price * quantity
    if int(user.usd) < finite_price:
        return {"status": "skipped", "summary": "not_enough_usd"}

    user.usd -= finite_price
    user.amount_expenses_usd += finite_price
    await add_animal(self=user, code_name_animal=code_name, quantity=quantity)
    return {"status": "ok", "summary": f"buy_animal:{code_name}x{quantity}"}


async def execute_buy_merchant_discount_offer(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    merchant = await ensure_random_merchant_for_user(session=session, user=user)
    if merchant.first_offer_bought:
        return {"status": "skipped", "summary": "merchant_offer_used"}
    remain_seats = await get_remain_seats(session=session, user=user)
    if remain_seats < merchant.quantity_animals:
        return {"status": "skipped", "summary": "not_enough_seats"}
    if int(user.usd) < merchant.price_with_discount:
        return {"status": "skipped", "summary": "not_enough_usd"}

    user.usd -= merchant.price_with_discount
    user.amount_expenses_usd += merchant.price_with_discount
    await add_animal(
        self=user,
        code_name_animal=merchant.code_name_animal,
        quantity=merchant.quantity_animals,
    )
    merchant.first_offer_bought = True
    return {
        "status": "ok",
        "summary": f"merchant_discount:{merchant.code_name_animal}x{merchant.quantity_animals}",
    }


async def execute_buy_merchant_random_offer(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    from tools.animals import gen_quantity_animals, get_random_animal

    merchant = await ensure_random_merchant_for_user(session=session, user=user)
    max_quantity_animals = await get_value(
        session=session, value_name="MAX_QUANTITY_ANIMALS"
    )
    remain_seats = await get_remain_seats(session=session, user=user)
    if remain_seats < max_quantity_animals:
        return {"status": "skipped", "summary": "not_enough_seats"}
    if int(user.usd) < merchant.price:
        return {"status": "skipped", "summary": "not_enough_usd"}

    user.usd -= merchant.price
    user.amount_expenses_usd += merchant.price
    quantity_animals = await gen_quantity_animals(session=session, user=user)
    rewards = []
    while quantity_animals > 0:
        animal_obj = await get_random_animal(session=session, user_animals=user.animals)
        part_animals = min(quantity_animals, max(1, quantity_animals // 2))
        quantity_animals -= part_animals
        await add_animal(
            self=user,
            code_name_animal=animal_obj.code_name,
            quantity=part_animals,
        )
        rewards.append(f"{animal_obj.code_name}x{part_animals}")
    merchant.price = await gen_price(session=session, animals=user.animals)
    return {"status": "ok", "summary": f"merchant_random:{','.join(rewards)}"}


async def execute_buy_merchant_targeted_offer(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    from tools.animals import get_animal_with_random_rarity

    animal = str(params.get("animal", "")).strip()
    quantity = safe_int(params.get("quantity", 1), default=1, min_value=1)
    if not animal:
        return {"status": "skipped", "summary": "animal_missing"}
    remain_seats = await get_remain_seats(session=session, user=user)
    if remain_seats < quantity:
        return {"status": "skipped", "summary": "not_enough_seats"}
    animal_price = await session.scalar(
        select(Animal.price).where(Animal.code_name == f"{animal}-")
    )
    if not animal_price:
        return {"status": "skipped", "summary": "animal_not_found"}
    finite_price = int(animal_price) * quantity
    if int(user.usd) < finite_price:
        return {"status": "skipped", "summary": "not_enough_usd"}

    user.usd -= finite_price
    user.amount_expenses_usd += finite_price
    rewards = []
    while quantity > 0:
        animal_obj = await get_animal_with_random_rarity(session=session, animal=animal)
        part_animals = min(quantity, max(1, quantity // 2))
        quantity -= part_animals
        await add_animal(
            self=user,
            code_name_animal=animal_obj.code_name,
            quantity=part_animals,
        )
        rewards.append(f"{animal_obj.code_name}x{part_animals}")
    return {"status": "ok", "summary": f"merchant_targeted:{','.join(rewards)}"}


async def execute_create_item(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    create_price = await gen_price_to_create_item(session=session, id_user=user.id_user)
    if int(user.usd) < create_price:
        return {"status": "skipped", "summary": "not_enough_usd"}

    user.usd -= create_price
    user.amount_expenses_usd += create_price
    item_info, item_props = await create_item(session=session)
    await add_item_to_db(
        session=session,
        item_info=item_info,
        item_props=item_props,
        id_user=user.id_user,
    )
    await optimize_items_for_user(session=session, user=user)
    return {"status": "ok", "summary": f"create_item:{item_info['key']}"}


async def execute_activate_item(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    id_item = str(params.get("id_item", "")).strip()
    item = await get_user_item(session=session, user=user, id_item=id_item)
    if not item:
        return {"status": "skipped", "summary": "item_not_found"}
    if item.is_active:
        return {"status": "skipped", "summary": "item_already_active"}

    active_items = await get_active_user_items(session=session, user=user)
    if len(active_items) >= 3:
        return {"status": "skipped", "summary": "max_active_items"}

    item.is_active = True
    active_items.append(item)
    user.info_about_items = await synchronize_info_about_items(items=active_items)
    return {"status": "ok", "summary": f"activate_item:{id_item}"}


async def execute_deactivate_item(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    id_item = str(params.get("id_item", "")).strip()
    item = await get_user_item(session=session, user=user, id_item=id_item)
    if not item:
        return {"status": "skipped", "summary": "item_not_found"}
    if not item.is_active:
        return {"status": "skipped", "summary": "item_not_active"}

    item.is_active = False
    active_items = await get_active_user_items(session=session, user=user)
    user.info_about_items = await synchronize_info_about_items(items=active_items)
    return {"status": "ok", "summary": f"deactivate_item:{id_item}"}


async def execute_sell_item(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    id_item = str(params.get("id_item", "")).strip()
    item = await get_user_item(session=session, user=user, id_item=id_item)
    if not item:
        return {"status": "skipped", "summary": "item_not_found"}

    usd_to_create_item = await get_value(
        session=session, value_name="USD_TO_CREATE_ITEM"
    )
    percent_markdown_item = await get_value(
        session=session,
        value_name="PERCENT_MARKDOWN_ITEM",
    )
    sell_price = int(int(usd_to_create_item) * (int(percent_markdown_item) / 100))
    item.id_user = 0
    item.is_active = False
    user.usd += sell_price
    active_items = await get_active_user_items(session=session, user=user)
    user.info_about_items = await synchronize_info_about_items(items=active_items)
    return {"status": "ok", "summary": f"sell_item:{id_item}:{sell_price}"}


async def execute_optimize_items(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    changed = await optimize_items_for_user(session=session, user=user)
    return {"status": "ok", "summary": f"optimize_items:{changed}"}


async def execute_upgrade_item(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    id_item = str(params.get("id_item", "")).strip()
    if not id_item:
        return {"status": "skipped", "summary": "item_missing"}
    item = await session.scalar(
        select(Item).where(Item.id_item == id_item, Item.id_user == user.id_user)
    )
    if not item:
        return {"status": "skipped", "summary": "item_not_found"}
    max_lvl_item = await get_value(session=session, value_name="MAX_LVL_ITEM")
    if item.lvl >= max_lvl_item:
        return {"status": "skipped", "summary": "item_max_level"}
    usd_to_up_item = await get_value(session=session, value_name="USD_TO_UP_ITEM")
    cost = int(usd_to_up_item) * (int(item.lvl) + 1)
    if int(user.usd) < cost:
        return {"status": "skipped", "summary": "not_enough_usd"}

    user.usd -= cost
    user.amount_expenses_usd += cost
    if not await able_to_enhance(session=session, current_item_lvl=item.lvl):
        return {"status": "ok", "summary": f"upgrade_failed:{id_item}"}

    new_item_properties, updated_property, parameter = await random_up_property_item(
        session=session,
        item_properties=item.properties,
    )
    if item.is_active:
        user.info_about_items = await update_prop_iai(
            info_about_items=user.info_about_items,
            prop=updated_property,
            value=parameter,
        )
    item.properties = new_item_properties
    item.lvl += 1
    return {
        "status": "ok",
        "summary": f"upgrade_item:{id_item}:{updated_property}+={parameter}",
    }


async def execute_merge_items(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    id_item_1 = str(params.get("id_item_1", "")).strip()
    id_item_2 = str(params.get("id_item_2", "")).strip()
    if not id_item_1 or not id_item_2 or id_item_1 == id_item_2:
        return {"status": "skipped", "summary": "bad_merge_params"}

    item_1 = await session.scalar(
        select(Item).where(Item.id_item == id_item_1, Item.id_user == user.id_user)
    )
    item_2 = await session.scalar(
        select(Item).where(Item.id_item == id_item_2, Item.id_user == user.id_user)
    )
    if not item_1 or not item_2:
        return {"status": "skipped", "summary": "merge_items_not_found"}

    usd_to_merge_items = await get_value(
        session=session, value_name="USD_TO_MERGE_ITEMS"
    )
    q_props = len(json.loads(item_1.properties)) + len(json.loads(item_2.properties))
    lvl_sum = max(1, int(item_1.lvl) + int(item_2.lvl))
    cost = int(usd_to_merge_items) * (q_props + lvl_sum)
    if int(user.usd) < cost:
        return {"status": "skipped", "summary": "not_enough_usd"}

    user.usd -= cost
    user.amount_expenses_usd += cost
    new_item = await merge_items(
        session=session,
        id_item_1=id_item_1,
        id_item_2=id_item_2,
    )
    new_item.id_user = user.id_user
    session.add(new_item)
    await optimize_items_for_user(session=session, user=user)
    return {"status": "ok", "summary": f"merge_items:{id_item_1}+{id_item_2}"}


async def execute_create_unity(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    if user.current_unity:
        return {"status": "skipped", "summary": "already_in_unity"}
    price_for_create_unity = await get_value(
        session=session,
        value_name="PRICE_FOR_CREATE_UNITY",
    )
    if int(user.usd) < price_for_create_unity:
        return {"status": "skipped", "summary": "not_enough_usd"}

    name = str(params.get("name", "")).strip() or await generate_npc_unity_name(
        session=session,
        user=user,
    )
    unity = Unity(idpk_user=user.idpk, name=name)
    user.usd -= price_for_create_unity
    user.amount_expenses_usd += price_for_create_unity
    session.add(unity)
    await session.flush()
    user.current_unity = f"owner:{unity.idpk}"
    return {"status": "ok", "summary": f"create_unity:{unity.name}"}


async def execute_join_best_unity(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    if user.current_unity:
        return {"status": "skipped", "summary": "already_in_unity"}

    owner_idpk = safe_int(params.get("owner_idpk"), default=0)
    candidates = observation["unity"]["candidates"]
    chosen = None
    for candidate in candidates:
        if owner_idpk and candidate["owner_idpk"] == owner_idpk:
            chosen = candidate
            break
    if not chosen and candidates:
        chosen = candidates[0]
    if not chosen:
        return {"status": "skipped", "summary": "no_unity_candidates"}

    unity = await session.scalar(
        select(Unity).where(Unity.idpk_user == chosen["owner_idpk"])
    )
    if not unity:
        return {"status": "skipped", "summary": "unity_not_found"}

    owner = await session.get(User, chosen["owner_idpk"])
    if owner and owner.id_user < 0:
        unity.add_member(idpk_member=user.idpk)
        user.current_unity = f"member:{unity.idpk}"
        return {"status": "ok", "summary": f"join_npc_unity:{unity.name}"}

    existing_request = await session.scalar(
        select(RequestToUnity).where(RequestToUnity.idpk_user == user.idpk)
    )
    if existing_request:
        return {"status": "skipped", "summary": "unity_request_exists"}

    min_to_end_request = await get_value(
        session=session, value_name="MIN_TO_END_REQUEST"
    )
    request = RequestToUnity(
        idpk_user=user.idpk,
        idpk_unity_owner=unity.idpk_user,
        date_request=datetime.now(),
        date_request_end=datetime.now() + timedelta(minutes=int(min_to_end_request)),
    )
    session.add(request)
    return {"status": "ok", "summary": f"request_unity:{unity.name}"}


async def execute_recruit_top_player(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    current_unity = observation["unity"].get("current")
    if not current_unity or not current_unity.get("is_owner"):
        return {"status": "skipped", "summary": "not_unity_owner"}

    target_idpk = safe_int(params.get("idpk_user"), default=0)
    recruit_targets = observation["unity"].get("recruit_targets", [])
    target = None
    for candidate in recruit_targets:
        if target_idpk and candidate["idpk"] == target_idpk:
            target = candidate
            break
    if not target and recruit_targets:
        target = recruit_targets[0]
    if not target:
        return {"status": "skipped", "summary": "no_recruit_targets"}

    invited_user = await session.get(User, target["idpk"])
    unity = await session.get(Unity, current_unity["idpk"])
    if not invited_user or not unity or invited_user.current_unity:
        return {"status": "skipped", "summary": "recruit_target_unavailable"}

    invite_key = npc_unity_invite_key(user.idpk, invited_user.idpk)
    if await redis.get(invite_key):
        return {"status": "skipped", "summary": "invite_already_sent"}

    await redis.set(invite_key, str(unity.idpk), ex=settings.unity_invite_ttl_seconds)
    await bot.send_message(
        chat_id=invited_user.id_user,
        text=(
            f'NPC {user.nickname} приглашает вас в объединение "{unity.name}". '
            f"Доход объединения: {current_unity['income']} RUB/мин."
        ),
        reply_markup=await ik_npc_unity_invitation(
            unity_idpk=unity.idpk,
            owner_idpk=user.idpk,
        ),
    )
    return {"status": "ok", "summary": f"recruit_invite:{invited_user.nickname}"}


async def execute_upgrade_unity_level(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    current_unity = observation["unity"]["current"]
    if not current_unity or not current_unity["is_owner"]:
        return {"status": "skipped", "summary": "not_unity_owner"}
    unity = await session.get(Unity, current_unity["idpk"])
    if not unity or unity.level >= 3:
        return {"status": "skipped", "summary": "unity_max_level"}
    if not await can_upgrade_unity(session=session, unity=unity):
        return {"status": "skipped", "summary": "unity_conditions_not_met"}
    unity.level += 1
    return {"status": "ok", "summary": f"upgrade_unity_level:{unity.level}"}


async def execute_review_unity_request(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    current_unity = observation["unity"].get("current")
    if not current_unity or not current_unity.get("is_owner"):
        return {"status": "skipped", "summary": "not_unity_owner"}

    applicant_idpk = safe_int(params.get("idpk_user"), default=0)
    decision = str(params.get("decision", "accept")).strip().lower()
    if decision not in {"accept", "reject"}:
        decision = "accept"

    request = await session.scalar(
        select(RequestToUnity).where(
            and_(
                RequestToUnity.idpk_user == applicant_idpk,
                RequestToUnity.idpk_unity_owner == user.idpk,
            )
        )
    )
    if not request:
        return {"status": "skipped", "summary": "unity_request_not_found"}

    applicant = await session.get(User, applicant_idpk)
    unity = await session.get(Unity, current_unity["idpk"])
    if not applicant or not unity:
        await session.delete(request)
        return {"status": "skipped", "summary": "unity_request_stale"}

    if applicant.current_unity:
        await session.delete(request)
        return {"status": "skipped", "summary": "applicant_already_in_unity"}

    if decision == "reject":
        await session.delete(request)
        return {"status": "ok", "summary": f"reject_unity_request:{applicant.nickname}"}

    unity.add_member(idpk_member=applicant.idpk)
    applicant.current_unity = f"member:{unity.idpk}"
    await session.delete(request)
    return {"status": "ok", "summary": f"accept_unity_request:{applicant.nickname}"}


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
) -> None:
    history = json.loads(user.history_moves)
    key = datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")
    history[key] = json.dumps(
        {
            "npc": user.nickname,
            "action": action["action"],
            "params": action["params"],
            "reason": action.get("reason", ""),
            "result": result,
        },
        ensure_ascii=False,
    )
    limit_on_write_moves = await get_value(
        session=session,
        value_name="LIMIT_ON_WRITE_MOVES",
    )
    while len(history) > limit_on_write_moves:
        first_key = next(iter(history))
        del history[first_key]
    user.history_moves = json.dumps(history, ensure_ascii=False)
    user.moves += 1
