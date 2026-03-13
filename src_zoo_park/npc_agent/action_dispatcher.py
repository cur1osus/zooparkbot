import contextlib
import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any


from aiogram.utils.deep_linking import create_start_link
from config import CHAT_ID
from db import Animal, Game, Gamer, Item, RequestToUnity, TransferMoney, Unity, User
from game_variables import games
from init_bot import bot
from init_db_redis import redis
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from tools.animals import add_animal, get_price_animal
from tools.aviaries import add_aviary, get_price_aviaries, get_remain_seats
from tools.bank import exchange, get_rate
from tools.bonus import apply_bonus, get_bonus

from tools.items import (
    CREATE_ITEM_PAW_PRICE,
    add_item_to_db,
    able_to_enhance,
    create_item,
    gen_price_to_create_item,
    get_value_prop_from_iai,
    merge_items,
    random_up_property_item,
    synchronize_info_about_items,
    update_prop_iai,
)
from tools.random_merchant import gen_price
from tools.unity import get_unity_idpk

from tools import add_to_currency, add_user_to_used, gen_key, in_used
from tools.value import get_value

from .client import NpcDecisionClient
from .settings import settings

from bot.keyboards import (
    ik_get_money,
    ik_get_money_one_piece,
    ik_npc_unity_invitation,
    ik_start_created_game,
)
from tools.message import get_id_for_edit_message

from .state_builder import (
    can_upgrade_unity,
    ensure_random_merchant_for_user,
    find_cheapest_affordable_animal,
    generate_npc_unity_name_via_llm,
    get_active_user_items,
    get_user_item,
    npc_unity_invite_key,
    optimize_items_for_user,
    safe_int,
    sanitize_unity_name,
)


async def execute_action(
    session: AsyncSession,
    user: User,
    action: dict[str, Any],
    observation: dict[str, Any],
    client: NpcDecisionClient | None = None,
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
        "exit_from_unity": execute_exit_from_unity,
        "send_chat_transfer": execute_send_chat_transfer,
        "claim_chat_transfer": execute_claim_chat_transfer,
        "create_chat_game": execute_create_chat_game,
        "join_chat_game": execute_join_chat_game,
    }
    handler = handlers.get(action_name, execute_wait)
    extra_kwargs = {}
    if handler is execute_create_unity:
        extra_kwargs["client"] = client
    result = await handler(
        session=session,
        user=user,
        params=params,
        observation=observation,
        **extra_kwargs,
    )

    # Normalize non-ok outcomes so the planner can reflect on explicit failure context.
    status = str(result.get("status", "")).strip().lower()
    if status != "ok":
        summary = str(result.get("summary", "")).strip() or "action_unavailable"
        error_code = str(result.get("error_code", "")).strip() or summary
        allowed_actions = []
        for row in observation.get("allowed_actions", []) or []:
            if not isinstance(row, dict):
                continue
            name = str(row.get("action", "")).strip()
            if name and name not in allowed_actions:
                allowed_actions.append(name)

        blocked_actions = [
            {
                "action": str(name),
                "reason": "blocked_by_guard",
            }
            for name in (observation.get("anti_loop_guard", {}).get("blocked_actions", []) or [])
            if str(name).strip()
        ]

        result["failed_action"] = action_name
        result["error_code"] = error_code
        result["error_message"] = summary
        result["allowed_actions"] = allowed_actions
        if blocked_actions:
            result["blocked_actions"] = blocked_actions

    return result


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
        best_aviary = signal["best_aviary_option"]
        aviary_size = max(1, int(best_aviary.get("size", 1) or 1))
        affordable_quantity = max(1, int(best_aviary.get("affordable_quantity", 1) or 1))
        # Open a practical seat buffer in one move when possible.
        target_new_seats = max(aviary_size, 6)
        quantity = max(1, (target_new_seats + aviary_size - 1) // aviary_size)
        quantity = min(quantity, affordable_quantity)

        return await execute_buy_aviary(
            session=session,
            user=user,
            params={
                "code_name_aviary": best_aviary["code_name"],
                "quantity": quantity,
            },
            observation=observation,
        )
    best_income_option = signal.get("best_income_option")
    if best_income_option:
        quantity = max(
            1,
            min(
                int(best_income_option.get("affordable_quantity", 1) or 1),
                int(observation.get("zoo", {}).get("remain_seats", 1) or 1),
            ),
        )
        return await execute_buy_rarity_animal(
            session=session,
            user=user,
            params={
                "animal": best_income_option["animal"],
                "rarity": best_income_option["rarity"],
                "quantity": quantity,
            },
            observation=observation,
        )
    if (
        int(user.usd) >= observation["items"]["create_price_usd"]
        or int(user.paw_coins) >= CREATE_ITEM_PAW_PRICE
    ):
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
    if remain_seats <= 0:
        return {"status": "skipped", "summary": "no_seat_capacity"}
    quantity = min(quantity, int(remain_seats))
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
    if int(user.usd) >= create_price:
        user.usd -= create_price
        user.amount_expenses_usd += create_price
    elif int(user.paw_coins) >= CREATE_ITEM_PAW_PRICE:
        user.paw_coins -= CREATE_ITEM_PAW_PRICE
        user.amount_expenses_paw_coins += CREATE_ITEM_PAW_PRICE
    else:
        return {"status": "skipped", "summary": "not_enough_create_currency"}

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
    client: NpcDecisionClient | None = None,
) -> dict[str, Any]:
    if user.current_unity:
        return {"status": "skipped", "summary": "already_in_unity"}
    price_for_create_unity = await get_value(
        session=session,
        value_name="PRICE_FOR_CREATE_UNITY",
    )
    if int(user.usd) < price_for_create_unity:
        return {"status": "skipped", "summary": "not_enough_usd"}

    provided_name = sanitize_unity_name(
        str(params.get("name", "")).strip(),
        int(await get_value(session=session, value_name="NAME_UNITY_LENGTH_MAX")),
    )
    name = provided_name or await generate_npc_unity_name_via_llm(
        session=session,
        user=user,
        observation=observation,
        client=client,
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


async def execute_exit_from_unity(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    current_unity = user.current_unity
    if not current_unity:
        return {"status": "skipped", "summary": "not_in_unity"}

    unity_idpk = int(get_unity_idpk(current_unity) or 0)
    unity = await session.get(Unity, unity_idpk)
    if not unity:
        user.current_unity = None
        return {"status": "ok", "summary": "exit_unity:stale"}

    # Member exit
    if unity.idpk_user != user.idpk:
        unity.remove_member(idpk_member=str(user.idpk))
        user.current_unity = None
        return {"status": "ok", "summary": "exit_unity:member"}

    # Owner exit: promote first member or delete unity
    user.current_unity = None
    idpk_next_owner = unity.remove_first_member()
    if idpk_next_owner:
        next_owner: User = await session.get(User, idpk_next_owner)
        if next_owner:
            next_owner.current_unity = f"owner:{unity.idpk}"
            unity.idpk_user = next_owner.idpk
        return {"status": "ok", "summary": "exit_unity:owner_promoted"}

    await session.delete(unity)
    return {"status": "ok", "summary": "exit_unity:deleted"}


async def execute_send_chat_transfer(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    currency = str(params.get("currency", "usd")).strip().lower()
    if currency not in {"usd", "rub"}:
        return {"status": "skipped", "summary": "bad_currency"}

    amount = safe_int(params.get("amount", 0), default=0, min_value=1)
    pieces = safe_int(params.get("pieces", 1), default=1, min_value=1)
    if pieces > amount:
        pieces = amount
    if pieces > 500:
        pieces = 500

    balance = int(user.usd) if currency == "usd" else int(user.rub)
    if balance < amount:
        return {"status": "skipped", "summary": "not_enough_currency"}

    one_piece = max(1, amount // pieces)
    total_spend = one_piece * pieces
    if currency == "usd":
        user.usd -= total_spend
        user.amount_expenses_usd += total_spend
    else:
        user.rub -= total_spend
        user.amount_expenses_rub += total_spend

    transfer = TransferMoney(
        id_transfer=gen_key(length=10),
        idpk_user=user.idpk,
        currency=currency,
        one_piece_sum=one_piece,
        pieces=pieces,
        status=True,
        source_chat_id=CHAT_ID,
    )
    session.add(transfer)
    await session.flush()

    keyboard = (
        await ik_get_money(
            one_piece=f"{one_piece}{'$' if currency == 'usd' else '₽'}",
            remain_pieces=pieces,
            idpk_tr=transfer.idpk,
        )
        if pieces > 1
        else await ik_get_money_one_piece(idpk_tr=transfer.idpk)
    )
    await bot.send_message(
        chat_id=CHAT_ID,
        text=(
            f"{user.nickname} устроил раздачу: {total_spend}{'$' if currency == 'usd' else '₽'} "
            f"на {pieces} частей. Забирайте 👇"
        ),
        reply_markup=keyboard,
    )
    return {"status": "ok", "summary": f"chat_transfer:{currency}:{total_spend}:{pieces}"}


async def execute_claim_chat_transfer(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    idpk_tr = safe_int(params.get("idpk_tr", 0), default=0, min_value=1)
    if not idpk_tr:
        return {"status": "skipped", "summary": "idpk_tr_missing"}

    tr = await session.get(TransferMoney, idpk_tr)
    if not tr or not tr.status:
        return {"status": "skipped", "summary": "transfer_not_found"}
    if int(getattr(tr, "source_chat_id", 0) or 0) != int(CHAT_ID):
        return {"status": "skipped", "summary": "transfer_not_official_chat"}
    if int(tr.idpk_user) == int(user.idpk):
        return {"status": "skipped", "summary": "own_transfer"}
    if int(tr.pieces or 0) <= 0:
        return {"status": "skipped", "summary": "transfer_empty"}

    if await in_used(session=session, idpk_tr=tr.idpk, idpk_user=user.idpk):
        return {"status": "skipped", "summary": "transfer_already_used"}

    await add_user_to_used(session=session, idpk_tr=tr.idpk, idpk_user=user.idpk)
    await add_to_currency(self=user, currency=tr.currency, amount=int(tr.one_piece_sum))
    tr.pieces -= 1

    if tr.pieces <= 0:
        tr.status = False

    # Keep chat button state in sync when NPC claims a piece.
    if tr.id_mess:
        cur = "$" if tr.currency == "usd" else "₽"
        keyboard = (
            await ik_get_money(
                one_piece=f"{int(tr.one_piece_sum):,d}{cur}",
                remain_pieces=int(tr.pieces),
                idpk_tr=tr.idpk,
            )
            if int(tr.pieces) > 0
            else None
        )
        with contextlib.suppress(Exception):
            await bot.edit_message_reply_markup(
                reply_markup=keyboard,
                **get_id_for_edit_message(str(tr.id_mess)),
            )

    return {
        "status": "ok",
        "summary": f"claim_chat_transfer:{tr.currency}:{int(tr.one_piece_sum)}",
    }


async def execute_create_chat_game(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    game_type = str(params.get("game_type", "🎲"))
    if game_type not in games:
        return {"status": "skipped", "summary": "bad_game_type"}

    amount_gamers = safe_int(params.get("amount_gamers", 3), default=3, min_value=2)
    amount_gamers = min(amount_gamers, 80)
    amount_award = safe_int(params.get("amount_award", 0), default=0, min_value=1)
    currency = str(params.get("currency", "usd")).strip().lower()
    if currency not in {"usd", "rub"}:
        return {"status": "skipped", "summary": "bad_currency"}

    balance = int(user.usd) if currency == "usd" else int(user.rub)
    if balance < amount_award:
        return {"status": "skipped", "summary": "not_enough_currency"}

    if currency == "usd":
        user.usd -= amount_award
        user.amount_expenses_usd += amount_award
    else:
        user.rub -= amount_award
        user.amount_expenses_rub += amount_award

    sec_to_expire_game = int(await get_value(session=session, value_name="SEC_TO_EXPIRE_GAME"))
    game = Game(
        id_game=f"game_{gen_key(length=12)}",
        idpk_user=user.idpk,
        type_game=game_type,
        amount_gamers=amount_gamers,
        amount_award=Decimal(amount_award),
        currency_award=currency,
        end_date=datetime.now() + timedelta(seconds=sec_to_expire_game),
        amount_moves=safe_int(params.get("amount_moves", 5), default=5, min_value=1),
        activate=True,
        source_chat_id=CHAT_ID,
    )
    session.add(game)
    await session.flush()

    link = await create_start_link(bot=bot, payload=game.id_game)
    msg = await bot.send_message(
        chat_id=CHAT_ID,
        text=(
            f"{user.nickname} создал мини-игру {game_type}: "
            f"игроков {amount_gamers}, приз {amount_award}{'$' if currency == 'usd' else '₽'}."
        ),
        reply_markup=await ik_start_created_game(
            link=link,
            current_gamers=0,
            total_gamers=amount_gamers,
        ),
        disable_web_page_preview=True,
    )
    game.id_mess = str(msg.message_id)
    return {"status": "ok", "summary": f"create_chat_game:{game.id_game}"}


async def execute_join_chat_game(
    session: AsyncSession,
    user: User,
    params: dict[str, Any],
    observation: dict[str, Any],
) -> dict[str, Any]:
    id_game = str(params.get("id_game", "")).strip()
    if not id_game:
        return {"status": "skipped", "summary": "id_game_missing"}

    game = await session.scalar(select(Game).where(Game.id_game == id_game))
    if not game or game.end:
        return {"status": "skipped", "summary": "game_not_found"}
    if game.idpk_user == user.idpk:
        return {"status": "skipped", "summary": "game_owner_cannot_join"}

    gamer = await session.scalar(
        select(Gamer).where(Gamer.id_game == id_game, Gamer.idpk_gamer == user.idpk)
    )
    if gamer:
        return {"status": "skipped", "summary": "already_joined"}

    active_game = await session.scalar(
        select(Gamer).where(Gamer.idpk_gamer == user.idpk, Gamer.game_end == False)  # noqa: E712
    )
    if active_game:
        return {"status": "skipped", "summary": "has_active_game"}

    current_gamers = int(
        await session.scalar(select(func.count()).select_from(Gamer).where(Gamer.id_game == id_game))
        or 0
    )
    if current_gamers >= int(game.amount_gamers):
        return {"status": "skipped", "summary": "game_full"}

    session.add(Gamer(id_game=id_game, idpk_gamer=user.idpk, moves=int(game.amount_moves)))
    return {"status": "ok", "summary": f"join_chat_game:{id_game}"}
