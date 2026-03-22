import contextlib
import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any


from aiogram.utils.deep_linking import create_start_link
from config import CHAT_ID
from db import Animal, Game, Gamer, Item, RequestToUnity, TransferMoney, Unity, User
from db.structured_state import (
    add_unity_member,
    get_user_aviaries_map,
    pop_next_unity_owner,
    remove_unity_member,
)
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
from tools.unity_projects import (
    contribute_to_project,
    get_user_chests,
    open_user_chests,
)
from text_utils import fit_db_field, normalize_choice, preview_text

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

from .v2.actions import ActionRegistry, ActionContext


def _infer_resource_deficit(error_code: str) -> str | None:
    code = str(error_code or "").lower()
    if "not_enough_usd" in code:
        return "usd"
    if "not_enough_rub" in code or "amount_too_small" in code:
        return "rub"
    if "not_enough_seat" in code or "no_seat" in code:
        return "seats"
    return None


def _compute_cooldown_for_failure(
    error_code: str,
    action_name: str,
    action_history: list[dict[str, Any]],
) -> int:
    """
    Динамический cooldown на основе частоты повторения ошибки.
    Увеличивает задержку при повторяющихся ошибках одного типа.
    """
    base_cooldown = 120 if error_code in {
        "not_enough_usd",
        "not_enough_rub",
        "amount_too_small",
        "not_enough_seats",
        "no_seat_capacity",
        "invite_already_sent",
        "recruit_target_unavailable",
        "unity_request_exists",
        "unity_request_not_found",
        "applicant_already_in_unity",
    } else 300
    
    # Считаем повторения той же ошибки в последних 5 действиях
    recent_failures = 0
    for h in reversed(action_history[-5:]):
        if not isinstance(h, dict):
            continue
        h_action = str(h.get("action", "")).strip()
        h_result = h.get("result", {}) or {}
        h_error = str(h_result.get("error_code", "")).strip().lower()
        
        if h_action == action_name and h_result.get("status") == "error":
            if error_code in h_error or h_error in error_code:
                recent_failures += 1
    
    # Экспоненциальное увеличение cooldown при повторениях
    if recent_failures >= 3:
        return min(1800, base_cooldown * 4)  # 30 минут макс
    elif recent_failures >= 2:
        return min(900, base_cooldown * 2)  # 15 минут макс
    elif recent_failures >= 1:
        return int(base_cooldown * 1.5)
    
    return base_cooldown


async def execute_action(
    session: AsyncSession,
    user: User,
    action: dict[str, Any],
    observation: dict[str, Any],
    client: NpcDecisionClient | None = None,
    action_history: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    action_name = action["action"]
    params_dict = action.get("params") or {}
    action_history = action_history or []

    registry_action = ActionRegistry.get(action_name)
    if not registry_action:
        # fallback to wait
        registry_action = ActionRegistry.get("wait")
    
    ctx = ActionContext(
        session=session,
        user=user,
        observation=observation,
        client=client
    )
    
    try:
        # Use registry_action.params_model to validate params
        params = registry_action.params_model.model_validate(params_dict)
        response = await registry_action.execute(ctx, params)
        result = response.model_dump()
    except Exception as e:
        # Fallback for unexpected failures or validation errors
        result = {
            "status": "error",
            "summary": f"action_execution_failed: {str(e)}",
            "error_code": "execution_failed"
        }

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

        retryable_codes = {
            "not_enough_usd",
            "not_enough_rub",
            "amount_too_small",
            "not_enough_seats",
            "no_seat_capacity",
            "invite_already_sent",
            "recruit_target_unavailable",
            "unity_request_exists",
            "unity_request_not_found",
            "applicant_already_in_unity",
        }
        retryable = any(code in error_code for code in retryable_codes)
        
        # Dynamic cooldown based on repeated failures
        cooldown_sec = _compute_cooldown_for_failure(
            error_code=error_code,
            action_name=action_name,
            action_history=action_history,
        )
        
        suggested_alternatives = [
            name for name in allowed_actions if name != action_name
        ][:5]

        result["failed_action"] = action_name
        result["error_code"] = error_code
        result["error_message"] = summary
        result["allowed_actions"] = allowed_actions
        result["retryable"] = bool(retryable)
        result["cooldown_sec"] = cooldown_sec
        result["suggested_alternatives"] = suggested_alternatives
        result["resource_deficit"] = _infer_resource_deficit(error_code)

    return result
