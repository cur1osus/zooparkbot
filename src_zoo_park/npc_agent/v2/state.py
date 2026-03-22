from __future__ import annotations

import abc
import contextlib
from datetime import datetime
from typing import Any, TYPE_CHECKING
from pydantic import BaseModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from db import User

class ContextProvider(abc.ABC):
    """Базовый класс для сборки части состояния игры."""
    
    @abc.abstractmethod
    async def provide(self, session: AsyncSession, user: User, current_obs: dict[str, Any]) -> dict[str, Any]:
        """Возвращает словарь с данными, которые будут слиты (update) с observation."""
        pass

class WakeContextProvider(ContextProvider):
    def __init__(self, wake_context: dict[str, Any] | None, execution_feedback: dict[str, Any] | None):
        self.wake_context = wake_context or {}
        self.execution_feedback = execution_feedback or {}

    async def provide(self, session: AsyncSession, user: User, current_obs: dict[str, Any]) -> dict[str, Any]:
        from npc_agent.settings import settings
        return {
            "schema_version": 6,
            "current_time": datetime.now().isoformat(),
            "wake_context": {
                "source": self.wake_context.get("source", "scheduled"),
                "reason": self.wake_context.get("reason", "planned_wake"),
                "scheduled_at": self.wake_context.get("scheduled_at"),
                "constraints": {
                    "min_sleep_seconds": settings.min_sleep_seconds,
                    "max_sleep_seconds": settings.max_sleep_seconds,
                    "default_sleep_seconds": settings.step_seconds,
                },
            },
            "execution_feedback": self.execution_feedback,
        }

class PlayerContextProvider(ContextProvider):
    async def provide(self, session: AsyncSession, user: User, current_obs: dict[str, Any]) -> dict[str, Any]:
        from tools.income import income_
        from tools.items import get_value_prop_from_iai
        from tools.unity import get_unity_idpk

        current_income = await income_(session=session, user=user)
        return {
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
            }
        }

class ZooContextProvider(ContextProvider):
    async def provide(self, session: AsyncSession, user: User, current_obs: dict[str, Any]) -> dict[str, Any]:
        from db.structured_state import get_user_animals_map, get_user_aviaries_map
        from tools.aviaries import get_total_number_seats, get_remain_seats

        animals_state = await get_user_animals_map(session=session, user=user)
        aviaries_state = await get_user_aviaries_map(session=session, user=user)
        total_seats = await get_total_number_seats(session=session, aviaries=aviaries_state)
        remain_seats = await get_remain_seats(session=session, user=user)
        
        return {
            "zoo": {
                "animals": animals_state,
                "aviaries": aviaries_state,
                "total_seats": int(total_seats),
                "remain_seats": int(remain_seats),
            }
        }

class EconomyContextProvider(ContextProvider):
    async def provide(self, session: AsyncSession, user: User, current_obs: dict[str, Any]) -> dict[str, Any]:
        from npc_agent.state_builder import ensure_random_merchant_for_user, _build_rate_history_snapshot, _build_market_analysis
        from tools.bank import get_rate
        from tools.value import get_value

        merchant = await ensure_random_merchant_for_user(session=session, user=user)
        rate = await get_rate(session=session, user=user)
        bank_storage = await get_value(session=session, value_name="BANK_STORAGE", value_type="str", cache_=False)
        bank_percent_fee = await get_value(session=session, value_name="BANK_PERCENT_FEE")
        min_rate_rub_usd = await get_value(session=session, value_name="MIN_RATE_RUB_USD")
        max_rate_rub_usd = await get_value(session=session, value_name="MAX_RATE_RUB_USD")
        rate_history_raw = await get_value(session=session, value_name="RATE_RUB_USD_HISTORY_JSON", value_type="str", cache_=False)
        rate_history = _build_rate_history_snapshot(rate_history_raw)

        return {
            "bank": {
                "rate_rub_usd": int(rate),
                "min_rate_rub_usd": int(min_rate_rub_usd),
                "max_rate_rub_usd": int(max_rate_rub_usd),
                "percent_fee": int(bank_percent_fee),
                "bank_storage": str(bank_storage),
                "rate_history_1h": rate_history.get("points_1h", []),
                "rate_history_1h_summary": rate_history.get("summary_1h", {}),
                "market_analysis": _build_market_analysis(
                    rate=int(rate),
                    min_rate=int(min_rate_rub_usd),
                    max_rate=int(max_rate_rub_usd),
                    history=rate_history.get("points_1h", []),
                ),
            },
            "merchant": {
                "name": merchant.name,
                "first_offer_bought": merchant.first_offer_bought,
                "code_name_animal": merchant.code_name_animal,
                "quantity_animals": int(merchant.quantity_animals),
                "discount": int(merchant.discount),
                "price_with_discount": int(merchant.price_with_discount),
                "random_offer_price": int(merchant.price),
            }
        }

class MarketContextProvider(ContextProvider):
    async def provide(self, session: AsyncSession, user: User, current_obs: dict[str, Any]) -> dict[str, Any]:
        from npc_agent.state_builder import build_animal_market, build_aviary_market, build_item_opportunities, build_item_state
        
        remain_seats = current_obs.get("zoo", {}).get("remain_seats", 0)
        rate = current_obs.get("bank", {}).get("rate_rub_usd", 1)
        current_income = current_obs.get("player", {}).get("income_per_minute_rub", 0)

        animal_market = await build_animal_market(
            session=session, user=user, remain_seats=remain_seats, rate_rub_usd=int(rate), income_per_minute_rub=int(current_income)
        )
        aviary_market = await build_aviary_market(session=session, user=user)
        items = await build_item_state(session=session, user=user)
        item_opportunities = await build_item_opportunities(session=session, user=user)

        return {
            "animal_market": animal_market,
            "aviary_market": aviary_market,
            "items": items,
            "item_opportunities": item_opportunities,
        }

class SocialContextProvider(ContextProvider):
    async def provide(self, session: AsyncSession, user: User, current_obs: dict[str, Any]) -> dict[str, Any]:
        from db import Unity
        from npc_agent.state_builder import build_standings, build_unity_state, build_chat_games_state, build_chat_transfers_state, build_momentum_signal
        from tools.unity import get_unity_idpk
        from tools.unity_projects import get_or_create_project, get_project_reward_preview

        standings = await build_standings(session=session, user=user)
        unity = await build_unity_state(session=session, user=user)
        chat_games = await build_chat_games_state(session=session, user=user)
        chat_transfers = await build_chat_transfers_state(session=session, user=user)
        
        current_income = current_obs.get("player", {}).get("income_per_minute_rub", 0)
        momentum = await build_momentum_signal(session=session, user=user, current_income=int(current_income))

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

        return {
            "standings": standings,
            "unity": unity,
            "chat_games": chat_games,
            "chat_transfers": chat_transfers,
            "momentum": momentum,
            "clan_project": clan_project_summary,
        }

class AIStrategyContextProvider(ContextProvider):
    async def provide(self, session: AsyncSession, user: User, current_obs: dict[str, Any]) -> dict[str, Any]:
        from npc_agent.state_builder import build_allowed_actions, build_strategy_signals, build_decision_brief, build_npc_plan, build_action_contract
        from npc_agent.memory import build_npc_memory_context
        from npc_agent.settings import settings

        allowed_actions = await build_allowed_actions(session=session, user=user, observation=current_obs)
        current_obs["allowed_actions"] = allowed_actions  # Inject early for downstream dependencies
        
        strategy_signals = build_strategy_signals(observation=current_obs)
        current_obs["strategy_signals"] = strategy_signals

        decision_brief = build_decision_brief(observation=current_obs)
        
        memory = await build_npc_memory_context(session=session, user=user, observation=current_obs)
        current_obs["memory"] = memory

        planner = build_npc_plan(observation=current_obs)
        action_contract = build_action_contract(observation=current_obs)

        strategy_signals["goal_focus"] = [
            goal.get("topic") for goal in memory.get("active_goals", [])
        ][: settings.memory_goal_limit]

        # Return the new keys and a special player update
        player_updates = dict(current_obs.get("player", {}))
        player_updates["current_mood"] = memory.get("profile", {}).get("current_mood", "neutral")
        player_updates["affinity_score"] = memory.get("profile", {}).get("affinity_score", 50)

        return {
            "allowed_actions": allowed_actions,
            "strategy_signals": strategy_signals,
            "decision_brief": decision_brief,
            "memory": memory,
            "planner": planner,
            "action_contract": action_contract,
            "player": player_updates,
        }

class ObservationBuilder:
    def __init__(self):
        self._providers: list[ContextProvider] = []

    def add_provider(self, provider: ContextProvider):
        self._providers.append(provider)

    async def build(self, session: AsyncSession, user: User) -> dict[str, Any]:
        observation: dict[str, Any] = {}
        for provider in self._providers:
            data = await provider.provide(session, user, observation)
            
            # Deep merge specific keys to preserve nested data
            for k, v in data.items():
                if k == "player" and "player" in observation:
                    observation["player"].update(v)
                else:
                    observation[k] = v
                    
        return observation
