import math
from datetime import datetime

from db import Animal, SickAnimalEvent, Unity, User, Value
from db.structured_state import get_user_animals_map, get_user_total_animals
from fastjson import loads_or_default
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import tools

# Milestone thresholds: powers of 10 — each gives +MILESTONE_BONUS_PERCENT%
_MILESTONES = [10**i for i in range(1, 19)]


async def sync_user_income(session: AsyncSession, user: User) -> int:
    """Recalculate income, cache it, then recalculate maintenance."""
    income = await income_(session=session, user=user)
    user.income_per_minute = int(income)
    await session.flush()
    # Maintenance depends on income, so sync after
    await sync_maintenance_cost(session=session, user=user)
    return int(income)


async def sync_maintenance_cost(session: AsyncSession, user: User) -> int:
    """
    Recalculate and cache maintenance_per_minute.

    Formula: rate = BASE_RATE + LOG_SCALE * log10(total_animals)
    capped at MAX_RATE. Maintenance = income * rate / 100.

    Logarithmic scaling keeps the rate meaningful across the full range
    from hundreds to quadrillions of animals.

    Config values (DB):
      MAINTENANCE_BASE_RATE     — base % of income (default 5)
      MAINTENANCE_LOG_SCALE     — stored as int×10, e.g. 25 → 2.5 per log10 step
      MAINTENANCE_MAX_RATE      — cap on the rate (default 45)
    """
    total_animals = await get_user_total_animals(session=session, user=user)
    if total_animals == 0 or user.income_per_minute == 0:
        user.maintenance_per_minute = 0
        await session.flush()
        return 0

    base_rate = await tools.get_value(session=session, value_name="MAINTENANCE_BASE_RATE")
    log_scale_x10 = await tools.get_value(session=session, value_name="MAINTENANCE_LOG_SCALE")
    max_rate = await tools.get_value(session=session, value_name="MAINTENANCE_MAX_RATE")

    log_scale = log_scale_x10 / 10
    rate = base_rate + log_scale * math.log10(max(1, total_animals))
    rate = min(rate, max_rate)

    # megapark specialization: +15% maintenance
    unity_idpk = int(user.current_unity.split(":")[-1]) if user.current_unity else None
    if unity_idpk:
        unity = await session.get(Unity, unity_idpk)
        if unity and unity.specialization == "megapark":
            rate = min(rate * 1.15, max_rate)

    maintenance = int(user.income_per_minute * rate / 100)
    user.maintenance_per_minute = maintenance
    await session.flush()
    return maintenance


async def income_(session: AsyncSession, user: User):
    unity_idpk = int(user.current_unity.split(":")[-1]) if user.current_unity else None
    animals = await get_user_animals_map(session=session, user=user)

    income = await income_from_animal(
        session=session,
        animals=animals,
        unity_idpk=unity_idpk,
        info_about_items=user.info_about_items,
        user_idpk=user.idpk,
    )

    # Item bonus: general_income
    if v := tools.get_value_prop_from_iai(
        info_about_items=user.info_about_items, name_prop="general_income"
    ):
        income = income * (1 + v / 100)

    # Diversity bonus: +N% per effective species (Shannon entropy-based)
    # N_eff = exp(H), where H = -sum(p_i * log(p_i)), p_i = species_count / total
    # Even distribution → N_eff = unique_species; monopoly → N_eff ≈ 1
    if animals:
        bonus_per_species = await tools.get_value(
            session=session, value_name="DIVERSITY_BONUS_PER_SPECIES"
        )
        total_animals = sum(animals.values())
        shannon_h = -sum(
            (q / total_animals) * math.log(q / total_animals)
            for q in animals.values()
            if q > 0
        )
        n_eff = math.exp(shannon_h)
        diversity_mult = 1 + (n_eff * bonus_per_species / 100)
        income = income * diversity_mult

    if unity_idpk:
        unity_data = await get_unity_data_for_income(
            session=session, idpk_unity=unity_idpk
        )
        if unity_data["lvl"] in [1, 2, 3]:
            income *= 1 + (unity_data["bonus"] / 100)

        if await _has_active_income_buff(session=session, unity_idpk=unity_idpk):
            income *= 1.10

        # Clan specialization bonuses
        income = await _apply_specialization_bonus(
            session=session,
            unity_idpk=unity_idpk,
            income=income,
            animals=animals,
        )

    return int(income)


async def income_from_animal(
    session: AsyncSession,
    animals: dict,
    unity_idpk: int,
    info_about_items: str,
    user_idpk: int | None = None,
):
    """
    Calculate total income from all owned animals.

    Applies:
    - Per-animal item/unity bonuses
    - Milestone multipliers (+MILESTONE_BONUS_PERCENT% per threshold hit)
    - Sick animal penalty (-50% income for that species)
    """
    income = 0
    if not animals:
        return income

    milestone_bonus_pct = await tools.get_value(
        session=session, value_name="MILESTONE_BONUS_PERCENT"
    )

    # Fetch sick animals for this user (those past their deadline)
    sick_animals: set[str] = set()
    if user_idpk is not None:
        sick_animals = await _get_sick_animal_codenames(
            session=session, user_idpk=user_idpk
        )

    animal_rows = await session.scalars(
        select(Animal).where(Animal.code_name.in_(list(animals.keys())))
    )
    animals_by_code = {animal.code_name: animal for animal in animal_rows.all()}

    for animal_code, quantity in animals.items():
        animal = animals_by_code.get(animal_code)
        if not animal:
            continue

        animal_income = await tools.get_income_animal(
            session=session,
            animal=animal,
            unity_idpk=unity_idpk,
            info_about_items=info_about_items,
        )

        # Milestone multipliers — "bumpy curve": each threshold adds a flat bonus
        milestone_bonus = sum(
            milestone_bonus_pct / 100
            for threshold in _MILESTONES
            if quantity >= threshold
        )
        contribution = int(animal_income * quantity * (1.0 + milestone_bonus))

        # Sick animal: -50% income until cured
        if animal_code in sick_animals:
            contribution = contribution // 2

        income += contribution

    return int(income)


async def _get_sick_animal_codenames(
    session: AsyncSession, user_idpk: int
) -> set[str]:
    """Return code names of animals currently penalizing income (sick, past deadline)."""
    now = datetime.now()
    rows = await session.scalars(
        select(SickAnimalEvent).where(
            SickAnimalEvent.idpk_user == user_idpk,
            SickAnimalEvent.is_cured == False,  # noqa: E712
            SickAnimalEvent.deadline <= now,
        )
    )
    return {row.animal_code_name for row in rows.all()}


async def _apply_specialization_bonus(
    session: AsyncSession,
    unity_idpk: int,
    income: float,
    animals: dict,
) -> float:
    """
    Apply clan specialization multipliers to income.

    specialist — +50% from epic/mythical/leg animals, -20% from common.
    megapark   — +1% per 10 total animals (max +60%).
    wild       — +3% additional per unique species (on top of diversity bonus).
    """
    unity = await session.get(Unity, unity_idpk)
    if not unity or not unity.specialization:
        return income

    spec = unity.specialization

    if spec == "specialist":
        # Already applied per-animal above would be complex; approximate globally:
        # Count income split by rarity suffix
        rare_suffixes = ("_epic", "_mythical", "_leg")
        common_suffixes = ("_rare",)  # _rare is the "common" tier in this game

        epic_count = sum(qty for code, qty in animals.items() if any(code.endswith(s) for s in rare_suffixes))
        common_count = sum(qty for code, qty in animals.items() if any(code.endswith(s) for s in common_suffixes))
        total = sum(animals.values()) or 1

        epic_share = epic_count / total
        common_share = common_count / total

        bonus = epic_share * 0.50 - common_share * 0.20
        income = income * (1.0 + bonus)

    elif spec == "megapark":
        total_animals = sum(animals.values())
        # +1% per 10 animals, max +60%
        megapark_bonus = min(60, total_animals // 10)
        income = income * (1.0 + megapark_bonus / 100)

    elif spec == "wild":
        # +3% per effective species (same entropy-based N_eff as diversity bonus)
        total_animals = sum(animals.values())
        shannon_h = -sum(
            (q / total_animals) * math.log(q / total_animals)
            for q in animals.values()
            if q > 0
        )
        n_eff = math.exp(shannon_h)
        income = income * (1.0 + n_eff * 3 / 100)

    return income


async def get_unity_data_for_income(session: AsyncSession, idpk_unity: int):
    unity = await session.get(Unity, idpk_unity)
    data = {"lvl": unity.level}
    if unity.level in [1, 2]:
        data["bonus"] = await tools.get_value(
            session=session, value_name="BONUS_ADD_TO_INCOME_1ST_LVL"
        )
    elif unity.level == 3:
        data["bonus"] = await tools.get_value(
            session=session, value_name="BONUS_ADD_TO_INCOME_3RD_LVL"
        )
    else:
        data["bonus"] = 0
    return data


async def _has_active_income_buff(session: AsyncSession, unity_idpk: int) -> bool:
    row = await session.scalar(
        select(Value).where(Value.name == f"CLAN_BUFF_{int(unity_idpk)}")
    )
    if not row or not row.value_str:
        return False

    try:
        payload = loads_or_default(row.value_str, {})
        if not isinstance(payload, dict):
            return False
        if payload.get("type") != "income_boost":
            return False

        ends_at_raw = payload.get("ends_at")
        if not ends_at_raw:
            return False

        return datetime.now() <= datetime.fromisoformat(str(ends_at_raw))
    except Exception:
        return False
