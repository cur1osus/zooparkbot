import json
import math
import random
from datetime import datetime, timedelta
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db import Unity, User, Value
from init_bot import bot
from tools.animals import add_animal
from tools.aviaries import get_remain_seats
from tools.income import income_

PROJECT_KEY_PREFIX = "UNITY_PROJECT_"
CHEST_KEY_PREFIX = "CLAN_CHESTS_"
PROJECT_DURATION_HOURS = 72


def _now() -> datetime:
    return datetime.now()


def _project_key(unity_idpk: int) -> str:
    return f"{PROJECT_KEY_PREFIX}{unity_idpk}"


def _chest_key(user_idpk: int) -> str:
    return f"{CHEST_KEY_PREFIX}{user_idpk}"


def _clan_size_factor(member_count: int) -> float:
    # Each additional clan member raises the project target.
    # Keep growth predictable and cap it to protect the economy.
    return min(3.0, 1.0 + 0.12 * max(0, member_count - 1))


def _base_targets(level: int) -> tuple[int, int]:
    # x4 scaled baseline from design discussion
    rub = int(200_000 * (1.75 ** max(0, level - 1)))
    usd = int(20_000 * (1.75 ** max(0, level - 1)))
    return rub, usd


def _new_project(unity: Unity, level: int = 1) -> dict[str, Any]:
    member_count = max(1, unity.get_number_members())
    factor = _clan_size_factor(member_count)
    base_rub, base_usd = _base_targets(level)
    target_rub = int(base_rub * factor)
    target_usd = int(base_usd * factor)
    start = _now()
    end = start + timedelta(hours=PROJECT_DURATION_HOURS)

    project_names = [
        "Заповедник",
        "Центр выкупа",
        "Ветеринарная станция",
        "Расширение вольеров",
        "Зоопитомник",
    ]
    name = random.choice(project_names)

    # Map project names to buff types
    buff_map = {
        "Заповедник": "rare_chance",  # +5% к шансу редких животных (или просто визуально)
        "Центр выкупа": "shop_discount",  # Скидка в магазине
        "Ветеринарная станция": "income_boost",  # +10% к доходы
        "Расширение вольеров": "extra_seats",  # +1 место в вольере
        "Зоопитомник": "chest_luck",  # Повышенный уровень сундуков
    }
    buff_type = buff_map.get(name, "income_boost")

    return {
        "unity_idpk": unity.idpk,
        "name": name,
        "buff": buff_type,
        "level": level,
        "member_count": member_count,
        "difficulty_factor": round(factor, 2),
        "target": {"rub": target_rub, "usd": target_usd},
        "progress": {"rub": 0, "usd": 0},
        "contributors": {},
        "status": "active",
        "started_at": start.isoformat(),
        "ends_at": end.isoformat(),
        "rewarded": False,
    }


def _sync_project_target_with_clan(project: dict[str, Any], unity: Unity) -> bool:
    if project.get("status") != "active":
        return False

    level = int(project.get("level", 1) or 1)
    if level < 1:
        level = 1
    member_count = max(1, unity.get_number_members())
    factor = _clan_size_factor(member_count)
    base_rub, base_usd = _base_targets(level)

    target = {"rub": int(base_rub * factor), "usd": int(base_usd * factor)}
    changed = (
        project.get("member_count") != member_count
        or float(project.get("difficulty_factor", 0) or 0) != round(factor, 2)
        or project.get("target") != target
    )
    if not changed:
        return False

    project["member_count"] = member_count
    project["difficulty_factor"] = round(factor, 2)
    project["target"] = target
    return True


async def _get_or_create_value(session: AsyncSession, name: str) -> Value:
    row = await session.scalar(select(Value).where(Value.name == name))
    if row is None:
        row = Value(name=name, value_int=0, value_str="{}")
        session.add(row)
        await session.flush()
    return row


async def get_or_create_project(session: AsyncSession, unity: Unity) -> dict[str, Any]:
    row = await _get_or_create_value(session, _project_key(unity.idpk))
    payload: dict[str, Any]
    try:
        payload = json.loads(row.value_str or "{}")
    except Exception:
        payload = {}
    if not isinstance(payload, dict) or payload.get("status") != "active":
        payload = _new_project(
            unity=unity, level=int(payload.get("level", 0) or 0) + 1 if payload else 1
        )
        row.value_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        await session.flush()
    elif _sync_project_target_with_clan(payload, unity):
        row.value_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        await session.flush()
    return payload


async def save_project(
    session: AsyncSession, unity_idpk: int, payload: dict[str, Any]
) -> None:
    row = await _get_or_create_value(session, _project_key(unity_idpk))
    row.value_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _calc_ratio(payload: dict[str, Any]) -> float:
    pr = payload.get("progress", {})
    tg = payload.get("target", {})
    rub_ratio = int(pr.get("rub", 0)) / max(1, int(tg.get("rub", 1)))
    usd_ratio = int(pr.get("usd", 0)) / max(1, int(tg.get("usd", 1)))
    return min(1.0, (rub_ratio + usd_ratio) / 2)


def _reward_pool(success: bool, ratio: float) -> dict[str, int]:
    full = {"common": 12, "rare": 5, "epic": 2}
    if success:
        return full
    scale = max(0.2, min(1.0, ratio))
    return {
        k: max(1 if v > 0 else 0, int(math.floor(v * scale))) for k, v in full.items()
    }


async def _add_chests(
    session: AsyncSession, user_idpk: int, chests: dict[str, int]
) -> None:
    row = await _get_or_create_value(session, _chest_key(user_idpk))
    try:
        payload = json.loads(row.value_str or "{}")
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    for k, v in chests.items():
        payload[k] = int(payload.get(k, 0) or 0) + int(v or 0)
    row.value_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


async def settle_project_if_due(
    session: AsyncSession, unity: Unity, project: dict[str, Any], force: bool = False
) -> dict[str, Any] | None:
    if project.get("status") != "active":
        return None
    now = _now()
    end = datetime.fromisoformat(str(project.get("ends_at")))
    ratio = _calc_ratio(project)
    completed = ratio >= 1.0
    if not force and not completed and now < end:
        return None

    contributors = project.get("contributors", {}) or {}
    totals: dict[int, int] = {}
    total_weight = 0
    for uid, c in contributors.items():
        w = int((c or {}).get("rub", 0)) + int((c or {}).get("usd", 0)) * 10
        if w > 0:
            totals[int(uid)] = w
            total_weight += w

    project["status"] = "completed" if completed else "expired"
    project["completed_at"] = now.isoformat()

    if total_weight > 0 and not project.get("rewarded"):
        pool = _reward_pool(success=completed, ratio=ratio)
        for chest_kind, chest_count in pool.items():
            if chest_count <= 0:
                continue
            # proportional integer distribution + largest remainders
            raw = [(uid, chest_count * (w / total_weight)) for uid, w in totals.items()]
            base = {uid: int(math.floor(val)) for uid, val in raw}
            remain = chest_count - sum(base.values())
            if remain > 0:
                raw.sort(key=lambda x: x[1] - math.floor(x[1]), reverse=True)
                for uid, _ in raw[:remain]:
                    base[uid] += 1
            for uid, cnt in base.items():
                if cnt <= 0:
                    continue
                await _add_chests(
                    session=session, user_idpk=uid, chests={chest_kind: cnt}
                )

        # Apply global clan buff
        if completed:
            buff_type = project.get("buff", "income_boost")
            buff_row = await _get_or_create_value(session, f"CLAN_BUFF_{unity.idpk}")
            buff_payload = {
                "type": buff_type,
                "ends_at": (now + timedelta(hours=72)).isoformat(),
                "name": project.get("name", "Проект"),
            }
            buff_row.value_str = json.dumps(buff_payload, ensure_ascii=False)

        # Calculate MVP (Top 1 contributor) if completed
        mvp_uid = None
        if completed and totals:
            mvp_uid = max(totals.items(), key=lambda item: item[1])[0]
            await _add_chests(session=session, user_idpk=mvp_uid, chests={"epic": 1})

        # notify contributors
        users = await session.scalars(
            select(User).where(User.idpk.in_(list(totals.keys())))
        )
        users_by_id = {u.idpk: u for u in users.all()}
        for uid in totals.keys():
            u = users_by_id.get(uid)
            if not u:
                continue
            chest_row = await _get_or_create_value(session, _chest_key(uid))
            try:
                chest_payload = json.loads(chest_row.value_str or "{}")
            except Exception:
                chest_payload = {}

            mvp_text = (
                "\n🌟 Ты стал MVP проекта и получил +1 Эпический Сундук!"
                if uid == mvp_uid
                else ""
            )

            msg = (
                f"🏗 Клан-проект {project.get('name', 'Проект')} {('завершён' if completed else 'не закрыт за 3 дня')}\n"
                f"Твой вклад учтён. Текущие сундуки: "
                f"обычн {int(chest_payload.get('common', 0))}, редк {int(chest_payload.get('rare', 0))}, эпик {int(chest_payload.get('epic', 0))}"
                f"{mvp_text}"
            )
            try:
                await bot.send_message(chat_id=u.id_user, text=msg)
            except Exception:
                pass

    project["rewarded"] = True
    await save_project(session=session, unity_idpk=unity.idpk, payload=project)
    return project


async def contribute_to_project(
    session: AsyncSession,
    user: User,
    unity: Unity,
    rub: int = 0,
    usd: int = 0,
) -> tuple[bool, str, dict[str, Any]]:
    project = await get_or_create_project(session=session, unity=unity)
    if project.get("status") != "active":
        return False, "Проект сейчас неактивен", project

    await settle_project_if_due(session=session, unity=unity, project=project)
    if project.get("status") != "active":
        return False, "Текущий проект уже закрыт/истёк", project

    rub = max(0, int(rub or 0))
    usd = max(0, int(usd or 0))
    if rub == 0 and usd == 0:
        return False, "Нулевой вклад", project
    if user.rub < rub or user.usd < usd:
        return False, "Недостаточно средств", project

    user.rub -= rub
    user.usd -= usd
    c_map = project.setdefault("contributors", {})
    key = str(user.idpk)
    current = c_map.get(key) or {"rub": 0, "usd": 0, "name": ""}
    current["rub"] = int(current.get("rub", 0)) + rub
    current["usd"] = int(current.get("usd", 0)) + usd

    # Update latest known name
    current_name = "Игрок"
    user_any = cast(Any, user)
    username = getattr(user_any, "username", None)
    first_name = getattr(user_any, "first_name", None)
    if username:
        current_name = f"@{username}"
    elif first_name:
        current_name = str(first_name)
    current["name"] = current_name

    c_map[key] = current

    pr = project.setdefault("progress", {"rub": 0, "usd": 0})
    pr["rub"] = int(pr.get("rub", 0)) + rub
    pr["usd"] = int(pr.get("usd", 0)) + usd

    await save_project(session=session, unity_idpk=unity.idpk, payload=project)
    await settle_project_if_due(session=session, unity=unity, project=project)

    return True, "Вклад принят", project


async def settle_all_due_projects(session: AsyncSession) -> int:
    rows = await session.scalars(select(Unity))
    count = 0
    for unity in rows.all():
        project = await get_or_create_project(session=session, unity=unity)
        changed = await settle_project_if_due(
            session=session, unity=unity, project=project
        )
        if changed is not None:
            count += 1
    return count


def format_project_text(
    project: dict[str, Any], active_buff: dict[str, Any] | None = None
) -> str:
    pr = project.get("progress", {})
    tg = project.get("target", {})
    ratio = _calc_ratio(project)
    raw_status = str(project.get("status", "active"))
    status_map = {
        "active": "активен",
        "completed": "завершён",
        "expired": "истёк",
    }
    status = status_map.get(raw_status, raw_status)
    ends = project.get("ends_at", "-")
    member_count = int(project.get("member_count", 1) or 1)
    if member_count < 1:
        member_count = 1
    difficulty_factor = float(project.get("difficulty_factor", 1.0) or 1.0)

    bar_length = 10
    filled_length = int(bar_length * ratio)
    bar = "█" * filled_length + "░" * (bar_length - filled_length)

    rub_pr = f"{int(pr.get('rub', 0)):,}".replace(",", " ")
    rub_tg = f"{int(tg.get('rub', 0)):,}".replace(",", " ")
    usd_pr = f"{int(pr.get('usd', 0)):,}".replace(",", " ")
    usd_tg = f"{int(tg.get('usd', 0)):,}".replace(",", " ")

    # Calculate top contributors
    contributors = project.get("contributors", {}) or {}
    leaderboard = []
    for uid, c_data in contributors.items():
        w = int((c_data or {}).get("rub", 0)) + int((c_data or {}).get("usd", 0)) * 10
        if w > 0:
            name = str(c_data.get("name") or f"Игрок {uid}")
            leaderboard.append(
                {
                    "name": name,
                    "weight": w,
                    "rub": c_data.get("rub", 0),
                    "usd": c_data.get("usd", 0),
                }
            )

    leaderboard.sort(key=lambda x: x["weight"], reverse=True)
    top_3 = leaderboard[:3]

    board_text = ""
    if top_3:
        board_text = "\n\n🏆 <b>Топ вкладчиков:</b>\n"
        medals = ["🥇", "🥈", "🥉"]
        for idx, entry in enumerate(top_3):
            # Try to show how much they contributed roughly
            contrib_str = []
            if entry["usd"] > 0:
                contrib_str.append(f"{entry['usd']:,}".replace(",", " ") + " USD")
            if entry["rub"] > 0:
                contrib_str.append(f"{entry['rub']:,}".replace(",", " ") + " RUB")
            c_text = " + ".join(contrib_str) if contrib_str else "0"
            board_text += f"{medals[idx]} {entry['name']} — <i>{c_text}</i>\n"

    # Display prospective project buff
    buff_type = project.get("buff", "")
    buff_desc_map = {
        "rare_chance": "🍀 +Шанс на редких животных",
        "shop_discount": "🛍 Скидка на покупку животных",
        "income_boost": "💰 +10% к доходу (бафф)",
        "extra_seats": "🏠 +Места в вольере",
        "chest_luck": "🎁 Улучшенный дроп сундуков",
    }
    buff_str = ""
    active_buff_str = ""
    if active_buff:
        btype = active_buff.get("type", "")
        bname = active_buff.get("name", "Проект")
        ends_raw = active_buff.get("ends_at", "")
        active_buff_str = f"✨ <b>Действующий бафф клана:</b> {buff_desc_map.get(btype, btype)} (от '{bname}') до {ends_raw[:16].replace('T', ' ')}\n\n"

    success_pool = _reward_pool(success=True, ratio=1.0)
    current_fail_pool = _reward_pool(success=False, ratio=ratio)
    reward_lines = [
        "🎁 <b>Награды проекта:</b>",
        (
            "За успех вкладчики делят: "
            f"обычн {success_pool['common']}, редк {success_pool['rare']}, эпик {success_pool['epic']}"
        ),
        f"Бафф клана на 72 часа: {buff_desc_map.get(buff_type, buff_type)}",
        "MVP получает +1 эпический сундук",
    ]
    if raw_status == "active":
        reward_lines.append(
            "Если проект закончится сейчас, вкладчики получат: "
            f"обычн {current_fail_pool['common']}, редк {current_fail_pool['rare']}, эпик {current_fail_pool['epic']}"
        )
    reward_text = "\n".join(reward_lines)

    return (
        f"🏗 Клан-проект: {project.get('name', 'Заповедник')} L{int(project.get('level', 1))}\n"
        f"Статус: {status}\n"
        f"Участников в клане: {member_count} | Сложность цели: x{difficulty_factor:.2f}\n"
        f"Дедлайн: {ends[:16].replace('T', ' ')}\n\n"
        f"{active_buff_str}"
        f"{buff_str}"
        f"RUB: {rub_pr} / {rub_tg}\n"
        f"USD: {usd_pr} / {usd_tg}\n"
        f"Прогресс: [{bar}] {ratio * 100:.1f}%{board_text}\n\n"
        f"{reward_text}\n\n"
        f"Если проект не закрыт за 3 дня — награды (сундуки) получают только вкладчики."
    )


async def get_active_clan_buff(
    session: AsyncSession, unity_idpk: int
) -> dict[str, Any] | None:
    row = await _get_or_create_value(session, f"CLAN_BUFF_{unity_idpk}")
    try:
        payload = json.loads(row.value_str or "{}")
        if not payload:
            return None
        ends_at = datetime.fromisoformat(str(payload.get("ends_at")))
        if _now() > ends_at:
            return None
        return payload
    except Exception:
        return None


def get_chests_balance_from_value(value_str: str | None) -> dict[str, int]:
    try:
        payload = json.loads(value_str or "{}")
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    return {
        "common": int(payload.get("common", 0) or 0),
        "rare": int(payload.get("rare", 0) or 0),
        "epic": int(payload.get("epic", 0) or 0),
    }


async def get_user_chests(session: AsyncSession, user_idpk: int) -> dict[str, int]:
    row = await _get_or_create_value(session, _chest_key(user_idpk))
    return get_chests_balance_from_value(row.value_str)


def _income_scale(income_per_minute_rub: int) -> float:
    # smooth scaling for high-income players, capped to protect economy
    val = max(0, int(income_per_minute_rub or 0))
    # ~1.0 at low income, ~2.0 around 5k/min, cap at 2.8
    import math

    return min(2.8, 1.0 + 0.35 * math.log10(1 + val))


def _chest_reward_roll(kind: str, income_per_minute_rub: int) -> tuple[int, int]:
    # returns (rub, usd)
    scale = _income_scale(income_per_minute_rub)
    if kind == "common":
        rub = random.randint(15_000, 60_000)
        usd = random.randint(80, 260)
    elif kind == "rare":
        rub = random.randint(80_000, 260_000)
        usd = random.randint(400, 1400)
    else:  # epic
        rub = random.randint(300_000, 900_000)
        usd = random.randint(1500, 5000)
    return int(rub * scale), int(usd * scale)


def _animal_drop_roll(kind: str) -> tuple[str | None, int]:
    # returns (animal_code_name or None, quantity)
    # chances and rarity pools by chest type
    roll = random.random()
    if kind == "common":
        if roll > 0.15:
            return None, 0
        rarity = random.choices(["_rare", "_epic"], weights=[0.9, 0.1], k=1)[0]
    elif kind == "rare":
        if roll > 0.35:
            return None, 0
        rarity = random.choices(
            ["_rare", "_epic", "_mythical"], weights=[0.65, 0.3, 0.05], k=1
        )[0]
    else:  # epic
        if roll > 0.60:
            return None, 0
        rarity = random.choices(
            ["_epic", "_mythical", "_leg"], weights=[0.55, 0.35, 0.10], k=1
        )[0]

    base_animal = random.choice(["animal1", "animal2", "animal3"])
    qty = 1 if kind != "epic" else random.choice([1, 1, 2])
    return f"{base_animal}{rarity}", qty


async def open_user_chests(
    session: AsyncSession,
    user: User,
    open_common: int = 0,
    open_rare: int = 0,
    open_epic: int = 0,
) -> tuple[bool, str, dict[str, int], dict[str, Any]]:
    row = await _get_or_create_value(session, _chest_key(user.idpk))
    bal = get_chests_balance_from_value(row.value_str)

    oc = max(0, int(open_common or 0))
    orr = max(0, int(open_rare or 0))
    oe = max(0, int(open_epic or 0))

    if oc == 0 and orr == 0 and oe == 0:
        if bal["epic"] > 0:
            oe = 1
        elif bal["rare"] > 0:
            orr = 1
        elif bal["common"] > 0:
            oc = 1
        else:
            return (
                False,
                "Нет сундуков для открытия",
                bal,
                {"rub": 0, "usd": 0, "animals": []},
            )

    if oc > bal["common"] or orr > bal["rare"] or oe > bal["epic"]:
        return False, "Недостаточно сундуков", bal, {"rub": 0, "usd": 0, "animals": []}

    income_now = int(await income_(session=session, user=user))
    total_rub = 0
    total_usd = 0
    animal_drops: list[dict[str, Any]] = []

    remain_seats = int(await get_remain_seats(session=session, user=user))

    def apply_one(kind: str):
        nonlocal total_rub, total_usd, remain_seats
        r, u = _chest_reward_roll(kind, income_now)
        total_rub += r
        total_usd += u
        code, qty = _animal_drop_roll(kind)
        if code and qty > 0 and remain_seats > 0:
            q = min(qty, remain_seats)
            if q > 0:
                animal_drops.append({"code_name": code, "quantity": q})
                remain_seats -= q

    for _ in range(oc):
        apply_one("common")
    for _ in range(orr):
        apply_one("rare")
    for _ in range(oe):
        apply_one("epic")

    for drop in animal_drops:
        await add_animal(user, drop["code_name"], int(drop["quantity"]))

    bal["common"] -= oc
    bal["rare"] -= orr
    bal["epic"] -= oe

    user.rub += total_rub
    user.usd += total_usd

    row.value_str = json.dumps(bal, ensure_ascii=False, separators=(",", ":"))
    return (
        True,
        "Сундуки открыты",
        bal,
        {"rub": total_rub, "usd": total_usd, "animals": animal_drops},
    )
