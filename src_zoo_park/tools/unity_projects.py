import json
import math
import random
from datetime import datetime, timedelta
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from text_utils import format_iso_datetime_short

from db import Animal, Unity, User, Value
from db.structured_state import count_unity_members, list_unity_member_ids
from init_bot import bot
from tools.animals import add_animal
from tools.aviaries import get_remain_seats
from tools.income import income_

PROJECT_KEY_PREFIX = "UNITY_PROJECT_"
CHEST_KEY_PREFIX = "CLAN_CHESTS_"
STREAK_KEY_PREFIX = "CLAN_STREAK_"
STATS_KEY_PREFIX = "USER_PROJECT_STATS_"
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


def _new_project(unity: Unity, member_count: int, level: int = 1) -> dict[str, Any]:
    member_count = max(1, int(member_count or 1))
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


def _sync_project_target_with_clan(project: dict[str, Any], member_count: int) -> bool:
    if project.get("status") != "active":
        return False

    level = int(project.get("level", 1) or 1)
    if level < 1:
        level = 1
    member_count = max(1, int(member_count or 1))
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
    member_count = await count_unity_members(session=session, unity=unity)
    payload: dict[str, Any]
    try:
        payload = json.loads(row.value_str or "{}")
    except Exception:
        payload = {}
    if not isinstance(payload, dict) or payload.get("status") != "active":
        payload = _new_project(
            unity=unity,
            member_count=member_count,
            level=int(payload.get("level", 0) or 0) + 1 if payload else 1,
        )
        row.value_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        await session.flush()
    elif _sync_project_target_with_clan(payload, member_count=member_count):
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
    rub_ratio = min(1.0, int(pr.get("rub", 0)) / max(1, int(tg.get("rub", 1))))
    usd_ratio = min(1.0, int(pr.get("usd", 0)) / max(1, int(tg.get("usd", 1))))
    return (rub_ratio + usd_ratio) / 2


def _is_project_completed(payload: dict[str, Any]) -> bool:
    pr = payload.get("progress", {})
    tg = payload.get("target", {})
    return int(pr.get("rub", 0)) >= int(tg.get("rub", 0)) and int(
        pr.get("usd", 0)
    ) >= int(tg.get("usd", 0))


def _reward_pool(
    success: bool, ratio: float, member_count: int, level: int
) -> dict[str, int]:
    member_count = max(1, int(member_count or 1))
    level = max(1, int(level or 1))
    size_scale = min(1.0, 0.12 + 0.18 * member_count)
    level_scale = min(1.8, 1.0 + 0.15 * max(0, level - 1))
    full = {
        "common": max(2, int(math.floor(12 * size_scale * level_scale))),
        "rare": max(1, int(math.floor(5 * size_scale * level_scale))),
        "epic": max(0, int(math.floor(2 * size_scale * level_scale))),
    }
    if success:
        return full
    scale = max(0.2, min(1.0, ratio))
    return {
        k: max(1 if v > 0 else 0, int(math.floor(v * scale))) for k, v in full.items()
    }


def get_project_reward_preview(project: dict[str, Any]) -> dict[str, Any]:
    member_count = max(1, int(project.get("member_count", 1) or 1))
    level = max(1, int(project.get("level", 1) or 1))
    ratio = _calc_ratio(project)
    return {
        "member_count": member_count,
        "level": level,
        "success": _reward_pool(
            success=True,
            ratio=1.0,
            member_count=member_count,
            level=level,
        ),
        "current": _reward_pool(
            success=False,
            ratio=ratio,
            member_count=member_count,
            level=level,
        ),
        "mvp_epic_bonus": 1,
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


async def _get_streak(session: AsyncSession, unity_idpk: int) -> int:
    row = await _get_or_create_value(session, f"{STREAK_KEY_PREFIX}{unity_idpk}")
    return int(row.value_int or 0)


async def _set_streak(session: AsyncSession, unity_idpk: int, value: int) -> None:
    row = await _get_or_create_value(session, f"{STREAK_KEY_PREFIX}{unity_idpk}")
    row.value_int = value


async def _update_user_stats(session: AsyncSession, user_idpk: int, won: bool) -> None:
    row = await _get_or_create_value(session, f"{STATS_KEY_PREFIX}{user_idpk}")
    try:
        stats = json.loads(row.value_str or "{}")
    except Exception:
        stats = {}
    if not isinstance(stats, dict):
        stats = {}
    stats["participated"] = int(stats.get("participated", 0)) + 1
    if won:
        stats["won"] = int(stats.get("won", 0)) + 1
    row.value_str = json.dumps(stats, ensure_ascii=False, separators=(",", ":"))


async def get_user_project_stats(session: AsyncSession, user_idpk: int) -> dict:
    row = await _get_or_create_value(session, f"{STATS_KEY_PREFIX}{user_idpk}")
    try:
        stats = json.loads(row.value_str or "{}")
        return stats if isinstance(stats, dict) else {}
    except Exception:
        return {}


async def _notify_milestone(
    session: AsyncSession, unity: Unity, project: dict, milestone: int
) -> None:
    """Send milestone notification (50% or 75%) to all clan members."""
    name = project.get("name", "Проект")
    member_idpks = await list_unity_member_ids(session=session, unity=unity)
    users = await session.scalars(select(User).where(User.idpk.in_(member_idpks)))
    for u in users.all():
        try:
            await bot.send_message(
                chat_id=u.id_user,
                text=(
                    f"🏗 <b>{name}</b> — {milestone}% выполнено!\n"
                    f"Навалимся вместе — до победы осталось совсем чуть-чуть 💪"
                ),
            )
        except Exception:
            pass


async def settle_project_if_due(
    session: AsyncSession, unity: Unity, project: dict[str, Any], force: bool = False
) -> dict[str, Any] | None:
    if project.get("status") != "active":
        return None
    now = _now()
    end = datetime.fromisoformat(str(project.get("ends_at")))
    ratio = _calc_ratio(project)
    completed = _is_project_completed(project)
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

    # Streak: increment on success, reset on failure
    old_streak = await _get_streak(session=session, unity_idpk=unity.idpk)
    new_streak = (old_streak + 1) if completed else 0
    await _set_streak(session=session, unity_idpk=unity.idpk, value=new_streak)
    streak_bonus = completed and new_streak > 0 and new_streak % 3 == 0

    if total_weight > 0 and not project.get("rewarded"):
        pool = _reward_pool(
            success=completed,
            ratio=ratio,
            member_count=int(project.get("member_count", 1) or 1),
            level=int(project.get("level", 1) or 1),
        )
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

        # Apply global clan buff (with fast-completion bonus hours)
        if completed:
            buff_type = project.get("buff", "income_boost")
            buff_row = await _get_or_create_value(session, f"CLAN_BUFF_{unity.idpk}")
            base_hours = 72
            if now < end:
                days_remaining = (end - now).total_seconds() / 86400
                base_hours += min(48, int(days_remaining) * 24)
            buff_payload = {
                "type": buff_type,
                "ends_at": (now + timedelta(hours=base_hours)).isoformat(),
                "name": project.get("name", "Проект"),
            }
            buff_row.value_str = json.dumps(buff_payload, ensure_ascii=False)

        # Streak bonus: +1 rare chest to all clan members every 3 wins
        if streak_bonus:
            member_idpks = await list_unity_member_ids(session=session, unity=unity)
            for mid in member_idpks:
                await _add_chests(session=session, user_idpk=mid, chests={"rare": 1})

        # Calculate MVP (Top 1 contributor) if completed
        mvp_uid = None
        if completed and totals:
            mvp_uid = max(totals.items(), key=lambda item: item[1])[0]
            await _add_chests(session=session, user_idpk=mvp_uid, chests={"epic": 1})

        # Update history and notify contributors
        users = await session.scalars(
            select(User).where(User.idpk.in_(list(totals.keys())))
        )
        users_by_id = {u.idpk: u for u in users.all()}
        for uid in totals.keys():
            await _update_user_stats(session=session, user_idpk=uid, won=completed)
            u = users_by_id.get(uid)
            if not u:
                continue
            chest_row = await _get_or_create_value(session, _chest_key(uid))
            try:
                chest_payload = json.loads(chest_row.value_str or "{}")
            except Exception:
                chest_payload = {}

            lines = [
                f"🏗 <b>{project.get('name', 'Проект')}</b> — {'✅ завершён!' if completed else '❌ не закрыт за 3 дня'}",
                f"🟤 ×{int(chest_payload.get('common', 0))}  🔵 ×{int(chest_payload.get('rare', 0))}  🟣 ×{int(chest_payload.get('epic', 0))}",
            ]
            if uid == mvp_uid:
                lines.append("🌟 Ты MVP проекта — +1 🟣 эпический сундук!")
            if streak_bonus:
                lines.append(f"🔥 Стрик ×{new_streak} — +1 🔵 редкий сундук всем участникам клана!")
            elif completed:
                lines.append(f"🔥 Стрик клана: {new_streak}")
            try:
                await bot.send_message(chat_id=u.id_user, text="\n".join(lines))
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

    requested_rub = max(0, int(rub or 0))
    requested_usd = max(0, int(usd or 0))
    if requested_rub == 0 and requested_usd == 0:
        return False, "Нулевой вклад", project

    pr = project.setdefault("progress", {"rub": 0, "usd": 0})
    tg = project.get("target", {}) or {}
    need_rub = max(0, int(tg.get("rub", 0)) - int(pr.get("rub", 0)))
    need_usd = max(0, int(tg.get("usd", 0)) - int(pr.get("usd", 0)))

    rub = min(requested_rub, need_rub)
    usd = min(requested_usd, need_usd)

    if rub == 0 and usd == 0:
        return False, "Эта цель уже закрыта", project
    if user.rub < rub or user.usd < usd:
        return False, "Недостаточно средств", project

    user.rub -= rub
    user.usd -= usd

    # If clan has active income buff, apply +10% bonus to project progress.
    # Bonus affects project bar only (not user spend, not contributor MVP weights).
    rub_progress_add = rub
    usd_progress_add = usd
    active_buff = await get_active_clan_buff(session=session, unity_idpk=unity.idpk)
    if active_buff and str(active_buff.get("type", "")) == "income_boost":
        rub_progress_add += int(rub * 0.10)
        usd_progress_add += int(usd * 0.10)

        # Never overflow project targets because of the bonus.
        rub_progress_add = min(rub_progress_add, need_rub)
        usd_progress_add = min(usd_progress_add, need_usd)

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

    pr["rub"] = int(pr.get("rub", 0)) + rub_progress_add
    pr["usd"] = int(pr.get("usd", 0)) + usd_progress_add

    # Milestone notifications (50% / 75%)
    old_ratio = _calc_ratio({"progress": {"rub": int(pr.get("rub", 0)) - rub_progress_add, "usd": int(pr.get("usd", 0)) - usd_progress_add}, "target": tg})
    new_ratio = _calc_ratio(project)
    for threshold in (50, 75):
        flag = f"notified_{threshold}"
        if not project.get(flag) and old_ratio < threshold / 100 <= new_ratio:
            project[flag] = True
            await _notify_milestone(session=session, unity=unity, project=project, milestone=threshold)

    await save_project(session=session, unity_idpk=unity.idpk, payload=project)
    await settle_project_if_due(session=session, unity=unity, project=project)

    bonus_rub = rub_progress_add - rub
    bonus_usd = usd_progress_add - usd
    bonus_suffix = (
        f" (бафф: +{bonus_rub} RUB, +{bonus_usd} USD)"
        if (bonus_rub > 0 or bonus_usd > 0)
        else ""
    )

    if rub < requested_rub or usd < requested_usd:
        return (
            True,
            f"Вклад принят частично: +{rub} RUB, +{usd} USD{bonus_suffix}",
            project,
        )
    return True, f"Вклад принят{bonus_suffix}", project


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


def _format_remaining(ends_at: str) -> str:
    try:
        end = datetime.fromisoformat(ends_at)
        delta = end - datetime.now()
        if delta.total_seconds() <= 0:
            return "время вышло"
        s = int(delta.total_seconds())
        days, s = divmod(s, 86400)
        hours, s = divmod(s, 3600)
        minutes = s // 60
        if days > 0:
            return f"{days}д {hours}ч"
        if hours > 0:
            return f"{hours}ч {minutes}мин"
        return f"{minutes}мин"
    except Exception:
        return "—"


def _make_bar(ratio: float, length: int = 10) -> str:
    filled = int(length * min(ratio, 1.0))
    return "█" * filled + "░" * (length - filled)


def _fmt_num(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def format_project_text(
    project: dict[str, Any],
    active_buff: dict[str, Any] | None = None,
    user_idpk: int | None = None,
    user_stats: dict | None = None,
) -> str:
    pr = project.get("progress", {})
    tg = project.get("target", {})
    ratio = _calc_ratio(project)
    raw_status = str(project.get("status", "active"))
    ends = project.get("ends_at", "-")
    level = int(project.get("level", 1))
    name = project.get("name", "Проект")

    status_map = {"active": ("🟢", "активен"), "completed": ("✅", "завершён"), "expired": ("🔴", "истёк")}
    status_emoji, status_label = status_map.get(raw_status, ("⚪", raw_status))

    buff_desc_map = {
        "rare_chance": "🍀 +Шанс на редких животных",
        "shop_discount": "🛍 Скидка на покупку животных",
        "income_boost": "💰 +10% к доходу",
        "extra_seats": "🏠 +5% мест в вольере",
        "chest_luck": "🎁 Улучшенный дроп сундуков",
    }

    rub_current = min(int(pr.get("rub", 0)), int(tg.get("rub", 0)))
    usd_current = min(int(pr.get("usd", 0)), int(tg.get("usd", 0)))
    rub_target = int(tg.get("rub", 0))
    usd_target = int(tg.get("usd", 0))
    rub_ratio = rub_current / rub_target if rub_target > 0 else 0
    usd_ratio = usd_current / usd_target if usd_target > 0 else 0

    lines = [
        f"🏗 <b>{name}  ·  Уровень {level}</b>",
        f"{status_emoji} {status_label}  ·  ⏳ {_format_remaining(ends)}",
    ]

    # Active buff block
    if active_buff:
        btype = active_buff.get("type", "")
        ends_raw = active_buff.get("ends_at", "")
        lines += [
            "",
            f"✨ <b>Бафф клана активен:</b>",
            f"└ {buff_desc_map.get(btype, btype)}  · до {format_iso_datetime_short(ends_raw)}",
        ]

    # Progress section
    lines += [
        "",
        "━━━━━━━━━━━━━━",
        f"💰 <b>RUB</b>  <code>{_fmt_num(rub_current)} / {_fmt_num(rub_target)}</code>",
        f"[{_make_bar(rub_ratio)}] {rub_ratio * 100:.0f}%",
        f"💵 <b>USD</b>  <code>{_fmt_num(usd_current)} / {_fmt_num(usd_target)}</code>",
        f"[{_make_bar(usd_ratio)}] {usd_ratio * 100:.0f}%",
        "",
        f"Общий прогресс: <b>{ratio * 100:.1f}%</b>",
    ]

    # Leaderboard
    contributors = project.get("contributors", {}) or {}
    leaderboard = []
    for uid, c_data in contributors.items():
        w = int((c_data or {}).get("rub", 0)) + int((c_data or {}).get("usd", 0)) * 10
        if w > 0:
            leaderboard.append({
                "name": str(c_data.get("name") or f"Игрок {uid}"),
                "weight": w,
                "rub": int(c_data.get("rub", 0)),
                "usd": int(c_data.get("usd", 0)),
            })
    leaderboard.sort(key=lambda x: x["weight"], reverse=True)
    top_3 = leaderboard[:3]

    if top_3:
        medals = ["🥇", "🥈", "🥉"]
        lines += ["", "━━━━━━━━━━━━━━", "🏆 <b>Лидеры</b>"]
        for idx, entry in enumerate(top_3):
            parts = [f"{_fmt_num(entry['rub'])}₽"]
            if entry["usd"] > 0:
                parts.append(f"{_fmt_num(entry['usd'])}$")
            lines.append(f"{medals[idx]} {entry['name']}  ·  {' + '.join(parts)}")

    # Rewards
    buff_type = project.get("buff", "")
    reward_preview = get_project_reward_preview(project)
    success_pool = reward_preview["success"]
    current_pool = reward_preview["current"]

    lines += [
        "",
        "━━━━━━━━━━━━━━",
        "🎁 <b>Награда за успех</b>",
        f"🟤 ×{success_pool['common']}  🔵 ×{success_pool['rare']}  🟣 ×{success_pool['epic']}",
        f"🌟 MVP: +1 🟣 эпический сундук",
        f"⚡ Бафф: {buff_desc_map.get(buff_type, buff_type)}",
    ]
    if raw_status == "active":
        lines += [
            "",
            f"<i>Сейчас: 🟤×{current_pool['common']}  🔵×{current_pool['rare']}  🟣×{current_pool['epic']}</i>",
        ]

    # Personal contribution
    if user_idpk is not None:
        contrib = contributors.get(str(user_idpk))
        if contrib:
            my_rub = int(contrib.get("rub", 0))
            my_usd = int(contrib.get("usd", 0))
            parts = [f"{_fmt_num(my_rub)}₽"]
            if my_usd > 0:
                parts.append(f"{_fmt_num(my_usd)}$")
            lines += ["", f"👤 Твой вклад: {' + '.join(parts)}"]

    # Personal history
    if user_stats:
        participated = int(user_stats.get("participated", 0))
        won = int(user_stats.get("won", 0))
        if participated > 0:
            lines.append(f"📊 Статистика: {participated} проектов, {won} побед")

    return "\n".join(lines)


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


async def _animal_drop_roll(
    session: AsyncSession, kind: str, active_buff: dict | None = None
) -> tuple[str | None, int]:
    # returns (animal_code_name or None, quantity)
    # chances and rarity pools by chest type
    roll = random.random()
    base_drop_chance = 0.15 if kind == "common" else 0.35 if kind == "rare" else 0.60

    # Rare chance buff: +5% to ANY animal drop chance from chest.
    if active_buff and active_buff.get("type") == "rare_chance":
        base_drop_chance += 0.05

    if roll > base_drop_chance:
        return None, 0

    if kind == "common":
        pop = ["_rare", "_epic"]
        w = [0.9, 0.1]
        if active_buff and active_buff.get("type") == "chest_luck":
            w = [0.75, 0.25]  # shift towards Epic
    elif kind == "rare":
        pop = ["_rare", "_epic", "_mythical"]
        w = [0.65, 0.3, 0.05]
        if active_buff and active_buff.get("type") == "chest_luck":
            w = [0.45, 0.45, 0.10]  # shift towards Epic/Mythical
    else:  # epic
        pop = ["_epic", "_mythical", "_leg"]
        w = [0.55, 0.35, 0.10]
        if active_buff and active_buff.get("type") == "chest_luck":
            w = [0.40, 0.45, 0.15]  # shift towards Mythical/Leg

    rarity = random.choices(pop, weights=w, k=1)[0]

    # Use a query to get real animal code names from the database
    # Matching the suffix (e.g. "_rare", "_epic", etc.)
    result = await session.execute(
        select(Animal.code_name).where(Animal.code_name.like(f"%{rarity}"))
    )
    codes = [row[0] for row in result.all()]

    if not codes:
        return None, 0

    code = random.choice(codes)
    qty = 1 if kind != "epic" else random.choice([1, 1, 2])
    return code, qty


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
    
    # Get active clan buff if any (affects drop rates and rarities)
    active_buff = None
    if user.current_unity:
        from tools.unity import get_unity_idpk
        unity_idpk = get_unity_idpk(user.current_unity)
        if unity_idpk:
            active_buff = await get_active_clan_buff(session=session, unity_idpk=unity_idpk)

    async def apply_one(kind: str):
        nonlocal total_rub, total_usd, remain_seats
        r, u = _chest_reward_roll(kind, income_now)
        total_rub += r
        total_usd += u
        
        # Now async and with real DB lookup
        code, qty = await _animal_drop_roll(session=session, kind=kind, active_buff=active_buff)
        if code and qty > 0 and remain_seats > 0:
            q = min(qty, remain_seats)
            if q > 0:
                animal_drops.append({"code_name": code, "quantity": q})
                remain_seats -= q

    for _ in range(oc):
        await apply_one("common")
    for _ in range(orr):
        await apply_one("rare")
    for _ in range(oe):
        await apply_one("epic")

    for drop in animal_drops:
        await add_animal(
            user,
            drop["code_name"],
            int(drop["quantity"]),
            session=session,
        )

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
