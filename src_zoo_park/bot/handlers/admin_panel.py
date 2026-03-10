import json
import math
from datetime import datetime
from enum import Enum

from aiogram import F, Router
from aiogram.filters.callback_data import CallbackData
from aiogram.filters import Command, StateFilter
from aiogram.fsm.state import any_state
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from bot.keyboards import ADMIN_PANEL_BUTTON
from bot.states import UserState
from config import ADMIN_ID
from db import NpcMemory, NpcState, User
from npc_agent.memory import (
    EVENT_KIND,
    GOAL_KIND,
    REFLECTION_KIND,
    RELATIONSHIP_KIND,
    build_npc_snapshot,
    ensure_npc_profile_memory,
)
from npc_agent.schedule import wake_npc_now
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from tools import formatter

router = Router()
flags = {"throttling_key": "default"}

SECTION_LABELS = {
    "overview": "Обзор",
    "tactics": "Тактики",
    "goals": "Цели",
    "reflections": "Рефлексия",
    "events": "События",
    "relationships": "Связи",
}

MAX_ADMIN_TEXT = 3900
USERS_PER_PAGE = 8
USER_EVENTS_PER_PAGE = 6


class AdminPanelAction(str, Enum):
    LIST = "list"
    REFRESH = "refresh"


class AdminNpcSection(str, Enum):
    OVERVIEW = "overview"
    TACTICS = "tactics"
    GOALS = "goals"
    REFLECTIONS = "reflections"
    EVENTS = "events"
    RELATIONSHIPS = "relationships"
    WAKE = "wake"


class AdminPanelCallback(CallbackData, prefix="admp"):
    action: AdminPanelAction


class AdminNpcCallback(CallbackData, prefix="admnpc"):
    npc_idpk: int
    section: AdminNpcSection


class AdminHistoryListCallback(CallbackData, prefix="ahlist"):
    page: int


class AdminHistoryUserCallback(CallbackData, prefix="ahuser"):
    user_idpk: int
    page: int
    list_page: int


class AdminHistoryEventCallback(CallbackData, prefix="ahevent"):
    user_idpk: int
    event_index: int
    return_page: int
    list_page: int


class AdminHistoryNoopCallback(CallbackData, prefix="ahnoop"):
    page: int


def _is_admin(user: User | None, telegram_id: int) -> bool:
    return telegram_id == ADMIN_ID or bool(user and user.id_user == ADMIN_ID)


def _fmt_number(value: int | float | None) -> str:
    return formatter.format_large_number(int(value or 0))


def _fmt_dt(value: datetime | None) -> str:
    if not value:
        return "-"
    return value.strftime("%d.%m %H:%M:%S")


def _fmt_iso(value: str | None) -> str:
    if not value:
        return "-"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value
    return parsed.strftime("%d.%m %H:%M:%S")


def _fmt_trait_delta(value: int | None) -> str:
    numeric = int(value or 0)
    return f"{numeric:+}"


def _load_payload(row: NpcMemory | None) -> dict:
    if not row or not row.payload:
        return {}
    try:
        value = json.loads(row.payload)
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _clip_text(text: str) -> str:
    if len(text) <= MAX_ADMIN_TEXT:
        return text
    return text[: MAX_ADMIN_TEXT - 20].rstrip() + "\n\n...[truncated]"


def _history_time_sort_key(value: str) -> datetime:
    return datetime.strptime(value, "%d.%m.%Y %H:%M:%S.%f")


def _safe_total_pages(total_items: int, per_page: int) -> int:
    return max(1, math.ceil(total_items / max(1, per_page)))


def _slice_page(items: list, page: int, per_page: int) -> tuple[list, int, int]:
    total_pages = _safe_total_pages(len(items), per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    end = start + per_page
    return items[start:end], page, total_pages


def _load_user_history_entries(target_user: User) -> list[dict]:
    if not target_user.history_moves or target_user.history_moves == "{}":
        return []
    try:
        payload = json.loads(target_user.history_moves)
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []
    entries = []
    for raw_time, event_text in payload.items():
        try:
            parsed_time = _history_time_sort_key(str(raw_time))
        except Exception:
            continue
        entries.append(
            {
                "raw_time": str(raw_time),
                "time": parsed_time,
                "event": str(event_text),
            }
        )
    entries.sort(key=lambda item: item["time"], reverse=True)
    return entries


def _history_event_preview(event_text: str, limit: int = 56) -> str:
    compact = " ".join(str(event_text).split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


async def _get_npc_users(session: AsyncSession) -> list[User]:
    rows = await session.scalars(
        select(User)
        .where(or_(User.id_user < 0, User.username.like("npc_%")))
        .order_by(User.id_user.asc())
    )
    return list(rows.all())


async def _get_users_with_history(session: AsyncSession) -> list[User]:
    rows = await session.scalars(
        select(User)
        .where(
            User.id_user > 0,
            User.history_moves != "{}",
        )
        .order_by(User.moves.desc(), User.idpk.desc())
    )
    users = list(rows.all())
    users.sort(
        key=lambda item: (
            _load_user_history_entries(item)[0]["time"]
            if _load_user_history_entries(item)
            else datetime.min,
            item.moves,
        ),
        reverse=True,
    )
    return users


async def _get_memory_rows(
    session: AsyncSession,
    npc: User,
    kind: str,
    limit: int = 8,
) -> list[NpcMemory]:
    order_by = [NpcMemory.importance.desc(), NpcMemory.updated_at.desc()]
    if kind == EVENT_KIND:
        order_by = [NpcMemory.created_at.desc()]
    rows = await session.scalars(
        select(NpcMemory)
        .where(
            NpcMemory.idpk_user == npc.idpk,
            NpcMemory.kind == kind,
            NpcMemory.status == "active",
        )
        .order_by(*order_by)
    )
    return list(rows.all()[:limit])


async def _build_admin_panel_text(session: AsyncSession) -> str:
    npcs = await _get_npc_users(session=session)
    if not npcs:
        return "Админ-панель NPC\n\nNPC не найдены."
    lines = ["Админ-панель NPC", "", f"Всего NPC: {len(npcs)}", ""]
    now = datetime.now()
    for npc in npcs:
        state = await session.scalar(
            select(NpcState).where(NpcState.idpk_user == npc.idpk)
        )
        due = bool(state and (state.next_wake_at is None or state.next_wake_at <= now))
        event_rows = await _get_memory_rows(
            session=session, npc=npc, kind=EVENT_KIND, limit=1
        )
        goal_rows = await _get_memory_rows(
            session=session, npc=npc, kind=GOAL_KIND, limit=6
        )
        last_event = _load_payload(event_rows[0]) if event_rows else {}
        lines.append(
            f"- {npc.nickname} | id {npc.idpk} | due {'yes' if due else 'no'} | next {_fmt_dt(state.next_wake_at if state else None)}"
        )
        if last_event:
            lines.append(
                f"  last: {last_event.get('action', {}).get('name', '-')} / {last_event.get('result', {}).get('summary', '-')[:90]}"
            )
        lines.append(f"  active goals: {len(goal_rows)}")
    lines.extend(["", "Выбери NPC кнопкой ниже."])
    return "\n".join(lines)


def _build_admin_panel_keyboard(npcs: list[User]):
    builder = InlineKeyboardBuilder()
    builder.button(
        text="История пользователей",
        callback_data=AdminHistoryListCallback(page=1),
    )
    for npc in npcs:
        builder.button(
            text=npc.nickname or f"NPC {npc.idpk}",
            callback_data=AdminNpcCallback(
                npc_idpk=npc.idpk,
                section=AdminNpcSection.OVERVIEW,
            ),
        )
    builder.button(
        text="Обновить",
        callback_data=AdminPanelCallback(action=AdminPanelAction.REFRESH),
    )
    builder.adjust(2)
    return builder.as_markup()


def _build_user_history_list_keyboard(users: list[User], page: int, total_pages: int):
    builder = InlineKeyboardBuilder()
    for target_user in users:
        history_entries = _load_user_history_entries(target_user)
        last_time = (
            history_entries[0]["time"].strftime("%d.%m %H:%M")
            if history_entries
            else "-"
        )
        builder.button(
            text=f"{target_user.nickname} ({len(history_entries)}) · {last_time}",
            callback_data=AdminHistoryUserCallback(
                user_idpk=target_user.idpk,
                page=1,
                list_page=page,
            ),
        )
    if total_pages > 1:
        builder.button(
            text="<",
            callback_data=AdminHistoryListCallback(page=max(1, page - 1)),
        )
        builder.button(
            text=f"{page}/{total_pages}",
            callback_data=AdminHistoryNoopCallback(page=page),
        )
        builder.button(
            text=">",
            callback_data=AdminHistoryListCallback(page=min(total_pages, page + 1)),
        )
    builder.button(
        text="К NPC",
        callback_data=AdminPanelCallback(action=AdminPanelAction.LIST),
    )
    builder.adjust(1, 1, 1, 3, 1)
    return builder.as_markup()


def _build_user_history_keyboard(
    target_user: User,
    page: int,
    total_pages: int,
    entries: list[dict],
    list_page: int,
):
    builder = InlineKeyboardBuilder()
    for entry in entries:
        builder.button(
            text=f"{entry['time'].strftime('%d.%m %H:%M')} · {_history_event_preview(entry['event'], 34)}",
            callback_data=AdminHistoryEventCallback(
                user_idpk=target_user.idpk,
                event_index=entry["index"],
                return_page=page,
                list_page=list_page,
            ),
        )
    if total_pages > 1:
        builder.button(
            text="<",
            callback_data=AdminHistoryUserCallback(
                user_idpk=target_user.idpk,
                page=max(1, page - 1),
                list_page=list_page,
            ),
        )
        builder.button(
            text=f"{page}/{total_pages}",
            callback_data=AdminHistoryNoopCallback(page=page),
        )
        builder.button(
            text=">",
            callback_data=AdminHistoryUserCallback(
                user_idpk=target_user.idpk,
                page=min(total_pages, page + 1),
                list_page=list_page,
            ),
        )
    builder.button(
        text="К списку",
        callback_data=AdminHistoryListCallback(page=list_page),
    )
    builder.adjust(1, 1, 1, 3, 1)
    return builder.as_markup()


def _build_user_event_detail_keyboard(
    target_user: User,
    entries: list[dict],
    index: int,
    return_page: int,
    list_page: int,
):
    builder = InlineKeyboardBuilder()
    if index > 0:
        builder.button(
            text="< Событие",
            callback_data=AdminHistoryEventCallback(
                user_idpk=target_user.idpk,
                event_index=index - 1,
                return_page=return_page,
                list_page=list_page,
            ),
        )
    if index + 1 < len(entries):
        builder.button(
            text="Событие >",
            callback_data=AdminHistoryEventCallback(
                user_idpk=target_user.idpk,
                event_index=index + 1,
                return_page=return_page,
                list_page=list_page,
            ),
        )
    builder.button(
        text="Назад к истории",
        callback_data=AdminHistoryUserCallback(
            user_idpk=target_user.idpk,
            page=return_page,
            list_page=list_page,
        ),
    )
    builder.button(
        text="К списку",
        callback_data=AdminHistoryListCallback(page=list_page),
    )
    builder.adjust(2, 1, 1)
    return builder.as_markup()


async def _build_user_history_list_text(
    session: AsyncSession,
    page: int,
) -> tuple[str, list[User], int, int]:
    users = await _get_users_with_history(session=session)
    page_users, page, total_pages = _slice_page(
        users, page=page, per_page=USERS_PER_PAGE
    )
    lines = [
        "История пользователей",
        "",
        f"Пользователей с историей: {len(users)}",
        f"Страница: {page}/{total_pages}",
        "",
    ]
    if not users:
        lines.append("История пользователей пока пуста.")
    else:
        for target_user in page_users:
            history_entries = _load_user_history_entries(target_user)
            last_event = history_entries[0] if history_entries else None
            lines.append(
                f"- {target_user.nickname} | id {target_user.idpk} | событий {len(history_entries)} | ходов {target_user.moves}"
            )
            if last_event:
                lines.append(
                    f"  last: {last_event['time'].strftime('%d.%m %H:%M:%S')} | {_history_event_preview(last_event['event'], 96)}"
                )
    return "\n".join(lines), page_users, page, total_pages


def _build_user_history_text(
    target_user: User,
    page_entries: list[dict],
    page: int,
    total_pages: int,
    total_events: int,
) -> str:
    lines = [
        f"История: {target_user.nickname}",
        f"Telegram ID: {target_user.id_user} | DB ID: {target_user.idpk}",
        f"Всего событий: {total_events} | Страница: {page}/{total_pages}",
        "",
    ]
    if not page_entries:
        lines.append("У пользователя пока нет событий.")
        return "\n".join(lines)
    for entry in page_entries:
        lines.append(
            f"{entry['index'] + 1}. {entry['time'].strftime('%d.%m.%Y %H:%M:%S')}"
        )
        lines.append(f"   {_history_event_preview(entry['event'], 180)}")
    return "\n".join(lines)


def _build_user_event_detail_text(
    target_user: User,
    entries: list[dict],
    index: int,
) -> str:
    entry = entries[index]
    lines = [
        f"Событие пользователя: {target_user.nickname}",
        f"Запись: {index + 1}/{len(entries)}",
        f"Время: {entry['time'].strftime('%d.%m.%Y %H:%M:%S.%f')}",
        "",
        str(entry["event"]),
    ]
    return "\n".join(lines)


async def _edit_admin_message(
    query: CallbackQuery,
    text: str,
    reply_markup,
) -> None:
    if not query.message:
        return
    try:
        await query.message.edit_text(
            text=_clip_text(text),
            reply_markup=reply_markup,
        )
    except Exception:
        pass


def _build_admin_npc_keyboard(npc: User, section: str):
    builder = InlineKeyboardBuilder()
    for key, label in SECTION_LABELS.items():
        prefix = "• " if key == section else ""
        builder.button(
            text=f"{prefix}{label}",
            callback_data=AdminNpcCallback(
                npc_idpk=npc.idpk,
                section=AdminNpcSection(key),
            ),
        )
    builder.button(
        text="Разбудить",
        callback_data=AdminNpcCallback(
            npc_idpk=npc.idpk,
            section=AdminNpcSection.WAKE,
        ),
    )
    builder.button(
        text="К списку",
        callback_data=AdminPanelCallback(action=AdminPanelAction.LIST),
    )
    builder.adjust(3, 2, 1)
    return builder.as_markup()


async def _build_npc_overview_text(session: AsyncSession, npc: User) -> str:
    state = await session.scalar(select(NpcState).where(NpcState.idpk_user == npc.idpk))
    profile_row = await ensure_npc_profile_memory(session=session, user=npc)
    profile = _load_payload(profile_row)
    snapshot = await build_npc_snapshot(session=session, user=npc)
    goal_rows = await _get_memory_rows(
        session=session, npc=npc, kind=GOAL_KIND, limit=4
    )
    reflection_rows = await _get_memory_rows(
        session=session,
        npc=npc,
        kind=REFLECTION_KIND,
        limit=1,
    )
    event_rows = await _get_memory_rows(
        session=session, npc=npc, kind=EVENT_KIND, limit=4
    )
    due = bool(
        state and (state.next_wake_at is None or state.next_wake_at <= datetime.now())
    )

    lines = [
        f"NPC: {npc.nickname}",
        f"Telegram ID: {npc.id_user} | DB ID: {npc.idpk}",
        f"Профиль: {profile.get('archetype', '-')}",
        f"Миссия: {profile.get('mission', '-')}",
        f"Активные тактики: {', '.join(profile.get('active_tactics', [])[:3]) or '-'}",
        "",
        "Экономика:",
        f"- USD: {_fmt_number(snapshot.get('usd'))} | RUB: {_fmt_number(snapshot.get('rub'))} | income: {_fmt_number(snapshot.get('income_per_minute_rub'))}/min",
        f"- animals: {_fmt_number(snapshot.get('total_animals'))} | seats: {_fmt_number(snapshot.get('total_seats'))} | free: {_fmt_number(snapshot.get('remain_seats'))}",
        f"- items: {_fmt_number(snapshot.get('active_items'))}/{_fmt_number(snapshot.get('items_owned'))} active | unity: {snapshot.get('current_unity') or '-'}",
        "",
        "Пробуждение:",
        f"- due: {'yes' if due else 'no'} | next: {_fmt_dt(state.next_wake_at if state else None)}",
        f"- last source: {state.last_wake_source if state else '-'} | last sleep: {getattr(state, 'last_sleep_seconds', None) or '-'}",
        f"- reason: {state.last_wake_reason if state else '-'}",
        "",
        "Трейты:",
        f"- effective: risk {profile.get('traits', {}).get('risk_tolerance', '-')} | social {profile.get('traits', {}).get('social_drive', '-')} | economy {profile.get('traits', {}).get('economy_focus', '-')}",
        f"- effective: expansion {profile.get('traits', {}).get('expansion_drive', '-')} | patience {profile.get('traits', {}).get('patience', '-')} | competition {profile.get('traits', {}).get('competitiveness', '-')}",
        f"- adaptive: risk {_fmt_trait_delta(profile.get('adaptive_traits', {}).get('risk_tolerance'))} | social {_fmt_trait_delta(profile.get('adaptive_traits', {}).get('social_drive'))} | economy {_fmt_trait_delta(profile.get('adaptive_traits', {}).get('economy_focus'))}",
        f"- adaptive: expansion {_fmt_trait_delta(profile.get('adaptive_traits', {}).get('expansion_drive'))} | patience {_fmt_trait_delta(profile.get('adaptive_traits', {}).get('patience'))} | competition {_fmt_trait_delta(profile.get('adaptive_traits', {}).get('competitiveness'))}",
        f"- success streak: {profile.get('adaptation_signals', {}).get('success_streak', 0)} | failure streak: {profile.get('adaptation_signals', {}).get('failure_streak', 0)}",
        "",
        "Активные цели:",
    ]
    if goal_rows:
        for row in goal_rows:
            payload = _load_payload(row)
            progress = payload.get("progress", {})
            lines.append(
                f"- {payload.get('title', row.topic)} | {progress.get('current', '-')} / {progress.get('target', '-')} | p={payload.get('priority', row.importance)}"
            )
    else:
        lines.append("- нет")

    lines.append("")
    lines.append("Последняя рефлексия:")
    if reflection_rows:
        payload = _load_payload(reflection_rows[0])
        lines.append(f"- {str(payload.get('summary', '-'))[:350]}")
    else:
        lines.append("- нет")

    lines.append("")
    lines.append("Последние события:")
    if event_rows:
        for row in event_rows:
            payload = _load_payload(row)
            action = payload.get("action", {})
            result = payload.get("result", {})
            lines.append(
                f"- {_fmt_iso(payload.get('time'))}: {action.get('name', '-')} -> {result.get('summary', '-')[:80]}"
            )
    else:
        lines.append("- нет")
    return "\n".join(lines)


async def _build_tactics_text(session: AsyncSession, npc: User) -> str:
    profile = _load_payload(await ensure_npc_profile_memory(session=session, user=npc))
    action_stats = profile.get("action_stats", {})
    tactic_scores = profile.get("tactic_scores", {})
    tactic_rows = sorted(
        tactic_scores.items(),
        key=lambda item: item[1],
        reverse=True,
    )
    lines = [f"Тактики NPC: {npc.nickname}", ""]
    lines.append(f"Активные: {', '.join(profile.get('active_tactics', [])[:3]) or '-'}")
    lines.append("")
    lines.append("Текущие очки тактик:")
    for name, score in tactic_rows[:6]:
        lines.append(f"- {name}: {score}")
    lines.append("")
    lines.append("Последние сдвиги:")
    for shift in profile.get("adaptation_signals", {}).get("recent_tactic_shifts", [])[
        -6:
    ]:
        lines.append(
            f"- {_fmt_iso(shift.get('time'))} | {shift.get('tactic')} {int(shift.get('delta', 0)):+} | {shift.get('reason') or '-'}"
        )
    if lines[-1] == "Последние сдвиги:":
        lines.append("- нет")
    lines.append("")
    lines.append("Последние сдвиги трейтов:")
    for shift in profile.get("adaptation_signals", {}).get("recent_trait_shifts", [])[
        -6:
    ]:
        lines.append(
            f"- {_fmt_iso(shift.get('time'))} | {shift.get('trait')} {int(shift.get('delta', 0)):+} | {shift.get('reason') or '-'}"
        )
    if lines[-1] == "Последние сдвиги трейтов:":
        lines.append("- нет")
    lines.append("")
    lines.append("Эффективность действий:")
    ranked_actions = sorted(
        [
            (name, payload)
            for name, payload in action_stats.items()
            if isinstance(payload, dict)
        ],
        key=lambda item: (
            int(item[1].get("successes", 0)) - int(item[1].get("failures", 0)),
            int(item[1].get("net_income_delta", 0)),
        ),
        reverse=True,
    )
    for name, payload in ranked_actions[:8]:
        lines.append(
            f"- {name}: tries {payload.get('attempts', 0)} | ok {payload.get('successes', 0)} | fail {payload.get('failures', 0)} | dIncome {int(payload.get('net_income_delta', 0)):+}"
        )
    if lines[-1] == "Эффективность действий:":
        lines.append("- нет")
    return "\n".join(lines)


async def _build_goal_text(session: AsyncSession, npc: User) -> str:
    rows = await _get_memory_rows(session=session, npc=npc, kind=GOAL_KIND, limit=8)
    lines = [f"Цели NPC: {npc.nickname}", ""]
    if not rows:
        lines.append("Нет активных целей.")
        return "\n".join(lines)
    for row in rows:
        payload = _load_payload(row)
        progress = payload.get("progress", {})
        lines.extend(
            [
                f"- {payload.get('title', row.topic)}",
                f"  topic: {payload.get('topic', row.topic)} | priority: {payload.get('priority', row.importance)} | horizon: {payload.get('horizon', '-')}",
                f"  progress: {progress.get('current', '-')} / {progress.get('target', '-')} ({progress.get('ratio', '-')})",
                f"  actions: {', '.join(payload.get('recommended_actions', [])[:4]) or '-'}",
                f"  success: {payload.get('success_signal', '-')}",
                "",
            ]
        )
    return "\n".join(lines).strip()


async def _build_reflection_text(session: AsyncSession, npc: User) -> str:
    rows = await _get_memory_rows(
        session=session,
        npc=npc,
        kind=REFLECTION_KIND,
        limit=6,
    )
    lines = [f"Рефлексия NPC: {npc.nickname}", ""]
    if not rows:
        lines.append("Рефлексии пока нет.")
        return "\n".join(lines)
    for row in rows:
        payload = _load_payload(row)
        lines.append(
            f"- {_fmt_iso(payload.get('generated_at'))}: {payload.get('summary', '-')}"
        )
        if payload.get("lessons"):
            lines.append(f"  lessons: {'; '.join(payload['lessons'][:3])}")
        if payload.get("opportunities"):
            lines.append(f"  opportunities: {'; '.join(payload['opportunities'][:3])}")
        if payload.get("risks"):
            lines.append(f"  risks: {'; '.join(payload['risks'][:3])}")
        lines.append("")
    return "\n".join(lines).strip()


async def _build_event_text(session: AsyncSession, npc: User) -> str:
    rows = await _get_memory_rows(session=session, npc=npc, kind=EVENT_KIND, limit=10)
    lines = [f"События NPC: {npc.nickname}", ""]
    if not rows:
        lines.append("Событий пока нет.")
        return "\n".join(lines)
    for row in rows:
        payload = _load_payload(row)
        action = payload.get("action", {})
        result = payload.get("result", {})
        delta = payload.get("delta", {})
        lines.append(
            f"- {_fmt_iso(payload.get('time'))} | {action.get('name', '-')} | {result.get('status', '-')} | {result.get('summary', '-')[:80]}"
        )
        lines.append(
            f"  dUSD {delta.get('usd', 0):+} | dIncome {delta.get('income_per_minute_rub', 0):+} | dAnimals {delta.get('animals', 0):+} | sleep {action.get('sleep_seconds', '-')}"
        )
    return "\n".join(lines)


async def _build_relationship_text(session: AsyncSession, npc: User) -> str:
    rows = await _get_memory_rows(
        session=session,
        npc=npc,
        kind=RELATIONSHIP_KIND,
        limit=10,
    )
    lines = [f"Связи NPC: {npc.nickname}", ""]
    if not rows:
        lines.append("Связей пока нет.")
        return "\n".join(lines)
    for row in rows:
        payload = _load_payload(row)
        name = payload.get("display_name") or payload.get("subject_idpk") or row.topic
        lines.append(
            f"- {name} | status {payload.get('status', '-')} | trust {payload.get('trust', '-')} | affinity {payload.get('affinity', '-')}"
        )
        lines.append(
            f"  last: {payload.get('last_event', '-')} at {_fmt_iso(payload.get('last_event_at'))} | interactions {payload.get('interactions', 0)}"
        )
    return "\n".join(lines)


async def _build_npc_section_text(
    session: AsyncSession,
    npc: User,
    section: str,
) -> str:
    if section == "goals":
        return await _build_goal_text(session=session, npc=npc)
    if section == "tactics":
        return await _build_tactics_text(session=session, npc=npc)
    if section == "reflections":
        return await _build_reflection_text(session=session, npc=npc)
    if section == "events":
        return await _build_event_text(session=session, npc=npc)
    if section == "relationships":
        return await _build_relationship_text(session=session, npc=npc)
    return await _build_npc_overview_text(session=session, npc=npc)


@router.message(StateFilter(any_state), Command(commands="admin"), flags=flags)
async def open_admin_panel_command(
    message: Message,
    session: AsyncSession,
    user: User | None,
) -> None:
    if not _is_admin(user=user, telegram_id=message.from_user.id):
        await message.answer("У вас нет прав")
        return
    npcs = await _get_npc_users(session=session)
    await message.answer(
        text=_clip_text(await _build_admin_panel_text(session=session)),
        reply_markup=_build_admin_panel_keyboard(npcs=npcs),
    )


@router.message(UserState.main_menu, F.text == ADMIN_PANEL_BUTTON, flags=flags)
async def open_admin_panel_button(
    message: Message,
    session: AsyncSession,
    user: User | None,
) -> None:
    if not _is_admin(user=user, telegram_id=message.from_user.id):
        await message.answer("У вас нет прав")
        return
    npcs = await _get_npc_users(session=session)
    await message.answer(
        text=_clip_text(await _build_admin_panel_text(session=session)),
        reply_markup=_build_admin_panel_keyboard(npcs=npcs),
    )


@router.callback_query(AdminPanelCallback.filter(), flags=flags)
async def admin_panel_callbacks(
    query: CallbackQuery,
    session: AsyncSession,
    user: User | None,
    callback_data: AdminPanelCallback,
) -> None:
    if not _is_admin(user=user, telegram_id=query.from_user.id):
        await query.answer("У вас нет прав", show_alert=True)
        return
    npcs = await _get_npc_users(session=session)
    action = callback_data.action.value
    text = _clip_text(await _build_admin_panel_text(session=session))
    if action not in {"list", "refresh"}:
        await query.answer("Неизвестное действие", show_alert=True)
        return
    await _edit_admin_message(
        query=query,
        text=text,
        reply_markup=_build_admin_panel_keyboard(npcs=npcs),
    )
    await query.answer("Обновлено")


@router.callback_query(AdminNpcCallback.filter(), flags=flags)
async def admin_npc_callbacks(
    query: CallbackQuery,
    session: AsyncSession,
    user: User | None,
    callback_data: AdminNpcCallback,
) -> None:
    if not _is_admin(user=user, telegram_id=query.from_user.id):
        await query.answer("У вас нет прав", show_alert=True)
        return
    npc = await session.get(User, callback_data.npc_idpk)
    if not npc:
        await query.answer("NPC не найден", show_alert=True)
        return
    section = callback_data.section.value
    if section == "wake":
        await wake_npc_now(
            session=session,
            user_idpk=npc.idpk,
            reason=f"admin_panel:{query.from_user.id}",
        )
        await session.commit()
        section = "overview"
        await query.answer("NPC разбужен")
    else:
        await query.answer()
    await _edit_admin_message(
        query=query,
        text=await _build_npc_section_text(session=session, npc=npc, section=section),
        reply_markup=_build_admin_npc_keyboard(npc=npc, section=section),
    )


@router.callback_query(AdminHistoryNoopCallback.filter(), flags=flags)
async def admin_user_history_noop(
    query: CallbackQuery,
    user: User | None,
) -> None:
    if not _is_admin(user=user, telegram_id=query.from_user.id):
        await query.answer("У вас нет прав", show_alert=True)
        return
    await query.answer()


@router.callback_query(AdminHistoryListCallback.filter(), flags=flags)
async def admin_user_history_list_callbacks(
    query: CallbackQuery,
    session: AsyncSession,
    user: User | None,
    callback_data: AdminHistoryListCallback,
) -> None:
    if not _is_admin(user=user, telegram_id=query.from_user.id):
        await query.answer("У вас нет прав", show_alert=True)
        return
    text, page_users, page, total_pages = await _build_user_history_list_text(
        session=session,
        page=callback_data.page,
    )
    await _edit_admin_message(
        query=query,
        text=text,
        reply_markup=_build_user_history_list_keyboard(
            users=page_users,
            page=page,
            total_pages=total_pages,
        ),
    )
    await query.answer("История пользователей")


@router.callback_query(AdminHistoryUserCallback.filter(), flags=flags)
async def admin_user_history_user_callbacks(
    query: CallbackQuery,
    session: AsyncSession,
    user: User | None,
    callback_data: AdminHistoryUserCallback,
) -> None:
    if not _is_admin(user=user, telegram_id=query.from_user.id):
        await query.answer("У вас нет прав", show_alert=True)
        return
    target_user = await session.get(User, callback_data.user_idpk)
    if not target_user:
        await query.answer("Пользователь не найден", show_alert=True)
        return
    entries = _load_user_history_entries(target_user)
    indexed_entries = [
        {
            **entry,
            "index": index,
        }
        for index, entry in enumerate(entries)
    ]
    page_entries, page, total_pages = _slice_page(
        indexed_entries,
        page=callback_data.page,
        per_page=USER_EVENTS_PER_PAGE,
    )
    await _edit_admin_message(
        query=query,
        text=_build_user_history_text(
            target_user=target_user,
            page_entries=page_entries,
            page=page,
            total_pages=total_pages,
            total_events=len(entries),
        ),
        reply_markup=_build_user_history_keyboard(
            target_user=target_user,
            page=page,
            total_pages=total_pages,
            entries=page_entries,
            list_page=callback_data.list_page,
        ),
    )
    await query.answer()


@router.callback_query(AdminHistoryEventCallback.filter(), flags=flags)
async def admin_user_history_callbacks(
    query: CallbackQuery,
    session: AsyncSession,
    user: User | None,
    callback_data: AdminHistoryEventCallback,
) -> None:
    if not _is_admin(user=user, telegram_id=query.from_user.id):
        await query.answer("У вас нет прав", show_alert=True)
        return
    target_user = await session.get(User, callback_data.user_idpk)
    if not target_user:
        await query.answer("Пользователь не найден", show_alert=True)
        return
    entries = _load_user_history_entries(target_user)
    if not entries:
        await query.answer("У пользователя нет истории", show_alert=True)
        return
    index = max(0, min(len(entries) - 1, callback_data.event_index))
    await _edit_admin_message(
        query=query,
        text=_build_user_event_detail_text(
            target_user=target_user,
            entries=entries,
            index=index,
        ),
        reply_markup=_build_user_event_detail_keyboard(
            target_user=target_user,
            entries=entries,
            index=index,
            return_page=callback_data.return_page,
            list_page=callback_data.list_page,
        ),
    )
    await query.answer()
