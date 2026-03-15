import json
import math
from datetime import datetime
from enum import Enum
from html import escape

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
    "overview": "🧭 Обзор",
    "tactics": "⚙️ Такт.",
    "goals": "🎯 Цели",
    "reflections": "🪞 Рефл.",
    "events": "🗂 Событ.",
    "relationships": "🤝 Связи",
}

MAX_ADMIN_TEXT = 3900
USERS_PER_PAGE = 6
USER_EVENTS_PER_PAGE = 5
NPC_BUTTON_LABEL_LIMIT = 12
HISTORY_USER_LABEL_LIMIT = 11
HISTORY_EVENT_LABEL_LIMIT = 26


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


def _short_label(text: str | None, limit: int) -> str:
    value = " ".join(str(text or "-").split())
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


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


def _page_bounds(page: int, per_page: int, total_items: int) -> tuple[int, int]:
    if total_items <= 0:
        return 0, 0
    start = (page - 1) * per_page + 1
    end = min(total_items, page * per_page)
    return start, end


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


def _format_event_body_html(event_text: str, expandable: bool = False) -> str:
    raw = str(event_text or "-").strip() or "-"
    formatted = escape(raw)
    formatted = formatted.replace(" | ", "\n")
    formatted = formatted.replace(" -> ", "\n→ ")
    formatted = formatted.replace("; ", ";\n")
    quote_tag = "blockquote expandable" if expandable else "blockquote"
    return f"<{quote_tag}>{formatted}</{quote_tag.split()[0]}>"


def _build_npc_button_text(npc: User) -> str:
    return _short_label(npc.nickname or f"NPC {npc.idpk}", NPC_BUTTON_LABEL_LIMIT)


def _build_history_user_button_text(
    target_user: User, history_entries: list[dict]
) -> str:
    last_time = (
        history_entries[0]["time"].strftime("%d.%m %H:%M") if history_entries else "-"
    )
    nickname = _short_label(target_user.nickname, HISTORY_USER_LABEL_LIMIT)
    return f"{nickname} · {len(history_entries)} · {last_time}"


def _build_history_event_button_text(entry: dict) -> str:
    preview = _history_event_preview(entry["event"], HISTORY_EVENT_LABEL_LIMIT)
    return f"{entry['time'].strftime('%d.%m %H:%M')} · {preview}"


def _append_pager_buttons(
    builder, page: int, total_pages: int, callback_factory
) -> int:
    if total_pages <= 1:
        return 0

    buttons = []
    if total_pages > 3:
        buttons.append(("⏮", callback_factory(1)))
    buttons.append(("◀️", callback_factory(max(1, page - 1))))
    buttons.append((f"{page}/{total_pages}", AdminHistoryNoopCallback(page=page)))
    buttons.append(("▶️", callback_factory(min(total_pages, page + 1))))
    if total_pages > 3:
        buttons.append(("⏭", callback_factory(total_pages)))

    for text, callback_data in buttons:
        builder.button(text=text, callback_data=callback_data)
    return len(buttons)


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
    users_with_history = []
    for item in rows.all():
        history_entries = _load_user_history_entries(item)
        if not history_entries:
            continue
        users_with_history.append((item, history_entries[0]["time"]))

    users_with_history.sort(
        key=lambda item: (item[1], item[0].moves),
        reverse=True,
    )
    return [item[0] for item in users_with_history]


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
        return "NPC админка\n\nNPC не найдены."
    lines = ["NPC админка", "", f"Всего NPC: {len(npcs)}", ""]
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
        due_label = "готов" if due else "ждет"
        lines.append(f"- {_short_label(npc.nickname, 24)} · {due_label}")
        lines.append(f"  след: {_fmt_dt(state.next_wake_at if state else None)}")
        if last_event:
            lines.append(
                f"  посл: {last_event.get('action', {}).get('name', '-')} -> {last_event.get('result', {}).get('summary', '-')[:64]}"
            )
        lines.append(f"  цели: {len(goal_rows)}")
    lines.extend(["", "Открой NPC кнопкой ниже."])
    return "\n".join(lines)


def _build_admin_panel_keyboard(npcs: list[User]):
    builder = InlineKeyboardBuilder()
    row_sizes = [2]
    builder.button(
        text="📜 История",
        callback_data=AdminHistoryListCallback(page=1),
    )
    builder.button(
        text="🔄 Обновить",
        callback_data=AdminPanelCallback(action=AdminPanelAction.REFRESH),
    )
    for npc in npcs:
        builder.button(
            text=_build_npc_button_text(npc),
            callback_data=AdminNpcCallback(
                npc_idpk=npc.idpk,
                section=AdminNpcSection.OVERVIEW,
            ),
        )
    if npcs:
        npc_rows = len(npcs) // 2
        row_sizes.extend([2] * npc_rows)
        if len(npcs) % 2:
            row_sizes.append(1)
    builder.adjust(*row_sizes)
    return builder.as_markup()


def _build_user_history_list_keyboard(users: list[User], page: int, total_pages: int):
    builder = InlineKeyboardBuilder()
    row_sizes = []
    for target_user in users:
        history_entries = _load_user_history_entries(target_user)
        builder.button(
            text=_build_history_user_button_text(target_user, history_entries),
            callback_data=AdminHistoryUserCallback(
                user_idpk=target_user.idpk,
                page=1,
                list_page=page,
            ),
        )
        row_sizes.append(1)
    nav_size = _append_pager_buttons(
        builder=builder,
        page=page,
        total_pages=total_pages,
        callback_factory=lambda target_page: AdminHistoryListCallback(page=target_page),
    )
    if nav_size:
        row_sizes.append(nav_size)
    builder.button(
        text="🤖 К NPC",
        callback_data=AdminPanelCallback(action=AdminPanelAction.LIST),
    )
    builder.button(
        text="🔄 Обновить",
        callback_data=AdminHistoryListCallback(page=page),
    )
    row_sizes.append(2)
    builder.adjust(*row_sizes)
    return builder.as_markup()


def _build_user_history_keyboard(
    target_user: User,
    page: int,
    total_pages: int,
    entries: list[dict],
    list_page: int,
):
    builder = InlineKeyboardBuilder()
    row_sizes = []
    for entry in entries:
        builder.button(
            text=_build_history_event_button_text(entry),
            callback_data=AdminHistoryEventCallback(
                user_idpk=target_user.idpk,
                event_index=entry["index"],
                return_page=page,
                list_page=list_page,
            ),
        )
        row_sizes.append(1)
    nav_size = _append_pager_buttons(
        builder=builder,
        page=page,
        total_pages=total_pages,
        callback_factory=lambda target_page: AdminHistoryUserCallback(
            user_idpk=target_user.idpk,
            page=target_page,
            list_page=list_page,
        ),
    )
    if nav_size:
        row_sizes.append(nav_size)
    builder.button(
        text="📋 К списку",
        callback_data=AdminHistoryListCallback(page=list_page),
    )
    builder.button(
        text="⏮ В начало",
        callback_data=AdminHistoryUserCallback(
            user_idpk=target_user.idpk,
            page=1,
            list_page=list_page,
        ),
    )
    row_sizes.append(2)
    builder.adjust(*row_sizes)
    return builder.as_markup()


def _build_user_event_detail_keyboard(
    target_user: User,
    entries: list[dict],
    index: int,
    return_page: int,
    list_page: int,
):
    builder = InlineKeyboardBuilder()
    nav_size = 0
    if index > 0:
        builder.button(
            text="⬅️ Новее",
            callback_data=AdminHistoryEventCallback(
                user_idpk=target_user.idpk,
                event_index=index - 1,
                return_page=return_page,
                list_page=list_page,
            ),
        )
        nav_size += 1
    if index + 1 < len(entries):
        builder.button(
            text="Старее ➡️",
            callback_data=AdminHistoryEventCallback(
                user_idpk=target_user.idpk,
                event_index=index + 1,
                return_page=return_page,
                list_page=list_page,
            ),
        )
        nav_size += 1
    builder.button(
        text="🧾 К истории",
        callback_data=AdminHistoryUserCallback(
            user_idpk=target_user.idpk,
            page=return_page,
            list_page=list_page,
        ),
    )
    builder.button(
        text="📋 К списку",
        callback_data=AdminHistoryListCallback(page=list_page),
    )
    row_sizes = []
    if nav_size:
        row_sizes.append(nav_size)
    row_sizes.append(2)
    builder.adjust(*row_sizes)
    return builder.as_markup()


async def _build_user_history_list_text(
    session: AsyncSession,
    page: int,
) -> tuple[str, list[User], int, int]:
    users = await _get_users_with_history(session=session)
    page_users, page, total_pages = _slice_page(
        users, page=page, per_page=USERS_PER_PAGE
    )
    range_start, range_end = _page_bounds(
        page=page,
        per_page=USERS_PER_PAGE,
        total_items=len(users),
    )
    lines = [
        "История пользователей",
        "",
        f"Пользователей: {len(users)}",
        f"Показано: {range_start}-{range_end}",
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
                f"- {_short_label(target_user.nickname, 24)} · {len(history_entries)} событий"
            )
            lines.append(f"  id: {target_user.idpk} · ходы: {target_user.moves}")
            if last_event:
                lines.append(f"  last: {last_event['time'].strftime('%d.%m %H:%M')}")
                lines.append(f"  {_history_event_preview(last_event['event'], 80)}")
    return "\n".join(lines), page_users, page, total_pages


def _build_user_history_text(
    target_user: User,
    page_entries: list[dict],
    page: int,
    total_pages: int,
    total_events: int,
) -> str:
    range_start, range_end = _page_bounds(
        page=page,
        per_page=USER_EVENTS_PER_PAGE,
        total_items=total_events,
    )
    lines = [
        f"<b>История:</b> {escape(str(target_user.nickname or '-'))}",
        f"<code>TG {target_user.id_user}</code> · <code>DB {target_user.idpk}</code>",
        f"<b>События:</b> {total_events} · <b>Показано:</b> {range_start}-{range_end}",
        f"<b>Страница:</b> {page}/{total_pages}",
        "",
    ]
    if not page_entries:
        lines.append("У пользователя пока нет событий.")
        return "\n".join(lines)
    for entry in page_entries:
        lines.append(
            f"<b>{entry['index'] + 1}.</b> <code>{entry['time'].strftime('%d.%m.%Y %H:%M:%S')}</code>"
        )
        lines.append(
            _format_event_body_html(_history_event_preview(entry["event"], 140))
        )
    return "\n".join(lines)


def _build_user_event_detail_text(
    target_user: User,
    entries: list[dict],
    index: int,
) -> str:
    entry = entries[index]
    lines = [
        f"<b>Событие пользователя:</b> {escape(str(target_user.nickname or '-'))}",
        f"<b>Запись:</b> <code>{index + 1}/{len(entries)}</code>",
        f"<b>Время:</b> <code>{entry['time'].strftime('%d.%m.%Y %H:%M:%S.%f')}</code>",
        "",
        _format_event_body_html(str(entry["event"]), expandable=True),
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
    row_sizes = []
    for key, label in SECTION_LABELS.items():
        prefix = "• " if key == section else ""
        builder.button(
            text=f"{prefix}{label}",
            callback_data=AdminNpcCallback(
                npc_idpk=npc.idpk,
                section=AdminNpcSection(key),
            ),
        )
    row_sizes.extend([2, 2, 2])
    builder.button(
        text="⚡ Разбудить",
        callback_data=AdminNpcCallback(
            npc_idpk=npc.idpk,
            section=AdminNpcSection.WAKE,
        ),
    )
    builder.button(
        text="📋 К списку",
        callback_data=AdminPanelCallback(action=AdminPanelAction.LIST),
    )
    row_sizes.append(2)
    builder.adjust(*row_sizes)
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
    latest_event = _load_payload(event_rows[0]) if event_rows else {}
    latest_plan = latest_event.get("planner", {}) if latest_event else {}
    latest_guard = latest_event.get("anti_loop_guard", {}) if latest_event else {}
    latest_strategy = latest_event.get("strategy_summary", {}) if latest_event else {}
    due = bool(
        state and (state.next_wake_at is None or state.next_wake_at <= datetime.now())
    )
    traits = profile.get("traits", {})
    adaptive_traits = profile.get("adaptive_traits", {})
    adaptation_signals = profile.get("adaptation_signals", {})
    wake_state = "готов" if due else "ждет"

    lines = [
        f"NPC: {npc.nickname}",
        f"TG ID: {npc.id_user} · DB ID: {npc.idpk}",
        f"Профиль: {profile.get('archetype', '-')}",
        f"Миссия: {profile.get('mission', '-')}",
        f"Голос: {profile.get('public_voice', '-')}",
        f"Тактики: {', '.join(profile.get('active_tactics', [])[:3]) or '-'}",
        "",
        "Экономика:",
        f"- USD: {_fmt_number(snapshot.get('usd'))}",
        f"- RUB: {_fmt_number(snapshot.get('rub'))}",
        f"- Доход: {_fmt_number(snapshot.get('income_per_minute_rub'))}/мин",
        f"- Животные: {_fmt_number(snapshot.get('total_animals'))}",
        f"- Места: {_fmt_number(snapshot.get('total_seats'))}",
        f"- Свободно: {_fmt_number(snapshot.get('remain_seats'))}",
        f"- Предметы: {_fmt_number(snapshot.get('active_items'))}/{_fmt_number(snapshot.get('items_owned'))} активны",
        f"- Союз: {snapshot.get('current_unity') or '-'}",
        "",
        "Пробуждение:",
        f"- Статус: {wake_state}",
        f"- Следующее: {_fmt_dt(state.next_wake_at if state else None)}",
        f"- Источник: {state.last_wake_source if state else '-'}",
        f"- Сон: {getattr(state, 'last_sleep_seconds', None) or '-'} сек",
        f"- Причина: {state.last_wake_reason if state else '-'}",
        "",
        "Трейты:",
        f"- Риск: {traits.get('risk_tolerance', '-')}",
        f"- Соц: {traits.get('social_drive', '-')}",
        f"- Эконом: {traits.get('economy_focus', '-')}",
        f"- Рост: {traits.get('expansion_drive', '-')}",
        f"- Терпение: {traits.get('patience', '-')}",
        f"- Соревн.: {traits.get('competitiveness', '-')}",
        f"- dРиск: {_fmt_trait_delta(adaptive_traits.get('risk_tolerance'))}",
        f"- dСоц: {_fmt_trait_delta(adaptive_traits.get('social_drive'))}",
        f"- dЭконом: {_fmt_trait_delta(adaptive_traits.get('economy_focus'))}",
        f"- dРост: {_fmt_trait_delta(adaptive_traits.get('expansion_drive'))}",
        f"- dТерпение: {_fmt_trait_delta(adaptive_traits.get('patience'))}",
        f"- dСоревн.: {_fmt_trait_delta(adaptive_traits.get('competitiveness'))}",
        f"- Серия+: {adaptation_signals.get('success_streak', 0)}",
        f"- Серия-: {adaptation_signals.get('failure_streak', 0)}",
        "",
        "План:",
        f"- Фаза: {latest_plan.get('phase', '-')}",
        f"- Цель: {latest_plan.get('primary_goal', '-')}",
        f"- Next unlock: {(latest_plan.get('next_unlock') or {}).get('label', '-')}",
        f"- ETA: {(latest_plan.get('next_unlock') or {}).get('eta_seconds', '-')}",
        "",
        "Guard:",
        "- disabled",
        "",
        "Соперники:",
    ]
    if latest_strategy.get("top_rivals"):
        for rival in latest_strategy.get("top_rivals", [])[:3]:
            lines.append(
                f"- {rival.get('nickname') or rival.get('idpk')} · pressure {rival.get('pressure', '-')} · {', '.join(rival.get('reasons', [])[:2])}"
            )
    else:
        lines.append("- нет")

    lines.extend(
        [
            "",
            "Цели:",
        ]
    )
    if goal_rows:
        for row in goal_rows:
            payload = _load_payload(row)
            progress = payload.get("progress", {})
            lines.append(f"- {payload.get('title', row.topic)}")
            lines.append(
                f"  {progress.get('current', '-')} / {progress.get('target', '-')} · p={payload.get('priority', row.importance)}"
            )
    else:
        lines.append("- нет")

    lines.append("")
    lines.append("Рефлексия:")
    if reflection_rows:
        payload = _load_payload(reflection_rows[0])
        lines.append(f"- {str(payload.get('summary', '-'))[:350]}")
    else:
        lines.append("- нет")

    lines.append("")
    lines.append("События:")
    if event_rows:
        for row in event_rows:
            payload = _load_payload(row)
            action = payload.get("action", {})
            result = payload.get("result", {})
            lines.append(f"- {_fmt_iso(payload.get('time'))}")
            lines.append(
                f"  {action.get('name', '-')} -> {result.get('summary', '-')[:72]}"
            )
    else:
        lines.append("- нет")
    return "\n".join(lines)


async def _build_tactics_text(session: AsyncSession, npc: User) -> str:
    profile = _load_payload(await ensure_npc_profile_memory(session=session, user=npc))
    action_stats = profile.get("action_stats", {})
    tactic_scores = profile.get("tactic_scores", {})
    event_rows = await _get_memory_rows(
        session=session, npc=npc, kind=EVENT_KIND, limit=1
    )
    latest_event = _load_payload(event_rows[0]) if event_rows else {}
    latest_guard = latest_event.get("anti_loop_guard", {}) if latest_event else {}
    latest_behavior = latest_event.get("behavior_guidance", {}) if latest_event else {}
    tactic_rows = sorted(
        tactic_scores.items(),
        key=lambda item: item[1],
        reverse=True,
    )
    lines = [f"Тактики NPC: {npc.nickname}", ""]
    lines.append(f"Активные: {', '.join(profile.get('active_tactics', [])[:3]) or '-'}")
    lines.append("")
    lines.append("Очки:")
    for name, score in tactic_rows[:6]:
        lines.append(f"- {name}: {score}")
    lines.append("")
    lines.append("Сдвиги тактик:")
    for shift in profile.get("adaptation_signals", {}).get("recent_tactic_shifts", [])[
        -6:
    ]:
        lines.append(
            f"- {_fmt_iso(shift.get('time'))} | {shift.get('tactic')} {int(shift.get('delta', 0)):+} | {shift.get('reason') or '-'}"
        )
    if lines[-1] == "Сдвиги тактик:":
        lines.append("- нет")
    lines.append("")
    lines.append("Сдвиги трейтов:")
    for shift in profile.get("adaptation_signals", {}).get("recent_trait_shifts", [])[
        -6:
    ]:
        lines.append(
            f"- {_fmt_iso(shift.get('time'))} | {shift.get('trait')} {int(shift.get('delta', 0)):+} | {shift.get('reason') or '-'}"
        )
    if lines[-1] == "Сдвиги трейтов:":
        lines.append("- нет")
    lines.append("")
    lines.append("Плейбук:")
    for row in latest_behavior.get("playbook", [])[:4]:
        lines.append(f"- {row}")
    if lines[-1] == "Плейбук:":
        lines.append("- нет")
    lines.append("")
    lines.append("Анти-луп:")
    lines.append("- disabled")
    lines.append("")
    lines.append("Действия:")
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
        lines.append(f"- {name}")
        lines.append(
            f"  tries {payload.get('attempts', 0)} · ok {payload.get('successes', 0)} · fail {payload.get('failures', 0)} · dIncome {int(payload.get('net_income_delta', 0)):+}"
        )
    if lines[-1] == "Действия:":
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
                f"  тема: {payload.get('topic', row.topic)}",
                f"  приоритет: {payload.get('priority', row.importance)} · горизонт: {payload.get('horizon', '-')}",
                f"  прогресс: {progress.get('current', '-')} / {progress.get('target', '-')} ({progress.get('ratio', '-')})",
                f"  действия: {', '.join(payload.get('recommended_actions', [])[:4]) or '-'}",
                f"  успех: {payload.get('success_signal', '-')}",
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
            lines.append(f"  уроки: {'; '.join(payload['lessons'][:2])}")
        if payload.get("opportunities"):
            lines.append(f"  шансы: {'; '.join(payload['opportunities'][:2])}")
        if payload.get("risks"):
            lines.append(f"  риски: {'; '.join(payload['risks'][:2])}")
        lines.append("")
    return "\n".join(lines).strip()


async def _build_event_text(session: AsyncSession, npc: User) -> str:
    rows = await _get_memory_rows(session=session, npc=npc, kind=EVENT_KIND, limit=10)
    lines = [f"<b>События NPC:</b> {escape(str(npc.nickname or '-'))}", ""]
    if not rows:
        lines.append("Событий пока нет.")
        return "\n".join(lines)
    for idx, row in enumerate(rows, start=1):
        payload = _load_payload(row)
        action = payload.get("action", {})
        result = payload.get("result", {})
        delta = payload.get("delta", {})
        lines.append(f"<b>{idx}.</b> <code>{_fmt_iso(payload.get('time'))}</code>")
        lines.append(
            f"<b>{escape(str(action.get('name', '-')))}</b> · <code>{escape(str(result.get('status', '-')))}</code>"
        )
        lines.append(
            _format_event_body_html(str(result.get("summary", "-") or "-")[:180])
        )
        lines.append(
            "<code>"
            f"dUSD {int(delta.get('usd', 0) or 0):+} · "
            f"dIncome {int(delta.get('income_per_minute_rub', 0) or 0):+} · "
            f"dAnimals {int(delta.get('animals', 0) or 0):+} · "
            f"сон {action.get('sleep_seconds', '-')}"
            "</code>"
        )
        lines.append("")
    return "\n".join(lines).rstrip()


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
        lines.append(f"- {name}")
        lines.append(
            f"  статус {payload.get('status', '-')} · trust {payload.get('trust', '-')} · affinity {payload.get('affinity', '-')}"
        )
        lines.append(f"  последнее: {payload.get('last_event', '-')}")
        lines.append(
            f"  {_fmt_iso(payload.get('last_event_at'))} · interactions {payload.get('interactions', 0)}"
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
