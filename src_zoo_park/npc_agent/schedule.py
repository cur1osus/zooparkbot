import hashlib
from datetime import datetime, timedelta

from db import NpcState, User
from init_db_redis import redis
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from text_utils import fit_db_field, normalize_choice

from .settings import settings


def npc_event_wake_key(user_idpk: int) -> str:
    return f"npc_event_wake:{user_idpk}"


async def ensure_npc_state(session: AsyncSession, user: User) -> NpcState:
    state = await session.scalar(
        select(NpcState).where(NpcState.idpk_user == user.idpk)
    )
    if state:
        return state
    state = NpcState(idpk_user=user.idpk, next_wake_at=datetime.now())
    session.add(state)
    await session.flush()
    return state


async def get_npc_wake_trigger(
    session: AsyncSession, user: User
) -> dict[str, str | bool | None]:
    state = await ensure_npc_state(session=session, user=user)
    event_reason = await get_npc_event_wake_reason(user_idpk=user.idpk)
    now = datetime.now()
    due_by_schedule = state.next_wake_at is None or state.next_wake_at <= now
    if event_reason:
        return {
            "due": True,
            "source": "event",
            "reason": event_reason,
            "scheduled_at": state.next_wake_at.isoformat()
            if state.next_wake_at
            else None,
        }
    return {
        "due": due_by_schedule,
        "source": "scheduled",
        "reason": "planned_wake",
        "scheduled_at": state.next_wake_at.isoformat() if state.next_wake_at else None,
    }


async def get_npc_event_wake_reason(user_idpk: int) -> str | None:
    raw_value = await redis.get(npc_event_wake_key(user_idpk))
    if raw_value is None:
        return None
    if isinstance(raw_value, bytes):
        return fit_db_field(
            raw_value.decode("utf-8", errors="ignore"),
            max_len=255,
            default="event",
        )
    return fit_db_field(raw_value, max_len=255, default="event")


async def clear_npc_event_wake(user_idpk: int) -> None:
    await redis.delete(npc_event_wake_key(user_idpk))


async def wake_npc_now(
    session: AsyncSession,
    user_idpk: int,
    reason: str,
) -> None:
    state = await session.scalar(
        select(NpcState).where(NpcState.idpk_user == user_idpk)
    )
    if not state:
        state = NpcState(idpk_user=user_idpk)
        session.add(state)
    state.next_wake_at = datetime.now()
    await session.flush()
    await redis.set(
        npc_event_wake_key(user_idpk),
        fit_db_field(reason or "event", max_len=255, default="event"),
        ex=settings.event_wake_ttl_seconds,
    )


async def wake_all_npcs_now(
    session: AsyncSession,
    reason: str,
) -> int:
    npc_ids = await session.scalars(
        select(User.idpk).where(or_(User.id_user < 0, User.username.like("npc_%")))
    )
    woken = 0
    for user_idpk in npc_ids.all():
        await wake_npc_now(
            session=session,
            user_idpk=int(user_idpk),
            reason=reason,
        )
        woken += 1
    return woken


async def schedule_next_npc_wake(
    session: AsyncSession,
    user: User,
    sleep_seconds: int,
    source: str,
    reason: str,
) -> None:
    state = await ensure_npc_state(session=session, user=user)
    now = datetime.now()
    state.last_wake_at = now
    state.last_sleep_seconds = int(sleep_seconds)
    state.last_wake_source = normalize_choice(
        source or "scheduled",
        allowed={"scheduled", "event"},
        default="scheduled",
    )
    state.last_wake_reason = fit_db_field(
        reason or "cycle_complete",
        max_len=255,
        default="cycle_complete",
    )
    state.next_wake_at = now + timedelta(seconds=int(sleep_seconds))


def clamp_npc_sleep_seconds(value: int) -> int:
    return max(settings.min_sleep_seconds, min(settings.max_sleep_seconds, int(value)))


def default_npc_sleep_seconds(user: User, salt: str = "") -> int:
    base_sleep = max(settings.step_seconds, settings.min_sleep_seconds)
    if settings.step_jitter_seconds > 0:
        seed = f"{user.id_user}:{salt}".encode("utf-8")
        digest = hashlib.sha256(seed).digest()
        jitter = int.from_bytes(digest[:4], "big") % (settings.step_jitter_seconds + 1)
        base_sleep += jitter
    return clamp_npc_sleep_seconds(base_sleep)
