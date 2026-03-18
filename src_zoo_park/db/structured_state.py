from __future__ import annotations

from datetime import datetime
from typing import Any

from fastjson import dumps as json_dumps, loads as json_loads
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    TransferMoney,
    TransferMoneyClaim,
    Unity,
    UnityMember,
    UserAnimalState,
    UserAviaryState,
    User,
    UserHistoryEvent,
)

_LEGACY_HISTORY_TIME_FORMAT = "%d.%m.%Y %H:%M:%S.%f"


def _parse_legacy_history_entries(user: User) -> list[dict[str, Any]]:
    raw_history = getattr(user, "history_moves", None)
    if not raw_history or raw_history == "{}":
        return []
    try:
        payload = json_loads(raw_history)
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []

    entries: list[dict[str, Any]] = []
    for raw_time, raw_value in payload.items():
        try:
            event_time = datetime.strptime(str(raw_time), _LEGACY_HISTORY_TIME_FORMAT)
        except (TypeError, ValueError):
            continue

        event_kind = "message"
        event_text: str | None = str(raw_value) if raw_value is not None else ""
        event_payload: dict[str, Any] | None = None

        if isinstance(raw_value, dict):
            event_kind = "npc_turn"
            event_text = None
            event_payload = raw_value
        elif isinstance(raw_value, str):
            try:
                decoded = json_loads(raw_value)
            except Exception:
                decoded = None
            if isinstance(decoded, dict):
                event_kind = "npc_turn"
                event_text = None
                event_payload = decoded

        entries.append(
            {
                "event_time": event_time,
                "event_kind": event_kind,
                "event_text": event_text,
                "payload": event_payload,
                "source": "legacy_history_moves",
            }
        )

    entries.sort(key=lambda item: item["event_time"])
    return entries


async def ensure_user_history_backfilled(session: AsyncSession, user: User) -> None:
    existing = await session.scalar(
        select(UserHistoryEvent.idpk)
        .where(UserHistoryEvent.idpk_user == user.idpk)
        .limit(1)
    )
    if existing is not None:
        return

    for entry in _parse_legacy_history_entries(user):
        session.add(
            UserHistoryEvent(
                idpk_user=user.idpk,
                event_time=entry["event_time"],
                event_kind=entry["event_kind"],
                event_text=entry["event_text"],
                payload=entry["payload"],
                source=entry["source"],
            )
        )
    await session.flush()


async def _prune_user_history(
    session: AsyncSession, user_idpk: int, limit: int
) -> None:
    if limit <= 0:
        return
    rows = await session.scalars(
        select(UserHistoryEvent)
        .where(UserHistoryEvent.idpk_user == user_idpk)
        .order_by(UserHistoryEvent.event_time.desc(), UserHistoryEvent.idpk.desc())
        .offset(limit)
    )
    for row in rows.all():
        await session.delete(row)


async def append_user_message_history(
    session: AsyncSession,
    user: User,
    message_text: str | None,
    *,
    limit: int,
    source: str = "user_chat",
) -> None:
    session.add(
        UserHistoryEvent(
            idpk_user=user.idpk,
            event_kind="message",
            event_text=message_text or "",
            source=source,
        )
    )
    await session.flush()
    await _prune_user_history(session=session, user_idpk=user.idpk, limit=limit)


async def append_npc_turn_history(
    session: AsyncSession,
    user: User,
    payload: dict[str, Any],
    *,
    limit: int,
) -> None:
    session.add(
        UserHistoryEvent(
            idpk_user=user.idpk,
            event_kind="npc_turn",
            payload=payload,
            source="npc_turn",
        )
    )
    await session.flush()
    await _prune_user_history(session=session, user_idpk=user.idpk, limit=limit)


def _history_entry_event_text(row: UserHistoryEvent) -> str:
    if row.event_text:
        return str(row.event_text)
    if isinstance(row.payload, dict):
        return json_dumps(row.payload)
    return ""


async def list_user_history_entries(
    session: AsyncSession,
    user: User,
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    await ensure_user_history_backfilled(session=session, user=user)
    stmt = (
        select(UserHistoryEvent)
        .where(UserHistoryEvent.idpk_user == user.idpk)
        .order_by(UserHistoryEvent.event_time.desc(), UserHistoryEvent.idpk.desc())
    )
    if limit is not None:
        stmt = stmt.limit(limit)
    rows = await session.scalars(stmt)
    return [
        {
            "raw_time": row.event_time.strftime(_LEGACY_HISTORY_TIME_FORMAT),
            "time": row.event_time,
            "event": _history_entry_event_text(row),
            "event_kind": row.event_kind,
            "payload": row.payload if isinstance(row.payload, dict) else None,
            "source": row.source,
        }
        for row in rows.all()
    ]


async def list_recent_npc_history_payloads(
    session: AsyncSession,
    user: User,
    *,
    limit: int = 12,
) -> list[dict[str, Any]]:
    await ensure_user_history_backfilled(session=session, user=user)
    rows = await session.scalars(
        select(UserHistoryEvent)
        .where(
            UserHistoryEvent.idpk_user == user.idpk,
            UserHistoryEvent.event_kind == "npc_turn",
        )
        .order_by(UserHistoryEvent.event_time.desc(), UserHistoryEvent.idpk.desc())
        .limit(limit)
    )
    payloads: list[dict[str, Any]] = []
    for row in rows.all():
        if isinstance(row.payload, dict):
            payloads.append(row.payload)
    return list(reversed(payloads))


def _parse_legacy_unity_members(unity: Unity) -> list[tuple[int, str]]:
    if not unity.members or unity.members == "{}":
        return []
    try:
        payload = json_loads(unity.members)
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []

    result: list[tuple[int, str]] = []
    for member_idpk, role in payload.items():
        try:
            result.append((int(member_idpk), str(role or "member") or "member"))
        except (TypeError, ValueError):
            continue
    return result


def _serialize_unity_members(rows: list[UnityMember]) -> str:
    payload = {str(row.idpk_user): row.role for row in rows}
    return json_dumps(payload)


async def ensure_unity_members_backfilled(
    session: AsyncSession, unity: Unity
) -> list[UnityMember]:
    rows = await session.scalars(
        select(UnityMember)
        .where(UnityMember.idpk_unity == unity.idpk)
        .order_by(UnityMember.idpk.asc())
    )
    member_rows = list(rows.all())
    if member_rows or not unity.members or unity.members == "{}":
        return member_rows

    for member_idpk, role in _parse_legacy_unity_members(unity):
        session.add(
            UnityMember(idpk_unity=unity.idpk, idpk_user=member_idpk, role=role)
        )
    await session.flush()
    rows = await session.scalars(
        select(UnityMember)
        .where(UnityMember.idpk_unity == unity.idpk)
        .order_by(UnityMember.idpk.asc())
    )
    return list(rows.all())


async def list_unity_member_rows(
    session: AsyncSession, unity: Unity
) -> list[UnityMember]:
    return await ensure_unity_members_backfilled(session=session, unity=unity)


async def list_unity_member_ids(session: AsyncSession, unity: Unity) -> list[int]:
    rows = await list_unity_member_rows(session=session, unity=unity)
    return [int(row.idpk_user) for row in rows] + [int(unity.idpk_user)]


async def count_unity_members(session: AsyncSession, unity: Unity) -> int:
    rows = await list_unity_member_rows(session=session, unity=unity)
    return len(rows) + 1


async def add_unity_member(
    session: AsyncSession,
    unity: Unity,
    member_idpk: int,
    *,
    role: str = "member",
) -> None:
    rows = await list_unity_member_rows(session=session, unity=unity)
    if any(int(row.idpk_user) == int(member_idpk) for row in rows):
        return
    session.add(
        UnityMember(idpk_unity=unity.idpk, idpk_user=int(member_idpk), role=role)
    )
    await session.flush()
    rows = await list_unity_member_rows(session=session, unity=unity)
    unity.members = _serialize_unity_members(rows)


async def remove_unity_member(
    session: AsyncSession,
    unity: Unity,
    member_idpk: int,
) -> None:
    rows = await list_unity_member_rows(session=session, unity=unity)
    for row in rows:
        if int(row.idpk_user) != int(member_idpk):
            continue
        await session.delete(row)
        await session.flush()
        break
    rows = await list_unity_member_rows(session=session, unity=unity)
    unity.members = _serialize_unity_members(rows)


async def pop_next_unity_owner(session: AsyncSession, unity: Unity) -> int | None:
    rows = await list_unity_member_rows(session=session, unity=unity)
    if not rows:
        unity.members = "{}"
        return None
    promoted = rows[0]
    await session.delete(promoted)
    await session.flush()
    rows = await list_unity_member_rows(session=session, unity=unity)
    unity.members = _serialize_unity_members(rows)
    return int(promoted.idpk_user)


def _parse_legacy_transfer_claims(transfer: TransferMoney) -> list[int]:
    used_raw = str(transfer.used or "")
    result: list[int] = []
    for chunk in used_raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            result.append(int(chunk))
        except ValueError:
            continue
    return result


async def ensure_transfer_claims_backfilled(
    session: AsyncSession,
    transfer: TransferMoney,
) -> None:
    existing = await session.scalar(
        select(TransferMoneyClaim.idpk)
        .where(TransferMoneyClaim.idpk_transfer == transfer.idpk)
        .limit(1)
    )
    if existing is not None:
        return
    for user_idpk in _parse_legacy_transfer_claims(transfer):
        session.add(
            TransferMoneyClaim(idpk_transfer=transfer.idpk, idpk_user=user_idpk)
        )
    await session.flush()


async def has_transfer_claim(
    session: AsyncSession,
    transfer_idpk: int,
    user_idpk: int,
) -> bool:
    transfer = await session.get(TransferMoney, transfer_idpk)
    if not transfer:
        return False
    await ensure_transfer_claims_backfilled(session=session, transfer=transfer)
    existing = await session.scalar(
        select(TransferMoneyClaim.idpk)
        .where(
            TransferMoneyClaim.idpk_transfer == transfer_idpk,
            TransferMoneyClaim.idpk_user == user_idpk,
        )
        .limit(1)
    )
    return existing is not None


async def add_transfer_claim(
    session: AsyncSession,
    transfer_idpk: int,
    user_idpk: int,
) -> None:
    transfer = await session.get(TransferMoney, transfer_idpk)
    if not transfer:
        return
    await ensure_transfer_claims_backfilled(session=session, transfer=transfer)
    if await has_transfer_claim(
        session=session, transfer_idpk=transfer_idpk, user_idpk=user_idpk
    ):
        return
    session.add(TransferMoneyClaim(idpk_transfer=transfer_idpk, idpk_user=user_idpk))
    await session.flush()


async def list_transfer_claim_user_ids(
    session: AsyncSession,
    transfer_idpk: int,
) -> set[int]:
    transfer = await session.get(TransferMoney, transfer_idpk)
    if not transfer:
        return set()
    await ensure_transfer_claims_backfilled(session=session, transfer=transfer)
    rows = await session.scalars(
        select(TransferMoneyClaim.idpk_user).where(
            TransferMoneyClaim.idpk_transfer == transfer_idpk
        )
    )
    return {int(item) for item in rows.all()}


def _parse_legacy_animals(user: User) -> dict[str, int]:
    raw = getattr(user, "animals", None)
    if not raw or raw == "{}":
        return {}
    try:
        payload = json_loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    result: dict[str, int] = {}
    for code_name, quantity in payload.items():
        code = str(code_name).strip()
        if not code:
            continue
        try:
            qty = int(quantity or 0)
        except (TypeError, ValueError):
            continue
        if qty > 0:
            result[code] = qty
    return result


def _serialize_animals(payload: dict[str, int]) -> str:
    return json_dumps(payload)


async def ensure_user_animals_backfilled(
    session: AsyncSession, user: User
) -> list[UserAnimalState]:
    rows = await session.scalars(
        select(UserAnimalState)
        .where(UserAnimalState.idpk_user == user.idpk)
        .order_by(UserAnimalState.idpk.asc())
    )
    animal_rows = list(rows.all())
    if animal_rows or not user.animals or user.animals == "{}":
        return animal_rows

    for code_name, quantity in _parse_legacy_animals(user).items():
        session.add(
            UserAnimalState(
                idpk_user=user.idpk,
                animal_code_name=code_name,
                quantity=quantity,
            )
        )
    await session.flush()
    rows = await session.scalars(
        select(UserAnimalState)
        .where(UserAnimalState.idpk_user == user.idpk)
        .order_by(UserAnimalState.idpk.asc())
    )
    return list(rows.all())


async def list_user_animal_rows(
    session: AsyncSession, user: User
) -> list[UserAnimalState]:
    return await ensure_user_animals_backfilled(session=session, user=user)


async def get_user_animals_map(session: AsyncSession, user: User) -> dict[str, int]:
    rows = await list_user_animal_rows(session=session, user=user)
    payload = {
        str(row.animal_code_name): int(row.quantity)
        for row in rows
        if int(row.quantity or 0) > 0
    }
    user.animals = _serialize_animals(payload)
    return payload


async def get_user_total_animals(session: AsyncSession, user: User) -> int:
    animals = await get_user_animals_map(session=session, user=user)
    return sum(animals.values())


async def add_user_animals(
    session: AsyncSession,
    user: User,
    animal_code_name: str,
    quantity: int,
) -> None:
    if quantity <= 0:
        return
    rows = await list_user_animal_rows(session=session, user=user)
    target = next(
        (row for row in rows if str(row.animal_code_name) == str(animal_code_name)),
        None,
    )
    if target is None:
        session.add(
            UserAnimalState(
                idpk_user=user.idpk,
                animal_code_name=str(animal_code_name),
                quantity=int(quantity),
            )
        )
    else:
        target.quantity = int(target.quantity) + int(quantity)
        target.updated_at = datetime.now()
    await session.flush()
    user.animals = _serialize_animals(
        await get_user_animals_map(session=session, user=user)
    )


def _parse_legacy_aviaries(user: User) -> dict[str, dict[str, int]]:
    raw = getattr(user, "aviaries", None)
    if not raw or raw == "{}":
        return {}
    try:
        payload = json_loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    result: dict[str, dict[str, int]] = {}
    for code_name, row in payload.items():
        if not isinstance(row, dict):
            continue
        code = str(code_name).strip()
        if not code:
            continue
        try:
            quantity = int(row.get("quantity", 0) or 0)
            buy_count = int(row.get("buy_count", 0) or 0)
            price = int(row.get("price", 0) or 0)
        except (TypeError, ValueError):
            continue
        if quantity > 0:
            result[code] = {
                "quantity": quantity,
                "buy_count": buy_count,
                "price": price,
            }
    return result


def _serialize_aviaries(payload: dict[str, dict[str, int]]) -> str:
    return json_dumps(payload)


async def ensure_user_aviaries_backfilled(
    session: AsyncSession, user: User
) -> list[UserAviaryState]:
    rows = await session.scalars(
        select(UserAviaryState)
        .where(UserAviaryState.idpk_user == user.idpk)
        .order_by(UserAviaryState.idpk.asc())
    )
    aviary_rows = list(rows.all())
    if aviary_rows or not user.aviaries or user.aviaries == "{}":
        return aviary_rows

    for code_name, row in _parse_legacy_aviaries(user).items():
        session.add(
            UserAviaryState(
                idpk_user=user.idpk,
                aviary_code_name=code_name,
                quantity=int(row.get("quantity", 0) or 0),
                buy_count=int(row.get("buy_count", 0) or 0),
                current_price=int(row.get("price", 0) or 0),
            )
        )
    await session.flush()
    rows = await session.scalars(
        select(UserAviaryState)
        .where(UserAviaryState.idpk_user == user.idpk)
        .order_by(UserAviaryState.idpk.asc())
    )
    return list(rows.all())


async def list_user_aviary_rows(
    session: AsyncSession, user: User
) -> list[UserAviaryState]:
    return await ensure_user_aviaries_backfilled(session=session, user=user)


async def get_user_aviaries_map(
    session: AsyncSession, user: User
) -> dict[str, dict[str, int]]:
    rows = await list_user_aviary_rows(session=session, user=user)
    payload = {
        str(row.aviary_code_name): {
            "quantity": int(row.quantity),
            "buy_count": int(row.buy_count),
            "price": int(row.current_price),
        }
        for row in rows
        if int(row.quantity or 0) > 0
    }
    user.aviaries = _serialize_aviaries(payload)
    return payload


async def upsert_user_aviary(
    session: AsyncSession,
    user: User,
    aviary_code_name: str,
    *,
    quantity_delta: int,
    buy_count_delta: int = 0,
    current_price: int | None = None,
) -> None:
    rows = await list_user_aviary_rows(session=session, user=user)
    target = next(
        (row for row in rows if str(row.aviary_code_name) == str(aviary_code_name)),
        None,
    )
    if target is None:
        session.add(
            UserAviaryState(
                idpk_user=user.idpk,
                aviary_code_name=str(aviary_code_name),
                quantity=max(0, int(quantity_delta)),
                buy_count=max(0, int(buy_count_delta)),
                current_price=max(0, int(current_price or 0)),
            )
        )
    else:
        target.quantity = max(0, int(target.quantity) + int(quantity_delta))
        target.buy_count = max(0, int(target.buy_count) + int(buy_count_delta))
        if current_price is not None:
            target.current_price = max(0, int(current_price))
        target.updated_at = datetime.now()
    await session.flush()
    user.aviaries = _serialize_aviaries(
        await get_user_aviaries_map(session=session, user=user)
    )
