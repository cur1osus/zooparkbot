from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import delete, inspect, insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from .models import (
    TransferMoneyClaim,
    UnityMember,
    UserAnimalState,
    UserAviaryState,
    UserHistoryEvent,
)
from .structured_state import (
    parse_legacy_animals_payload,
    parse_legacy_aviaries_payload,
    parse_legacy_history_payload,
    parse_legacy_transfer_claims_payload,
    parse_legacy_unity_members_payload,
)


def _column_names(sync_conn, table_name: str) -> set[str]:
    inspector = inspect(sync_conn)
    try:
        return {column["name"] for column in inspector.get_columns(table_name)}
    except Exception:
        return set()


async def _get_columns(engine: AsyncEngine, table_name: str) -> set[str]:
    async with engine.begin() as conn:
        return await conn.run_sync(_column_names, table_name)


async def _backfill_history(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        rows = await conn.execute(
            text(
                "SELECT idpk, history_moves FROM users "
                "WHERE history_moves IS NOT NULL AND history_moves <> '{}'"
            )
        )
        for row in rows.mappings().all():
            user_idpk = int(row["idpk"])
            entries = parse_legacy_history_payload(row["history_moves"])
            await conn.execute(
                delete(UserHistoryEvent).where(
                    UserHistoryEvent.idpk_user == user_idpk,
                    UserHistoryEvent.source == "legacy_history_moves",
                )
            )
            if not entries:
                continue
            await conn.execute(
                insert(UserHistoryEvent),
                [
                    {
                        "idpk_user": user_idpk,
                        "event_time": entry["event_time"],
                        "event_kind": entry["event_kind"],
                        "event_text": entry["event_text"],
                        "payload": entry["payload"],
                        "source": entry["source"],
                    }
                    for entry in entries
                ],
            )


async def _backfill_unity_members(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        rows = await conn.execute(
            text(
                "SELECT idpk, members FROM unity "
                "WHERE members IS NOT NULL AND members <> '{}'"
            )
        )
        for row in rows.mappings().all():
            unity_idpk = int(row["idpk"])
            members = parse_legacy_unity_members_payload(row["members"])
            await conn.execute(
                delete(UnityMember).where(UnityMember.idpk_unity == unity_idpk)
            )
            if not members:
                continue
            await conn.execute(
                insert(UnityMember),
                [
                    {
                        "idpk_unity": unity_idpk,
                        "idpk_user": int(member_idpk),
                        "role": role,
                    }
                    for member_idpk, role in members
                ],
            )


async def _backfill_transfer_claims(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        rows = await conn.execute(
            text(
                "SELECT idpk, used FROM transfer_money "
                "WHERE used IS NOT NULL AND used <> ''"
            )
        )
        for row in rows.mappings().all():
            transfer_idpk = int(row["idpk"])
            legacy_claims = parse_legacy_transfer_claims_payload(row["used"])
            if not legacy_claims:
                continue
            existing_rows = await conn.execute(
                select(TransferMoneyClaim.idpk_user).where(
                    TransferMoneyClaim.idpk_transfer == transfer_idpk
                )
            )
            existing = {int(item) for item in existing_rows.scalars().all()}
            missing = [
                user_idpk for user_idpk in legacy_claims if user_idpk not in existing
            ]
            if not missing:
                continue
            await conn.execute(
                insert(TransferMoneyClaim),
                [
                    {
                        "idpk_transfer": transfer_idpk,
                        "idpk_user": int(user_idpk),
                    }
                    for user_idpk in missing
                ],
            )


async def _replace_state_rows(
    engine: AsyncEngine,
    *,
    table_name: str,
    column_name: str,
    parse_payload,
    model,
    key_field: str,
    value_builder,
) -> None:
    async with engine.begin() as conn:
        rows = await conn.execute(
            text(
                f"SELECT idpk, {column_name} FROM users "
                f"WHERE {column_name} IS NOT NULL AND {column_name} <> '{{}}'"
            )
        )
        for row in rows.mappings().all():
            user_idpk = int(row["idpk"])
            payload = parse_payload(row[column_name])
            await conn.execute(
                delete(model).where(getattr(model, "idpk_user") == user_idpk)
            )
            if not payload:
                continue
            await conn.execute(
                insert(model),
                [
                    value_builder(user_idpk, key, value)
                    for key, value in payload.items()
                ],
            )


async def _backfill_animals(engine: AsyncEngine) -> None:
    await _replace_state_rows(
        engine,
        table_name="users",
        column_name="animals",
        parse_payload=parse_legacy_animals_payload,
        model=UserAnimalState,
        key_field="animal_code_name",
        value_builder=lambda user_idpk, key, value: {
            "idpk_user": user_idpk,
            "animal_code_name": str(key),
            "quantity": int(value),
        },
    )


async def _backfill_aviaries(engine: AsyncEngine) -> None:
    await _replace_state_rows(
        engine,
        table_name="users",
        column_name="aviaries",
        parse_payload=parse_legacy_aviaries_payload,
        model=UserAviaryState,
        key_field="aviary_code_name",
        value_builder=lambda user_idpk, key, value: {
            "idpk_user": user_idpk,
            "aviary_code_name": str(key),
            "quantity": int(value.get("quantity", 0) or 0),
            "buy_count": int(value.get("buy_count", 0) or 0),
            "current_price": int(value.get("price", 0) or 0),
        },
    )


async def _drop_columns(
    engine: AsyncEngine, table_name: str, columns: Iterable[str]
) -> None:
    current_columns = await _get_columns(engine, table_name)
    columns_to_drop = [column for column in columns if column in current_columns]
    if not columns_to_drop:
        return
    clause = ", ".join(f"DROP COLUMN {column}" for column in columns_to_drop)
    async with engine.begin() as conn:
        await conn.execute(text(f"ALTER TABLE {table_name} {clause}"))


async def finalize_legacy_schema(engine: AsyncEngine) -> None:
    user_columns = await _get_columns(engine, "users")
    unity_columns = await _get_columns(engine, "unity")
    transfer_columns = await _get_columns(engine, "transfer_money")

    if "history_moves" in user_columns:
        await _backfill_history(engine)
    if "members" in unity_columns:
        await _backfill_unity_members(engine)
    if "used" in transfer_columns:
        await _backfill_transfer_claims(engine)
    if "animals" in user_columns:
        await _backfill_animals(engine)
    if "aviaries" in user_columns:
        await _backfill_aviaries(engine)

    await _drop_columns(engine, "users", ["history_moves", "animals", "aviaries"])
    await _drop_columns(engine, "unity", ["members"])
    await _drop_columns(engine, "transfer_money", ["used"])
