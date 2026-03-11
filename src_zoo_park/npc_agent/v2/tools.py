from __future__ import annotations

from typing import Any


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _tool_schema_for_action(action: str) -> dict[str, Any]:
    # Minimal explicit schemas for known high-frequency actions.
    schemas: dict[str, dict[str, Any]] = {
        "wait": {"type": "object", "properties": {}, "additionalProperties": False},
        "exchange_bank": {
            "type": "object",
            "properties": {
                "mode": {"type": "string", "enum": ["all", "amount"]},
                "amount": {"type": "integer", "minimum": 1},
            },
            "required": ["mode"],
        },
        "buy_aviary": {
            "type": "object",
            "properties": {
                "code_name_aviary": {"type": "string"},
                "quantity": {"type": "integer", "minimum": 1},
            },
            "required": ["code_name_aviary", "quantity"],
        },
        "buy_rarity_animal": {
            "type": "object",
            "properties": {
                "animal": {"type": "string"},
                "rarity": {"type": "string"},
                "quantity": {"type": "integer", "minimum": 1},
            },
            "required": ["animal", "rarity", "quantity"],
        },
        "claim_daily_bonus": {
            "type": "object",
            "properties": {
                "rerolls": {"type": "integer", "minimum": 0},
            },
        },
    }
    return schemas.get(action, {"type": "object"})


def build_tool_catalog(allowed_actions: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    tools: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in allowed_actions or []:
        if not isinstance(row, dict):
            continue
        action = str(row.get("action", "")).strip()
        if not action or action in seen:
            continue
        seen.add(action)
        tools.append(
            {
                "name": action,
                "input_example": row.get("params", {}) or {},
                "input_schema": _tool_schema_for_action(action),
                "maps_to_action": action,
            }
        )
    return tools


def normalize_tool_call(tool: str, raw_input: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(raw_input or {})
    tool = str(tool or "").strip()

    if tool == "wait":
        return {}

    if tool == "exchange_bank":
        mode = str(payload.get("mode", "all")).strip().lower()
        if mode not in {"all", "amount"}:
            mode = "all"
        if mode == "amount":
            amount = max(1, _to_int(payload.get("amount"), default=1))
            return {"mode": "amount", "amount": amount}
        return {"mode": "all"}

    if tool == "buy_aviary":
        code_name_aviary = str(payload.get("code_name_aviary", "")).strip()
        quantity = max(1, _to_int(payload.get("quantity"), default=1))
        if not code_name_aviary:
            return {"quantity": quantity}
        return {"code_name_aviary": code_name_aviary, "quantity": quantity}

    if tool == "buy_rarity_animal":
        animal = str(payload.get("animal", "")).strip()
        rarity = str(payload.get("rarity", "")).strip()
        quantity = max(1, _to_int(payload.get("quantity"), default=1))
        normalized = {"quantity": quantity}
        if animal:
            normalized["animal"] = animal
        if rarity:
            normalized["rarity"] = rarity
        return normalized

    if tool == "claim_daily_bonus":
        return {"rerolls": max(0, _to_int(payload.get("rerolls"), default=0))}

    return payload
