import json
from pathlib import Path
from typing import Any

import aiofiles


def _resolve_log_path(log_path: str) -> Path:
    path = Path(log_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


async def _append_jsonl(log_path: str, payload: dict[str, Any]) -> None:
    path = _resolve_log_path(log_path)
    async with aiofiles.open(path, mode="a", encoding="utf-8") as file:
        await file.write(json.dumps(payload, ensure_ascii=False) + "\n")


async def log_npc_decision(log_path: str, payload: dict[str, Any]) -> None:
    await _append_jsonl(log_path=log_path, payload=payload)


async def log_npc_usage(log_path: str, payload: dict[str, Any]) -> None:
    base_path = _resolve_log_path(log_path)
    usage_path = base_path.with_name(f"{base_path.stem}_usage.jsonl")
    await _append_jsonl(log_path=str(usage_path), payload=payload)
