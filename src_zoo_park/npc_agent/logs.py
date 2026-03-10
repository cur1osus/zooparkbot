import json
from pathlib import Path
from typing import Any

import aiofiles


async def log_npc_decision(log_path: str, payload: dict[str, Any]) -> None:
    path = Path(log_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(path, mode="a", encoding="utf-8") as file:
        await file.write(json.dumps(payload, ensure_ascii=False) + "\n")
