import json
from typing import Any

import aiohttp

from .settings import NpcAgentSettings

SYSTEM_PROMPT = """
You are an autonomous NPC player in a Telegram zoo economy game.
Your task is to choose exactly one next action for your player.

Rules:
- Use only the allowed actions from the observation.
- Optimize for long-term compounding income, efficient spending, leaderboard progress, and unity value.
- If you own a unity, prefer accepting strong applicants and recruiting strong free players.
- For unity decisions, prioritize players with high income, many animals, and no current unity.
- Avoid wasting money on weak item upgrades or overpriced purchases when a stronger ROI option exists.
- You may use item activation/deactivation/selling and daily bonus rerolls when useful.
- Prefer legal, concrete, high-EV actions.
- Respond with JSON only.
- JSON shape: {"action": "...", "params": {...}, "reason": "short text"}
- Keep reason short.
- Never invent fields outside action, params, reason.
- If no action is attractive, return {"action": "wait", "params": {}, "reason": "..."}
""".strip()


class NpcDecisionClient:
    def __init__(self, settings: NpcAgentSettings):
        self.settings = settings

    async def choose_action(self, observation: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "model": self.settings.model,
            "temperature": self.settings.temperature,
            "max_tokens": self.settings.max_tokens,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "task": "Choose the single best next action for this NPC.",
                            "required_output": {
                                "action": "string",
                                "params": "object",
                                "reason": "short string",
                            },
                            "observation": observation,
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                },
            ],
        }
        timeout = aiohttp.ClientTimeout(total=self.settings.timeout_seconds)
        headers = {
            "Authorization": f"Bearer {self.settings.api_key}",
            "Content-Type": "application/json",
        }
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                self.settings.base_url,
                headers=headers,
                json=payload,
            ) as response:
                response.raise_for_status()
                data = await response.json()
        content = self._extract_content(data)
        return self._parse_json(content)

    def _extract_content(self, data: dict[str, Any]) -> str:
        choices = data.get("choices") or []
        if not choices:
            raise ValueError("LLM response does not contain choices")
        message = choices[0].get("message") or {}
        content = message.get("content", "")
        if isinstance(content, list):
            parts = []
            for chunk in content:
                if isinstance(chunk, dict) and chunk.get("type") == "text":
                    parts.append(chunk.get("text", ""))
            return "".join(parts)
        return str(content)

    def _parse_json(self, content: str) -> dict[str, Any]:
        clean_content = content.strip()
        if clean_content.startswith("```"):
            clean_content = clean_content.strip("`")
            if clean_content.startswith("json"):
                clean_content = clean_content[4:].strip()
        start = clean_content.find("{")
        end = clean_content.rfind("}")
        if start == -1 or end == -1:
            raise ValueError("LLM response does not contain JSON object")
        return json.loads(clean_content[start : end + 1])
