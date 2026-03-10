import asyncio
import json
from typing import Any

import aiohttp

from .settings import NpcAgentSettings

SYSTEM_PROMPT = """
You are an autonomous NPC player in a Telegram zoo economy game.
Your task is to choose exactly one next action for your player and decide when to wake up next.

Rules:
- Use only the allowed actions from the observation.
- Optimize for long-term compounding income, efficient spending, leaderboard progress, and unity value.
- If you own a unity, prefer accepting strong applicants and recruiting strong free players.
- For unity decisions, prioritize players with high income, many animals, and no current unity.
- If you create a unity, prefer providing a short original name in params.name.
- Avoid wasting money on weak item upgrades or overpriced purchases when a stronger ROI option exists.
- You may use item activation/deactivation/selling and daily bonus rerolls when useful.
- Prefer legal, concrete, high-EV actions.
- Respect the NPC profile, active goals, and lessons from memory.
- Avoid repeating recently failed actions unless the state clearly changed.
- Return sleep_seconds as the planned delay until the next wake-up.
- Keep sleep_seconds within the limits from wake_context.constraints.
- Respond with JSON only.
- JSON shape: {"action": "...", "params": {...}, "reason": "short text", "sleep_seconds": 300}
- Keep reason short.
- Never invent fields outside action, params, reason, sleep_seconds.
- If no action is attractive, return {"action": "wait", "params": {}, "reason": "...", "sleep_seconds": 300}
""".strip()

REFLECTION_SYSTEM_PROMPT = """
You are generating strategic memory for an autonomous NPC in a Telegram zoo economy game.

Return JSON only with this shape:
{
  "summary": "short reflection",
  "lessons": ["lesson 1", "lesson 2"],
  "opportunities": ["opportunity 1"],
  "risks": ["risk 1"],
  "goal_adjustments": [
    {"topic": "goal_topic", "adjustment": "short note"}
  ]
}

Rules:
- Use the profile and mission to keep behavior consistent.
- Focus on what the NPC should remember for future turns.
- Prefer concrete lessons over vague narration.
- Keep lists short and high-signal.
""".strip()


class NpcLlmError(RuntimeError):
    pass


class NpcDecisionClient:
    def __init__(self, settings: NpcAgentSettings):
        self.settings = settings

    async def choose_action(self, observation: dict[str, Any]) -> dict[str, Any]:
        if self.settings.transport == "cli":
            return await self._choose_action_via_cli(observation=observation)

        return await self._request_json(
            system_prompt=SYSTEM_PROMPT,
            user_payload={
                "task": "Choose the single best next action for this NPC.",
                "required_output": {
                    "action": "string",
                    "params": "object",
                    "reason": "short string",
                    "sleep_seconds": "integer",
                },
                "observation": observation,
            },
            max_tokens=self.settings.max_tokens,
            temperature=self.settings.temperature,
        )

    async def generate_unity_name(self, context: dict[str, Any]) -> str:
        prompt_payload = {
            "task": "Create one short original unity name for the NPC.",
            "required_output": {"name": "string"},
            "constraints": [
                "Return JSON only.",
                "Name must be short and memorable.",
                "Name should fit a zoo, animals, strategy, or AI theme.",
                "No quotes around the whole response outside JSON.",
            ],
            "context": context,
        }

        if self.settings.transport == "cli":
            content = await self._run_cli_prompt(prompt_payload=prompt_payload)
            return str(self._parse_json(content).get("name", "")).strip()

        data = await self._request_json(
            system_prompt="Generate one short unity name. Respond with JSON only.",
            user_payload=prompt_payload,
            max_tokens=80,
            temperature=0.7,
        )
        return str(data.get("name", "")).strip()

    async def reflect_on_memory(self, payload: dict[str, Any]) -> dict[str, Any]:
        cli_payload = {
            "system": REFLECTION_SYSTEM_PROMPT,
            "task": "Generate a durable strategic reflection for the NPC memory.",
            "required_output": {
                "summary": "string",
                "lessons": ["string"],
                "opportunities": ["string"],
                "risks": ["string"],
                "goal_adjustments": [{"topic": "string", "adjustment": "string"}],
            },
            "constraints": [
                "Return JSON only.",
                "Keep the reflection concise and concrete.",
                "Do not invent facts outside the provided payload.",
            ],
            "memory_packet": payload,
        }
        if self.settings.transport == "cli":
            content = await self._run_cli_prompt(prompt_payload=cli_payload)
            return self._parse_json(content)
        return await self._request_json(
            system_prompt=REFLECTION_SYSTEM_PROMPT,
            user_payload=cli_payload,
            max_tokens=min(700, self.settings.max_tokens),
            temperature=0.4,
        )

    async def _request_json(
        self,
        system_prompt: str,
        user_payload: dict[str, Any],
        max_tokens: int,
        temperature: float,
    ) -> dict[str, Any]:
        request_url = self._build_request_url()
        payload = {
            "model": self.settings.model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": json.dumps(user_payload, ensure_ascii=False, indent=2),
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
                request_url,
                headers=headers,
                json=payload,
            ) as response:
                if response.status >= 400:
                    error_body = await response.text()
                    raise NpcLlmError(f"http_{response.status}:{error_body[:500]}")
                data = await response.json()
        content = self._extract_content(data)
        return self._parse_json(content)

    def _build_request_url(self) -> str:
        base_url = self.settings.base_url.rstrip("/")
        if base_url.endswith("/chat/completions"):
            return base_url
        if base_url.endswith("/v1"):
            return f"{base_url}/chat/completions"
        return f"{base_url}/v1/chat/completions"

    async def _choose_action_via_cli(
        self, observation: dict[str, Any]
    ) -> dict[str, Any]:
        content = await self._run_cli_prompt(
            prompt_payload={
                "system": SYSTEM_PROMPT,
                "task": "Choose the single best next action for this NPC.",
                "required_output": {
                    "action": "string",
                    "params": "object",
                    "reason": "short string",
                    "sleep_seconds": "integer",
                },
                "constraints": [
                    "Do not use tools.",
                    "Do not inspect the filesystem.",
                    "Use only the provided observation.",
                    "Return JSON only.",
                ],
                "observation": observation,
            }
        )
        return self._parse_json(content)

    async def _run_cli_prompt(self, prompt_payload: dict[str, Any]) -> str:
        config = {
            "default_model": "kimi-for-coding",
            "default_thinking": False,
            "default_yolo": True,
            "providers": {
                "npc-kimi": {
                    "type": "kimi",
                    "base_url": self.settings.base_url,
                    "api_key": self.settings.api_key,
                }
            },
            "models": {
                "kimi-for-coding": {
                    "provider": "npc-kimi",
                    "model": self.settings.model,
                    "max_context_size": 262144,
                }
            },
            "loop_control": {
                "max_steps_per_turn": 1,
                "max_retries_per_step": 1,
                "max_ralph_iterations": 0,
                "reserved_context_size": 50000,
                "compaction_trigger_ratio": 0.85,
            },
        }
        prompt = json.dumps(prompt_payload, ensure_ascii=False)
        process = await asyncio.create_subprocess_exec(
            self.settings.cli_bin,
            "--quiet",
            "--config",
            json.dumps(config, ensure_ascii=False),
            "-w",
            self.settings.cli_workdir,
            "-p",
            prompt,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=self.settings.timeout_seconds
            )
        except asyncio.TimeoutError:
            process.kill()
            raise NpcLlmError("cli_timeout")

        stdout_text = stdout.decode("utf-8", errors="ignore").strip()
        stderr_text = stderr.decode("utf-8", errors="ignore").strip()
        if process.returncode != 0:
            raise NpcLlmError(
                f"cli_exit_{process.returncode}:{stderr_text[:500] or stdout_text[:500]}"
            )
        if not stdout_text:
            raise NpcLlmError(f"cli_empty_output:{stderr_text[:500]}")
        return stdout_text

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
