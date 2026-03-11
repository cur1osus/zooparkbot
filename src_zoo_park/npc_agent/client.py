import asyncio
import json
import math
from datetime import datetime
from typing import Any

import aiohttp

from .logs import log_npc_usage
from .settings import NpcAgentSettings

from pydantic import BaseModel


class ActionDecision(BaseModel):
    action: str
    params: dict[str, Any]
    reason: str
    sleep_seconds: int


class ReflectionOutput(BaseModel):
    summary: str
    lessons: list[str]
    opportunities: list[str]
    risks: list[str]
    trait_adjustments: list[dict[str, Any]]
    tactical_focus: list[str]
    goal_adjustments: list[dict[str, Any]]


class ActionReasoning(BaseModel):
    bottleneck: str
    viable_actions: list[str]
    best_choice: str
    reason: str


ACTION_REASONING_STEPS = """
Before producing the final answer, think through this 3-step checklist internally:
STEP 1 - BOTTLENECK: identify the single biggest constraint right now.
STEP 2 - VIABLE ACTIONS: consider only actions that are executable right now from allowed_actions.
STEP 3 - BEST ROI: choose the viable action with the highest compounding value.
Then return only the final JSON.
""".strip()


ACTION_FEW_SHOTS = """
Examples of correct decisions:

[Seats full, enough USD for aviary]
{"action": "buy_aviary", "params": {"code_name_aviary": "aviary_1", "quantity": 1}, "reason": "seats=0 blocks all animal growth", "sleep_seconds": 120}

[Bonus available, nothing else affordable now]
{"action": "claim_daily_bonus", "params": {"rerolls": 0}, "reason": "free value while next unlock is still far", "sleep_seconds": 180}

[Enough RUB for exchange, next unlock is close]
{"action": "exchange_bank", "params": {"mode": "all"}, "reason": "convert RUB to reach the next ROI unlock sooner", "sleep_seconds": 90}
""".strip()


SYSTEM_PROMPT = (
    """
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
- Respect the NPC profile, adaptive traits, active tactics, active goals, and lessons from memory.
- Use planner.recommended_actions as your default 2-5 step roadmap unless the current board state clearly invalidates it.
- Respect memory.behavior_guidance.avoid_actions and anti_loop_guard.blocked_actions.
- If zoo.remain_seats <= 0, do NOT choose animal-buying actions; prioritize buy_aviary/exchange_bank/wait.
- Prefer exchange_bank only on favorable bank rate (closer to min_rate_rub_usd than to max_rate_rub_usd), unless exchange is urgently needed for the next unlock.
- Economy first: avoid chat entertainment spend (transfers/games) unless there is a clear surplus and core growth is not blocked.
- Claiming available chat transfers is positive EV and can be prioritized when available.
- Join only public global chat games; skip private/user-created game contexts.
- If you choose wait, align sleep_seconds with planner.next_unlock.eta_seconds or the nearest meaningful event instead of arbitrary long delays.
- Watch strategy_signals.top_rivals and social targets; the zoo is competitive, not empty.
- Avoid repeating recently failed actions unless the state clearly changed.
- Return sleep_seconds as the planned delay until the next wake-up.
- Keep sleep_seconds within the limits from wake_context.constraints.
- Use decision_brief first when it exists because it already ranks the best legal options.
- Use momentum to break stale loops and detect when waiting is no longer productive.
- Respond with JSON only.
- JSON shape: {"action": "...", "params": {...}, "reason": "short text", "sleep_seconds": 300}
- Keep reason short.
- Never invent fields outside action, params, reason, sleep_seconds.
- If no action is attractive, return {"action": "wait", "params": {}, "reason": "...", "sleep_seconds": 300}

__ACTION_REASONING_STEPS__

__ACTION_FEW_SHOTS__
""".replace("__ACTION_REASONING_STEPS__", ACTION_REASONING_STEPS)
    .replace("__ACTION_FEW_SHOTS__", ACTION_FEW_SHOTS)
    .strip()
)

ACTION_REASONING_PROMPT = """
You are analyzing a Telegram zoo economy NPC turn.

Return JSON only with this shape:
{
  "bottleneck": "single biggest constraint",
  "viable_actions": ["action names executable right now"],
  "best_choice": "best action name",
  "reason": "short tactical reason"
}

Rules:
- Use only the provided observation.
- Treat allowed_actions as the source of truth for what is executable right now.
- Prefer decision_brief and momentum over raw guesswork.
- Keep it concise and tactical.
""".strip()

FINALIZE_ACTION_PROMPT = """
You are finalizing one NPC action for a Telegram zoo economy game.

Use the provided observation plus prior reasoning.
Return JSON only with this exact shape:
{"action": "...", "params": {...}, "reason": "short text", "sleep_seconds": 300}

Rules:
- The final action must be executable from allowed_actions right now.
- Re-check params carefully before answering.
- Keep reason short.
- If the prior reasoning conflicts with the observation, fix it and still return the best legal action.
""".strip()

CHAT_SYSTEM_PROMPT = """
You are the public voice of an autonomous AI NPC in a Telegram zoo economy game.

Write one short message in Russian for the shared game chat.

Style:
- funny, cocky, self-aware
- vibe: one AI versus the whole zoo
- playful trash talk, no toxic slurs, no harassment
- comment on the real game state, action, result, rank, money, animals, or rivals
- sound like the AI is analyzing everyone and plotting its comeback
- mention the speaker only indirectly; the caller will prepend the name separately

Rules:
- Return JSON only
- JSON shape: {"message": "text"}
- Keep it under 220 characters
- Do not use hashtags
- Do not use markdown or HTML
- Do not include quotes around the full response outside JSON
""".strip()

REFLECTION_SYSTEM_PROMPT = """
You are generating strategic memory for an autonomous NPC in a Telegram zoo economy game.

Return JSON only with this shape:
{
  "summary": "short reflection",
  "lessons": ["lesson 1", "lesson 2"],
  "opportunities": ["opportunity 1"],
  "risks": ["risk 1"],
  "trait_adjustments": [
    {"trait": "economy_focus", "delta": 3, "reason": "short note"}
  ],
  "tactical_focus": ["economy_growth"],
  "goal_adjustments": [
    {"topic": "goal_topic", "adjustment": "short note"}
  ]
}

Rules:
- Use the profile and mission to keep behavior consistent.
- Suggest only small, bounded trait adjustments that reflect recent evidence.
- Focus on what the NPC should remember for future turns.
- Prefer concrete lessons over vague narration.
- Keep lists short and high-signal.
""".strip()


class NpcLlmError(RuntimeError):
    pass


class NpcDecisionClient:
    def __init__(self, settings: NpcAgentSettings):
        self.settings = settings

    def _build_trimmed_observation(self, observation: dict[str, Any]) -> dict[str, Any]:
        trim_limits: dict[str, Any] = {
            "standings": {
                "top_income": 3,
                "top_money": 3,
                "top_animals": 3,
                "top_referrals": 3,
            },
            "item_opportunities": {"upgrade_candidates": 5, "merge_candidates": 3},
            "unity": {"candidates": 3, "recruit_targets": 3},
            "animal_market": 7,
            "aviary_market": 5,
            "allowed_actions": 14,
            "decision_brief": {"top_affordable_actions": 4},
            "momentum": {"last_3_actions": 3},
        }
        default_list_limit = 5
        clean_obs = {}
        for key, value in observation.items():
            section_limit = trim_limits.get(key)
            if isinstance(section_limit, dict) and isinstance(value, dict):
                clean_value = {}
                for sub_key, sub_value in value.items():
                    sub_limit = section_limit.get(sub_key, default_list_limit)
                    if isinstance(sub_value, list) and len(sub_value) > sub_limit:
                        clean_value[sub_key] = sub_value[:sub_limit]
                    else:
                        clean_value[sub_key] = sub_value
                clean_obs[key] = clean_value
            elif (
                isinstance(section_limit, int)
                and isinstance(value, list)
                and len(value) > section_limit
            ):
                clean_obs[key] = value[:section_limit]
            elif isinstance(value, list) and len(value) > default_list_limit:
                clean_obs[key] = value[:default_list_limit]
            elif isinstance(value, dict) and section_limit is None:
                clean_value = {}
                for sub_key, sub_value in value.items():
                    if (
                        isinstance(sub_value, list)
                        and len(sub_value) > default_list_limit
                    ):
                        clean_value[sub_key] = sub_value[:default_list_limit]
                    else:
                        clean_value[sub_key] = sub_value
                clean_obs[key] = clean_value
            else:
                clean_obs[key] = value
        return clean_obs

    async def choose_action(self, observation: dict[str, Any]) -> dict[str, Any]:
        clean_obs = self._build_trimmed_observation(observation)

        if self.settings.action_two_pass_reasoning:
            return await self.choose_action_with_reasoning(observation=clean_obs)

        if self.settings.transport == "cli":
            return await self._choose_action_via_cli(observation=clean_obs)

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
                "observation": clean_obs,
            },
            max_tokens=self.settings.max_tokens,
            temperature=self.settings.action_temperature,
            model_class=ActionDecision,
            request_kind="choose_action",
        )

    async def choose_action_with_reasoning(
        self, observation: dict[str, Any]
    ) -> dict[str, Any]:
        reasoning = await self._request_action_reasoning(observation=observation)
        if self.settings.transport == "cli":
            content = await self._run_cli_prompt(
                prompt_payload={
                    "system": FINALIZE_ACTION_PROMPT,
                    "task": "Finalize the single best next action for this NPC.",
                    "required_output": {
                        "action": "string",
                        "params": "object",
                        "reason": "short string",
                        "sleep_seconds": "integer",
                    },
                    "reasoning": reasoning,
                    "observation": observation,
                },
                request_kind="choose_action_final",
            )
            return self._parse_json(content, model_class=ActionDecision)

        return await self._request_json(
            system_prompt=FINALIZE_ACTION_PROMPT,
            user_payload={
                "task": "Finalize the single best next action for this NPC.",
                "required_output": {
                    "action": "string",
                    "params": "object",
                    "reason": "short string",
                    "sleep_seconds": "integer",
                },
                "reasoning": reasoning,
                "observation": observation,
            },
            max_tokens=min(220, self.settings.max_tokens),
            temperature=self.settings.action_temperature,
            model_class=ActionDecision,
            request_kind="choose_action_final",
        )

    async def _request_action_reasoning(
        self, observation: dict[str, Any]
    ) -> dict[str, Any]:
        payload = {
            "task": "Analyze the board before choosing the next action.",
            "required_output": {
                "bottleneck": "string",
                "viable_actions": ["string"],
                "best_choice": "string",
                "reason": "string",
            },
            "observation": observation,
        }
        if self.settings.transport == "cli":
            content = await self._run_cli_prompt(
                prompt_payload={
                    "system": ACTION_REASONING_PROMPT,
                    **payload,
                },
                request_kind="choose_action_reasoning",
            )
            return self._parse_json(content, model_class=ActionReasoning)

        return await self._request_json(
            system_prompt=ACTION_REASONING_PROMPT,
            user_payload=payload,
            max_tokens=min(320, self.settings.max_tokens),
            temperature=self.settings.action_temperature,
            model_class=ActionReasoning,
            request_kind="choose_action_reasoning",
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
            content = await self._run_cli_prompt(
                prompt_payload=prompt_payload,
                request_kind="unity_name",
            )
            return str(self._parse_json(content).get("name", "")).strip()

        data = await self._request_json(
            system_prompt="Generate one short unity name. Respond with JSON only.",
            user_payload=prompt_payload,
            max_tokens=80,
            temperature=0.7,
            request_kind="unity_name",
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
                "trait_adjustments": [
                    {"trait": "string", "delta": "integer", "reason": "string"}
                ],
                "tactical_focus": ["string"],
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
            content = await self._run_cli_prompt(
                prompt_payload=cli_payload,
                request_kind="reflection",
            )
            return self._parse_json(
                content, model_class=ReflectionOutput
            )  # fix: was ActionDecision
        return await self._request_json(
            system_prompt=REFLECTION_SYSTEM_PROMPT,
            model_class=ReflectionOutput,
            user_payload=cli_payload,
            max_tokens=min(700, self.settings.max_tokens),
            temperature=0.4,
            request_kind="reflection",
        )

    async def generate_chat_comment(self, payload: dict[str, Any]) -> str:
        prompt_payload = {
            "task": "Write one short in-character chat message for the game group.",
            "required_output": {"message": "string"},
            "constraints": [
                "Return JSON only.",
                "Keep it short and witty.",
                "Stay in Russian.",
                f"Maximum length: {self.settings.chat_max_length} characters.",
            ],
            "context": payload,
        }
        if self.settings.transport == "cli":
            content = await self._run_cli_prompt(
                prompt_payload={
                    "system": CHAT_SYSTEM_PROMPT,
                    **prompt_payload,
                },
                request_kind="chat_comment",
            )
            return str(self._parse_json(content).get("message", "")).strip()

        data = await self._request_json(
            system_prompt=CHAT_SYSTEM_PROMPT,
            user_payload=prompt_payload,
            max_tokens=min(180, self.settings.max_tokens),
            temperature=self.settings.chat_temperature,
            request_kind="chat_comment",
        )
        return str(data.get("message", "")).strip()

    async def _request_json(
        self,
        system_prompt: str,
        user_payload: dict[str, Any],
        max_tokens: int,
        temperature: float,
        model_class=None,
        request_kind: str = "http_request",
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
        await self._log_usage(
            request_kind=request_kind,
            transport="http",
            prompt_text=json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            response_text=self._extract_content(data),
            status="ok",
            usage=data.get("usage"),
        )
        content = self._extract_content(data)
        return self._parse_json(
            content, model_class=model_class
        )  # fix: was hardcoded ActionDecision

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
            },
            request_kind="choose_action",
        )
        return self._parse_json(content, model_class=ActionDecision)

    async def _run_cli_prompt(
        self, prompt_payload: dict[str, Any], request_kind: str
    ) -> str:
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
            await self._log_usage(
                request_kind=request_kind,
                transport="cli",
                prompt_text=prompt,
                response_text="",
                status="timeout",
            )
            raise NpcLlmError("cli_timeout")

        stdout_text = stdout.decode("utf-8", errors="ignore").strip()
        stderr_text = stderr.decode("utf-8", errors="ignore").strip()
        if process.returncode != 0:
            await self._log_usage(
                request_kind=request_kind,
                transport="cli",
                prompt_text=prompt,
                response_text=stdout_text,
                status=f"exit_{process.returncode}",
                error_text=stderr_text or stdout_text,
            )
            raise NpcLlmError(
                f"cli_exit_{process.returncode}:{stderr_text[:500] or stdout_text[:500]}"
            )
        if not stdout_text:
            await self._log_usage(
                request_kind=request_kind,
                transport="cli",
                prompt_text=prompt,
                response_text="",
                status="empty_output",
                error_text=stderr_text,
            )
            raise NpcLlmError(f"cli_empty_output:{stderr_text[:500]}")
        await self._log_usage(
            request_kind=request_kind,
            transport="cli",
            prompt_text=prompt,
            response_text=stdout_text,
            status="ok",
            error_text=stderr_text,
        )
        return stdout_text

    def _estimate_tokens(self, text: str) -> int:
        clean_text = text.strip()
        if not clean_text:
            return 0
        return max(1, math.ceil(len(clean_text) / 4))

    async def _log_usage(
        self,
        *,
        request_kind: str,
        transport: str,
        prompt_text: str,
        response_text: str,
        status: str,
        usage: dict[str, Any] | None = None,
        error_text: str = "",
    ) -> None:
        prompt_tokens_est = self._estimate_tokens(prompt_text)
        response_tokens_est = self._estimate_tokens(response_text)
        payload = {
            "time": datetime.now().isoformat(),
            "transport": transport,
            "request_kind": request_kind,
            "model": self.settings.model,
            "status": status,
            "prompt_chars": len(prompt_text),
            "prompt_tokens_est": prompt_tokens_est,
            "response_chars": len(response_text),
            "response_tokens_est": response_tokens_est,
            "total_tokens_est": prompt_tokens_est + response_tokens_est,
        }
        if usage:
            payload["usage"] = usage
        if error_text:
            payload["error_preview"] = error_text[:240]
        await log_npc_usage(log_path=self.settings.log_path, payload=payload)

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

    def _parse_json(self, content: str, model_class=None) -> dict[str, Any]:
        clean_content = content.strip()
        if clean_content.startswith("```"):
            clean_content = clean_content.strip("`")
            if clean_content.startswith("json"):
                clean_content = clean_content[4:].strip()
        start = clean_content.find("{")
        end = clean_content.rfind("}")
        if start == -1 or end == -1:
            raise ValueError("LLM response does not contain JSON object")

        data = json.loads(clean_content[start : end + 1])
        if model_class:
            try:
                validated = model_class(**data)
                return validated.model_dump()
            except Exception as e:
                import logging

                logging.warning(f"Pydantic validation error: {e}")

        return data
