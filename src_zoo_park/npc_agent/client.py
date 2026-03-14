import asyncio
import json
import math
from datetime import datetime
from typing import Any

import aiohttp

from .logs import log_npc_usage
from .settings import NpcAgentSettings
from .v2.tools import build_tool_catalog, normalize_tool_call

from pydantic import BaseModel


class ActionDecision(BaseModel):
    action: str
    params: dict[str, Any]
    reason: str
    sleep_seconds: int
    trait_update: dict[str, Any] | None = None


class ReflectionOutput(BaseModel):
    summary: str
    lessons: list[str]
    opportunities: list[str]
    risks: list[str]
    trait_adjustments: list[dict[str, Any]]
    tactical_focus: list[str]
    goal_adjustments: list[dict[str, Any]]


class ToolDecision(BaseModel):
    tool: str
    input: dict[str, Any]
    reason: str
    sleep_seconds: int
    trait_update: dict[str, Any] | None = None


DECISION_JSON_CONTRACT = (
    '{"action":"string","params":{},"reason":"short","sleep_seconds":300,'
    '"trait_update":{"trait":"optional","delta":0,"reason":"optional"}}'
)


BASE_DECISION_PROMPT = f"""
You choose exactly one next legal action for an autonomous NPC in a Telegram zoo economy.

Directives:
- Allowed actions are the only executable actions.
- action_contract is mandatory policy (must_do / must_not_do / hard_constraints).
- execution_feedback is mandatory (avoid failed repeats, prefer suggested alternatives).
- Optimize long-term compounding value and unblock hard bottlenecks first.
- Keep sleep_seconds inside wake_context.constraints.
- Return JSON only using this contract: {DECISION_JSON_CONTRACT}
- Do not output any fields outside the contract.
""".strip()


SYSTEM_PROMPT = (
    BASE_DECISION_PROMPT
    + "\n\n"
    + "Specific policy:\n"
    + "- Use decision_brief and planner.phase_a_candidates as primary shortlist.\n"
    + "- Prefer planner.recommended_actions unless invalid now.\n"
    + "- Respect anti_loop_guard and momentum to avoid stale loops.\n"
    + "- Keep reason short and concrete."
)

V2_TOOL_SYSTEM_PROMPT = (
    BASE_DECISION_PROMPT
    + "\n\n"
    + "Tool mode output contract:\n"
    + "- Return JSON only: {\"tool\":\"name\",\"input\":{},\"reason\":\"short\",\"sleep_seconds\":300,\"trait_update\":{\"trait\":\"optional\",\"delta\":0,\"reason\":\"optional\"}}\n"
    + "- tool must be from available_tools only.\n"
    + "- input must match the selected tool schema.\n"
    + "- Keep reason short and concrete."
)

CHAT_SYSTEM_PROMPT = """
You are the public voice of an autonomous AI NPC in a Telegram zoo economy game.

Write one short message in Russian for the shared game chat.

Style:
- friendly, witty, self-aware
- light humor and playful tone, without aggression
- kind banter only; no insults, no bullying, no harassment
- strictly no profanity, obscenity, or sexual jokes
- comment on the real game state, action, result, rank, money, animals, or rivals
- sound like the AI is focused on growth and healthy competition
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
            },
            "item_opportunities": {"upgrade_candidates": 5, "merge_candidates": 3},
            "unity": {"candidates": 3, "recruit_targets": 3},
            "animal_market": 7,
            "aviary_market": 5,
            "allowed_actions": 14,
            "decision_brief": {"top_affordable_actions": 4},
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

        npc_id_user = int((observation.get("player") or {}).get("id_user", 0) or 0)
        v2_tool_npcs = {-1001, -1002}  # ИИван, тИИмоха

        # Shared reductions for all NPCs.
        clean_obs.pop("momentum", None)
        standings = clean_obs.get("standings")
        if isinstance(standings, dict):
            standings.pop("top_referrals", None)

        if npc_id_user in v2_tool_npcs:
            # V2 payload (experimental): clean-room OpenClaw-style context.
            # Keep only directly actionable state, remove planner/memory directives.
            v2_obs: dict[str, Any] = {}
            for key in (
                "schema_version",
                "current_time",
                "wake_context",
                "player",
                "zoo",
                "bank",
                "merchant",
                "items",
                "unity",
                "standings",
                "aviary_market",
                "allowed_actions",
                "v2_memory",
            ):
                if key in clean_obs:
                    v2_obs[key] = clean_obs[key]

            # Make wake context minimal.
            wake_context = v2_obs.get("wake_context")
            if isinstance(wake_context, dict):
                v2_obs["wake_context"] = {
                    "source": wake_context.get("source"),
                    "reason": wake_context.get("reason"),
                }

            # Deduplicate allowed actions and cap size.
            allowed_actions = v2_obs.get("allowed_actions")
            if isinstance(allowed_actions, list):
                deduped: list[dict[str, Any]] = []
                seen_actions: set[str] = set()
                for item in allowed_actions:
                    if not isinstance(item, dict):
                        continue
                    action_name = str(item.get("action", "")).strip()
                    if not action_name or action_name in seen_actions:
                        continue
                    seen_actions.add(action_name)
                    deduped.append(item)
                    if len(deduped) >= 6:
                        break
                v2_obs["allowed_actions"] = deduped

            # Add compact animal facts for in-model evaluation.
            animal_facts: list[dict[str, Any]] = []
            for animal_row in clean_obs.get("animal_market", []) or []:
                animal_name = str(animal_row.get("animal", "")).strip()
                for variant in animal_row.get("variants", []) or []:
                    animal_facts.append(
                        {
                            "animal": animal_name,
                            "rarity": variant.get("rarity"),
                            "code_name": variant.get("code_name"),
                            "price_usd": int(variant.get("price_usd", 0) or 0),
                            "income_rub": int(variant.get("income_rub", 0) or 0),
                            "payback_minutes": float(
                                variant.get("payback_minutes", 10**9) or 10**9
                            ),
                            "owned": int(variant.get("owned", 0) or 0),
                            "affordable_quantity": int(
                                variant.get("affordable_quantity", 0) or 0
                            ),
                            "eta_seconds": int(variant.get("eta_seconds", 0) or 0),
                        }
                    )
            animal_facts.sort(
                key=lambda row: (
                    row["affordable_quantity"] <= 0,
                    row["payback_minutes"],
                    -row["income_rub"],
                    row["price_usd"],
                )
            )
            v2_obs["animal_facts"] = animal_facts[:10]

            return v2_obs

        return clean_obs

    async def choose_action_v2_tools(
        self,
        observation: dict[str, Any],
        model_override: str | None = None,
        base_url_override: str | None = None,
        api_key_override: str | None = None,
    ) -> dict[str, Any]:
        available_tools = build_tool_catalog(observation.get("allowed_actions", []) or [])
        payload = {
            "task": "Select one tool call for this NPC turn.",
            "available_tools": available_tools,
            "observation": observation,
        }

        use_cli = (
            self.settings.transport == "cli"
            and not model_override
            and not base_url_override
            and not api_key_override
        )
        if use_cli:
            content = await self._run_cli_prompt(
                prompt_payload={
                    "system": V2_TOOL_SYSTEM_PROMPT,
                    **payload,
                },
                request_kind="choose_action_v2_tools",
            )
            tool_decision = self._parse_json(content, model_class=ToolDecision)
        else:
            tool_decision = await self._request_json(
                system_prompt=V2_TOOL_SYSTEM_PROMPT,
                user_payload=payload,
                max_tokens=min(260, self.settings.max_tokens),
                temperature=self.settings.action_temperature,
                model_class=ToolDecision,
                request_kind="choose_action_v2_tools",
                model_override=model_override,
                base_url_override=base_url_override,
                api_key_override=api_key_override,
            )
        tool_name = str(tool_decision.get("tool", "wait")).strip() or "wait"
        allowed_tool_names = {str(item.get("name", "")).strip() for item in available_tools}
        if tool_name not in allowed_tool_names:
            tool_name = "wait" if "wait" in allowed_tool_names else next(iter(allowed_tool_names), "wait")

        params = normalize_tool_call(tool_name, tool_decision.get("input", {}) or {})

        return {
            "action": tool_name,
            "params": params,
            "reason": str(tool_decision.get("reason", "v2_tool_selection"))[:220],
            "sleep_seconds": int(tool_decision.get("sleep_seconds", 300) or 300),
            "trait_update": tool_decision.get("trait_update"),
        }

    async def choose_action(self, observation: dict[str, Any]) -> dict[str, Any]:
        clean_obs = self._build_trimmed_observation(observation)
        npc_id_user = int((observation.get("player") or {}).get("id_user", 0) or 0)
        v2_tool_npcs = {-1001, -1002}  # ИИван, тИИмоха

        if npc_id_user in v2_tool_npcs:
            return await self.choose_action_v2_tools(observation=clean_obs)

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

    async def choose_action_with_provider(
        self,
        observation: dict[str, Any],
        model_override: str,
        base_url_override: str | None = None,
        api_key_override: str | None = None,
    ) -> dict[str, Any]:
        clean_obs = self._build_trimmed_observation(observation)
        npc_id_user = int((observation.get("player") or {}).get("id_user", 0) or 0)
        v2_tool_npcs = {-1001, -1002}

        if npc_id_user in v2_tool_npcs:
            return await self.choose_action_v2_tools(
                observation=clean_obs,
                model_override=model_override,
                base_url_override=base_url_override,
                api_key_override=api_key_override,
            )

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
            request_kind="choose_action_fallback_model",
            model_override=model_override,
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
        model_override: str | None = None,
        base_url_override: str | None = None,
        api_key_override: str | None = None,
    ) -> dict[str, Any]:
        request_url = self._build_request_url(base_url_override=base_url_override)
        payload = {
            "model": (model_override or self.settings.model),
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
            "Authorization": f"Bearer {api_key_override or self.settings.api_key}",
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

    def _build_request_url(self, base_url_override: str | None = None) -> str:
        base_url = (base_url_override or self.settings.base_url).rstrip("/")
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
            "default_model": "npc-kimi-decision",
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
                "npc-kimi-decision": {
                    "provider": "npc-kimi",
                    "model": self.settings.cli_model,
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
            "--thinking",
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
        model_used = (
            self.settings.cli_model if transport == "cli" else self.settings.model
        )
        payload = {
            "time": datetime.now().isoformat(),
            "transport": transport,
            "request_kind": request_kind,
            "model": model_used,
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
