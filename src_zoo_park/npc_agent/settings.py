import os
from dataclasses import dataclass


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class NpcAgentSettings:
    enabled: bool
    transport: str
    api_key: str
    base_url: str
    model: str
    max_tokens: int
    npc_id: int
    npc_username: str
    npc_nickname: str
    npc_unity_prefix: str
    step_seconds: int
    step_jitter_seconds: int
    min_sleep_seconds: int
    max_sleep_seconds: int
    max_actions_per_cycle: int
    timeout_seconds: int
    temperature: float
    max_observation_animals: int
    top_candidates_limit: int
    log_path: str
    unity_invite_ttl_seconds: int
    event_wake_ttl_seconds: int
    memory_recent_events_limit: int
    memory_reflections_limit: int
    memory_goal_limit: int
    memory_relationship_limit: int
    memory_max_active_events: int
    memory_reflection_every_events: int
    memory_reflection_event_window: int
    memory_reflection_min_importance: int
    memory_use_llm_reflection: bool
    memory_trait_delta_limit: int
    memory_trait_step_limit: int
    memory_tactic_step_limit: int
    cli_bin: str
    cli_workdir: str


def load_npc_agent_settings() -> NpcAgentSettings:
    min_sleep_seconds = max(60, int(os.getenv("NPC_MIN_SLEEP_SECONDS", "300")))
    max_sleep_seconds = max(
        min_sleep_seconds,
        int(os.getenv("NPC_MAX_SLEEP_SECONDS", "21600")),
    )
    return NpcAgentSettings(
        enabled=_get_bool("NPC_AGENT_ENABLED", False),
        transport=os.getenv("NPC_LLM_TRANSPORT", "http").strip().lower(),
        api_key=os.getenv(
            "MOONSHOT_API_KEY",
            os.getenv("KIMI_API_KEY", os.getenv("NPC_LLM_API_KEY", "")),
        ).strip(),
        base_url=os.getenv(
            "NPC_LLM_BASE_URL",
            "https://api.moonshot.ai/v1",
        ).strip(),
        model=os.getenv("NPC_LLM_MODEL", "kimi-k2-0711-preview").strip(),
        max_tokens=max(256, int(os.getenv("NPC_LLM_MAX_TOKENS", "900"))),
        npc_id=int(os.getenv("NPC_USER_ID", "-1001")),
        npc_username=os.getenv("NPC_USERNAME", "npc_kimi"),
        npc_nickname=os.getenv("NPC_NICKNAME", "Kimi Keeper"),
        npc_unity_prefix=os.getenv("NPC_UNITY_PREFIX", "Kimi Clan").strip(),
        step_seconds=max(300, int(os.getenv("NPC_STEP_SECONDS", "300"))),
        step_jitter_seconds=max(0, int(os.getenv("NPC_STEP_JITTER_SECONDS", "180"))),
        min_sleep_seconds=min_sleep_seconds,
        max_sleep_seconds=max_sleep_seconds,
        max_actions_per_cycle=max(1, int(os.getenv("NPC_MAX_ACTIONS_PER_CYCLE", "3"))),
        timeout_seconds=max(5, int(os.getenv("NPC_LLM_TIMEOUT_SECONDS", "30"))),
        temperature=float(os.getenv("NPC_LLM_TEMPERATURE", "0.2")),
        max_observation_animals=max(
            3, int(os.getenv("NPC_MAX_OBSERVATION_ANIMALS", "12"))
        ),
        top_candidates_limit=max(3, int(os.getenv("NPC_TOP_CANDIDATES_LIMIT", "5"))),
        log_path=os.getenv(
            "NPC_AGENT_LOG_PATH",
            "logs/npc_agent_decisions.jsonl",
        ).strip(),
        unity_invite_ttl_seconds=max(
            300, int(os.getenv("NPC_UNITY_INVITE_TTL_SECONDS", "21600"))
        ),
        event_wake_ttl_seconds=max(
            60, int(os.getenv("NPC_EVENT_WAKE_TTL_SECONDS", "21600"))
        ),
        memory_recent_events_limit=max(
            5, int(os.getenv("NPC_MEMORY_RECENT_EVENTS_LIMIT", "12"))
        ),
        memory_reflections_limit=max(
            2, int(os.getenv("NPC_MEMORY_REFLECTIONS_LIMIT", "4"))
        ),
        memory_goal_limit=max(3, int(os.getenv("NPC_MEMORY_GOAL_LIMIT", "6"))),
        memory_relationship_limit=max(
            4, int(os.getenv("NPC_MEMORY_RELATIONSHIP_LIMIT", "8"))
        ),
        memory_max_active_events=max(
            40, int(os.getenv("NPC_MEMORY_MAX_ACTIVE_EVENTS", "160"))
        ),
        memory_reflection_every_events=max(
            2, int(os.getenv("NPC_MEMORY_REFLECTION_EVERY_EVENTS", "6"))
        ),
        memory_reflection_event_window=max(
            4, int(os.getenv("NPC_MEMORY_REFLECTION_EVENT_WINDOW", "8"))
        ),
        memory_reflection_min_importance=max(
            100, int(os.getenv("NPC_MEMORY_REFLECTION_MIN_IMPORTANCE", "700"))
        ),
        memory_use_llm_reflection=_get_bool("NPC_MEMORY_USE_LLM_REFLECTION", True),
        memory_trait_delta_limit=max(
            8, int(os.getenv("NPC_MEMORY_TRAIT_DELTA_LIMIT", "28"))
        ),
        memory_trait_step_limit=max(
            1, int(os.getenv("NPC_MEMORY_TRAIT_STEP_LIMIT", "6"))
        ),
        memory_tactic_step_limit=max(
            2, int(os.getenv("NPC_MEMORY_TACTIC_STEP_LIMIT", "14"))
        ),
        cli_bin=os.getenv("NPC_KIMI_CLI_BIN", "/root/kimi-cli-venv/bin/kimi").strip(),
        cli_workdir=os.getenv("NPC_KIMI_CLI_WORKDIR", "/root/zooparkbot").strip(),
    )


settings = load_npc_agent_settings()
