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
    api_key: str
    base_url: str
    model: str
    max_tokens: int
    npc_id: int
    npc_username: str
    npc_nickname: str
    npc_unity_prefix: str
    step_seconds: int
    timeout_seconds: int
    temperature: float
    max_observation_animals: int
    top_candidates_limit: int
    log_path: str
    unity_invite_ttl_seconds: int


def load_npc_agent_settings() -> NpcAgentSettings:
    return NpcAgentSettings(
        enabled=_get_bool("NPC_AGENT_ENABLED", False),
        api_key=os.getenv("KIMI_API_KEY", os.getenv("NPC_LLM_API_KEY", "")).strip(),
        base_url=os.getenv(
            "NPC_LLM_BASE_URL",
            "https://api.moonshot.cn/v1/chat/completions",
        ).strip(),
        model=os.getenv("NPC_LLM_MODEL", "moonshot-v1-8k").strip(),
        max_tokens=max(256, int(os.getenv("NPC_LLM_MAX_TOKENS", "900"))),
        npc_id=int(os.getenv("NPC_USER_ID", "-1001")),
        npc_username=os.getenv("NPC_USERNAME", "npc_kimi"),
        npc_nickname=os.getenv("NPC_NICKNAME", "Kimi Keeper"),
        npc_unity_prefix=os.getenv("NPC_UNITY_PREFIX", "Kimi Clan").strip(),
        step_seconds=max(900, int(os.getenv("NPC_STEP_SECONDS", "900"))),
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
    )


settings = load_npc_agent_settings()
