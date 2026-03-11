# NPC V2 Core (OpenClaw-style)

This file defines the "core runtime" contract for experimental NPCs (currently: тИИмоха).

## Loop
1. Build minimal actionable observation.
2. Build `available_tools` from allowed actions.
3. LLM chooses exactly one tool call.
4. Runtime maps tool->action and validates via guardrails.
5. Execute, log, remember, schedule next wake.

## Principles
- Tools-first: model selects a tool call, not free-form strategy prose.
- One move per step.
- Legality first: only allowed tools/actions.
- Economy-first compounding.
- Capacity bottlenecks override greed.
- Short reasons, deterministic execution.

## Tool Selection Heuristics
- Prefer affordable positive-EV actions.
- Avoid unaffordable buys when `affordable_quantity == 0`, unless near-term ETA and no stronger move.
- When seats are full, prioritize capacity unlock path.
- Use wait only when no meaningful legal edge exists.

## Output contract
```json
{"tool":"tool_name","input":{},"reason":"short text","sleep_seconds":300}
```

## Notes
- This is intentionally isolated from legacy planner/memory directives.
- Shared execution guardrails still apply after tool selection.
