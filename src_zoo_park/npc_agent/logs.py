import json
from datetime import datetime
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


def _compute_resource_efficiency(payload: dict[str, Any]) -> float:
    """
    Вычисляет эффективность использования ресурсов для действия.
    Возвращает score от 0 до 100.
    """
    action = payload.get("action", {}) or {}
    result = payload.get("result", {}) or {}
    before = payload.get("snapshot_before", {}) or {}
    after = payload.get("snapshot_after", {}) or {}
    
    action_name = str(action.get("action", "")).strip()
    status = str(result.get("status", "")).strip().lower()
    
    # Базовый score по статусу
    if status == "ok":
        base_score = 50.0
    elif status == "skipped":
        base_score = 20.0
    else:
        base_score = 0.0
    
    # Вычисляем изменения ресурсов
    delta_usd = int(after.get("usd", 0) or 0) - int(before.get("usd", 0) or 0)
    delta_income = int(after.get("income_per_minute_rub", 0) or 0) - int(before.get("income_per_minute_rub", 0) or 0)
    
    # ROI для действий с инвестициями
    if action_name in {"buy_rarity_animal", "buy_aviary", "buy_merchant_discount_offer"}:
        if delta_usd < 0 and delta_income > 0:
            # Положительный ROI: потратили USD, получили доход
            roi = (delta_income / abs(delta_usd)) * 100
            base_score += min(40.0, roi * 2)
        elif delta_usd < 0:
            # Отрицательный ROI: потратили без роста дохода
            base_score -= min(30.0, abs(delta_usd) * 0.05)
    
    # Бонус за рост дохода
    if delta_income > 0:
        base_score += min(30.0, delta_income * 0.3)
    
    # Штраф за неэффективные траты
    if action_name == "wait" and int(before.get("usd", 0) or 0) > 500:
        base_score -= 10.0  # Хоarding penalty
    
    return max(0.0, min(100.0, base_score))


def _compute_goal_progress_delta(payload: dict[str, Any]) -> dict[str, float]:
    """
    Вычисляет прогресс по целям после действия.
    """
    before = payload.get("snapshot_before", {}) or {}
    after = payload.get("snapshot_after", {}) or {}
    
    return {
        "income_delta": int(after.get("income_per_minute_rub", 0) or 0) - int(before.get("income_per_minute_rub", 0) or 0),
        "usd_delta": int(after.get("usd", 0) or 0) - int(before.get("usd", 0) or 0),
        "animals_delta": int(after.get("total_animals", 0) or 0) - int(before.get("total_animals", 0) or 0),
        "seats_delta": int(after.get("total_seats", 0) or 0) - int(before.get("total_seats", 0) or 0),
    }


async def log_decision_metrics(log_path: str, payload: dict[str, Any]) -> None:
    """
    Логирует метрики для последующего анализа качества решений NPC.
    Включает efficiency, goal progress, и action distribution.
    """
    action = payload.get("action", {}) or {}
    result = payload.get("result", {}) or {}
    
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "npc_id": payload.get("npc", {}).get("id_user"),
        "npc_nickname": payload.get("npc", {}).get("nickname"),
        "step": payload.get("step"),
        "action_name": action.get("action"),
        "action_reason": action.get("reason"),
        "result_status": result.get("status"),
        "result_summary": result.get("summary"),
        "resource_efficiency": round(_compute_resource_efficiency(payload), 2),
        "goal_progress": _compute_goal_progress_delta(payload),
        "exploration_mode": "exploration:" in str(action.get("reason", "")),
        "llm_error": "llm_error:" in str(action.get("reason", "")),
        "fallback_mode": "fallback" in str(action.get("reason", "")),
    }
    
    # Добавляем контекст
    decision_trace = payload.get("decision_trace", {}) or {}
    if decision_trace:
        phase_1 = decision_trace.get("phase_1_observation", {})
        metrics["planner_phase"] = phase_1.get("planner", {}).get("phase")
        metrics["action_contract_violations"] = len(
            phase_1.get("action_contract", {}).get("must_not_do", [])
        )
    
    await _append_jsonl(
        log_path=log_path.replace(".jsonl", "_metrics.jsonl"),
        payload=metrics,
    )


async def log_action_distribution(log_path: str, action_stats: dict[str, Any]) -> None:
    """
    Логирует распределение действий за сессию.
    """
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "total_actions": action_stats.get("total", 0),
        "action_counts": action_stats.get("counts", {}),
        "success_rate": action_stats.get("success_rate", 0.0),
        "exploration_rate": action_stats.get("exploration_rate", 0.0),
        "avg_efficiency": action_stats.get("avg_efficiency", 0.0),
    }
    
    await _append_jsonl(
        log_path=log_path.replace(".jsonl", "_distribution.jsonl"),
        payload=metrics,
    )
