import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize NPC LLM usage logs")
    parser.add_argument(
        "log_path",
        nargs="?",
        default="logs/npc_agent_decisions_usage.jsonl",
        help="Path to usage jsonl log",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="How many recent dates to show",
    )
    return parser.parse_args()


def load_rows(path: Path) -> List[Dict]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def main() -> None:
    args = parse_args()
    path = Path(args.log_path)
    if not path.exists():
        raise SystemExit(f"Log not found: {path}")

    rows = load_rows(path)
    if not rows:
        raise SystemExit("No usage rows found")

    total_prompt = 0
    total_response = 0
    total_tokens = 0
    status_counter = Counter()
    kind_counter = Counter()
    day_totals: dict[str, dict[str, int]] = defaultdict(
        lambda: {"prompt": 0, "response": 0, "total": 0, "calls": 0}
    )
    day_kind_totals: dict[str, dict[str, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: {"total": 0, "calls": 0})
    )

    for row in rows:
        prompt = int(row.get("prompt_tokens_est", 0) or 0)
        response = int(row.get("response_tokens_est", 0) or 0)
        total = int(row.get("total_tokens_est", prompt + response) or 0)
        status = str(row.get("status", "unknown"))
        kind = str(row.get("request_kind", "unknown"))
        day = str(row.get("time", "unknown"))[:10]

        total_prompt += prompt
        total_response += response
        total_tokens += total
        status_counter[status] += 1
        kind_counter[kind] += 1

        day_totals[day]["prompt"] += prompt
        day_totals[day]["response"] += response
        day_totals[day]["total"] += total
        day_totals[day]["calls"] += 1

        day_kind_totals[day][kind]["total"] += total
        day_kind_totals[day][kind]["calls"] += 1

    print(f"rows: {len(rows)}")
    print(f"prompt_tokens_est: {total_prompt}")
    print(f"response_tokens_est: {total_response}")
    print(f"total_tokens_est: {total_tokens}")
    print(f"avg_tokens_per_call: {round(total_tokens / max(1, len(rows)), 1)}")
    print()

    print("by_request_kind:")
    for kind, count in kind_counter.most_common():
        print(f"  {kind}: {count}")
    print()

    print("by_status:")
    for status, count in status_counter.most_common():
        print(f"  {status}: {count}")
    print()

    print("by_day:")
    for day in sorted(day_totals.keys(), reverse=True)[: max(1, args.days)]:
        totals = day_totals[day]
        print(
            f"  {day}: calls={totals['calls']} total={totals['total']} prompt={totals['prompt']} response={totals['response']}"
        )
        for kind, values in sorted(
            day_kind_totals[day].items(),
            key=lambda item: item[1]["total"],
            reverse=True,
        ):
            print(f"    {kind}: calls={values['calls']} total={values['total']}")


if __name__ == "__main__":
    main()
