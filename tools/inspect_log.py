"""inspect_log.py —— 读最近一次运行的日志，给出定位结论。

用法：

    python tools/inspect_log.py              # 看最近一次
    python tools/inspect_log.py --list       # 列出所有运行
    python tools/inspect_log.py --run 20261102-115334
    python tools/inspect_log.py --errors     # 只看异常与失败

回答的问题是：这次输入了什么、分别怎么被处理的、哪里失败、哪里慢、有没有异常。
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def load_events(run_dir: Path) -> list[dict]:
    path = run_dir / "events.jsonl"
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def summarize(run_dir: Path, *, errors_only: bool = False) -> int:
    events = load_events(run_dir)
    if not events:
        print(f"{run_dir} 里没有 events.jsonl")
        return 1

    inputs = [e for e in events if e.get("kind") == "input"]
    gates = [e for e in events if e.get("kind") == "gate"]
    decisions = [e for e in events if e.get("kind") == "decision"]
    exceptions = [e for e in events if e.get("kind") == "exception"]
    ends = [e for e in events if e.get("kind") == "run_end"]
    traces = [e for e in events if e.get("kind") == "trace"]

    print(f"运行  {run_dir.name}")
    if ends:
        stats = ends[-1]
        print(f"结束  任务 {stats.get('tasks')} 个  "
              f"Router 判断 {stats.get('router_calls')} 次  "
              f"省下 {stats.get('router_skipped')} 次")

    print(f"\n输入 {len(inputs)} 条，模型判定 {len(decisions)} 次，异常 {len(exceptions)} 条")

    if not errors_only:
        print("\n── 每条输入的去向 ──")
        dec_by_event = {d.get("event"): d for d in decisions}
        for i, item in enumerate(inputs, 1):
            raw = item.get("raw", "")
            # 按顺序配对 gate
            gate = gates[i - 1] if i - 1 < len(gates) else None
            dec = None
            if gate is not None:
                dec = dec_by_event.get(gate.get("event"))
            route = "拦截" if gate and not gate.get("program") else (dec or {}).get("mode", "—")
            note = (gate or {}).get("program_reason", "")
            extra = ""
            if dec:
                extra = f"  → {dec.get('mode')} {dec.get('why') or ''}"
            print(f"  {i}. {raw[:40]:<42} {route:<7} {note[:38]}{extra}")

    # 失败与挂起
    failed, blocked, alerts = {}, {}, []
    for e in traces:
        task = e.get("task") or {}
        tid = task.get("task_id")
        if not tid:
            continue
        st = task.get("status")
        if st == "FAILED":
            failed[tid] = task
        elif st in ("NEEDS_HUMAN", "WAITING"):
            blocked[tid] = task
        if e.get("trace_kind") == "alert":
            alerts.append(e.get("alert") or {})

    if failed:
        print("\n── 失败的任务 ──")
        for tid, task in failed.items():
            print(f"  {task.get('worker_type')}-{tid}  {task.get('error') or task.get('result')}")
    if blocked:
        print("\n── 卡在等人的任务 ──")
        for tid, task in blocked.items():
            print(f"  {task.get('worker_type')}-{tid}  {task.get('status')}")

    if errors_only:
        print("\n── 异常 ──")
        if not exceptions:
            print("  无")
        for exc in exceptions:
            print(f"  [{exc.get('where')}] {exc.get('type')}: {exc.get('message')}")
            tb = (exc.get("traceback") or "").strip().splitlines()
            for line in tb[-4:]:
                print(f"      {line}")

    # 慢步骤（只看执行单元，事件/Worker 的聚合 span 不计）
    slow = []
    for e in traces:
        span = e.get("span") or {}
        if span.get("kind") in ("worker", "tool", "router") and span.get("duration"):
            if span["duration"] > 1.0:
                slow.append((span["duration"], span.get("name"), span.get("status")))
    if slow:
        print("\n── 超过 1 秒的步骤 ──")
        for dur, name, status in sorted(slow, reverse=True)[:8]:
            print(f"  {dur:5.2f}s  {name}  [{status}]")

    # 告警
    if alerts:
        levels = Counter(a.get("level", "INFO") for a in alerts)
        print(f"\n── 告警 {len(alerts)} 条  " + " ".join(f"{k}={v}" for k, v in levels.items()))
        for a in alerts[-5:]:
            print(f"  {a.get('level')}  {a.get('source')}  {a.get('message')}")

    verdict = []
    if exceptions:
        verdict.append(f"{len(exceptions)} 条异常")
    if failed:
        verdict.append(f"{len(failed)} 个任务失败")
    print("\n结论：" + ("、".join(verdict) if verdict else "没有发现异常"))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="读运行日志定位问题")
    p.add_argument("--dir", default=str(ROOT / "logs"))
    p.add_argument("--run", default=None)
    p.add_argument("--list", action="store_true")
    p.add_argument("--errors", action="store_true")
    args = p.parse_args(argv)

    root = Path(args.dir)
    runs = sorted([d for d in root.glob("run-*") if d.is_dir()]) if root.exists() else []
    if args.list:
        if not runs:
            print("还没有运行记录")
            return 0
        for d in runs:
            size = sum(f.stat().st_size for f in d.iterdir() if f.is_file()) // 1024
            print(f"  {d.name}  {size} KB")
        return 0
    if not runs:
        print(f"{root} 下没有运行记录")
        return 1
    target = root / f"run-{args.run}" if args.run else runs[-1]
    if not target.exists():
        print(f"找不到 {target}")
        return 1
    return summarize(target, errors_only=args.errors)


if __name__ == "__main__":
    sys.exit(main())
