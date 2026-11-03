"""headless.py —— 不用终端也能看完整链路。

    python headless.py                 # 跑主剧本「忙的上午」，逐条打印系统怎么接的
    python headless.py --scenario stall
    python headless.py --list
    python headless.py --ui --svg out.svg --text out.txt   # 真启动控制台并导出画面

每个场景都带机器断言，所以它同时是冒烟测试（任何一环挂掉都非零退出）。
"""

from __future__ import annotations

import argparse
import asyncio
import io
import sys
from pathlib import Path

from rich.console import Console

import scenarios as SC
from runtime import FakeChatModel, Runtime
from state import NEEDS_HUMAN
from trace import lifecycle_lines, timeline_lines


def _print_rich(lines) -> None:
    c = Console(width=min(120, 120), file=io.StringIO(), force_terminal=False, color_system=None)
    for line in lines:
        c.print(line)
    print(c.file.getvalue().rstrip())


async def run_scenario(sc: SC.Scenario, *, verbose: bool = True, speed: float = 1.0) -> dict:
    rt = Runtime(model=FakeChatModel(latency=(0.06 / speed, 0.12 / speed)),
                 watchdog_interval=0.3, suspect_after=60)
    rt.watchdog.stall_after = 2
    await rt.start()

    if verbose:
        print("=" * 88)
        print(f"[{sc.id}] {sc.title}")
        print("=" * 88)

    for step in sc.steps:
        result = await rt.process(step.say)
        if not verbose:
            continue
        print(f"\n> {step.say}")
        print(f"  ({step.note})")
        print(f"  系统这样接：{result.route or '—'}  {result.gate.route_note}")
        for t in result.dispatched:
            detail = t.needs_human_reason or t.waiting_on or ""
            print(f"    · {t.name:<20} {t.status:<12} {detail}")
        if result.final_text:
            for line in result.final_text.splitlines()[:4]:
                print(f"    {line}")
        # 等一会儿，让 Watchdog / 挂起有机会发生
        await asyncio.sleep(step.hold / speed)

    # 人工介入：有挂起就批准（模拟用户在控制台按 A）
    if sc.expect.get("needs_human_seen"):
        pending = rt.needs_human()
        if pending and verbose:
            print(f"\n  控制台提示：{pending[0].name} 在等你确认 —— "
                  f"「{pending[0].needs_human_reason}」")
            print("  模拟按 A 批准…")
        for t in pending:
            await rt.approve(t.task_id)
        if pending and verbose:
            print(f"    批准后 → {rt.table[pending[0].task_id].status}")

    stats = rt.stats()
    routes = [h.route for h in rt.history]
    result = {
        "scenario": sc.id,
        "routes": routes,
        "tasks": len(rt.table),
        "statuses": [t.status for t in rt.table.all()],
        "human_touches": stats["attention"]["human_touches"],
        "router_skipped": stats["router_skipped"],
        "router_calls": stats["router_calls"],
        "watchdog_injections": len(rt.watchdog.injections),
        "peak": stats.get("peak") or 0,
        "timeline": None,
        "runtime": rt,
    }
    from trace import build_timeline
    rep = build_timeline(rt.trace)
    result["timeline"] = rep.to_dict()

    if verbose:
        print("\n--- 时间轴（只统计执行时间，等人/等结果单独标出）---")
        _print_rich(timeline_lines(rt.trace))
        print("\n--- 任务来龙去脉 ---")
        _print_rich(lifecycle_lines(rt.trace, rt.table))
        print(f"\n  Router 判断 {stats['router_calls']} 次，被直通/免掉 {stats['router_skipped']} 次；"
              f"你出手 {stats['attention']['human_touches']} 次")

    await rt.stop()
    return result


def check(sc: SC.Scenario, res: dict) -> tuple[bool, list[str]]:
    problems: list[str] = []
    exp = sc.expect
    if "routes" in exp:
        missing = [r for r in exp["routes"] if r not in res["routes"]]
        if missing:
            problems.append(f"缺少这些处理方式 {missing}；实际 {res['routes']}")
    if "min_tasks" in exp and res["tasks"] < exp["min_tasks"]:
        problems.append(f"任务数 {res['tasks']} < {exp['min_tasks']}")
    if "peak_min" in exp and res["peak"] < exp["peak_min"]:
        problems.append(f"并发峰值 {res['peak']} < {exp['peak_min']}")
    if exp.get("needs_human_seen") and NEEDS_HUMAN not in res["statuses"] and not res["human_touches"]:
        problems.append("没有出现需要人工确认的环节")
    if "router_skipped_min" in exp and res["router_skipped"] < exp["router_skipped_min"]:
        problems.append(f"直通省下的 Router 判断 {res['router_skipped']} < {exp['router_skipped_min']}")
    if "watchdog_inject_min" in exp and res["watchdog_injections"] < exp["watchdog_inject_min"]:
        problems.append(f"Watchdog 注入 {res['watchdog_injections']} 次")
    return (not problems, problems)


async def run_all(ids: list[str] | None = None, *, speed: float = 1.0) -> int:
    wanted = set(ids) if ids else {"morning", "stall"}
    fails = 0
    for sc in SC.SCENARIOS:
        if sc.id not in wanted:
            continue
        res = await run_scenario(sc, verbose=True, speed=speed)
        ok, problems = check(sc, res)
        print(f"\n  [{'PASS' if ok else 'FAIL'}] {sc.id}" + (f"  {problems}" if problems else ""))
        print()
        if not ok:
            fails += 1
    print("=" * 88)
    print("全部通过" if fails == 0 else f"{fails} 个场景失败")
    return 0 if fails == 0 else 1


async def run_ui(ids: list[str] | None, *, size: tuple[int, int], svg: str | None,
                 text: str | None) -> int:
    import main as app_module

    app = app_module.ConsoleApp(watchdog_interval=0.5)
    async with app.run_test(size=size) as pilot:
        await pilot.pause(0.3)
        await app._run_morning()
        await pilot.pause(0.5)
        # 有挂起就现场批准一次，把整条人机协作演完
        pending = app.runtime.needs_human()
        if pending:
            await app.runtime.approve(pending[0].task_id)
            await pilot.pause(0.5)
        app.action_toggle_view()      # 时间轴
        await pilot.pause(0.4)
        screen = app.export_text()
        print(screen)
        print("\n" + "=" * 88)
        print("统计：", app.runtime.stats())
        if svg:
            print("SVG 截图：", app.export_svg(svg))
        if text:
            p = Path(text)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(screen, encoding="utf-8")
            print("文本画面：", p)
        ok = not app.runtime.table.active() or all(
            t.is_blocked for t in app.runtime.table.active())
        print(f"\n[{'PASS' if ok else 'FAIL'}] 没有任务卡死在执行中")
        return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Super Agent headless 演示 / 冒烟")
    p.add_argument("--ui", action="store_true")
    p.add_argument("--scenario", action="append", default=None)
    p.add_argument("--list", action="store_true")
    p.add_argument("--svg")
    p.add_argument("--text")
    p.add_argument("--size", default="150x50")
    p.add_argument("--speed", type=float, default=1.0)
    args = p.parse_args(argv)

    if args.list:
        for sc in SC.SCENARIOS:
            print(f"{sc.id:<10} {sc.title}（{len(sc.steps)} 步）")
        return 0
    if args.ui:
        w, h = (int(x) for x in args.size.lower().split("x"))
        return asyncio.run(run_ui(args.scenario, size=(w, h), svg=args.svg, text=args.text))
    return asyncio.run(run_all(args.scenario, speed=args.speed))


if __name__ == "__main__":
    sys.exit(main())
