"""main.py（Textual 控制台）：真实 App 的端到端测试。

覆盖用户实际会走的路径：输入 → 系统怎么接 → 三视图 → 批准 → 剧本。
"""

from __future__ import annotations

import asyncio

import pytest

import main as app_module


async def _wait(pilot, cond, *, timeout: float = 25.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        await pilot.pause(0.05)
        if cond():
            return
    raise AssertionError("等待条件超时")


@pytest.fixture()
def make_app():
    def _make(**kw):
        kw.setdefault("speed", 1.0)
        kw.setdefault("watchdog_interval", 0.2)
        return app_module.ConsoleApp(**kw)

    return _make


@pytest.mark.asyncio
async def test_layout_and_watchdog(make_app):
    app = make_app()
    async with app.run_test(size=(150, 48)) as pilot:
        await pilot.pause(0.2)
        for sel in ("#event-stream", "#trace-tree", "#trace-view", "#span-detail",
                    "#alerts", "#snapshot", "#prompt", "#summary"):
            assert app.query_one(sel) is not None
        assert app.runtime.watchdog.running


@pytest.mark.asyncio
async def test_input_submits_without_blocking(make_app):
    app = make_app()
    async with app.run_test(size=(150, 48)) as pilot:
        await pilot.pause(0.2)
        prompt = app.query_one("#prompt", app_module.Input)
        prompt.value = "帮我查一下成都周边徒步路线，排个两天计划"
        await pilot.press("enter")
        await pilot.pause(0.05)
        assert prompt.value == "", "回车后输入框应立刻清空（处理器没有 await 长任务）"
        await _wait(pilot, lambda: app.runtime.table.all() and not app.runtime.table.active())
        assert app.runtime.history[-1].route == "ROUTER"


@pytest.mark.asyncio
async def test_event_stream_marks_route(make_app):
    app = make_app()
    async with app.run_test(size=(150, 48)) as pilot:
        await pilot.pause(0.2)
        app.submit("帮我查一下成都周边徒步路线，排个两天计划")
        await _wait(pilot, lambda: app.runtime.table.all() and not app.runtime.table.active())
        app.submit("路线那条再补一句：避开收费景区")
        await _wait(pilot, lambda: len(app.runtime.history) >= 2
                    and not app.runtime.table.active())
        stream = "\n".join(app._richlog_lines("#event-stream"))
        assert "[拆单]" in stream
        assert "[直通]" in stream
        assert "过滤" in stream


@pytest.mark.asyncio
async def test_direct_delivery_does_not_add_router_decisions(make_app):
    app = make_app()
    async with app.run_test(size=(150, 48)) as pilot:
        await pilot.pause(0.2)
        app.submit("帮我查一下成都周边徒步路线，排个两天计划")
        await _wait(pilot, lambda: app.runtime.table.all() and not app.runtime.table.active())
        before = app.runtime.router.decision_count
        app.submit("路线那条再补一句：避开收费景区")
        await _wait(pilot, lambda: len(app.runtime.history) >= 2
                    and not app.runtime.table.active())
        assert app.runtime.router.decision_count == before
        assert app.runtime.stats()["router_skipped"] >= 1
        summary = str(app.query_one("#summary", app_module.Static).render())
        assert "省下" in summary


@pytest.mark.asyncio
async def test_three_views_switch(make_app):
    app = make_app()
    async with app.run_test(size=(150, 48)) as pilot:
        await pilot.pause(0.2)
        app.submit("帮我查一下成都周边徒步路线，排个两天计划，再写一版介绍文案")
        await _wait(pilot, lambda: app.runtime.table.all() and not app.runtime.table.active())
        tree = app.query_one("#trace-tree", app_module.TraceTree)
        alt = app.query_one("#trace-view", app_module.RichLog)
        assert tree.display and not alt.display

        app.action_toggle_view()
        await pilot.pause(0.2)
        assert app._view == "timeline" and alt.display
        timeline = "\n".join(app._richlog_lines("#trace-view"))
        assert "并发峰值" in timeline and "关键路径" in timeline

        app.action_toggle_view()
        await pilot.pause(0.2)
        lifecycle = "\n".join(app._richlog_lines("#trace-view"))
        for task in app.runtime.table.all():
            assert task.name in lifecycle

        app.action_toggle_view()
        await pilot.pause(0.2)
        assert app._view == "tree"


@pytest.mark.asyncio
async def test_span_detail_on_tree_selection(make_app):
    app = make_app()
    async with app.run_test(size=(150, 48)) as pilot:
        await pilot.pause(0.2)
        app.submit("帮我查一下成都周边徒步路线")
        await _wait(pilot, lambda: app.runtime.table.all() and not app.runtime.table.active())
        tools = [s for s in app.runtime.trace.all_spans() if s.kind == "tool"]
        assert tools
        app._tree_view.select(tools[0].span_id)
        await pilot.pause(0.2)
        detail = "\n".join(app._richlog_lines("#span-detail"))
        assert "latency" in detail
        assert "webfetch" in detail or "draft" in detail or "make_plan" in detail


@pytest.mark.asyncio
async def test_human_approval_flow_in_console(make_app):
    app = make_app()
    async with app.run_test(size=(150, 48)) as pilot:
        await pilot.pause(0.2)
        app.submit("帮我订一张下周三去成都的机票，并排一下行程")
        await _wait(pilot, lambda: app.runtime.needs_human(), timeout=25)
        task = app.runtime.needs_human()[0]
        # 任务表里能看到"等人批"
        from textual.widgets import DataTable
        assert app.query_one("#snapshot", DataTable).row_count >= 1
        # 挂起原因现在写在对话区（告警区只放 Watchdog 等系统提示）。
        # 要等 process() 返回后才有这句，所以等它出现而不是立刻断言。
        await _wait(pilot, lambda: any("确认" in line
                                       for line in app._richlog_lines("#dialog")), timeout=25)
        assert task.needs_human_reason

        app._selected_task_id = task.task_id
        app.action_approve_selected()
        await _wait(pilot, lambda: app.runtime.table[task.task_id].status == "SUCCEEDED")
        assert app.runtime.table[task.task_id].human_touches == 1


@pytest.mark.asyncio
async def test_reject_selected_task(make_app):
    app = make_app()
    async with app.run_test(size=(150, 48)) as pilot:
        await pilot.pause(0.2)
        app.submit("帮我订一张下周三去成都的机票")
        await _wait(pilot, lambda: app.runtime.needs_human(), timeout=25)
        task = app.runtime.needs_human()[0]
        app._selected_task_id = task.task_id
        app.action_reject_selected()
        await pilot.pause(0.1)
        assert app.runtime.table[task.task_id].status == "CANCELLED"


@pytest.mark.asyncio
async def test_morning_script_covers_all_routes(make_app):
    app = make_app()
    async with app.run_test(size=(160, 50)) as pilot:
        await pilot.pause(0.2)
        await app._run_morning()
        await pilot.pause(0.5)
        routes = [h.route for h in app.runtime.history]
        assert "ROUTER" in routes
        assert "DIRECT" in routes
        assert "RECALL" in routes
        assert "GATE" in routes
        human = [t for t in app.runtime.table.all() if t.interventions]
        assert human, "剧本里应该有需要人出手的环节"


@pytest.mark.asyncio
async def test_watchdog_alert_reaches_console(make_app):
    """`<<LOOP>>` 任务会反复调同一个工具；Watchdog 应该在它跑完之前抓到并注入提示。

    这里把扫描间隔和阈值都压小，并把模型调慢一点：否则任务可能先跑完，
    测试就变成看运气（全量跑的时候机器负载不同，曾经偶发失败）。
    """
    app = make_app()
    app.runtime.watchdog.stall_after = 1
    app.runtime.watchdog.interval = 0.05
    app.runtime.model.latency = (0.05, 0.08)
    async with app.run_test(size=(150, 48)) as pilot:
        await pilot.pause(0.2)
        app.submit("<<LOOP>> 帮我查一下竞品定价")
        await _wait(pilot, lambda: bool(app.runtime.watchdog.injections), timeout=30)
        # 注入是 Runtime 侧的；告警还要经过队列和一次 flush 才出现在面板上
        await _wait(pilot, lambda: any("注入" in l or "无进展" in l
                                       for l in app._richlog_lines("#alerts")), timeout=10)
        alerts = "\n".join(app._richlog_lines("#alerts"))
        assert "注入提示" in alerts or "无进展" in alerts


@pytest.mark.asyncio
async def test_export_and_screenshot(make_app, tmp_path):
    app = make_app()
    async with app.run_test(size=(150, 48)) as pilot:
        await pilot.pause(0.2)
        app.submit("帮我查一下成都周边徒步路线")
        await _wait(pilot, lambda: app.runtime.table.all() and not app.runtime.table.active())
        text = app.export_text()
        assert "EVENT STREAM" in text and "TRACE" in text
        dump = await app.export_debug_dump(tmp_path / "d.txt")
        content = dump.read_text(encoding="utf-8")
        for section in ("EVENT STREAM", "TRACE TREE", "TIMELINE", "TASKS"):
            assert section in content
        svg = app.export_svg(tmp_path / "s.svg")
        assert "<svg" in svg.read_text(encoding="utf-8")[:2000]


@pytest.mark.asyncio
async def test_cancel_all(make_app):
    app = make_app()
    async with app.run_test(size=(150, 48)) as pilot:
        await pilot.pause(0.2)
        app.submit("帮我查一下成都周边徒步路线，排个两天计划，再写一版介绍文案")
        await pilot.pause(0.1)
        app.action_cancel_all()
        await _wait(pilot, lambda: not app.runtime.table.active())
        assert not app.runtime.table.active()
