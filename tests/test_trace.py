"""trace.py：Span / 树索引 / 时间轴口径 / 生命周期 / 增量视图。"""

from __future__ import annotations

import asyncio

import pytest
from rich.text import Text

import trace as T
from state import TaskTable
from trace import (
    KIND_EVENT, KIND_ROUTER, KIND_TOOL, KIND_WORKER,
    ST_ERROR, ST_OK, ST_RUNNING, ST_WAITING, ST_WARN,
    Span, TraceStore, build_timeline, lifecycle_lines, timeline_lines,
)


# =============================================================================
# Span / TraceStore
# =============================================================================
def test_span_lifecycle_idempotent_end():
    s = Span(span_id="", parent_id=None, kind=KIND_WORKER, name="w")
    assert s.is_open and s.status == ST_RUNNING
    s.end(ST_OK)
    ended = s.ended_at
    s.end(ST_ERROR)
    assert s.ended_at == ended


def test_span_warning_upgrades_to_warn():
    s = Span(span_id="x", parent_id=None, kind=KIND_TOOL, name="t")
    s.add_warning("empty")
    assert s.status == ST_WARN and s.warning_count == 1
    s.end()
    assert s.status == ST_WARN


def test_store_keeps_child_index_in_sync():
    st = TraceStore()
    root = st.start_span(KIND_EVENT, "EV-001")
    worker = st.start_span(KIND_WORKER, "researcher-1", parent_id=root.span_id)
    tool = st.start_span(KIND_TOOL, "web_search#1", parent_id=worker.span_id)
    assert [s.span_id for s in st.roots()] == [root.span_id]
    assert [s.name for s in st.children_of(root.span_id)] == ["researcher-1"]
    assert root.child_ids == [worker.span_id]
    assert worker.child_ids == [tool.span_id]
    assert [s.name for s in st.walk(None)] == ["EV-001", "researcher-1", "web_search#1"]


def test_store_publishes_single_span_events():
    events: list[T.TraceEvent] = []
    st = TraceStore(publish=events.append)
    root = st.start_span(KIND_EVENT, "E")
    st.end_span(root.span_id)
    assert [e.kind for e in events] == ["span_upsert", "span_upsert"]
    assert all(e.span is not None for e in events)


def test_tree_text_shows_hierarchy():
    st = TraceStore()
    root = st.start_span(KIND_EVENT, "EV-001")
    w = st.start_span(KIND_WORKER, "writer-1", parent_id=root.span_id)
    st.start_span(KIND_TOOL, "draft#1", parent_id=w.span_id)
    lines = st.tree_text().splitlines()
    assert lines[0].startswith(("✓", "●"))
    assert "├─" in lines[1]
    assert lines[2].startswith("    ")


# =============================================================================
# 时间轴：口径必须正确，否则会得出现在"并行很厉害"的假结论
# =============================================================================
def _worker(st, parent, name, start, end, tools=()):
    span = st.start_span(KIND_WORKER, name, parent_id=parent)
    span.started_at = start
    span.ended_at = end
    for tname, ts, te in tools:
        t = st.start_span(KIND_TOOL, tname, parent_id=span.span_id)
        t.started_at = ts
        t.ended_at = te
    return span


def test_timeline_measures_overlap_and_peak():
    st = TraceStore()
    now = __import__("time").monotonic()
    root = st.start_span(KIND_EVENT, "E")
    # 两条各 2 秒、重叠 1 秒
    _worker(st, root.span_id, "a-1", now, now + 2)
    _worker(st, root.span_id, "a-2", now + 1, now + 3)
    rep = build_timeline(st)
    assert rep.peak == 2
    assert rep.wall == pytest.approx(3, abs=0.05)
    assert rep.busy == pytest.approx(3, abs=0.05)
    assert rep.serial_total == pytest.approx(4, abs=0.05)
    assert rep.savings == pytest.approx(1, abs=0.05)


def test_timeline_excludes_waiting_lanes_from_busy():
    """挂起的时间不能算成"忙"，否则会得出虚假的并行收益。"""
    st = TraceStore()
    now = __import__("time").monotonic()
    root = st.start_span(KIND_EVENT, "E")
    _worker(st, root.span_id, "run-1", now, now + 1)
    blocked = st.start_span(KIND_WORKER, "wait-1", parent_id=root.span_id)
    blocked.started_at = now + 1
    blocked.end(ST_WAITING, waiting_on="人工批准")
    rep = build_timeline(st)
    assert rep.busy == pytest.approx(1, abs=0.05)
    assert any(l.blocked for l in rep.lanes)
    assert rep.savings <= 0.1


def test_timeline_critical_path_points_at_slowest():
    st = TraceStore()
    now = __import__("time").monotonic()
    root = st.start_span(KIND_EVENT, "E")
    _worker(st, root.span_id, "fast", now, now + 1, tools=[("draft#1", now, now + 1)])
    _worker(st, root.span_id, "slow", now, now + 5, tools=[("fetch_url#1", now + 3, now + 5)])
    rep = build_timeline(st)
    assert rep.critical_path[0] == "slow"
    assert "fetch_url#1" in rep.critical_path


def test_timeline_lines_render():
    st = TraceStore()
    now = __import__("time").monotonic()
    root = st.start_span(KIND_EVENT, "E")
    _worker(st, root.span_id, "a-1", now, now + 1)
    lines = timeline_lines(st)
    plain = "\n".join(l.plain for l in lines)
    assert "并发峰值" in plain and "关键路径" in plain
    assert "a-1" in plain


def test_lifecycle_lines_show_chain_and_human_touches():
    table = TaskTable()
    a = table.add_pending("writer", "写文案", event_id="EV-001", topic="文案")
    b = table.add_pending("writer", "改成正式腔调", event_id="EV-002",
                          origin_task_id=a.task_id, topic="文案")
    b.add_intervention("steer", "x")
    lines = lifecycle_lines(TraceStore(), table)
    plain = "\n".join(l.plain for l in lines)
    assert a.name in plain and b.name in plain
    assert f"{a.task_id}→{b.task_id}" in plain
    assert "人出手1次" in plain


# =============================================================================
# 渲染
# =============================================================================
def test_span_label_states():
    s = Span(span_id="w", parent_id=None, kind=KIND_WORKER, name="writer-1")
    s.add_warning("x")
    label = T.span_label(s)
    assert "writer-1" in label.plain and "WARN1" in label.plain
    assert T.DURATION_PLACEHOLDER in label.plain
    rendered = T.render_live_duration(label, s)
    assert T.DURATION_PLACEHOLDER not in rendered.plain


def test_detail_lines_single_line_and_clipped():
    s = Span(span_id="r", parent_id="p", kind=KIND_ROUTER, name="Router 判断")
    s.note(input="很长的输入 " * 60, chain="a→b")
    s.end(ST_OK)
    lines = T.detail_lines(s, width=80)
    assert lines
    for line in lines:
        assert isinstance(line, Text)
        assert "\n" not in line.plain
        assert len(line.plain) <= 80


def test_fmt_duration():
    assert T.fmt_duration(None) == "—"
    assert T.fmt_duration(0.42) == "0.42s"
    assert T.fmt_duration(61) == "61s"


def test_route_labels_cover_all_routes():
    for route in (T.ROUTE_DIRECT, T.ROUTE_RECALL, T.ROUTE_ROUTER, T.ROUTE_GATE):
        assert route in T.ROUTE_LABEL


# =============================================================================
# 增量视图（真实 Textual 控件）
# =============================================================================
def test_span_tree_view_incremental_and_nested():
    from textual.app import App, ComposeResult
    from textual.widgets import Tree

    class A(App):
        def __init__(self):
            super().__init__()
            self.store = TraceStore()
            self.view = None

        def compose(self) -> ComposeResult:
            yield Tree("root", id="t")

        def on_mount(self) -> None:
            self.view = T.SpanTreeView(self.query_one("#t", Tree), self.store)
            self.view.attach()

    app = A()
    st = app.store

    async def scenario():
        async with app.run_test(size=(100, 30)) as pilot:
            await pilot.pause(0.1)
            root = st.start_span(KIND_EVENT, "EV-001")
            app.view.sync()
            await pilot.pause(0.05)
            assert app.view.node_count == 1

            renders = app.view.render_count
            app.view.sync()
            app.view.sync()
            assert app.view.render_count == renders, "没变化就不该重建"

            w = st.start_span(KIND_WORKER, "writer-1", parent_id=root.span_id)
            st.start_span(KIND_TOOL, "draft#1", parent_id=w.span_id)
            app.view.sync()
            await pilot.pause(0.05)
            assert app.view.node_count == 3, "子节点（含嵌套）必须挂上"

            before = app.view.render_count
            st.end_span(w.span_id, ST_OK)
            app.view.sync()
            assert app.view.render_count == before + 1, "只有状态变了的那个节点需要重建"
            assert app.view.labels()[w.span_id].startswith("✓")

    asyncio.run(scenario())


def test_task_table_model_stable_keys_and_cells():
    from textual.app import App, ComposeResult
    from textual.widgets import DataTable

    class A(App):
        def __init__(self):
            super().__init__()
            self.table = TaskTable()
            self.model = None

        def compose(self) -> ComposeResult:
            yield DataTable(id="dt")

        def on_mount(self) -> None:
            self.model = T.TaskTableModel(self.table, self.query_one("#dt", DataTable))
            self.model.attach()

    app = A()
    table = app.table

    async def scenario():
        async with app.run_test(size=(100, 30)) as pilot:
            await pilot.pause(0.1)
            task = table.add_pending("writer", "x", topic="文案")
            app.model.sync()
            await pilot.pause(0.05)
            assert app.model.row_adds == 1
            assert app.model.row_keys() == [task.task_id]

            assert app.model.sync() == 0, "指纹不变时一个 cell 都不写"
            writes = app.model.cell_writes
            table.update(task.task_id, status="NEEDS_HUMAN", needs_human_reason="确认订票")
            written = app.model.sync()
            assert 1 <= written <= len(T.TaskTableModel.COLUMNS)
            assert app.model.row_adds == 1, "稳定 key：不新增行"
            assert app.model.cell_writes > writes

    asyncio.run(scenario())
