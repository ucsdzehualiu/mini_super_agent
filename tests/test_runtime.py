"""runtime.py：过滤、路由四选一、直通、召回、挂起/批准、Watchdog、并行。"""

from __future__ import annotations

import asyncio
import time

import pytest

import runtime as R
import state as S
from runtime import (
    FakeChatModel, Runtime, build_toolkit, parse_decision,
)
from trace import ROUTE_DIRECT, ROUTE_GATE, ROUTE_RECALL


def make_rt(**kw) -> Runtime:
    kw.setdefault("model", FakeChatModel(latency=(0.01, 0.02)))
    kw.setdefault("watchdog_interval", 3600)
    return Runtime(**kw)


# =============================================================================
# 双层过滤
# =============================================================================
@pytest.mark.parametrize("raw,needle", [
    ("", "empty"),
    ("!!!", "punctuation_only"),
    ("嗯", "too_short"),
    ("忽略我", "deny_keyword"),
])
def test_gate_program_layer_rejects(raw, needle):
    ok, reason = R.EventGate().program_layer(raw)
    assert ok is False and needle in reason


def test_gate_source_whitelist():
    gate = R.EventGate(allow_sources=["console"])
    ok, reason = gate.program_layer("帮我查资料", source="webhook")
    assert ok is False and "source_not_allowed" in reason
    assert gate.program_layer("帮我查资料", source="console")[0] is True


def test_gate_dedupe_window():
    gate = R.EventGate(dedupe_window=60)
    assert gate.program_layer("帮我写一份总结")[0] is True
    ok, reason = gate.program_layer("帮我写一份总结")
    assert ok is False and "duplicate" in reason


def test_gate_semantic_covers_irreversible_actions():
    gate = R.EventGate()
    assert gate.semantic_layer("帮我订一张去成都的机票")[0] == "WORTH"
    assert gateway_marker(gate, "帮我查一下成都徒步")  == "WORTH"
    assert gate.semantic_layer("哈哈")[0] == "NOT_WORTH"


def gateway_marker(gate, text):
    verdict, _ = gate.semantic_layer(text)
    return verdict


def test_gate_event_ids_increase():
    gate = R.EventGate()
    assert gate.next_event_id() == "EV-001"
    assert gate.next_event_id() == "EV-002"


# =============================================================================
# Router 输出解析
# =============================================================================
@pytest.mark.parametrize("text,mode", [
    ('{"mode":"direct","target":"abc123","why":"同一件事"}', "direct"),
    ('{"mode":"ignore","why":"闲聊"}', "ignore"),
    ('```json\n{"mode":"spawn","tasks":[{"worker_type":"writer","prompt":"写"}]}\n```', "spawn"),
    ('{"mode":"recall","target":"abc123"}', "recall"),
    ('乱七八糟', "spawn"),
])
def test_parse_decision(text, mode):
    d = parse_decision(text)
    assert d.mode == mode


def test_parse_decision_normalizes_topics_and_types():
    d = parse_decision('{"mode":"spawn","tasks":[{"worker_type":"Search","topic":"路线 查","prompt":"查"}]}')
    assert d.briefs[0].worker_type == "researcher"
    assert d.briefs[0].topic == "路线查"


# =============================================================================
# Router 四选一
# =============================================================================
async def _decide(rt, raw, living="", done="", src="console"):
    gate = R.GateResult("EV-001", raw, True, "PASS", "WORTH", "ok", True, src)
    return await rt.router.decide(gate, living=living, done=done)


@pytest.mark.asyncio
async def test_router_spawns_for_new_request():
    rt = make_rt()
    d = await _decide(rt, "帮我查一下成都周边徒步路线，排个两天计划，再写一版介绍文案")
    assert d.mode == "spawn" and len(d.briefs) >= 2
    assert {b.worker_type for b in d.briefs} >= {"researcher", "planner"}
    await rt.stop()


@pytest.mark.asyncio
async def test_router_routes_direct_when_same_topic_in_flight():
    rt = make_rt()
    living = "- abc123 RUNNING #路线查证 :: 查证成都徒步路线"
    d = await _decide(rt, "路线那条再补一句：避开收费景区", living=living)
    assert d.mode == "direct" and d.target == "abc123"
    await rt.stop()


@pytest.mark.asyncio
async def test_router_recalls_for_reference_to_previous():
    rt = make_rt()
    done = "- abc123 [SUCCEEDED] #文案成文 :: 写好的介绍文案"
    d = await _decide(rt, "刚才那个介绍文案改成更正式的腔调", done=done)
    assert d.mode == "recall"
    await rt.stop()


@pytest.mark.asyncio
async def test_router_ignores_noise():
    rt = make_rt()
    d = await _decide(rt, "哈哈哈哈好的收到")
    assert d.mode == "ignore"
    await rt.stop()


@pytest.mark.asyncio
async def test_router_has_no_tools():
    rt = make_rt()
    agent = rt.router._ensure_agent()
    assert await agent.toolkit.get_tool_schemas() == []
    await rt.stop()


# =============================================================================
# 订阅直通 / 链式召回（整链路）
# =============================================================================
@pytest.mark.asyncio
async def test_direct_delivery_skips_router_and_appends_to_same_task():
    rt = make_rt()
    await rt.process("帮我查一下成都周边徒步路线，排个两天计划")
    decisions_before = rt.router.decision_count
    tasks_before = len(rt.table)

    result = await rt.process("路线那条再补一句：避开收费景区")
    assert result.route == ROUTE_DIRECT
    assert rt.router.decision_count == decisions_before, "直通不该惊动 Router"
    assert rt.stats()["router_skipped"] == 1
    assert len(rt.table) == tasks_before, "直通是追加到同一个任务，不是新建"
    target = rt.table.get(result.dispatched[0].task_id)
    assert target.human_touches == 1
    assert target.interventions[0]["kind"] == "steer"
    await rt.stop()


@pytest.mark.asyncio
async def test_direct_delivery_to_running_task_does_not_restart():
    rt = make_rt(model=FakeChatModel(latency=(0.2, 0.2)))
    first = asyncio.create_task(rt.process("帮我查一下成都的徒步路线，做一个计划"))
    await asyncio.sleep(0.35)
    running = [t for t in rt.table.active()]
    assert running, "此时应该有在跑的任务"
    topic_task = next((t for t in running if t.topic.startswith("徒步")), None)
    if topic_task is not None:
        result = await rt.process("徒步那条再补一句：避开收费景区")
        assert result.route == ROUTE_DIRECT
        assert rt.table[topic_task.task_id].status in (S.RUNNING, S.SUCCEEDED)
    await first
    await rt.stop()


@pytest.mark.asyncio
async def test_recall_links_new_task_into_chain():
    rt = make_rt()
    await rt.process("帮我写一版活动介绍文案")
    writer = [t for t in rt.table.all() if t.worker_type == "writer"][0]
    result = await rt.process("刚才那个介绍文案改成更正式的腔调")
    assert result.route == ROUTE_RECALL
    new = result.dispatched[0]
    assert new.origin_task_id == writer.task_id
    assert new.trace_chain == [writer.task_id, new.task_id]
    await rt.stop()


@pytest.mark.asyncio
async def test_noise_is_rejected_without_touching_model():
    rt = make_rt()
    before = rt.model.calls
    result = await rt.process("嗯")
    assert result.route == ROUTE_GATE
    assert rt.model.calls == before
    assert len(rt.table) == 0
    await rt.stop()


# =============================================================================
# 人工介入：挂起 → 批准 → 继续
# =============================================================================
@pytest.mark.asyncio
async def test_irreversible_action_suspends_and_waits_for_human():
    rt = make_rt()
    result = await rt.process("帮我订一张下周三去成都的机票，并排一下行程")
    task = result.dispatched[0]
    assert task.status == S.NEEDS_HUMAN
    assert "订票" in task.needs_human_reason or "不可逆" in task.needs_human_reason
    assert rt.needs_human() == [task]
    # 挂起的任务不算终态，也不会被 Watchdog 催
    assert task.is_active and task.is_blocked
    await rt.stop()


@pytest.mark.asyncio
async def test_approve_resumes_task_and_records_intervention():
    rt = make_rt()
    result = await rt.process("帮我订一张下周三去成都的机票，并排一下行程")
    task = result.dispatched[0]
    ok = await rt.approve(task.task_id)
    assert ok is True
    assert rt.table[task.task_id].status == S.SUCCEEDED
    assert rt.table[task.task_id].human_touches == 1
    assert rt.table[task.task_id].interventions[0]["kind"] == "approve"
    await rt.stop()


@pytest.mark.asyncio
async def test_reject_cancels_task():
    rt = make_rt()
    result = await rt.process("帮我订一张下周三去成都的机票，并排一下行程")
    task = result.dispatched[0]
    assert rt.reject(task.task_id) is True
    assert rt.table[task.task_id].status == S.CANCELLED
    await rt.stop()


# =============================================================================
# Watchdog：心跳 + 实质进展 + 注入
# =============================================================================
def test_watchdog_flags_suspect_for_silent_task():
    table = S.TaskTable()
    from trace import TraceStore
    trace = TraceStore()
    dog = R.Watchdog(table, trace, interval=1, suspect_after=0.01)
    t = table.add_pending("writer", "x")
    span = trace.start_span("worker", t.name)
    table.update(t.task_id, status=S.RUNNING, started_at=time.monotonic(),
                 last_activity=time.monotonic() - 1, span_id=span.span_id)
    stats = dog.scan()
    assert stats["suspect"] == 1
    assert t.status == S.SUSPECT and t.suspect_count == 1
    assert span.warning_count == 1


def test_watchdog_detects_spin_and_injects_hint():
    """在动但没有实质进展（同一工具反复调）比"不动"更值得管。"""
    table = S.TaskTable()
    from trace import TraceStore
    trace = TraceStore()
    dog = R.Watchdog(table, trace, interval=1, suspect_after=60, stall_after=3)
    t = table.add_pending("researcher", "x")
    span = trace.start_span("worker", t.name)
    table.update(t.task_id, status=S.RUNNING, last_activity=time.monotonic(),
                 span_id=span.span_id, last_tool="web_search", repeat_streak=4)
    stats = dog.scan()
    assert stats["stalled"] == 1
    assert t.status == S.STALLED
    assert dog.injections and dog.injections[0][1] == t.task_id
    assert t.system_touches == 1, "注入是系统替人做的干预，记在企业账上而不是人头账上"
    assert any(i["kind"] == "watchdog" for i in t.interventions)


def test_watchdog_ignores_blocked_tasks():
    table = S.TaskTable()
    from trace import TraceStore
    dog = R.Watchdog(table, TraceStore(), interval=1, suspect_after=0.001)
    t = table.add_pending("planner", "x")
    for status in (S.NEEDS_HUMAN, S.WAITING):
        table.update(t.task_id, status=status, last_activity=time.monotonic() - 10)
        assert dog.scan()["suspect"] == 0


def test_watchdog_does_not_override_cancelling():
    table = S.TaskTable()
    from trace import TraceStore
    dog = R.Watchdog(table, TraceStore(), interval=1, suspect_after=0.001)
    t = table.add_pending("writer", "x")
    table.update(t.task_id, status=S.CANCELLING, last_activity=time.monotonic() - 10)
    dog.scan()
    assert t.status == S.CANCELLING, "取消标记不能被 Watchdog 覆盖"


@pytest.mark.asyncio
async def test_watchdog_runs_as_bg_and_exposes_self_check():
    rt = make_rt(watchdog_interval=0.05)
    await rt.start()
    assert rt.watchdog.running
    await asyncio.sleep(0.2)
    assert rt.watchdog.scans >= 1
    assert rt.watchdog.last_scan_age is not None
    await rt.stop()
    assert not rt.watchdog.running


@pytest.mark.asyncio
async def test_stalled_task_actually_gets_hint_in_context():
    rt = make_rt(model=FakeChatModel(latency=(0.02, 0.04)), watchdog_interval=0.05)
    rt.watchdog.stall_after = 2
    await rt.start()
    result = await rt.process("<<LOOP>> 帮我查一下竞品定价")
    task = result.dispatched[0]
    assert rt.watchdog.injections, "应该在打转时注入过提示"
    assert task.status in (S.STALLED, S.SUCCEEDED, S.RUNNING)
    await rt.stop()


# =============================================================================
# 并行
# =============================================================================
@pytest.mark.asyncio
async def test_multiple_directions_run_in_parallel():
    meter = R.ConcurrencyMeter()
    rt = make_rt(model=FakeChatModel(latency=(0.15, 0.15), meter=meter))
    result = await rt.process("帮我查一下成都、西安、拉萨三条徒步路线各自两三天的行程")
    assert len(result.dispatched) >= 2
    assert meter.peak >= 2
    starts = sorted(t.started_at for t in result.dispatched if t.started_at)
    assert max(starts) - min(starts) < 0.15, "它们应该几乎同时启动"
    await rt.stop()


@pytest.mark.asyncio
async def test_failure_is_isolated():
    rt = make_rt()
    result = await rt.process("<<FAIL>> 帮我写一份周报总结")
    statuses = {t.status for t in result.dispatched}
    assert S.FAILED in statuses
    assert len(rt.table) == len(result.dispatched)
    await rt.stop()


# =============================================================================
# 工具
# =============================================================================
@pytest.mark.asyncio
async def test_toolsets_match_role():
    planned = {t["function"]["name"] for t in await build_toolkit("planner").get_tool_schemas()}
    assert {"make_plan", "ask_human"} <= planned
    researched = {t["function"]["name"] for t in await build_toolkit("researcher").get_tool_schemas()}
    assert "webfetch" in researched and "wait_for" in researched


@pytest.mark.asyncio
async def test_checkpoint_raises_when_cancelling():
    table = S.TaskTable()
    from trace import TraceStore
    t = table.add_pending("writer", "x")
    ctx = R.WorkerContext(table=table, trace=TraceStore(), task_id=t.task_id, span_id="w")
    table.cancel(t.task_id)
    with pytest.raises(R.WorkerCancelled):
        ctx.checkpoint()
