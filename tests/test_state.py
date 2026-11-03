"""state.py：任务状态外置、订阅、链式召回、人工介入、进度信号。"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

import state as S
from state import TaskBrief, TaskTable


def test_task_brief_defaults_and_ids():
    t = TaskBrief(worker_type="writer", prompt="x")
    assert len(t.task_id) == 6
    assert t.name == f"writer-{t.task_id}"
    assert t.root_task_id == t.task_id
    assert t.trace_chain == [t.task_id]
    assert t.status == S.PENDING and t.is_active and not t.is_terminal


def test_blocked_states_are_active_but_not_progressing():
    t = TaskBrief(worker_type="planner", prompt="x")
    t.status = S.NEEDS_HUMAN
    assert t.is_active and t.is_blocked and not t.is_terminal
    t.status = S.WAITING
    assert t.is_blocked


def test_chain_recall_fields():
    table = TaskTable()
    a = table.add_pending("writer", "写文案", topic="文案")
    b = table.add_pending("writer", "改成正式腔调", origin_task_id=a.task_id, topic="文案")
    assert b.origin_task_id == a.task_id
    assert b.root_task_id == a.task_id
    assert b.trace_chain == [a.task_id, b.task_id]
    assert [x.task_id for x in table.ancestors(b.task_id)] == [a.task_id]


def test_subscribe_and_lookup():
    table = TaskTable()
    a = table.add_pending("researcher", "查路线", topic="路线")
    assert table.subscriber("路线") is a
    assert table.subscriptions() == {"路线": a.task_id}
    table.drop_subscription("路线")
    assert table.subscriber("路线") is None


def test_latest_by_topic_and_latest_finished():
    table = TaskTable()
    a = table.add_pending("researcher", "查", topic="路线")
    b = table.add_pending("researcher", "再查", topic="路线")
    assert table.latest_by_topic("路线") is b
    table.update(a.task_id, status=S.SUCCEEDED, ended_at=time.monotonic())
    assert table.latest_finished(1)[0] is a


def test_human_interventions_are_recorded():
    table = TaskTable()
    t = table.add_pending("planner", "订票")
    t.add_intervention("steer", "避开收费景区")
    t.add_intervention("approve", "准了")
    assert t.human_touches == 2
    assert [i["kind"] for i in t.interventions] == ["steer", "approve"]
    assert table.human_attention()["human_touches"] == 2
    assert t.system_touches == 0


def test_system_touches_not_counted_as_human():
    t = TaskBrief(worker_type="writer", prompt="x")
    t.add_intervention("steer", "补一句", actor="human")
    t.add_intervention("watchdog", "别重复搜了", actor="watchdog")
    t.add_intervention("resume", "结果回来了", actor="system")
    assert t.human_touches == 1
    assert t.system_touches == 2


def test_progress_and_repeat_streak():
    table = TaskTable()
    t = table.add_pending("researcher", "查")
    table.mark_progress(t.task_id, tool="web_search")
    table.mark_progress(t.task_id, tool="web_search")
    table.mark_progress(t.task_id, tool="web_search")
    assert t.repeat_streak == 3
    table.mark_progress(t.task_id, tool="fetch_url")
    assert t.repeat_streak == 1   # 换了工具 = 有实质进展
    assert t.tool_calls == 4
    assert len(t.progress_marks) == 4


def test_bump_tokens():
    table = TaskTable()
    t = table.add_pending("writer", "写")
    table.bump_tokens(t.task_id, tokens_in=10, tokens_out=5)
    table.bump_tokens(t.task_id, tokens_in=1, tokens_out=1)
    assert (t.tokens_in, t.tokens_out) == (11, 6)


@pytest.mark.parametrize("status", [S.PENDING, S.RUNNING, S.WAITING, S.NEEDS_HUMAN, S.SUSPECT, S.STALLED])
def test_cancel_from_any_active_state(status):
    table = TaskTable()
    t = table.add_pending("writer", "x")
    table.update(t.task_id, status=status)
    assert table.cancel(t.task_id) is True
    assert t.status == S.CANCELLING


def test_cancel_terminal_returns_false():
    table = TaskTable()
    t = table.add_pending("writer", "x")
    table.update(t.task_id, status=S.SUCCEEDED)
    assert table.cancel(t.task_id) is False


def test_fingerprint_changes_on_state_and_interventions():
    table = TaskTable()
    t = table.add_pending("writer", "x")
    fp = table.fingerprint()
    assert table.fingerprint() == fp
    t.add_intervention("steer", "y")
    table.update(t.task_id, status=S.RUNNING)
    assert table.fingerprint() != fp


def test_requeue_orphans():
    table = TaskTable()
    mine = table.add_pending("writer", "mine", owner_node="A")
    orphan = table.add_pending("writer", "orphan", owner_node="B")
    table.update(mine.task_id, status=S.RUNNING, started_at=1.0)
    table.update(orphan.task_id, status=S.RUNNING, started_at=1.0, result="partial")
    assert [t.task_id for t in table.orphaned("A")] == [orphan.task_id]
    assert [t.task_id for t in table.requeue_orphans("A")] == [orphan.task_id]
    assert orphan.status == S.PENDING and orphan.attempt == 2 and orphan.result is None


def test_jsonl_roundtrip_both_formats(tmp_path: Path):
    path = tmp_path / "t.jsonl"
    table = TaskTable(jsonl_path=path)
    t = table.add_pending("writer", "写", topic="文案")
    table.update(t.task_id, status=S.SUCCEEDED)
    loaded = TaskTable.load_jsonl(path)
    assert loaded[t.task_id].status == S.SUCCEEDED
    assert loaded.subscriber("文案") is not None

    snap = tmp_path / "snap.jsonl"
    table.save_jsonl(snap)
    loaded2 = TaskTable.load_jsonl(snap)
    assert len(loaded2) == 1
    assert json.loads(snap.read_text(encoding="utf-8").splitlines()[0])["worker_type"] == "writer"


def test_update_rejects_unknown_field():
    table = TaskTable()
    t = table.add_pending("writer", "x")
    with pytest.raises(AttributeError):
        table.update(t.task_id, nope=1)


def test_status_label_covers_all_statuses():
    assert set(S.STATUS_LABEL) == set(S.ALL_STATUSES)
