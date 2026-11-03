"""runlog + web 的测试：日志可定位、Web API 能收发。"""

from __future__ import annotations

import json
import threading
import time
import urllib.request

import pytest

import runlog
import runtime as R


# =============================================================================
# RunLog
# =============================================================================
def test_runlog_creates_three_files(tmp_path):
    log = runlog.RunLog(tmp_path, run_id="t1")
    log.start(ui="test")
    log.write("input", raw="你好")
    log.close({"tasks": 1})
    assert (log.dir / "events.jsonl").exists()
    assert (log.dir / "session.log").exists()
    assert json.loads((log.dir / "summary.json").read_text(encoding="utf-8"))["tasks"] == 1
    body = (log.dir / "session.log").read_text(encoding="utf-8")
    assert "run_start" in body and "input" in body


def test_runlog_records_exception_with_traceback(tmp_path):
    log = runlog.RunLog(tmp_path, run_id="t2")
    try:
        raise ValueError("boom")
    except ValueError as exc:
        log.exception("worker:demo", exc)
    log.close()
    events = [json.loads(l) for l in (log.dir / "events.jsonl").read_text(
        encoding="utf-8").splitlines() if l.strip()]
    exc = [e for e in events if e["kind"] == "exception"]
    assert exc and "ValueError" in exc[0]["traceback"]


def test_runlog_records_trace_events(tmp_path):
    from trace import TraceStore
    log = runlog.RunLog(tmp_path, run_id="t3")
    store = TraceStore()
    store.observe(log.event)
    span = store.start_span("worker", "w-1")
    store.end_span(span.span_id)
    log.close()
    events = [json.loads(l) for l in (log.dir / "events.jsonl").read_text(
        encoding="utf-8").splitlines() if l.strip()]
    traced = [e for e in events if e["kind"] == "trace"]
    assert traced and traced[-1]["span"]["status"] == "ok"


def test_runlog_prunes_old_runs(tmp_path):
    for i in range(4):
        runlog.RunLog(tmp_path, run_id=f"r{i}", keep=2).close()
    dirs = sorted(p.name for p in tmp_path.glob("run-*"))
    assert len(dirs) == 2 and dirs[-1] == "run-r3"


def test_latest_run(tmp_path):
    runlog.RunLog(tmp_path, run_id="a").close()
    runlog.RunLog(tmp_path, run_id="b").close()
    assert runlog.latest_run(tmp_path).name == "run-b"


# =============================================================================
# Runtime 与日志联动
# =============================================================================
@pytest.mark.asyncio
async def test_runtime_logs_decisions_and_inputs(tmp_path):
    log = runlog.RunLog(tmp_path, run_id="t4")
    rt = R.Runtime(model=R.FakeChatModel(latency=(0.01, 0.02)),
                   watchdog_interval=3600, log=log)
    await rt.start()
    await rt.process("帮我查一下成都周边徒步路线")
    await rt.process("嗯")
    await rt.stop()
    log.close(rt.stats())

    events = [json.loads(l) for l in (log.dir / "events.jsonl").read_text(
        encoding="utf-8").splitlines() if l.strip()]
    kinds = {e["kind"] for e in events}
    assert {"input", "gate", "decision", "run_end"} <= kinds
    decisions = [e for e in events if e["kind"] == "decision"]
    assert decisions and decisions[0]["mode"] == "spawn"


@pytest.mark.asyncio
async def test_every_input_gets_a_reply(tmp_path):
    """用户说话必须有回应——这是当初最大的坑：有些路径一句话都不回。"""
    rt = R.Runtime(model=R.FakeChatModel(latency=(0.01, 0.02)), watchdog_interval=3600)
    await rt.start()
    for q in ["你好", "你能做什么", "嗯", "帮我查一下成都周边徒步路线"]:
        result = await rt.process(q)
        assert result.final_text, f"{q!r} 没有回话"
    await rt.stop()


@pytest.mark.asyncio
async def test_ignore_path_answers_instead_of_silence(tmp_path):
    rt = R.Runtime(model=R.FakeChatModel(latency=(0.01, 0.02)), watchdog_interval=3600)
    await rt.start()
    result = await rt.process("你能做什么")
    assert result.route == "IGNORE"
    assert "调度" in (result.final_text or "") or "任务" in (result.final_text or "")
    await rt.stop()


# =============================================================================
# Web API
# =============================================================================
@pytest.fixture()
def web_service():
    import web
    from http.server import ThreadingHTTPServer

    service = web.Service(model_kind="fake", watchdog=3600, log=None, speed=4.0)
    service.start()
    server = ThreadingHTTPServer(("127.0.0.1", 0), web.make_handler(service))
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{port}", service
    server.shutdown()


def _get(url: str) -> str:
    with urllib.request.urlopen(url, timeout=5) as r:
        return r.read().decode("utf-8")


def _post(url: str, payload: dict) -> dict:
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read().decode())


def test_web_serves_page(web_service):
    base, _ = web_service
    html = _get(base + "/")
    assert "<title>Super Agent Console</title>" in html
    assert "对话" in html


def test_web_message_gets_reply(web_service):
    base, _ = web_service
    _post(base + "/api/message", {"text": "你能做什么"})
    deadline = time.time() + 20
    replies = []
    while time.time() < deadline:
        time.sleep(0.3)
        state = json.loads(_get(base + "/api/state?since=0"))
        replies = [m for m in state["replies"] if m["who"] == "Router"]
        if replies:
            break
    assert replies and "调度" in replies[-1]["text"]


def test_web_state_has_tasks_and_tree(web_service):
    base, _ = web_service
    _post(base + "/api/message", {"text": "帮我查一下成都周边徒步路线，排个两天计划"})
    deadline = time.time() + 25
    state = {}
    while time.time() < deadline:
        time.sleep(0.4)
        state = json.loads(_get(base + "/api/state?since=0"))
        if state.get("tasks") and any(t["status"] == "SUCCEEDED" for t in state["tasks"]):
            break
    assert state["tasks"], "应该有任务"
    assert state["tree"].get("roots"), "应该有一棵 trace 树"
