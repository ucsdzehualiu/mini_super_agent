"""web.py —— 浏览器版控制台。

和 TUI 共用同一个 ``Runtime``，只是换了个观察者。这本身也验证了一件事：
运行时不依赖界面，``trace.py`` 不引 agentscope，``runtime.py`` 不引 textual。

实现上刻意不引第三方库：

* HTTP 用标准库 ``ThreadingHTTPServer``；
* Runtime 跑在独立线程的 asyncio 循环里，提交消息走 ``run_coroutine_threadsafe``；
* 前端是一个内嵌的 HTML 单页，轮询 ``/api/state?since=N`` 增量拉事件。

启动：``python web.py --port 8080``，浏览器打开 http://127.0.0.1:8080
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import config as config_module
from runlog import RunLog
from reply import reply_text
from runtime import FakeChatModel, Runtime, build_chat_model
from trace import TraceEvent


class SharedState:
    """HTTP 线程与 asyncio 线程之间唯一的共享区。"""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.events: list[dict[str, Any]] = []
        self.seq = 0
        self.summary: dict[str, Any] = {}
        self.tasks: list[dict[str, Any]] = []
        self.tree: dict[str, Any] = {}
        self.replies: list[dict[str, Any]] = []
        self.run_id = ""
        self.model_label = ""

    # ---- 写（来自 asyncio 线程） -----------------------------------------
    def push_event(self, evt: TraceEvent) -> None:
        if evt.kind == "span_upsert" and evt.span is not None:
            detail = evt.span.detail or {}
            item = {"kind": "span", "seq": evt.seq, "span_id": evt.span.span_id,
                    "parent_id": evt.span.parent_id, "name": evt.span.name,
                    "span_kind": evt.span.kind, "status": evt.span.status,
                    "duration": evt.span.duration,
                    "warnings": detail.get("warnings") or [],
                    "error": detail.get("error")}
        elif evt.kind == "task_upsert" and evt.task is not None:
            item = {"kind": "task", "seq": evt.seq, "task_id": evt.task.get("task_id"),
                    "status": evt.task.get("status")}
        elif evt.kind == "alert" and evt.alert is not None:
            item = {"kind": "alert", "seq": evt.seq, **evt.alert}
        elif evt.kind == "event_gate":
            item = {"kind": "gate", "seq": evt.seq, "data": json.loads(evt.text)}
        else:
            item = {"kind": "log", "seq": evt.seq, "text": evt.text}
        with self.lock:
            self.events.append(item)
            self.events = self.events[-600:]
            self.seq = max(self.seq, evt.seq)

    def set_snapshot(self, summary: dict[str, Any], tasks: list[dict[str, Any]],
                     tree: dict[str, Any]) -> None:
        with self.lock:
            self.summary = summary
            self.tasks = tasks
            self.tree = tree

    def push_reply(self, who: str, text: str, route: str = "", note: str = "") -> None:
        with self.lock:
            self.replies.append({"who": who, "text": text, "route": route,
                                 "note": note, "at": time.strftime("%H:%M:%S")})
            self.replies = self.replies[-200:]
    # ---- 读（来自 HTTP 线程） --------------------------------------------
    def since(self, seq: int) -> dict[str, Any]:
        with self.lock:
            fresh = [e for e in self.events if e["seq"] > seq]
            return {"events": fresh, "seq": self.seq, "summary": self.summary,
                    "tasks": self.tasks, "tree": self.tree,
                    "replies": self.replies, "run_id": self.run_id,
                    "model": self.model_label}


def _tree_dict(rt: Runtime) -> dict[str, Any]:
    """给前端的 trace 快照：带相对时间、层级、输入输出，能画时间轴也能画树。"""

    def node(span: Any, depth: int) -> dict[str, Any]:
        detail = span.detail or {}
        ended = span.ended_at
        return {
            "span_id": span.span_id,
            "name": span.name,
            "kind": span.kind,
            "status": span.status,
            "start": span.started_at,
            "end": ended,
            "depth": depth,
            "warnings": span.warning_count,
            "input": detail.get("input") or detail.get("prompt") or "",
            "output": detail.get("output") or detail.get("result") or "",
            "error": detail.get("error") or "",
            "tokens": detail.get("tokens") or detail.get("usage") or "",
            "children": [node(c, depth + 1) for c in rt.trace.children_of(span.span_id)],
        }

    return {"roots": [node(r, 0) for r in rt.trace.roots()]}


def _text_lines(lines: list[Any]) -> list[str]:
    return [line.plain for line in lines]


class Service:
    """持有 Runtime 的后台线程。"""

    def __init__(self, *, model_kind: str = "auto", watchdog: float = 3.0,
                 suspect: float = 60.0, log: RunLog | None = None,
                 speed: float = 1.0) -> None:
        self.state = SharedState()
        self.log = log
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, daemon=True,
                                       name="super-agent-runtime")
        self.config = config_module.load()
        self.speed = max(0.2, speed)
        self.model_kind = (self.config.model.provider if model_kind == "auto"
                           else model_kind)
        model = (FakeChatModel(latency=(0.12 / speed, 0.3 / speed))
                 if self.model_kind == "fake"
                 else build_chat_model(self.model_kind, config=self.config.model))
        self.runtime = Runtime(model=model, watchdog_interval=watchdog,
                               suspect_after=suspect, log=log)
        self.runtime.trace.observe(self.state.push_event)
        if log is not None:
            self.state.run_id = log.run_id
        self.state.model_label = model_kind_label(self.model_kind, self.config)

    def start(self) -> None:
        self.thread.start()
        for _ in range(100):  # 等循环起来
            if getattr(self, "_ready", False):
                break
            time.sleep(0.02)

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._main())

    async def _main(self) -> None:
        await self.runtime.start()
        self._ready = True
        asyncio.create_task(self._snapshot_loop())
        await asyncio.Event().wait()

    async def _snapshot_loop(self) -> None:
        while True:
            await asyncio.sleep(0.4)
            try:
                rows = []
                for t in self.runtime.table.all():
                    d = t.to_dict()
                    # 给前端算好的字段，避免在 JS 里对单调时钟做减法
                    d["duration"] = round(t.duration, 2) if t.started_at else None
                    d["todo"] = (t.needs_human_reason if t.status == "NEEDS_HUMAN"
                                 else t.waiting_on if t.status == "WAITING" else "")
                    rows.append(d)
                self.state.set_snapshot(self.runtime.stats(), rows, _tree_dict(self.runtime))
            except Exception:  # pragma: no cover
                pass

    def reload_model(self) -> tuple[bool, str]:
        """按当前 config.json 重建模型，换进正在跑的 Runtime。

        这样在设置页改完就用新的，不用重启控制台。
        """
        try:
            self.config = config_module.load()
            self.model_kind = self.config.model.provider
            if self.config.model.is_fake:
                model = FakeChatModel(latency=(0.12 / self.speed, 0.3 / self.speed))
            else:
                model = build_chat_model(self.config.model.provider, config=self.config.model)
        except Exception as exc:  # noqa: BLE001
            return False, f"{type(exc).__name__}: {exc}"
        self.runtime.model = model
        self.runtime.pool.model = model
        self.runtime.router.model = model
        self.state.model_label = model_kind_label(self.model_kind, self.config)
        return True, self.state.model_label

    # ---- 供 HTTP 调用 -----------------------------------------------------
    def submit(self, text: str) -> None:
        self.state.push_reply("你", text)
        async def run() -> None:
            try:
                result = await self.runtime.process(text, source="web")
                self.state.push_reply("Router", reply_text(result),
                                      route=result.route, note=result.gate.route_note)
            except Exception as exc:  # noqa: BLE001
                if self.log is not None:
                    self.log.exception("web.submit", exc)
                self.state.push_reply("Router", f"出错了：{exc}")

        asyncio.run_coroutine_threadsafe(run(), self.loop)

    def approve(self, task_id: str) -> None:
        async def run() -> None:
            ok = await self.runtime.approve(task_id)
            self.state.push_reply("Router", f"{task_id} 已批准" if ok else f"{task_id} 不在待批状态")
        asyncio.run_coroutine_threadsafe(run(), self.loop)

    def reject(self, task_id: str) -> None:
        ok = self.runtime.reject(task_id)
        self.state.push_reply("Router", f"{task_id} 已驳回" if ok else f"{task_id} 不能驳回")

    def cancel_all(self) -> None:
        n = self.runtime.pool.cancel_all()
        self.state.push_reply("Router", f"已请求取消 {n} 个任务")

    def demo(self) -> None:
        """按剧本顺序播一遍，步与步之间留间隔，方便观察每一步的效应。"""

        async def run() -> None:
            import scenarios as SC

            for step in SC.MORNING:
                if not step.say:
                    continue
                self.state.push_reply("你", step.say)
                result = await self.runtime.process(step.say, source="web")
                self.state.push_reply("Router", reply_text(result),
                                      route=result.route,
                                      note=result.gate.route_note)
                await asyncio.sleep(1.2)

        asyncio.run_coroutine_threadsafe(run(), self.loop)


UI_DIR = Path(__file__).resolve().parent / "ui"


def model_kind_label(kind: str, cfg: Any = None) -> str:
    """给用户看的一句话：现在到底用的是什么模型。"""
    if kind == "fake":
        return "内置假数据（不联网，答案来自本地桩）"
    if cfg is not None:
        return config_module.describe(cfg)
    return kind


def _load_ui(name: str) -> str:
    """前端页面放在 ui/ 里，改了不用动 Python。"""
    return (UI_DIR / name).read_text(encoding="utf-8")


HTML = _load_ui("console.html")
SETTINGS_HTML = _load_ui("settings.html")



def _decode_json(raw: bytes) -> dict[str, Any] | None:
    """解析请求体。

    浏览器一定发 UTF-8，但脚本客户端（例如 PowerShell 5.1）会按本地代码页发，
    中文会变成乱码。这里依次试 utf-8 / gbk，能救回来就别报错。
    """
    if not raw:
        return {}
    for enc in ("utf-8", "gbk", "utf-16"):
        try:
            payload = json.loads(raw.decode(enc))
            if isinstance(payload, dict):
                return payload
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
    return None


def make_handler(service: Service):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, payload: Any, code: int = 200) -> None:
            self._send(code, json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"),
                       "application/json; charset=utf-8")

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path in ("/", "/index.html"):
                self._send(200, HTML.encode("utf-8"), "text/html; charset=utf-8")
                return
            if parsed.path == "/api/state":
                since = int(parse_qs(parsed.query).get("since", ["0"])[0] or 0)
                self._json(service.state.since(since))
                return
            if parsed.path == "/api/health":
                self._json({"ok": True})
                return
            if parsed.path == "/api/config":
                self._json({"config": config_module.load().public_dict(),
                            "presets": config_module.PRESETS,
                            "current": service.state.model_label})
                return
            if parsed.path == "/settings":
                self._send(200, SETTINGS_HTML.encode("utf-8"), "text/html; charset=utf-8")
                return
            self._json({"error": "not found"}, 404)

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            data = _decode_json(raw)
            if data is None:
                self._json({"error": "bad json"}, 400)
                return
            path = urlparse(self.path).path
            if path == "/api/message":
                text = str(data.get("text", "")).strip()
                if text:
                    service.submit(text)
                self._json({"ok": True})
                return
            if path in ("/api/approve", "/api/reject"):
                tid = str(data.get("task_id", ""))
                if path.endswith("approve"):
                    service.approve(tid)
                else:
                    service.reject(tid)
                self._json({"ok": True})
                return
            if path == "/api/cancel_all":
                service.cancel_all()
                self._json({"ok": True})
                return
            if path == "/api/demo":
                service.demo()
                self._json({"ok": True})
                return
            if path == "/api/config":
                saved = config_module.update(**{
                    k: data[k] for k in
                    ("provider", "base_url", "api_key", "model_name",
                     "temperature", "max_tokens")
                    if k in data and data[k] not in ("", None)
                })
                ok, info = service.reload_model()
                self._json({"ok": True, "config": saved.public_dict(),
                            "applied": ok, "current": info,
                            "note": "已保存并生效" if ok else f"已保存，但加载失败：{info}"})
                return
            self._json({"error": "not found"}, 404)

        def log_message(self, fmt: str, *args: Any) -> None:  # 静默，避免刷屏
            return

    return Handler


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Super Agent 浏览器控制台")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--model", default="auto",
                   help="auto=用 config.json 的配置；fake=内置假数据；也可写供应商名")
    p.add_argument("--watchdog", type=float, default=3.0)
    p.add_argument("--suspect", type=float, default=60.0)
    p.add_argument("--fast", action="store_true")
    p.add_argument("--log-dir", default="logs")
    p.add_argument("--no-log", action="store_true")
    p.add_argument("--no-browser", action="store_true", help="不自动打开浏览器")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    log = None if args.no_log else RunLog(args.log_dir, echo=False)
    if log is not None:
        log.start(ui="web", port=args.port, model=args.model)
    speed = 3.0 if args.fast else 1.0
    service = Service(model_kind=args.model, watchdog=args.watchdog,
                      suspect=args.suspect, log=log, speed=speed)
    service.start()

    server = ThreadingHTTPServer((args.host, args.port), make_handler(service))
    url = f"http://{args.host}:{args.port}"
    print(f"控制台已启动：{url}")
    if log is not None:
        print(f"日志目录：{log.dir}")
    print(f"模型：{model_kind_label(args.model)}")
    print("Ctrl+C 退出")
    # 服务已经绑定端口，这时候再开浏览器才不会"打不开"
    if not args.no_browser:
        threading.Timer(0.4, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n退出中…")
    finally:
        server.shutdown()
        if log is not None:
            log.close(service.runtime.stats())
    return 0


if __name__ == "__main__":
    sys.exit(main())
