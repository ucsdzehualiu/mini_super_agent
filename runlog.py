"""runlog.py —— 每次运行落一份可追溯的日志。

用途只有一个：事后定位。用户说"上次它没回我"，得能翻出那一次到底发生了什么：
输入了什么、被谁挡了、Router 怎么判的、哪个任务失败、哪一步最慢。

产物（``logs/run-<时间>/``）：

* ``events.jsonl``  —— 每一个 TraceEvent，一行一条，带 seq 与时间戳
* ``session.log``   —— 给人读的流水：输入、判定、状态变化、异常栈
* ``summary.json``  —— 退出时的统计快照（崩溃也会尽量写）

日志失败不能影响主流程，所有写入都吞掉 OSError。
"""

from __future__ import annotations

import json
import os
import platform
import sys
import time
import traceback
from pathlib import Path
from typing import Any

from trace import TraceEvent


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


class RunLog:
    def __init__(self, base_dir: str | os.PathLike[str] = "logs", *,
                 run_id: str | None = None, keep: int = 20, echo: bool = False) -> None:
        self.run_id = run_id or time.strftime("%Y%m%d-%H%M%S")
        self.dir = Path(base_dir) / f"run-{self.run_id}"
        self.events_path = self.dir / "events.jsonl"
        self.log_path = self.dir / "session.log"
        self.summary_path = self.dir / "summary.json"
        self.echo = echo
        self._closed = False
        try:
            self.dir.mkdir(parents=True, exist_ok=True)
            self._events = self.events_path.open("a", encoding="utf-8")
            self._log = self.log_path.open("a", encoding="utf-8")
        except OSError:
            self._events = None
            self._log = None
        self._prune(keep)

    # ---- 基本写入 ---------------------------------------------------------
    def write(self, kind: str, **fields: Any) -> None:
        """一行结构化事件 + 一行人读流水。"""
        if self._closed:
            return
        record = {"t": round(time.time(), 3), "at": _now(), "kind": kind, **fields}
        line = json.dumps(record, ensure_ascii=False, default=str)
        try:
            if self._events is not None:
                self._events.write(line + "\n")
                self._events.flush()
        except OSError:
            pass
        self._human(kind, fields)

    def _human(self, kind: str, fields: dict[str, Any]) -> None:
        detail = " ".join(f"{k}={_short(v)}" for k, v in fields.items())
        text = f"{_now()}  {kind:<14} {detail}"
        try:
            if self._log is not None:
                self._log.write(text + "\n")
                self._log.flush()
        except OSError:
            pass
        if self.echo:
            print(text, file=sys.stderr)

    # ---- 与 TraceEvent 对接 -----------------------------------------------
    def event(self, evt: TraceEvent) -> None:
        """TraceStore 的旁路观察者：把每条 TraceEvent 也写进日志。"""
        if self._closed:
            return
        payload: dict[str, Any] = {"seq": evt.seq, "trace_kind": evt.kind}
        if evt.span is not None:
            detail = evt.span.detail or {}
            compact = {
                "span_id": evt.span.span_id, "parent_id": evt.span.parent_id,
                "kind": evt.span.kind, "name": evt.span.name,
                "status": evt.span.status, "duration": evt.span.duration,
            }
            # 只保留定位用得上的字段，别把整份 detail 抄进日志
            for key in ("chain", "route", "note", "tool", "error", "warnings",
                        "waiting_on", "tokens", "output"):
                if key in detail and detail[key] not in (None, "", [], {}):
                    compact[key] = detail[key]
            payload["span"] = compact
        if evt.task is not None:
            payload["task"] = {k: evt.task.get(k) for k in
                               ("task_id", "worker_type", "status", "topic",
                                "trace_chain", "result", "error")}
        if evt.alert is not None:
            payload["alert"] = evt.alert
        if evt.text:
            payload["text"] = evt.text
        self.write("trace", **payload)

    # ---- 异常 -------------------------------------------------------------
    def exception(self, where: str, exc: BaseException) -> None:
        self.write("exception", where=where, type=type(exc).__name__, message=str(exc),
                   traceback="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))

    # ---- 开闭 -------------------------------------------------------------
    def start(self, **config: Any) -> None:
        self.write("run_start", run_id=self.run_id, python=sys.version.split()[0],
                   platform=platform.platform(), cwd=os.getcwd(), **config)

    def close(self, summary: dict[str, Any] | None = None) -> None:
        if self._closed:
            return
        if summary is not None:
            self.write("run_end", **{k: v for k, v in summary.items() if k != "status"})
            try:
                self.summary_path.write_text(
                    json.dumps(summary, ensure_ascii=False, indent=1, default=str),
                    encoding="utf-8")
            except OSError:
                pass
        self._closed = True
        for fh in (getattr(self, "_events", None), getattr(self, "_log", None)):
            try:
                if fh is not None:
                    fh.close()
            except OSError:
                pass

    # ---- 清理旧日志 -------------------------------------------------------
    def _prune(self, keep: int) -> None:
        try:
            runs = sorted([p for p in self.dir.parent.glob("run-*") if p.is_dir()])
        except OSError:
            return
        for old in runs[:-keep] if len(runs) > keep else []:
            try:
                for f in old.iterdir():
                    f.unlink()
                old.rmdir()
            except OSError:
                pass


def _short(value: Any, limit: int = 120) -> str:
    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False, default=str)
    else:
        text = str(value)
    text = text.replace("\n", " ⏎ ")
    return text if len(text) <= limit else text[: limit - 1] + "…"


def latest_run(base_dir: str | os.PathLike[str] = "logs") -> Path | None:
    root = Path(base_dir)
    if not root.exists():
        return None
    runs = sorted([p for p in root.glob("run-*") if p.is_dir()])
    return runs[-1] if runs else None
