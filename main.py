"""main.py —— Textual 控制台。

控制台不是"日志查看器"，它只回答三个问题：

* 我提的事，系统接住了没有？（EVENT STREAM 每行标出 拆单 / 直通 / 召回 / 拦截）
* 现在谁在干什么、卡在哪？（TRACE 区可切：树 / 时间轴 / 任务）
* 有什么需要我拍板的？（NEEDS_HUMAN 的任务，按 A 批准 / X 驳回）

所有刷新都是增量的：Trace 树按节点指纹、任务表按任务指纹、日志只追加。
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Iterable

from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Input, RichLog, Static, Tree

import scenarios as SC
from runtime import FakeChatModel, Runtime, build_chat_model
from trace import (
    ROUTE_DIRECT, ROUTE_GATE, ROUTE_IGNORE, ROUTE_LABEL,
    Span, SpanTreeView, TaskTableModel, TraceEvent, detail_lines, lifecycle_lines,
    render_live_duration, timeline_lines,
)

MAX_DRAIN = 2000


def clock_str(ts: float | None = None) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ts if ts is not None else time.time()))


class TraceTree(Tree):
    """Trace 树。``render_label`` 里把"运行中"的耗时现算，避免每帧 set_label。"""

    span_lookup: dict[str, Span] = {}

    def __init__(self, label: str = "TRACE", **kwargs: Any) -> None:
        super().__init__(label, **kwargs)
        self.show_root = False
        self.span_lookup = {}

    def render_label(self, node: Any, base_style: Any, style: Any) -> Any:
        label = super().render_label(node, base_style, style)
        span = self.span_lookup.get(str(node.id)) if node is not None else None
        if span is None:
            return label
        try:
            return render_live_duration(label, span)
        except Exception:  # pragma: no cover
            return label


class ConsoleApp(App):
    CSS = """
    Screen { layout: vertical; background: $surface; }
    #main { height: 1fr; layout: horizontal; }
    #left { width: 34%; min-width: 36; border: round $primary-darken-2; }
    #right { width: 1fr; border: round $primary-darken-2; }
    .title { background: $boost; text-style: bold; }
    #event-stream { height: 1fr; padding: 0 1; }
    #dialog { height: 12; border-top: solid $primary-darken-2; padding: 0 1; }
    #trace-tree { height: 1fr; padding: 0 1; }
    #trace-view { height: 1fr; padding: 0 1; }
    #span-detail { height: 10; border-top: solid $primary-darken-2; padding: 0 1; }
    #summary { height: 1; padding: 0 1; background: $boost; }
    #alerts { height: 5; border: round $warning-darken-2; padding: 0 1; }
    #snapshot { height: 6; border: round $primary-darken-2; }
    #prompt-row { height: 3; layout: horizontal; }
    #prompt { width: 1fr; }
    #send { width: 10; margin-left: 1; }
    #status { height: 1; padding: 0 1; background: $boost; }
    """

    BINDINGS = [
        Binding("ctrl+c", "quit", "退出", priority=True),
        Binding("f1", "help", "帮助"),
        Binding("f2", "cancel_all", "全部取消"),
        Binding("f3", "run_morning", "播放剧本"),
        Binding("f4", "approve_all", "全部批准"),
        Binding("f5", "toggle_view", "切换视图"),
        Binding("f6", "toggle_flaky", "故障注入"),
        Binding("f9", "screenshot", "截图"),
        Binding("a", "approve_selected", "批准"),
        Binding("x", "reject_selected", "驳回"),
        Binding("escape", "focus_input", "输入"),
    ]

    def __init__(
        self,
        *,
        runtime: Runtime | None = None,
        model_kind: str = "fake",
        watchdog_interval: float = 3.0,
        suspect_after: float = 60.0,
        persist: str | None = None,
        speed: float = 1.0,
        log_dir: str | None = "logs",
        model_config: Any = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        import config as C

        self.config = model_config or C.load()
        # auto = 跟着 config.json 走；显式传 fake 才是假数据
        self.model_kind = (self.config.model.provider if model_kind == "auto" else model_kind)
        self.watchdog_interval = watchdog_interval
        self.suspect_after = suspect_after
        self.speed = max(0.1, speed)

        from runlog import RunLog  # 延迟导入：不开日志时不必建目录

        self.runlog = RunLog(log_dir, echo=False) if log_dir else None
        if self.runlog is not None:
            self.runlog.start(ui="tui", model=model_kind,
                              watchdog=watchdog_interval, suspect=suspect_after)
        if runtime is not None:
            self.runtime = runtime
        else:
            model = None
            if self.model_kind == "fake":
                model = FakeChatModel(latency=(0.12 / self.speed, 0.3 / self.speed))
            else:
                model = build_chat_model(self.model_kind, config=self.config.model)
            self.runtime = Runtime(model=model, persist=persist,
                                   watchdog_interval=watchdog_interval,
                                   suspect_after=suspect_after,
                                   log=self.runlog)

        self._tree_view: SpanTreeView | None = None
        self._table_model: TaskTableModel | None = None
        self._pending_spans: dict[str, Span] = {}
        self._pending_tasks: dict[str, dict[str, Any]] = {}
        self._pending_alerts: list[dict[str, Any]] = []
        self._pending_logs: list[str] = []
        self._pending_gates: dict[str, dict[str, Any]] = {}
        self._gate_state: dict[str, str] = {}
        self._selected_span_id: str | None = None
        self._detail_key: Any = None
        self._selected_task_id: str | None = None
        self._view = "tree"      # tree / timeline / lifecycle
        self._last_summary_fp: tuple = ()
        self._drainer: Any = None
        self._closing = False
        self._flush_scheduled = False
        self.counters = {
            "drain_ticks": 0, "events_seen": 0, "submitted": 0,
            "tree_ops": 0, "cell_writes": 0, "row_adds": 0, "detail_renders": 0,
        }
        self.post_mount_script: list[str] = []
        self.post_mount_morning = False

    # ------------------------------------------------------------------
    # 布局
    # ------------------------------------------------------------------
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="main"):
            with Vertical(id="left"):
                yield Static("EVENT STREAM  (消息 → 系统怎么接：拆单/直通/召回/拦截)", id="es-title", classes="title")
                yield RichLog(id="event-stream", wrap=True, markup=False, highlight=False, max_lines=800)
                yield Static("对话", id="dialog-title", classes="title")
                yield RichLog(id="dialog", wrap=True, markup=False, highlight=False, max_lines=400)
            with Vertical(id="right"):
                yield Static("TRACE  [F5 切换：树 / 时间轴 / 任务]", id="trace-title", classes="title")
                yield TraceTree("TRACE", id="trace-tree")
                yield RichLog(id="trace-view", wrap=False, markup=False, highlight=False, max_lines=400)
                yield Static("SPAN DETAIL", id="detail-title", classes="title")
                yield RichLog(id="span-detail", wrap=False, markup=False, highlight=False, max_lines=120)
        yield Static("", id="summary")
        yield Static("需要你 + 告警", id="alerts-title", classes="title")
        yield RichLog(id="alerts", wrap=True, markup=False, highlight=False, max_lines=200)
        yield DataTable(id="snapshot", cursor_type="row", zebra_stripes=True)
        with Horizontal(id="prompt-row"):
            yield Input(placeholder="说一句话…（Enter 发送；F3 剧本；A 批准选中任务；X 驳回）", id="prompt")
            yield Static("[Send]", id="send")
        yield Static("", id="status")
        yield Footer()

    async def on_mount(self) -> None:
        tree = self.query_one("#trace-tree", TraceTree)
        self._tree_view = SpanTreeView(tree, self.runtime.trace)
        self._tree_view.attach()
        tree.root.expand()
        self.call_after_refresh(lambda: tree.root.expand())
        # 初始视图：只显示树
        self.query_one("#trace-view", RichLog).display = False

        snapshot = self.query_one("#snapshot", DataTable)
        self._table_model = TaskTableModel(self.runtime.table, snapshot)
        self._table_model.attach()

        self.title = "Super Agent Console"
        self.sub_title = f"{self.model_kind} · {self.runtime.node_id}"
        await self.runtime.start()
        self._drainer = self.run_worker(self._drain_loop(), exclusive=False, thread=False)
        self._write_event_stream(f"{clock_str()}  就绪。输入一句话试试；F1 看说明，F3 播放「忙的上午」剧本。")
        self.say("说一句话，回车。想快速看全部机制，按 F3。", style="grey58")
        self.say(f"模型：{self.model_label()}", style="grey50")
        if self.runlog is not None:
            self.say(f"本次日志：{self.runlog.dir}", style="grey50")

        self._refresh_summary(force=True)
        self.set_interval(0.4, self._tick)
        # 没有这一行，光标不在输入框上，回车等于打在空气里
        self.query_one("#prompt", Input).focus()
        if self.post_mount_morning:
            self.action_run_morning()
        if self.post_mount_script:
            self.run_startup_script()

    def model_label(self) -> str:
        import config as C
        if self.model_kind == "fake":
            return "内置假数据（不联网，答案来自本地桩）"
        return C.describe(self.config)

    async def on_unmount(self) -> None:
        self._closing = True
        if self._drainer is not None:
            self._drainer.cancel()
        await self.runtime.stop()
        if self.runlog is not None:
            try:
                self.runlog.close(self.runtime.stats())
            except Exception:  # pragma: no cover
                pass

    # ------------------------------------------------------------------
    # 队列 -> 增量刷新
    # ------------------------------------------------------------------
    async def _drain_loop(self) -> None:
        q = self.runtime.events
        while True:
            evt = await q.get()
            self._ingest(evt)
            n = 1
            while n < MAX_DRAIN:
                try:
                    self._ingest(q.get_nowait())
                    n += 1
                except Exception:
                    break
            self._flush_soon()

    def _flush_soon(self) -> None:
        """合并同一批事件后再刷新。

        一条消息会产生几十个 TraceEvent。早期版本每个事件都排一次刷新，
        等于整屏重绘几十次，在传统控制台里就是"一直刷屏"。
        """
        if self._flush_scheduled or self._closing:
            return
        self._flush_scheduled = True

        async def once() -> None:
            await asyncio.sleep(0.08)
            self._flush_scheduled = False
            try:
                await self._flush()
            except Exception as exc:  # noqa: BLE001
                try:
                    self.query_one("#status", Static).update(f"渲染异常：{exc!r}")
                except Exception:
                    pass

        asyncio.get_running_loop().create_task(once())

    def _ingest(self, evt: TraceEvent) -> None:
        self.counters["events_seen"] += 1
        if evt.kind == "span_upsert" and evt.span is not None:
            self._pending_spans[evt.span.span_id] = evt.span
        elif evt.kind == "task_upsert" and evt.task is not None:
            self._pending_tasks[evt.task["task_id"]] = evt.task
        elif evt.kind == "alert" and evt.alert is not None:
            self._pending_alerts.append(evt.alert)
        elif evt.kind == "log":
            self._pending_logs.append(evt.text)
        elif evt.kind == "event_gate":
            data = json.loads(evt.text)
            self._pending_gates[data["event_id"]] = data

    async def _flush(self) -> None:
        if self._closing:
            return
        self.counters["drain_ticks"] += 1

        for _, data in sorted(self._pending_gates.items()):
            self._render_gate(data)
        self._pending_gates.clear()

        for alert in self._pending_alerts:
            self._render_alert(alert)
        self._pending_alerts.clear()

        for text in self._pending_logs:
            self._write_event_stream(f"{clock_str()}   · {text}", style="grey50")
        self._pending_logs.clear()

        if self._tree_view is not None:
            if self._pending_spans:
                self._pending_spans.clear()
            ops = self._tree_view.sync()
            self.counters["tree_ops"] += ops
        if self._table_model is not None:
            self._table_model.sync()
            self.counters["cell_writes"] = self._table_model.cell_writes
            self.counters["row_adds"] = self._table_model.row_adds

        if self._view != "tree":
            self._render_alt_view()

        self.render_selected_span(refresh_only=True)
        self._refresh_summary()

    # ------------------------------------------------------------------
    # 渲染
    # ------------------------------------------------------------------
    def _write_event_stream(self, text: str, style: str = "white") -> None:
        from rich.text import Text
        if self._closing:
            return
        try:
            self.query_one("#event-stream", RichLog).write(Text(text, style=style))
        except Exception:
            pass

    def _render_gate(self, data: dict[str, Any]) -> None:
        """每条消息一行：它走了哪条路、为什么。

        fingerprint = 事件 id + 是否已经有结论；同一事件第二次进来只补写结论行。
        """
        from rich.text import Text

        eid = data["event_id"]
        route = data.get("route") or ""
        phase = "final" if (route or not data["program_pass"]) else "initial"
        prev = self._gate_state.get(eid)
        if prev == "final" or (prev == "initial" and phase == "initial"):
            return
        self._gate_state[eid] = phase

        log = self.query_one("#event-stream", RichLog)
        if prev is None:
            head = Text()
            head.append(f"{clock_str()}  ", style="grey50")
            head.append(f"{eid} ", style="bold white")
            head.append(f"{data['raw'][:38]}", style="grey70")
            log.write(head)
            line2 = Text("    ")
            line2.append("过滤 ", style="grey50")
            line2.append(data["program_reason"], style="green" if data["program_pass"] else "red")
            line2.append("  ", style="grey50")
            line2.append(data["semantic_verdict"],
                         style="green" if data["semantic_verdict"] == "WORTH" else "grey58")
            log.write(line2)

        label, color = ROUTE_LABEL.get(route, ("…", "grey58"))
        out = Text("    ")
        out.append(f"[{label}]", style=f"bold {color}")
        out.append(f" {data.get('route_note', '')}", style="white")
        if data.get("task_ids"):
            out.append(f"  {data['task_ids']}", style="cyan")
        log.write(out)

    def _render_alert(self, alert: dict[str, Any]) -> None:
        from rich.text import Text

        if self._closing:
            return
        level = alert.get("level", "INFO").upper()
        style = {"WARN": "bold yellow", "ERROR": "bold red", "INFO": "bold cyan"}.get(level, "white")
        t = Text()
        t.append(f"{clock_str()}  ", style="grey50")
        t.append(f"{level:<5} ", style=style)
        t.append(f"{str(alert.get('source', ''))[:24]:<24}", style="bold")
        t.append(str(alert.get("message", "")), style="white")
        try:
            self.query_one("#alerts", RichLog).write(t)
        except Exception:
            pass

    def _render_alt_view(self) -> None:
        """时间轴 / 任务生命周期视图。只在切换时和 flush 时重画，口径与 headless 一致。"""
        if self._closing:
            return
        try:
            log = self.query_one("#trace-view", RichLog)
        except Exception:
            return
        log.clear()
        lines = (timeline_lines(self.runtime.trace, width=self.size.width)
                 if self._view == "timeline"
                 else lifecycle_lines(self.runtime.trace, self.runtime.table, width=self.size.width))
        for line in lines:
            log.write(line)

    def _refresh_summary(self, *, force: bool = False) -> None:
        rt = self.runtime
        st = rt.stats()
        attention = st["attention"]
        fp = (tuple(sorted(st["status"].items())), attention["human_touches"],
              attention["needs_human_now"], rt.router.decision_count,
              st.get("router_skipped", 0), len(rt.table.subscriptions()),
              attention.get("system_touches", 0))
        if not force and fp == self._last_summary_fp:
            return
        self._last_summary_fp = fp
        counts = st["status"]
        self.query_one("#summary", Static).update(
            f"在做 {counts.get('RUNNING', 0) + counts.get('PENDING', 0)}  "
            f"· 等结果 {counts.get('WAITING', 0)}  "
            f"· 等你批 {counts.get('NEEDS_HUMAN', 0)}  "
            f"· 完成 {counts.get('SUCCEEDED', 0)}  "
            f"· 失败 {counts.get('FAILED', 0)}  "
            f"· 无进展 {counts.get('STALLED', 0) + counts.get('SUSPECT', 0)}"
            f"   │ Router 判断 {rt.router.decision_count} 次，省下 {st.get('router_skipped', 0)} 次"
            f"   │ 你出手 {attention['human_touches']} 次，系统自己救 {attention.get('system_touches', 0)} 次"
        )
        self.query_one("#status", Static).update(
            f"视图:{self._view}  · 选中:{self._selected_task_id or '—'}  · "
            f"树 重建 {self.counters['tree_ops']} / 累计跳过 {self._tree_view.skip_count if self._tree_view else 0}  · "
            f"表 写入 {self.counters['cell_writes']} / 累计跳过 {self._table_model.skips if self._table_model else 0}  · "
            f"Watchdog {'运行中' if rt.watchdog.running else '停止'} "
            f"（上次扫描 {rt.watchdog.last_scan_age and f'{rt.watchdog.last_scan_age:.0f}s 前' or '—'}）"
        )

    def _tick(self) -> None:
        if self._closing:
            return
        if self._view == "tree" and self._tree_view is not None:
            self.counters["tree_ops"] += self._tree_view.sync(restyle=True)
        self._refresh_summary()

    def render_selected_span(self, *, refresh_only: bool = False) -> None:
        if self._closing:
            return
        try:
            log = self.query_one("#span-detail", RichLog)
        except Exception:
            return
        span = self.runtime.trace.get(self._selected_span_id) if self._selected_span_id else None
        if span is None and refresh_only:
            return
        # 内容没变就别 clear + 重写：那是整块面板的重绘，会闪
        key = (span.span_id, span.status, span.ended_at, span.warning_count,
               round(span.duration, 1) if span.ended_at is not None else None) if span else None
        if refresh_only and key == self._detail_key:
            return
        self._detail_key = key
        log.clear()
        if span is None:
            log.write(_rich("在树里用 ↑/↓ 选中节点，这里显示它的输入/输出/耗时/tokens"))
            return
        self.counters["detail_renders"] += 1
        width = max(40, self.size.width // 2)
        for line in detail_lines(span, width=width):
            log.write(line)

    def select_span(self, span_id: Any) -> None:
        if not span_id or self.runtime.trace.get(str(span_id)) is None:
            return
        self._selected_span_id = str(span_id)
        self.render_selected_span()

    # ------------------------------------------------------------------
    # 输入
    # ------------------------------------------------------------------
    @on(Input.Submitted, "#prompt")
    def on_prompt_submitted(self, event: Input.Submitted) -> None:
        raw = event.value.strip()
        if raw:
            event.input.value = ""
            self.submit(raw)

    def submit(self, raw: str) -> None:
        self.counters["submitted"] += 1
        self.handle_message(raw)

    def say(self, text: str, *, style: str = "white", who: str = "") -> None:
        """往对话区写一行。``who`` 为空表示是系统在说。"""
        from rich.text import Text
        if self._closing:
            return
        line = Text()
        if who:
            line.append(f"{who}  ", style="bold grey58")
        for i, seg in enumerate(text.splitlines() or [""]):
            if i:
                line.append("\n")
            line.append(seg, style=style)
        try:
            self.query_one("#dialog", RichLog).write(line)
        except Exception:  # pragma: no cover - 卸载竞态
            pass

    @work(exclusive=False, group="events")
    async def handle_message(self, raw: str) -> None:
        try:
            self.say(raw, who="你")
            result = await self.runtime.process(raw)
            await self._flush()
            self.say(self.reply_for(result), style="white")
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            if not self._closing:
                self.say(f"处理这条时出错：{traceback.format_exc(limit=2)}", style="red")

    def reply_for(self, result: Any) -> str:
        return reply_text(result)

    def log_alert(self, markup: str) -> None:
        if self._closing:
            return
        try:
            self.query_one("#alerts", RichLog).write(_rich(markup))
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 动作
    # ------------------------------------------------------------------
    def action_focus_input(self) -> None:
        self.query_one("#prompt", Input).focus()

    def action_help(self) -> None:
        self.log_alert(HELP)

    def action_cancel_all(self) -> None:
        n = self.runtime.pool.cancel_all()
        self.log_alert(f"[magenta]已请求取消 {n} 个任务（在下一个安全点退出）[/]")

    def action_approve_all(self) -> None:
        pending = self.runtime.needs_human()
        if not pending:
            self.log_alert("[grey58]没有需要批准的任务[/]")
            return
        for task in pending:
            self.run_approve(task.task_id)

    def action_approve_selected(self) -> None:
        tid = self._selected_task_id or (self.runtime.needs_human()[0].task_id
                                         if self.runtime.needs_human() else None)
        if tid is None:
            self.log_alert("[grey58]先选中一个「等人批」的任务[/]")
            return
        self.run_approve(tid)

    def action_reject_selected(self) -> None:
        tid = self._selected_task_id
        if tid is None:
            self.log_alert("[grey58]先选中一个任务[/]")
            return
        if self.runtime.reject(tid):
            self.log_alert(f"[magenta]已驳回 {tid}[/]")

    @work(exclusive=False, group="approve")
    async def run_approve(self, task_id: str) -> None:
        ok = await self.runtime.approve(task_id)
        self.log_alert(f"[green]批准 {task_id}[/]" if ok else f"[grey58]{task_id} 不在待批状态[/]")

    def action_toggle_view(self) -> None:
        order = ["tree", "timeline", "lifecycle"]
        self._view = order[(order.index(self._view) + 1) % len(order)]
        tree = self.query_one("#trace-tree", TraceTree)
        alt = self.query_one("#trace-view", RichLog)
        tree.display = self._view == "tree"
        alt.display = self._view != "tree"
        self.query_one("#trace-title", Static).update(
            f"TRACE  {'[树]' if self._view == 'tree' else '[时间轴]' if self._view == 'timeline' else '[任务]'}"
            f"  [F5 切换：树 / 时间轴 / 任务]"
        )
        if self._view != "tree":
            self._render_alt_view()

    def action_toggle_flaky(self) -> None:
        model = self.runtime.model
        if isinstance(model, FakeChatModel):
            model.fault_rate = 0.0 if model.fault_rate else 0.3
            self.log_alert(f"[yellow]故障注入 fault_rate={model.fault_rate:.2f}[/]")

    def action_screenshot(self, filename: str | None = None) -> None:
        path = self.save_screenshot(filename or f"console_{time.strftime('%H%M%S')}.svg")
        self.log_alert(f"[green]截图：{path}[/]")

    def action_run_morning(self) -> None:
        self.play_morning()

    @work(exclusive=True, group="script")
    async def play_morning(self) -> None:
        """F3：播放「忙的上午」剧本。"""
        await self._run_morning()

    async def _run_morning(self) -> None:
        """剧本的实际执行体（测试可以直接 await 它）。"""
        for step in SC.MORNING:
            self.log_alert(f"[bold cyan]▸ {step.note}[/]")
            self.submit(step.say)
            await asyncio.sleep(step.hold * self.speed)
            if step.wait_for == "idle":
                await self._wait_idle()
        self.log_alert("[bold cyan]剧本结束。F5 看时间轴和任务来龙去脉。[/]")

    async def _wait_idle(self, timeout: float = 30.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.1)
            if not self.runtime.table.active():
                return

    @work(exclusive=False, group="startup")
    async def run_startup_script(self) -> None:
        for raw in self.post_mount_script:
            self.submit(raw)
            await asyncio.sleep(0.2)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        tid = str(event.row_key.value)
        task = self.runtime.table.get(tid)
        self._selected_task_id = tid
        if task is not None and task.span_id and self._tree_view is not None:
            self._tree_view.select(task.span_id)
            self.select_span(task.span_id)

    def on_tree_node_highlighted(self, event: Tree.NodeHighlighted) -> None:
        sid = event.node.data
        if not sid:
            return
        self.select_span(sid)
        span = self.runtime.trace.get(str(sid))
        if span is not None:
            for task in self.runtime.table.all():
                if task.span_id == span.span_id:
                    self._selected_task_id = task.task_id
                    break

    # ------------------------------------------------------------------
    # 导出（headless / 测试用）
    # ------------------------------------------------------------------
    def export_text(self) -> str:
        w, h = self.size
        buf = io.StringIO()
        from rich.console import Console
        console = Console(width=w, height=h, file=buf, force_terminal=False, color_system=None)
        try:
            console.print(self.screen._compositor)
        except Exception:  # pragma: no cover
            console.print(f"(无法渲染，{w}x{h})")
        return buf.getvalue()

    def export_svg(self, path: str | Path) -> Path:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.export_screenshot(), encoding="utf-8")
        return p

    def _richlog_lines(self, selector: str) -> list[str]:
        out: list[str] = []
        try:
            log = self.query_one(selector, RichLog)
        except Exception:
            return out
        for line in getattr(log, "lines", []):
            segments = getattr(line, "_segments", None)
            out.append("".join(seg.text for seg in segments) if segments is not None else str(line))
        return out

    async def export_debug_dump(self, path: str | Path) -> Path:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        parts = ["=== EVENT STREAM ===", *self._richlog_lines("#event-stream"), "",
                 "=== TRACE TREE ===", self.runtime.trace.tree_text(), "",
                 "=== TIMELINE ==="]
        from rich.console import Console
        buf = io.StringIO()
        c = Console(width=120, file=buf, force_terminal=False, color_system=None)
        for line in timeline_lines(self.runtime.trace):
            c.print(line)
        parts.append(buf.getvalue())
        parts += ["", "=== ALERTS ===", *self._richlog_lines("#alerts"), "",
                  "=== TASKS ==="]
        for task in self.runtime.table.all():
            parts.append(f"{task.name} {task.status} chain={'→'.join(task.trace_chain)} "
                         f"topic=#{task.topic} human={task.human_touches} "
                         f"result={(task.result or '')[:60]!r}")
        parts += ["", "=== SUMMARY ===", str(self.query_one("#summary", Static).render())]
        p.write_text("\n".join(parts), encoding="utf-8")
        return p


def reply_text(result: Any) -> str:
    """把一次处理结果翻译成一句人话。

    每种去向都必须有回复：被过滤、被转交、挂起、失败都一样。
    早期版本只在 Router 合成时说话，于是"直通"和"被拦截"两种情况下
    用户敲完回车看不到任何反应，像是对着空气说话。
    """
    if result.error:
        return f"这条没处理成：{result.error}"
    if result.final_text:
        return result.final_text

    route = result.route
    if route == ROUTE_GATE:
        return f"这条没往下走 —— {result.gate.route_note}。"

    if route == ROUTE_IGNORE:
        return "这条我没派任务。"

    if route == ROUTE_DIRECT:
        task = result.dispatched[0] if result.dispatched else None
        if task is None:
            return "已经转给在办的任务。"
        if task.status == "NEEDS_HUMAN":
            return f"转给 {task.name} 了。它需要你确认：{task.needs_human_reason}（按 A 批准）"
        if task.status == "WAITING":
            return f"转给 {task.name} 了。它在等：{task.waiting_on}"
        if task.result:
            return f"转给 {task.name} 了，它按你的补充重做了一遍：\n{task.result}"
        return f"转给 {task.name} 了，正在按你的补充继续。"

    blocked = [t for t in result.dispatched if t.status in ("NEEDS_HUMAN", "WAITING")]
    if blocked:
        lines = [f"{t.name}：{t.needs_human_reason or t.waiting_on}" for t in blocked]
        return "派下去了，但卡在等：\n" + "\n".join(lines)
    if result.dispatched:
        done = [t for t in result.dispatched if t.status == "SUCCEEDED"]
        if done:
            return f"派了 {len(result.dispatched)} 条，完成 {len(done)} 条，结果见下方任务表。"
    return "这条没有产生可执行的任务。"


def _rich(markup: str, style: str | None = None) -> Any:
    from rich.text import Text
    try:
        text = Text.from_markup(markup)
    except Exception:
        text = Text(markup)
    if style:
        text.stylize(style)
    return text


HELP = """[bold]Super Agent 控制台[/]
一句话说明：你说的话会被系统判断成 [cyan]拆单[/] / [magenta]直通[/] / [blue]召回[/] / [grey58]拦截[/]，
你在左边 EVENT STREAM 能直接看到走了哪条。
[bold]值得试的几句话[/]
  帮我查一下成都周边徒步路线，排个两天计划，再写一版介绍文案
  路线那条再补一句：避开收费景区        → 直通到正在管的路线任务，不再拆单
  排期里那步订票，帮我直接订了吧           → 挂起，等你按 A 批准
  刚才那个介绍文案改成更正式的腔调          → 召回已完成的成文任务接着改
[bold]键位[/]：F3 播放剧本 · F5 树/时间轴/任务 · A 批准 · X 驳回 · F2 全部取消 · F1 帮助 · F9 截图"""


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Super Agent 控制台")
    p.add_argument("--model", default="auto",
                   help="auto=用 config.json 的配置；fake=内置假数据；也可写供应商名")
    p.add_argument("--watchdog", type=float, default=3.0, help="巡检间隔秒")
    p.add_argument("--suspect", type=float, default=60.0, help="多久没动静算疑似卡死")
    p.add_argument("--fast", action="store_true")
    p.add_argument("--slow", action="store_true")
    p.add_argument("--persist", default=None)
    p.add_argument("--demo", action="store_true", help="启动后播放剧本")
    p.add_argument("--script", action="append", default=None)
    p.add_argument("--log-dir", default="logs", help="运行日志目录（问题定位用），传空串关闭")
    return p


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    speed = 3.0 if args.fast else (0.5 if args.slow else 1.0)
    app = ConsoleApp(model_kind=args.model, watchdog_interval=args.watchdog,
                     suspect_after=args.suspect, persist=args.persist, speed=speed,
                     log_dir=args.log_dir or None)
    app.post_mount_morning = bool(args.demo)
    app.post_mount_script = list(args.script or [])
    app.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
