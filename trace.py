"""trace.py —— 可观测性：Span / 时间轴 / 任务生命周期。

Super Agent 里执行是并发的、被事件驱动的，所以「这条请求花了多久」这种单链路
思路不够用。这里按三个视角分别回答三个不同的问题：

1. **树（SpanTreeView）**：谁拉起了谁。回答「这个工具调用属于哪条链路」。
2. **时间轴（Timeline）**：并发重叠了多久、总耗时被哪条链决定（关键路径）、
   事件是走了直通还是被 Router 召回。回答「为什么是这个耗时、Router 少做了多少事」。
3. **任务生命周期（TaskTableModel + lifecycle_lines）**：每条任务从哪来、状态怎么迁移、
   被人插手几次、订阅给了谁。回答「这件事的来龙去脉」。

Runtime 与 UI 之间只走一个 ``asyncio.Queue[TraceEvent]``；本文件不 import agentscope。
Textual 只在真正需要挂控件的地方延迟导入。
"""

from __future__ import annotations

import itertools
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Iterable

from rich.text import Text

# =============================================================================
# 常量
# =============================================================================
KIND_EVENT = "event"
KIND_ROUTER = "router"
KIND_WORKER = "worker"
KIND_TOOL = "tool"
KIND_HUMAN = "human"      # 人工介入（批准 / 驳回 / 追加指令）

ST_RUNNING = "running"
ST_OK = "ok"
ST_WARN = "warn"
ST_ERROR = "error"
ST_CANCELLED = "cancelled"
ST_WAITING = "waiting"
ST_QUEUED = "queued"

STATUS_ICON: dict[str, str] = {
    ST_RUNNING: "●", ST_OK: "✓", ST_QUEUED: "○", ST_WAITING: "◔",
    ST_WARN: "⚠", ST_ERROR: "✕", ST_CANCELLED: "x",
}
STATUS_COLOR: dict[str, str] = {
    ST_RUNNING: "cyan", ST_OK: "green", ST_QUEUED: "grey58", ST_WAITING: "blue",
    ST_WARN: "yellow", ST_ERROR: "red", ST_CANCELLED: "magenta",
}

SLOW_THRESHOLD_S = 3.0

#: 事件派发方式（打在时间轴与 Event Stream 上）
ROUTE_DIRECT = "DIRECT"      # 订阅直通：没经过 Router 拆单
ROUTE_RECALL = "RECALL"      # 链式召回：接续到一条已有任务
ROUTE_ROUTER = "ROUTER"      # 正常走 Router 拆单
ROUTE_GATE = "GATE"          # 被程序层挡掉（噪声）
ROUTE_IGNORE = "IGNORE"      # Router 判断不用派活，但仍然回一句话
ROUTE_ANSWER = "ANSWER"      # 不需要动手，Router 直接回答
ROUTE_LABEL = {
    ROUTE_DIRECT: ("直通", "magenta"),
    ROUTE_RECALL: ("召回", "blue"),
    ROUTE_ROUTER: ("拆单", "cyan"),
    ROUTE_GATE: ("拦截", "grey58"),
    ROUTE_IGNORE: ("不派", "grey58"),
    ROUTE_ANSWER: ("回答", "green"),
}


def icon_for(status: str) -> str:
    return STATUS_ICON.get(status, "?")


def color_for(status: str) -> str:
    return STATUS_COLOR.get(status, "white")


def fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    if seconds < 10:
        return f"{seconds:.2f}s"
    if seconds < 60:
        return f"{seconds:.1f}s"
    return f"{seconds:.0f}s"


def short_id(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex[:8]}"


# =============================================================================
# Span
# =============================================================================
@dataclass
class Span:
    span_id: str
    parent_id: str | None
    kind: str
    name: str
    status: str = ST_RUNNING
    started_at: float = field(default_factory=time.monotonic)
    ended_at: float | None = None
    detail: dict[str, Any] = field(default_factory=dict)
    child_ids: list[str] = field(default_factory=list)
    revision: int = 0
    expanded: bool = True

    def __post_init__(self) -> None:
        if not self.span_id:
            self.span_id = short_id(self.kind[0] + "-")
        if self.ended_at is not None and self.started_at > self.ended_at:
            self.started_at = self.ended_at

    @property
    def duration(self) -> float | None:
        return None if self.ended_at is None else max(0.0, self.ended_at - self.started_at)

    @property
    def live_duration(self) -> float:
        if self.ended_at is not None:
            return max(0.0, self.ended_at - self.started_at)
        return max(0.0, time.monotonic() - self.started_at)

    @property
    def is_open(self) -> bool:
        return self.ended_at is None

    @property
    def warning_count(self) -> int:
        return int(self.detail.get("warning_count", 0) or 0)

    def end(self, status: str | None = None, **detail: Any) -> "Span":
        if self.ended_at is None:
            self.ended_at = time.monotonic()
        if detail:
            self.detail.update(detail)
        if status is None:
            status = ST_WARN if self.warning_count else ST_OK
        self.status = status
        self.revision += 1
        return self

    def add_warning(self, message: str) -> "Span":
        warnings = list(self.detail.get("warnings", []))
        warnings.append(message)
        self.detail["warnings"] = warnings
        self.detail["warning_count"] = len(warnings)
        if self.status in (ST_OK, ST_RUNNING):
            self.status = ST_WARN
        self.revision += 1
        return self

    def note(self, **detail: Any) -> "Span":
        self.detail.update(detail)
        self.revision += 1
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "span_id": self.span_id, "parent_id": self.parent_id, "kind": self.kind,
            "name": self.name, "status": self.status, "started_at": self.started_at,
            "ended_at": self.ended_at, "duration": self.duration,
            "detail": dict(self.detail), "child_ids": list(self.child_ids),
            "revision": self.revision,
        }


# =============================================================================
# TraceEvent
# =============================================================================
@dataclass
class TraceEvent:
    """Runtime -> UI 的唯一通道。"""

    kind: str
    """``span_upsert`` / ``task_upsert`` / ``alert`` / ``log`` / ``event_gate``"""

    span: Span | None = None
    task: dict[str, Any] | None = None
    alert: dict[str, Any] | None = None
    text: str = ""
    seq: int = 0
    created_at: float = field(default_factory=time.monotonic)

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind, "span": self.span.to_dict() if self.span else None,
            "task": self.task, "alert": self.alert, "text": self.text, "seq": self.seq,
        }


# =============================================================================
# TraceStore
# =============================================================================
class TraceStore:
    def __init__(self, publish: Any | None = None, *, max_spans: int = 4000) -> None:
        self._spans: dict[str, Span] = {}
        self._children: dict[str | None, list[str]] = {None: []}
        self._seq = itertools.count(1)
        self._publish = publish
        self.max_spans = max_spans
        self.published = 0
        #: 额外的观察者（例如写日志）。界面之外的消费者挂在这里，
        #: 这样 runtime 不需要知道是 UI 还是磁盘在收事件。
        self._observers: list[Any] = []

    def observe(self, callback: Any) -> None:
        """注册一个旁路观察者：每条 TraceEvent 都会额外投给它一份。"""
        self._observers.append(callback)

    # ---- 读 ---------------------------------------------------------------
    def __contains__(self, span_id: object) -> bool:
        return span_id in self._spans

    def __len__(self) -> int:
        return len(self._spans)

    def __getitem__(self, span_id: str) -> Span:
        return self._spans[span_id]

    def get(self, span_id: str) -> Span | None:
        return self._spans.get(span_id)

    def roots(self) -> list[Span]:
        return [self._spans[s] for s in self._children.get(None, []) if s in self._spans]

    def children_of(self, span_id: str | None) -> list[Span]:
        return [self._spans[s] for s in self._children.get(span_id, []) if s in self._spans]

    def all_spans(self) -> list[Span]:
        return list(self._spans.values())

    def walk(self, span_id: str | None = None) -> Iterable[Span]:
        for child in self.children_of(span_id):
            yield child
            yield from self.walk(child.span_id)

    # ---- 写 ---------------------------------------------------------------
    def start_span(
        self,
        kind: str,
        name: str,
        *,
        parent_id: str | None = None,
        span_id: str | None = None,
        status: str = ST_RUNNING,
        **detail: Any,
    ) -> Span:
        span = Span(
            span_id=span_id or short_id(kind[0] + "-"),
            parent_id=parent_id, kind=kind, name=name, status=status,
            detail=dict(detail),
        )
        self._spans[span.span_id] = span
        self._children.setdefault(parent_id, []).append(span.span_id)
        self._children.setdefault(span.span_id, [])
        if parent_id is not None and parent_id in self._spans:
            self._spans[parent_id].child_ids.append(span.span_id)
        self._trim()
        self.push_span(span)
        return span

    def end_span(self, span_id: str, status: str | None = None, **detail: Any) -> Span:
        span = self._spans[span_id]
        span.end(status=status, **detail)
        if status is None and span.warning_count and span.status == ST_OK:
            span.status = ST_WARN
        self.push_span(span)
        return span

    def update_span(self, span_id: str, *, status: str | None = None, **detail: Any) -> Span:
        span = self._spans[span_id]
        if status is not None:
            span.status = status
        span.note(**detail)
        self.push_span(span)
        return span

    def warn_span(self, span_id: str, message: str) -> Span:
        span = self._spans[span_id]
        span.add_warning(message)
        self.push_span(span)
        self.publish(TraceEvent(kind="alert", alert={
            "level": "WARN", "source": span.name, "span_id": span.span_id, "message": message,
        }))
        return span

    def set_status(self, span_id: str, status: str) -> Span:
        span = self._spans[span_id]
        span.status = status
        span.revision += 1
        self.push_span(span)
        return span

    # ---- 发布 -------------------------------------------------------------
    def publish(self, evt: TraceEvent) -> None:
        evt.seq = next(self._seq)
        self.published += 1
        if self._publish is not None:
            self._publish(evt)
        for observer in self._observers:
            try:
                observer(evt)
            except Exception:  # pragma: no cover - 观察者不能影响主流程
                pass

    def push_span(self, span: Span) -> None:
        self.publish(TraceEvent(kind="span_upsert", span=span))

    # ---- 统计 -------------------------------------------------------------
    def status_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for span in self._spans.values():
            counts[span.status] = counts.get(span.status, 0) + 1
        return counts

    def fingerprint(self) -> tuple:
        return (len(self._spans), sum(s.revision for s in self._spans.values()),
                len(self._children.get(None, [])))

    def tree_text(self) -> str:
        lines: list[str] = []

        def visit(span: Span, depth: int) -> None:
            pad = "  " * depth + ("├─ " if depth else "")
            warn = f" ⚠{span.warning_count}" if span.warning_count else ""
            lines.append(f"{pad}{icon_for(span.status)} {span.name}  [{span.kind}]  "
                         f"{fmt_duration(span.live_duration)}{warn}")
            for child in self.children_of(span.span_id):
                visit(child, depth + 1)

        for root in self.roots():
            visit(root, 0)
        return "\n".join(lines)

    # ---- 内部 -------------------------------------------------------------
    def _trim(self) -> None:
        if len(self._spans) <= self.max_spans:
            return
        roots = self._children.get(None, [])
        while len(self._spans) > self.max_spans and len(roots) > 1:
            self._drop(roots.pop(0))

    def _drop(self, span_id: str) -> None:
        for child_id in list(self._children.get(span_id, [])):
            self._drop(child_id)
        span = self._spans.pop(span_id, None)
        self._children.pop(span_id, None)
        if span is not None and span.parent_id in self._spans:
            parent = self._spans[span.parent_id]
            if span_id in parent.child_ids:
                parent.child_ids.remove(span_id)
                parent.revision += 1


# =============================================================================
# 渲染：树节点标签
# =============================================================================
DURATION_PLACEHOLDER = "  --.---"


def _duration_segment(span: Span) -> str:
    cur = span.live_duration if span.ended_at is None else span.duration
    return f"  {fmt_duration(cur)}"


def _duration_style(span: Span) -> str:
    cur = span.live_duration if span.ended_at is None else span.duration
    return "yellow" if (cur or 0) > SLOW_THRESHOLD_S else "grey62"


def span_label(span: Span) -> Text:
    icon = icon_for(span.status)
    color = color_for(span.status)
    dur = span.live_duration if span.ended_at is None else span.duration

    text = Text()
    text.append(f"{icon} ", style=f"bold {color}")
    text.append(span.name, style="bold" if span.kind in (KIND_EVENT, KIND_WORKER) else "")
    if span.kind != KIND_EVENT:
        text.append(f" [{span.kind}]", style="grey50")
    if dur is not None:
        if span.ended_at is None:
            text.append(DURATION_PLACEHOLDER, style=_duration_style(span))
        else:
            text.append(f"  {fmt_duration(dur)}",
                        style="yellow" if dur > SLOW_THRESHOLD_S else "grey62")
    if span.warning_count:
        text.append(f"  WARN{span.warning_count}", style="yellow")
    if span.status == ST_ERROR and span.detail.get("error"):
        text.append(f"  ERR{_clip(str(span.detail['error']), 40)}", style="red")
    elif span.status == ST_CANCELLED:
        text.append("  [cancelled]", style="magenta")
    return text


def render_live_duration(text: Text, span: Span | None) -> Text:
    if span is None or span.ended_at is not None:
        return text
    flat = getattr(text, "plain", None)
    if flat is None or DURATION_PLACEHOLDER not in flat:
        return text
    try:
        head, _, tail = flat.partition(DURATION_PLACEHOLDER)
        rebuilt = Text()
        if head:
            rebuilt.append(head)
        rebuilt.append(_duration_segment(span), style=_duration_style(span))
        if tail:
            rebuilt.append(tail)
        shift = len(_duration_segment(span)) - len(DURATION_PLACEHOLDER)
        start, end = len(head), len(head) + len(DURATION_PLACEHOLDER)
        for st in getattr(text, "_spans", []):
            s, e, style = st.start, st.end, st.style
            if e <= start:
                rebuilt.stylize(style, s, e)
            elif s >= end:
                rebuilt.stylize(style, s + shift, e + shift)
            else:
                if s < start:
                    rebuilt.stylize(style, s, start)
                if e > end:
                    rebuilt.stylize(style, end + shift, e + shift)
        return rebuilt
    except Exception:  # pragma: no cover
        return text


def detail_lines(span: Span, *, width: int = 120) -> list[Text]:
    """Span Detail：每行一个 Text，超宽截断（RichLog 默认不折行）。"""
    lines: list[Text] = []

    def line(label: str, value: str, *, label_style: str = "grey50",
             value_style: str = "white") -> None:
        t = Text()
        t.append(f"{label:<10}", style=label_style)
        t.append(_clip(value, width - 12), style=value_style)
        lines.append(t)

    head = Text()
    head.append(f"{icon_for(span.status)} ", style=f"bold {color_for(span.status)}")
    head.append(span.name, style="bold")
    head.append(f"   ({span.kind}, {span.status})", style="grey58")
    lines.append(head)

    line("span_id", span.span_id, value_style="grey70")
    line("parent", span.parent_id or "—", value_style="grey70")
    dur = fmt_duration(span.live_duration) + ("" if span.ended_at is not None else " (运行中)")
    line("latency", dur, value_style="yellow" if span.live_duration > SLOW_THRESHOLD_S else "grey70")
    for key in ("chain", "route", "input", "output", "tokens", "model", "tool",
                "note", "waiting_on", "mode"):
        if key in span.detail:
            line(key, _render_value(span.detail[key]))
    warnings = span.detail.get("warnings") or []
    if warnings:
        line("warnings", f"{len(warnings)} 条", value_style="yellow")
        for w in warnings:
            lines.append(Text(f"   ⚠ {_clip(str(w), width - 8)}", style="yellow"))
    if span.status == ST_ERROR and span.detail.get("error"):
        lines.append(Text(f"error     {_clip(str(span.detail['error']), width - 8)}", style="red"))
    known = {"chain", "route", "input", "output", "tokens", "model", "tool", "note",
             "waiting_on", "mode", "warnings", "error", "warning_count", "call_id", "tool_state"}
    for key, value in span.detail.items():
        if key not in known:
            line(str(key), _render_value(value), value_style="grey62")
    return lines


def _render_value(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(f"{k}={v}" for k, v in value.items())
    if isinstance(value, (list, tuple)):
        return " → ".join(str(v) for v in value)
    return _clip(str(value), 400)


def _clip(text: str, limit: int) -> str:
    text = str(text).replace("\n", " ⏎ ")
    return text if len(text) <= limit else text[: limit - 1] + "…"


def span_display_text(span: Span) -> str:
    warn = f" ⚠{span.warning_count}" if span.warning_count else ""
    return f"{icon_for(span.status)} {span.name}  {fmt_duration(span.live_duration)}{warn}"


# =============================================================================
# 时间轴：并发重叠 / 关键路径 / 直通召回打点
# =============================================================================
@dataclass
class Lane:
    """时间轴上的一条泳道。"""

    key: str
    label: str
    start: float
    end: float
    segments: list[tuple[float, float, str, str]] = field(default_factory=list)
    """(start, end, label, status) —— 泳道内的子段（工具调用 / 等待）"""

    kind: str = "worker"
    status: str = ST_OK
    blocked: bool = False
    """True = 这条泳道在等人/等结果，那段时长不计入"忙"。"""

    @property
    def length(self) -> float:
        return max(0.0, self.end - self.start)


@dataclass
class TimelineReport:
    t0: float
    t1: float
    lanes: list[Lane]
    wall: float
    busy: float
    peak: int
    serial_total: float
    critical_path: list[str]
    savings: float

    @property
    def overlap_ratio(self) -> float:
        return 0.0 if self.wall <= 0 else (self.busy / self.wall)

    @property
    def parallel_speedup(self) -> float:
        return 0.0 if self.wall <= 0 else (self.serial_total / self.wall)

    def to_dict(self) -> dict[str, Any]:
        return {
            "wall": round(self.wall, 3), "busy": round(self.busy, 3),
            "peak": self.peak, "serial_total": round(self.serial_total, 3),
            "critical_path": list(self.critical_path),
            "speedup": round(self.parallel_speedup, 2),
        }


def build_timeline(store: TraceStore, table: Any = None, *, only_active: bool = False) -> TimelineReport:
    """从 Span 树算出时间轴。

    只统计 **worker 与 tool** 的**执行区间**。刻意不统计两类时间，否则结论会失真：

    * Router 自己的耗时（常驻层，不属于并行收益）；
    * **挂起**（等人批 / 等结果）——那段时间系统没在干活，把它算进"忙"会显得
      并行省了很多时间，其实只是在等人。挂起区间单独标出来。

    ``only_active=True`` 时只算还没结束的执行段（用于 Console 里看"现在"）。
    """
    now = time.monotonic()
    t0 = t1 = None
    lanes: list[Lane] = []

    for span in store.walk(None):
        end = span.ended_at if span.ended_at is not None else now
        if span.kind == KIND_EVENT:
            t0 = span.started_at if t0 is None else min(t0, span.started_at)
            t1 = end if t1 is None else max(t1, end)

    for span in store.walk(None):
        if span.kind != KIND_WORKER:
            continue
        end = span.ended_at if span.ended_at is not None else now
        lane = Lane(key=span.span_id, label=span.name, start=span.started_at, end=end,
                    kind="worker", status=span.status)
        # 挂起段：worker 在挂起时就被结束（status=waiting，detail 里记在等什么），
        # 所以这里要把它从"忙"里剔除，单独标出来。
        if span.status == ST_WAITING or span.detail.get("waiting_on"):
            lane.blocked = True
        for child in store.children_of(span.span_id):
            if child.kind != KIND_TOOL:
                continue
            cend = child.ended_at if child.ended_at is not None else now
            lane.segments.append((child.started_at, cend, child.name, child.status))
        if not only_active or span.ended_at is None:
            lanes.append(lane)

    if t0 is None:
        for lane in lanes:
            t0 = lane.start if t0 is None else min(t0, lane.start)
            t1 = lane.end if t1 is None else max(t1, lane.end)
    # 时间窗要覆盖**所有**区间（事件 span 可能是瞬时的，不能只看它）
    for lane in lanes:
        t0 = min(t0, lane.start) if t0 is not None else lane.start
        t1 = max(t1, lane.end) if t1 is not None else lane.end
        for s, e, _, _ in lane.segments:
            t0 = min(t0, s)
            t1 = max(t1, e)
    if t0 is None:
        return TimelineReport(0, 0, [], 0, 0, 0, 0, [], 0.0)
    wall = max(0.0, (t1 or now) - t0)

    busy_intervals = [(lane.start, lane.end) for lane in lanes if not lane.blocked]
    points: list[tuple[float, int]] = []
    for s, e in busy_intervals:
        points.append((s, 1))
        points.append((e, -1))
    points.sort()
    peak = cur = 0
    for _, delta in points:
        cur += delta
        peak = max(peak, cur)
    busy = _union_length(busy_intervals)
    serial = sum(lane.length for lane in lanes if not lane.blocked)
    return TimelineReport(
        t0=t0, t1=t1 or t0, lanes=lanes, wall=wall, busy=busy, peak=peak,
        serial_total=serial, critical_path=_critical_path([l for l in lanes if not l.blocked]),
        savings=max(0.0, serial - busy),
    )


def _union_length(intervals: list[tuple[float, float]]) -> float:
    if not intervals:
        return 0.0
    intervals = sorted(intervals)
    total = 0.0
    cur_s, cur_e = intervals[0]
    for s, e in intervals[1:]:
        if s > cur_e:
            total += cur_e - cur_s
            cur_s, cur_e = s, e
        else:
            cur_e = max(cur_e, e)
    return total + (cur_e - cur_s)


def _critical_path(lanes: list[Lane]) -> list[str]:
    """关键路径的简化版：按「结束时间」从晚往前回溯的链。

    这里不建完整的依赖图（Demo 里 Worker 之间没有显式依赖），
    而是回答一个更实用的问题：**总耗时是被哪几条链串起来的**。
    做法：取最晚结束的 worker，再挑出它的工具段，给出「事件 → worker → 最慢工具」。
    """
    if not lanes:
        return []
    latest = max(lanes, key=lambda lane: lane.end)
    path = [latest.label]
    if latest.segments:
        slowest = max(latest.segments, key=lambda seg: seg[1] - seg[0])
        path.append(slowest[2])
    return path


def timeline_lines(store: TraceStore, *, width: int = 90, table: Any = None) -> list[Text]:
    """把时间轴渲染成几行 Text（headless / 测试 / Console 都用它，保证口径一致）。"""
    report = build_timeline(store, table)
    if not report.lanes:
        return [Text("（还没有在跑的 Worker）", style="grey50")]

    span = max(0.001, report.wall)
    lines: list[Text] = []

    head = Text()
    head.append("时间轴  ", style="bold")
    head.append(f"跨度 {fmt_duration(report.wall)}", style="white")
    head.append("   并发峰值 ", style="grey58")
    head.append(str(report.peak), style="bold cyan")
    head.append("   同时忙 ", style="grey58")
    head.append(fmt_duration(report.busy), style="white")
    head.append("   串行需 ", style="grey58")
    head.append(fmt_duration(report.serial_total), style="white")
    if report.savings > 0.05:
        head.append(f"   → 并行省下 {report.savings:.1f}s", style="bold green")
    lines.append(head)

    blocked_total = sum(l.length for l in report.lanes if l.blocked)
    if blocked_total > 0.05:
        lines.append(Text(f"（其中 {fmt_duration(blocked_total)} 是在等人/等结果，"
                          f"不计入并行收益）", style="grey50"))

    crit = " → ".join(report.critical_path) if report.critical_path else "—"
    lines.append(Text(f"关键路径  {crit}", style="grey58"))

    bar_w = max(20, min(width - 34, 70))
    for lane in report.lanes:
        row = Text()
        label = _clip(lane.label, 22)
        row.append(f"{label:<22} ", style="bold")
        start_frac = (lane.start - report.t0) / span
        end_frac = (lane.end - report.t0) / span
        left = int(round(start_frac * bar_w))
        length = max(1, int(round((end_frac - start_frac) * bar_w)))
        row.append(" " * left)
        if lane.blocked:
            row.append("░" * length, style="blue")
            row.append("  停住不动", style="blue")
        else:
            row.append("█" * length, style=color_for(lane.status))
            row.append(f"  {fmt_duration(lane.end - lane.start)}", style="grey62")
        lines.append(row)
        # 工具段：在泳道下面用符号标出来
        for s, e, name, status in sorted(lane.segments):
            sub = Text()
            sub.append(" " * 23)
            l2 = int(round((s - report.t0) / span * bar_w))
            n2 = max(1, int(round((e - s) / span * bar_w)))
            sub.append(" " * l2)
            sub.append("·" * n2, style=color_for(status))
            sub.append(f"  {_clip(name, 16)} {fmt_duration(e - s)}", style="grey50")
            lines.append(sub)
    return lines


# =============================================================================
# 任务生命周期
# =============================================================================
def lifecycle_lines(store: TraceStore, table: Any, *, width: int = 100,
                    limit: int = 20) -> list[Text]:
    """每条任务一行：来龙去脉 + 状态迁移 + 人工介入次数。"""
    if table is None or len(table) == 0:
        return [Text("（还没有任务）", style="grey50")]
    out: list[Text] = []
    for task in table.all()[-limit:]:
        t = Text()
        t.append(f"{task.name:<22}", style="bold")
        if task.status in ("SUCCEEDED",):
            t.append("✓ ", style="green")
        elif task.status in ("FAILED",):
            t.append("✕ ", style="red")
        elif task.status in ("NEEDS_HUMAN",):
            t.append("✋ ", style="bold yellow")
        elif task.status in ("WAITING",):
            t.append("◔ ", style="blue")
        elif task.status in ("SUSPECT", "STALLED"):
            t.append("⚠ ", style="yellow")
        else:
            t.append("● ", style="cyan")
        t.append(f"{task.status:<13}", style="grey70")
        chain = "→".join(task.trace_chain[-3:])
        t.append(f"{chain:<20}", style="grey50")
        t.append(f"{task.event_id:<9}", style="grey58")
        if task.topic:
            t.append(f"#{task.topic}  ", style="magenta")
        if task.human_touches:
            t.append(f"人出手{task.human_touches}次 ", style="yellow")
        if task.system_touches:
            t.append(f"系统自愈{task.system_touches}次 ", style="blue")
        if task.repeat_streak > 2:
            t.append(f"重复{task.repeat_streak}次 ", style="yellow")
        t.append(_clip(task.prompt, max(10, width - 96)), style="grey62")
        out.append(t)
    return out


# =============================================================================
# SpanTreeView —— 增量渲染到 Textual Tree
# =============================================================================
class SpanTreeView:
    """把 TraceStore 增量同步到 Textual ``Tree``。

    这个类踩过的坑都写在方法注释里（Textual 8 的缓存语义比较特殊），
    核心原则只有一条：**没变的东西一个 cell 都不碰**。
    """

    def __init__(self, tree: Any, store: TraceStore, *, show_root: bool = True) -> None:
        self.tree = tree
        self.store = store
        self.show_root = show_root
        self._nodes: dict[str, Any] = {}
        self._ui_keys: dict[str, tuple] = {}
        self._known_children: dict[str, tuple] = {}
        self._mounted: set[str] = set()
        self._structure_dirty = False
        self.last_render_ms = 0.0
        self.render_count = 0
        self.skip_count = 0

    def attach(self) -> None:
        try:
            self.tree.guide_depth = 2
        except Exception:  # pragma: no cover
            pass
        self.tree.root.expand()

    # ---- 同步 -------------------------------------------------------------
    def sync(self, *, force: bool = False, restyle: bool = False) -> int:
        t0 = time.perf_counter()
        changed = 0
        for root_span in self.store.roots():
            changed += self._sync_span(root_span, self.tree.root, force=force, restyle=restyle)
        self.last_render_ms = (time.perf_counter() - t0) * 1000
        changed += self._mount_missing()
        if self._structure_dirty:
            self._structure_dirty = False
            self._reload()
        elif changed:
            self._invalidate_widget_render(self.tree)
            self.tree.refresh()
        return changed

    def _reload(self) -> None:
        """结构变化后重建行列表。

        Textual 8 的 Tree 在 ``add()`` 之后**不会**让 ``_tree_lines_cached`` 失效，
        兄弟控件（RichLog）写入也不会触发兄弟重新布局 → 会让新节点不出现、
        被删节点的行残留。所以这里显式失效 + 重建 + 清行缓存 + 重新布局。
        """
        tree = self.tree
        tree._tree_lines_cached = None
        rebuild = getattr(tree, "_build", None)
        if callable(rebuild):
            try:
                rebuild()
            except Exception:  # pragma: no cover
                pass
        clear_cache = getattr(tree, "_clear_line_cache", None)
        if callable(clear_cache):
            try:
                clear_cache()
            except Exception:  # pragma: no cover
                pass
        try:
            tree.refresh(layout=True)
        except Exception:  # pragma: no cover
            pass
        self._invalidate_widget_render(tree)

    def _mount_missing(self) -> int:
        """兜底：把 store 里还没上树的 Span 按父子关系补挂。只在正常同步之后调用。"""
        mounted = 0
        progress = True
        while progress:
            progress = False
            for span_id in list(getattr(self.store, "_spans", {})):
                if span_id in self._mounted:
                    continue
                span = self.store.get(span_id)
                if span is None:
                    continue
                parent = self._nodes.get(span.parent_id) if span.parent_id else self.tree.root
                if parent is None:
                    continue
                self._sync_span(span, parent, force=False, restyle=False)
                mounted += 1
                progress = True
        return mounted

    @staticmethod
    def _invalidate_widget_render(widget: Any) -> None:
        compositor = getattr(getattr(widget, "screen", None), "_compositor", None)
        if compositor is None:
            return
        for attr in ("_renders", "_map"):
            cache = getattr(compositor, attr, None)
            if isinstance(cache, dict):
                cache.pop(widget, None)

    def _sync_span(self, span: Span, parent_node: Any, *, force: bool, restyle: bool) -> int:
        key = self._ui_key(span)
        node = self._nodes.get(span.span_id)
        changed = 0

        if node is None:
            node = parent_node.add(span_label(span), data=span.span_id, expand=True)
            node.expand()
            lookup = getattr(self.tree, "span_lookup", None)
            if lookup is not None:
                lookup[str(node.id)] = span
            self._nodes[span.span_id] = node
            self._mounted.add(span.span_id)
            self._ui_keys[span.span_id] = key
            self._structure_dirty = True
            self.render_count += 1
            changed += 1
            if not self.tree.root.is_expanded:
                self.tree.root.expand()
        else:
            lookup = getattr(self.tree, "span_lookup", None)
            if lookup is not None:
                lookup[str(node.id)] = span
            prev = self._ui_keys.get(span.span_id)
            if force or prev != key:
                node.set_label(span_label(span))
                self.render_count += 1
                changed += 1
            else:
                self.skip_count += 1
                if restyle and span.ended_at is None:
                    try:
                        node.refresh()
                    except Exception:  # pragma: no cover
                        pass
            self._ui_keys[span.span_id] = key

        # ⚠ 子节点**无条件**递归。早期版本只访问"尚未挂载"的子节点，
        # 结果已挂载的 Worker 状态永远停在 ●；返回值也必须累加，
        # 否则"新增子节点"不会被 sync() 看见，结构永远不会重建。
        current = tuple(span.child_ids)
        for child_id in current:
            child_span = self.store.get(child_id)
            if child_span is None:
                continue
            changed += self._sync_span(child_span, node, force=force, restyle=restyle)
        self._known_children[span.span_id] = current
        return changed

    def _ui_key(self, span: Span) -> tuple:
        """指纹必须包含 child_ids：父节点自己没变，但"多了子节点"同样要重建。"""
        return (
            span.name, span.status, span.warning_count,
            span.ended_at is not None, tuple(span.child_ids), self._node_expanded(span),
        )

    def _node_expanded(self, span: Span) -> bool:
        node = self._nodes.get(span.span_id)
        if node is None:
            return span.expanded
        span.expanded = node.is_expanded
        return span.expanded

    def select(self, span_id: str | None) -> bool:
        if not span_id or span_id not in self._nodes:
            return False
        try:
            self.tree.select_node(self._nodes[span_id])
            return True
        except Exception:  # pragma: no cover
            return False

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    def is_visible(self, span_id: str) -> bool:
        node = self._nodes.get(span_id)
        if node is None:
            return False
        parent = node.parent
        while parent is not None and not parent.is_root:
            if not parent.is_expanded:
                return False
            parent = parent.parent
        return True

    def labels(self) -> dict[str, str]:
        out: dict[str, str] = {}
        for sid, node in self._nodes.items():
            label = node.label
            out[sid] = label.plain if hasattr(label, "plain") else str(label)
        return out

    def expanded_state(self) -> dict[str, bool]:
        return {sid: node.is_expanded for sid, node in self._nodes.items()}


# =============================================================================
# TaskTableModel —— 增量渲染到 DataTable
# =============================================================================
class TaskTableModel:
    """行 key = task_id（稳定 key 是增量更新的前提），只 update_cell。"""

    COLUMNS: tuple[tuple[str, str], ...] = (
        ("task", "task"), ("status", "状态"), ("chain", "来源链"),
        ("todo", "在等/次数"), ("elapsed", "耗时"),
    )

    def __init__(self, table: Any, data_table: Any, *, max_rows: int = 200) -> None:
        self.table = table
        self.data_table = data_table
        self.max_rows = max_rows
        self._fingerprint: tuple | None = None
        self._rendered: dict[str, tuple] = {}
        self.cell_writes = 0
        self.row_adds = 0
        self.skips = 0

    def attach(self) -> None:
        if not getattr(self.data_table, "columns", None):
            for key, label in self.COLUMNS:
                self.data_table.add_column(label, key=key)

    def sync(self, *, force: bool = False) -> int:
        fp = self.table.fingerprint()
        if not force and fp == self._fingerprint:
            self.skips += 1
            return 0
        self._fingerprint = fp
        written = 0
        tasks = self.table.all()
        if len(tasks) > self.max_rows:
            active = [t for t in tasks if t.is_active]
            done = [t for t in tasks if not t.is_active]
            tasks = sorted(active + done[-(self.max_rows - len(active)):], key=lambda t: t.created_at)
        keep = {t.task_id for t in tasks}

        for task_id in list(self._rendered):
            if task_id not in keep:
                try:
                    self.data_table.remove_row(task_id)
                except Exception:  # pragma: no cover
                    pass
                self._rendered.pop(task_id, None)

        for task in tasks:
            cells = self._cells(task)
            prev = self._rendered.get(task.task_id)
            if prev is None:
                self.data_table.add_row(*cells, key=task.task_id)
                self._rendered[task.task_id] = cells
                self.row_adds += 1
                self.cell_writes += len(cells)
                written += len(cells)
                continue
            for (col_key, _), new, old in zip(self.COLUMNS, cells, prev):
                if new != old:
                    self.data_table.update_cell(task.task_id, col_key, new, update_width=False)
                    self.cell_writes += 1
                    written += 1
            self._rendered[task.task_id] = cells
        return written

    def _cells(self, task: Any) -> tuple[Any, ...]:
        style = {
            "PENDING": "grey58", "RUNNING": "cyan", "WAITING": "blue",
            "NEEDS_HUMAN": "bold yellow", "CANCELLING": "magenta",
            "SUCCEEDED": "green", "FAILED": "red", "CANCELLED": "magenta",
            "SUSPECT": "yellow", "STALLED": "yellow",
        }.get(task.status, "white")
        icon = {
            "PENDING": "○", "RUNNING": "●", "WAITING": "◔", "NEEDS_HUMAN": "✋",
            "CANCELLING": "x", "SUCCEEDED": "✓", "FAILED": "✕", "CANCELLED": "x",
            "SUSPECT": "⚠", "STALLED": "⚠",
        }.get(task.status, "?")
        from state import STATUS_LABEL

        status = Text(f"{icon} {STATUS_LABEL.get(task.status, task.status)}", style=style)
        chain = Text("→".join(task.trace_chain[-2:]), style="grey58")
        todo = ""
        if task.status == "NEEDS_HUMAN":
            todo = f"等批:{_clip(task.needs_human_reason, 14)}"
        elif task.status == "WAITING":
            todo = f"等:{_clip(task.waiting_on, 14)}"
        elif task.human_touches:
            todo = f"人出手{task.human_touches}次"
        elif task.system_touches:
            todo = f"自愈{task.system_touches}次"
        elif task.repeat_streak > 2:
            todo = f"重复{task.repeat_streak}次"
        todo_cell = Text(str(todo), style="yellow" if todo else "grey50")
        elapsed = Text(fmt_duration(task.duration if task.started_at else None), style="grey70")
        return (Text(task.name, style="bold"), status, chain, todo_cell, elapsed)

    def row_keys(self) -> list[str]:
        return list(self._rendered)


# =============================================================================
# 便捷
# =============================================================================
def build_tree(store: TraceStore) -> dict[str, Any]:
    def node(span: Span) -> dict[str, Any]:
        return {
            "span_id": span.span_id, "kind": span.kind, "name": span.name,
            "status": span.status, "duration": span.duration,
            "warnings": span.detail.get("warnings", []),
            "children": [node(c) for c in store.children_of(span.span_id)],
        }

    return {"roots": [node(r) for r in store.roots()]}


def find_span_by_name(store: TraceStore, name: str) -> Span | None:
    for span in store.walk(None):
        if span.name == name:
            return span
    return None


def count_spans(store: TraceStore, kind: str | None = None, status: str | None = None) -> int:
    n = 0
    for span in store.walk(None):
        if kind is not None and span.kind != kind:
            continue
        if status is not None and span.status != status:
            continue
        n += 1
    return n


__all__ = [
    "Span", "TraceStore", "TraceEvent", "SpanTreeView", "TaskTableModel",
    "TimelineReport", "Lane", "build_timeline", "timeline_lines", "lifecycle_lines",
    "KIND_EVENT", "KIND_ROUTER", "KIND_WORKER", "KIND_TOOL", "KIND_HUMAN",
    "ST_RUNNING", "ST_OK", "ST_WARN", "ST_ERROR", "ST_CANCELLED", "ST_WAITING", "ST_QUEUED",
    "ROUTE_DIRECT", "ROUTE_RECALL", "ROUTE_ROUTER", "ROUTE_GATE", "ROUTE_IGNORE", "ROUTE_LABEL",
    "icon_for", "color_for", "fmt_duration", "span_label", "detail_lines",
    "span_display_text", "build_tree", "find_span_by_name", "count_spans", "short_id",
    "render_live_duration", "DURATION_PLACEHOLDER",
]
