"""state.py —— 任务状态外置：TaskBrief / TaskTable。

Super Agent 的出发点是「尽可能少占用人的注意力」。要少占注意力，前提是
**任务本身能脱离某一次对话活下来**：人今天问一句、明天补一句、隔一会儿问
「刚才那个怎样了」，系统都得找得到同一个任务，而不是重新理解一遍。

所以这个文件只做一件事：把任务放进一张表，并让表本身承载

* 状态（PENDING / RUNNING / WAITING / NEEDS_HUMAN / SUCCEEDED / FAILED / CANCELLED / SUSPECT）
* **订阅关系**（topic）：后续同类事件可以直接投递给「已经在管这件事的那个 Worker」，
  不必再让 Router 重新拆一遍单；
* **上一跳**（origin / trace_chain）：从一条新消息顺着链找回旧任务；
* **进展信号**（progress）：Watchdog 判断「在动」还是「空转」的依据；
* **人工介入**记录：挂起原因、谁批的、批了什么。

表不挂在 Router 的对话历史里，也不属于任何 Worker 实例。
Worker 只是临时执行体，跑完就丢；表才是任务的家。
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any, Callable, Iterable

# =============================================================================
# 状态
# =============================================================================
PENDING = "PENDING"
RUNNING = "RUNNING"
WAITING = "WAITING"            # 在等外部结果（异步工具的 start() 已发出，还没 stop()）
NEEDS_HUMAN = "NEEDS_HUMAN"    # 关键动作前挂起，等人批准
CANCELLING = "CANCELLING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
CANCELLED = "CANCELLED"
SUSPECT = "SUSPECT"            # Watchdog：长时间没动静
STALLED = "STALLED"            # Watchdog：一直在动，但没有实质进展

ACTIVE_STATUSES: frozenset[str] = frozenset({
    PENDING, RUNNING, WAITING, NEEDS_HUMAN, CANCELLING, SUSPECT, STALLED,
})
TERMINAL_STATUSES: frozenset[str] = frozenset({SUCCEEDED, FAILED, CANCELLED})

ALL_STATUSES: tuple[str, ...] = (
    PENDING, RUNNING, WAITING, NEEDS_HUMAN, CANCELLING,
    SUCCEEDED, FAILED, CANCELLED, SUSPECT, STALLED,
)

#: 给人看的短标签（Console / README 共用，避免两处写法漂移）
STATUS_LABEL: dict[str, str] = {
    PENDING: "排队",
    RUNNING: "执行中",
    WAITING: "等结果",
    NEEDS_HUMAN: "等人批",
    CANCELLING: "取消中",
    SUCCEEDED: "完成",
    FAILED: "失败",
    CANCELLED: "已取消",
    SUSPECT: "疑似卡死",
    STALLED: "无进展",
}


def new_task_id() -> str:
    return uuid.uuid4().hex[:6]


# =============================================================================
# 人类干预记录：每一次"人插手"都要留痕，否则没法判断到底省了多少注意力
# =============================================================================
@dataclass
class Intervention:
    """人在任务生命周期里做的一次动作。"""

    kind: str
    """``approve`` / ``reject`` / ``steer`` / ``answer`` / ``watchdog`` / ``resume``"""

    at: float
    note: str = ""
    actor: str = "human"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# =============================================================================
# TaskBrief
# =============================================================================
@dataclass
class TaskBrief:
    task_id: str = ""
    worker_type: str = "researcher"
    prompt: str = ""

    status: str = PENDING
    result: str | None = None

    # ---- 时间 -------------------------------------------------------------
    created_at: float = field(default_factory=time.monotonic)
    started_at: float | None = None
    last_activity: float | None = None
    ended_at: float | None = None

    # ---- 归属：任务属于事件/主题，不属于某次对话 --------------------------
    event_id: str = ""
    """最初派生出这条任务的事件。"""

    topic: str = ""
    """订阅键。后续同 topic 的事件可以直接投递给这条任务，不再经过 Router 拆单。"""

    origin_task_id: str | None = None
    """新任务是从哪条旧任务派生的（链式召回的上一跳）。"""

    root_task_id: str = ""
    """整条任务链的起点。用它可以一句话回答「这件事最早从哪来」。"""

    trace_chain: list[str] = field(default_factory=list)
    """从根到本任务经过的任务 id 列表。"""

    # ---- 进展信号（Watchdog 用） -----------------------------------------
    tokens_in: int = 0
    tokens_out: int = 0
    tool_calls: int = 0
    last_tool: str = ""
    repeat_streak: int = 0
    """连续调用同一个工具的次数；空转的主要信号。"""

    progress_marks: list[float] = field(default_factory=list)
    """每次「有实质进展」的时间戳（换了工具、结果变长、模型给出新文本）。"""

    # ---- 人工介入 ---------------------------------------------------------
    waiting_on: str = ""
    """WAITING 时在等什么（例如「政策接口返回」）。"""

    wait_seconds: float = 0.0
    """等多久之后自动恢复执行（Demo 里用来模拟外部结果就绪）。"""

    needs_human_reason: str = ""
    interventions: list[dict[str, Any]] = field(default_factory=list)

    # ---- trace / 恢复 -----------------------------------------------------
    span_id: str = ""
    owner_node: str = ""
    attempt: int = 1
    suspect_count: int = 0
    warning_count: int = 0
    error: str | None = None

    def __post_init__(self) -> None:
        if not self.task_id:
            self.task_id = new_task_id()
        if not self.root_task_id:
            self.root_task_id = self.task_id
        if not self.trace_chain:
            self.trace_chain = [self.task_id]

    # ---- 便捷属性 ---------------------------------------------------------
    @property
    def name(self) -> str:
        return f"{self.worker_type}-{self.task_id}"

    @property
    def is_active(self) -> bool:
        return self.status in ACTIVE_STATUSES

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @property
    def is_blocked(self) -> bool:
        """在等人（批准或外部结果）。这类任务不占执行体，也不该被 Watchdog 催。"""
        return self.status in (WAITING, NEEDS_HUMAN)

    @property
    def duration(self) -> float:
        if self.started_at is None:
            return 0.0
        end = self.ended_at if self.ended_at is not None else time.monotonic()
        return max(0.0, end - self.started_at)

    @property
    def idle_seconds(self) -> float:
        ref = self.last_activity or self.started_at or self.created_at
        return max(0.0, time.monotonic() - ref)

    @property
    def human_touches(self) -> int:
        """**人**出手的次数（追加要求 / 批准 / 驳回）。系统自己的动作不算。"""
        return sum(1 for i in self.interventions if i.get("actor") == "human")

    @property
    def system_touches(self) -> int:
        """系统替人做的动作（Watchdog 注入、外部结果就绪后自动恢复）。"""
        return sum(1 for i in self.interventions
                   if i.get("actor") in ("system", "watchdog"))

    def add_intervention(self, kind: str, note: str = "", actor: str = "human") -> None:
        self.interventions.append(Intervention(kind, time.monotonic(), note, actor).to_dict())

    def mark_progress(self, kind: str = "") -> None:
        now = time.monotonic()
        self.progress_marks.append(now)
        if len(self.progress_marks) > 200:
            self.progress_marks = self.progress_marks[-100:]
        self.last_activity = now
        if kind:
            self.last_tool = kind

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # 前端和日志要的是"这条任务叫什么"。name 是属性不是字段，
        # asdict 不会带上，之前 Web 表格第一列是空的就因为这个。
        d["name"] = self.name
        d["duration"] = round(self.duration, 3)
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TaskBrief":
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})


# =============================================================================
# TaskTable
# =============================================================================
class TaskTable:
    """任务表。全部读写都经过它，UI 只需要比较 ``fingerprint()``。"""

    def __init__(self, jsonl_path: str | os.PathLike[str] | None = None) -> None:
        self._tasks: dict[str, TaskBrief] = {}
        self._subs: dict[str, str] = {}          # topic -> task_id
        self._version = 0
        self._lock = asyncio.Lock()
        self._jsonl_path: Path | None = Path(jsonl_path) if jsonl_path else None
        if self._jsonl_path is not None:
            self._jsonl_path.parent.mkdir(parents=True, exist_ok=True)

    # ---- 基本访问 ---------------------------------------------------------
    def __len__(self) -> int:
        return len(self._tasks)

    def __contains__(self, task_id: object) -> bool:
        return task_id in self._tasks

    def __iter__(self):
        return iter(self._tasks.values())

    def __getitem__(self, task_id: str) -> TaskBrief:
        return self._tasks[task_id]

    def get(self, task_id: str) -> TaskBrief | None:
        return self._tasks.get(task_id)

    def all(self) -> list[TaskBrief]:
        return sorted(self._tasks.values(), key=lambda t: t.created_at)

    @property
    def version(self) -> int:
        return self._version

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    # ---- 写入 -------------------------------------------------------------
    def put(self, brief: TaskBrief) -> TaskBrief:
        self._tasks[brief.task_id] = brief
        self._touch("put", brief)
        return brief

    def add_pending(
        self,
        worker_type: str,
        prompt: str,
        *,
        event_id: str = "",
        topic: str = "",
        origin_task_id: str | None = None,
        owner_node: str = "",
    ) -> TaskBrief:
        origin = self._tasks.get(origin_task_id) if origin_task_id else None
        task = TaskBrief(
            worker_type=worker_type,
            prompt=prompt,
            event_id=event_id,
            topic=topic,
            origin_task_id=origin_task_id,
            owner_node=owner_node,
            root_task_id=origin.root_task_id if origin else "",
            trace_chain=(origin.trace_chain + [""]) if origin else [],
        )
        # trace_chain 的最后一个元素要等 task_id 生成后再补上
        task.trace_chain = (origin.trace_chain + [task.task_id]) if origin else [task.task_id]
        if topic:
            self._subs[topic] = task.task_id
        return self.put(task)

    def update(self, task_id: str, **changes: Any) -> TaskBrief:
        task = self._tasks[task_id]
        for key, value in changes.items():
            if not hasattr(task, key):
                raise AttributeError(f"TaskBrief 没有字段 {key!r}")
            setattr(task, key, value)
        self._touch("update", task)
        return task

    def mark_progress(self, task_id: str, *, tool: str = "") -> None:
        task = self._tasks[task_id]
        if tool:
            if task.last_tool == tool:
                task.repeat_streak += 1
            else:
                task.repeat_streak = 1
            task.tool_calls += 1
        task.mark_progress(tool)
        self._version += 1

    def bump_tokens(self, task_id: str, *, tokens_in: int = 0, tokens_out: int = 0) -> None:
        task = self._tasks[task_id]
        task.tokens_in += tokens_in
        task.tokens_out += tokens_out
        self._version += 1

    def cancel(self, task_id: str) -> bool:
        task = self._tasks.get(task_id)
        if task is None or task.is_terminal:
            return False
        task.status = CANCELLING
        self._touch("cancel", task)
        return True

    def cancel_all_active(self) -> int:
        n = 0
        for task in self._tasks.values():
            if task.is_active and task.status != CANCELLING:
                task.status = CANCELLING
                n += 1
        if n:
            self._version += 1
        return n

    def is_cancel_requested(self, task_id: str) -> bool:
        task = self._tasks.get(task_id)
        return task is not None and task.status == CANCELLING

    # ---- 订阅直通 ---------------------------------------------------------
    def subscribe(self, topic: str, task_id: str) -> None:
        if topic:
            self._subs[topic] = task_id
            self._version += 1

    def subscriber(self, topic: str) -> TaskBrief | None:
        """topic 上的订阅者；订阅者已结束也算「有人认领」，由调用方决定怎么续。"""
        task_id = self._subs.get(topic)
        return self._tasks.get(task_id) if task_id else None

    def subscriptions(self) -> dict[str, str]:
        return dict(self._subs)

    def drop_subscription(self, topic: str) -> None:
        if topic in self._subs:
            del self._subs[topic]
            self._version += 1

    # ---- 链式召回 ---------------------------------------------------------
    def ancestors(self, task_id: str) -> list[TaskBrief]:
        task = self._tasks.get(task_id)
        if task is None:
            return []
        out = []
        for tid in task.trace_chain[:-1]:
            anc = self._tasks.get(tid)
            if anc is not None:
                out.append(anc)
        return out

    def latest_by_topic(self, topic: str) -> TaskBrief | None:
        candidates = [t for t in self._tasks.values() if t.topic == topic]
        return max(candidates, key=lambda t: t.created_at) if candidates else None

    def latest_finished(self, limit: int = 1) -> list[TaskBrief]:
        done = [t for t in self._tasks.values() if t.is_terminal]
        return sorted(done, key=lambda t: t.ended_at or 0, reverse=True)[:limit]

    # ---- 查询 -------------------------------------------------------------
    def active(self) -> list[TaskBrief]:
        return [t for t in self.all() if t.is_active]

    def blocked(self) -> list[TaskBrief]:
        return [t for t in self.all() if t.is_blocked]

    def by_status(self, status: str) -> list[TaskBrief]:
        return [t for t in self.all() if t.status == status]

    def by_event(self, event_id: str) -> list[TaskBrief]:
        return [t for t in self.all() if t.event_id == event_id]

    def status_counts(self) -> dict[str, int]:
        counts = {s: 0 for s in ALL_STATUSES}
        for task in self._tasks.values():
            counts[task.status] = counts.get(task.status, 0) + 1
        return counts

    def human_attention(self) -> dict[str, int]:
        """注意力账单：系统接了多少事、真正需要人出手几次、系统自己救了几次。"""
        return {
            "tasks": len(self._tasks),
            "needs_human_now": sum(1 for t in self._tasks.values() if t.status == NEEDS_HUMAN),
            "human_touches": sum(t.human_touches for t in self._tasks.values()),
            "system_touches": sum(t.system_touches for t in self._tasks.values()),
        }

    def orphaned(self, node_id: str) -> list[TaskBrief]:
        return [
            t for t in self._tasks.values()
            if t.status == RUNNING and t.owner_node != node_id
        ]

    # ---- 指纹 -------------------------------------------------------------
    def fingerprint(self) -> tuple:
        per_task = tuple(
            (t.task_id, t.status, t.suspect_count, t.warning_count, t.human_touches,
             len(t.progress_marks))
            for t in sorted(self._tasks.values(), key=lambda x: x.created_at)
        )
        return (self._version, len(self._subs), per_task)

    # ---- 恢复 -------------------------------------------------------------
    def requeue_orphans(self, node_id: str) -> list[TaskBrief]:
        requeued = []
        for task in self.orphaned(node_id):
            task.status = PENDING
            task.owner_node = ""
            task.attempt += 1
            task.result = None
            task.error = None
            task.started_at = None
            task.last_activity = None
            requeued.append(task)
        if requeued:
            self._version += 1
        return requeued

    # ---- 持久化 -----------------------------------------------------------
    def save_jsonl(self, path: str | os.PathLike[str] | None = None) -> Path:
        target = Path(path) if path else self._jsonl_path
        if target is None:
            raise ValueError("没有配置 jsonl_path")
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(target.parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                for task in self.all():
                    fh.write(json.dumps(task.to_dict(), ensure_ascii=False) + "\n")
            os.replace(tmp, target)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
        return target

    @classmethod
    def load_jsonl(
        cls,
        path: str | os.PathLike[str],
        *,
        warn: Callable[[str], None] | None = None,
    ) -> "TaskTable":
        table = cls(jsonl_path=path)
        p = Path(path)
        if not p.exists():
            return table
        with p.open("r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    payload = row.get("task") if isinstance(row, dict) and "task" in row else row
                    if not isinstance(payload, dict) or "task_id" not in payload:
                        raise TypeError("缺少 task_id")
                    brief = TaskBrief.from_dict(payload)
                    table._tasks[brief.task_id] = brief
                    if brief.topic:
                        table._subs.setdefault(brief.topic, brief.task_id)
                    table._version += 1
                except (json.JSONDecodeError, TypeError, ValueError) as exc:
                    if warn:
                        warn(f"跳过损坏的 JSONL 行 {lineno}: {exc}")
        return table

    def _touch(self, op: str, brief: TaskBrief) -> None:
        self._version += 1
        if self._jsonl_path is not None:
            try:
                with self._jsonl_path.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps({"op": op, "task": brief.to_dict()}, ensure_ascii=False) + "\n")
            except OSError:
                pass

    def set_jsonl(self, path: str | os.PathLike[str] | None) -> None:
        self._jsonl_path = Path(path) if path else None
        if self._jsonl_path is not None:
            self._jsonl_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def jsonl_path(self) -> Path | None:
        return self._jsonl_path

    def snapshot(self) -> list[dict[str, Any]]:
        return [t.to_dict() for t in self.all()]

    @classmethod
    def from_snapshot(cls, rows: Iterable[dict[str, Any]]) -> "TaskTable":
        table = cls()
        for row in rows:
            brief = TaskBrief.from_dict(row)
            table._tasks[brief.task_id] = brief
        return table
