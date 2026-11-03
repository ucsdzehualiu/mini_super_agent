"""runtime.py —— Router / WorkerPool / Watchdog / 事件分发。

这个文件对应的是 Super Agent 里「系统接管会话管理」的那部分。下面每个机制都能在
Console 上看到它实际发生了什么，而不是只存在于注释里：

| 机制 | 做法 | 在 Console 上看到 |
| --- | --- | --- |
| 双层过滤 | 程序层确定性条件 + 语义层判定 | 事件行上的 `program` / `semantic` |
| Router / Worker 分层 | Router 无工具，只产出决定；Worker 隔离上下文执行 | Trace 树层级 |
| **订阅直通** | 任务表记 `topic → task`；同 topic 后续事件跳过 Router 拆单 | 事件行 `直通 T-xxx`、Router 决定数不增长 |
| **链式召回** | 任务内联 `origin / trace_chain`；「刚才那个」顺着链找回 | 事件行 `召回 T-xxx → T-yyy` |
| **异步全双工** | 工具 `start()` 拿句柄、`stop()` 收结果；等待时任务状态是 WAITING | 任务行「等结果」，期间仍可接新事件 |
| **人工介入** | 不可逆动作前挂起 NEEDS_HUMAN，控制台批准/驳回 | 任务行「等人批」，介入次数 +1 |
| **Watchdog** | 心跳 + 实质进展（重复调用 / token 不涨）+ 注入提示 | WARN / 注入记录，不硬 cancel |

不接外部渠道：事件全部来自控制台输入，所以每个机制都能被手动触发、肉眼验证。
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import os
import random
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Iterable

from agentscope.agent import Agent, ModelConfig, ReActConfig
from agentscope.event import ModelCallEndEvent, ToolCallStartEvent
from agentscope.formatter import DeepSeekChatFormatter
from agentscope.message import Msg, TextBlock, ToolCallBlock, UserMsg
from agentscope.model import ChatModelBase, ChatResponse, ChatUsage, FinishedReason
from agentscope.permission import PermissionBehavior, PermissionDecision
from agentscope.tool import FunctionTool, Toolkit, ToolChunk

import prompts
from state import (
    CANCELLED, CANCELLING, FAILED, NEEDS_HUMAN, PENDING, RUNNING, STALLED,
    SUCCEEDED, SUSPECT, WAITING, TaskBrief, TaskTable,
)
from trace import (
    KIND_EVENT, KIND_HUMAN, KIND_ROUTER, KIND_TOOL, KIND_WORKER,
    ROUTE_ANSWER, ROUTE_DIRECT, ROUTE_GATE, ROUTE_IGNORE, ROUTE_RECALL, ROUTE_ROUTER,
    ST_CANCELLED, ST_ERROR, ST_OK, ST_QUEUED, ST_RUNNING, ST_WAITING, ST_WARN,
    Span, TraceEvent, TraceStore, short_id,
)

AUTO_ALLOW = PermissionDecision(behavior=PermissionBehavior.ALLOW, message="本地桩工具")

#: 故障注入 / 演示标记
_FAIL_MARKER = "<<FAIL>>"
_EMPTY_MARKER = "<<EMPTY_TOOL>>"
_SLOW_MARKER = "<<SLOW>>"
_LOOP_MARKER = "<<LOOP>>"        # 永远调同一个工具，用来演示无进展
_BLOCKED_MARKER = "<<BLOCKED>>"  # 工具返回「结果未就绪」，任务进入 WAITING

CURRENT_CONTEXT: contextvars.ContextVar["WorkerContext | None"] = contextvars.ContextVar(
    "msa_worker_context", default=None
)


# =============================================================================
# 并发观测
# =============================================================================
class ConcurrencyMeter:
    def __init__(self) -> None:
        self.current = 0
        self.peak = 0
        self.total = 0

    def enter(self) -> int:
        self.current += 1
        self.total += 1
        self.peak = max(self.peak, self.current)
        return self.current

    def exit(self) -> None:
        self.current = max(0, self.current - 1)

    def reset(self) -> None:
        self.current = self.peak = self.total = 0

    def snapshot(self) -> dict[str, int]:
        return {"current": self.current, "peak": self.peak, "total": self.total}


class _Metered:
    def __init__(self, meter: ConcurrencyMeter) -> None:
        self.meter = meter

    async def __aenter__(self) -> ConcurrencyMeter:
        self.meter.enter()
        return self.meter

    async def __aexit__(self, *exc: Any) -> None:
        self.meter.exit()


def meter_context(meter: ConcurrencyMeter) -> _Metered:
    return _Metered(meter)


# =============================================================================
# 模型层
# =============================================================================
def _text(s: str) -> TextBlock:
    return TextBlock(type="text", text=s)


def _tool_call(call_id: str, name: str, args: dict[str, Any]) -> ToolCallBlock:
    return ToolCallBlock(type="tool_call", id=call_id, name=name,
                         input=json.dumps(args, ensure_ascii=False))


def _user_text(messages: list[Msg]) -> str:
    for msg in reversed(messages):
        if msg.role == "user":
            return msg.get_text_content() or ""
    return ""


def _system_text(messages: list[Msg]) -> str:
    return "\n".join(m.get_text_content() or "" for m in messages if m.role == "system")


def _tools_called(messages: list[Msg]) -> list[str]:
    names: list[str] = []
    for msg in reversed(messages):
        if msg.role == "user":
            break
        for block in msg.get_content_blocks("tool_call"):
            names.append(block.name)
    return names


def _search_url(query: str) -> str:
    """把关键词拼成一个搜索结果页 URL，交给 webfetch 抓。"""
    import urllib.parse
    return "https://cn.bing.com/search?q=" + urllib.parse.quote(query)


def _strip_prompt_prefix(text: str) -> str:
    """任务 prompt 形如「查证并对比…要点：<用户原话>」，取回用户原话。"""
    for sep in ("：", ":"):
        if sep in text:
            head, _, tail = text.partition(sep)
            if len(head) > 6 and tail.strip():   # 前面是任务模板，不是用户原话
                return tail.strip()
    return text.strip()


def _tool_outputs(messages: list[Msg]) -> list[tuple[str, str]]:
    """把这一轮的工具返回捞出来，供假模型写"结论"用。

    只有把真实返回内容用上，用户问"深圳"时才看得到深圳，
    而不是把任务描述原样回述一遍。
    """
    out: list[tuple[str, str]] = []
    for msg in messages:
        for block in msg.get_content_blocks("tool_result"):
            payload = block.output
            if isinstance(payload, list):
                text = " ".join(getattr(b, "text", "") or "" for b in payload)
            else:
                text = str(payload)
            if text:
                out.append((block.name, text))
    return out


class ModelFault(Exception):
    """可重试的模型调用失败。"""


class FakeChatModel(ChatModelBase):
    """确定性的假模型。没有它，这个 Demo 就只能在有 API key 的机器上跑。

    它需要会三件事：
    1. Router：根据输入判断 direct / recall / spawn / ignore；
    2. Worker：决定调哪个工具、调几次、什么时候收尾；
    3. 故障注入：让 FAILED / 空结果 / 无进展这些分支可复现。
    """

    def __init__(
        self,
        name: str = "fake-scripted",
        *,
        latency: tuple[float, float] = (0.12, 0.30),
        meter: ConcurrencyMeter | None = None,
        seed: int = 11,
        fault_rate: float = 0.0,
    ) -> None:
        super().__init__(credential=None, model=name, parameters=None,  # type: ignore[arg-type]
                         stream=False, max_retries=2, retry_delay=0.05)
        self._prepare()
        self.latency = latency
        self.meter = meter or ConcurrencyMeter()
        self.rng = random.Random(seed)
        self.fault_rate = fault_rate

    @classmethod
    def _get_retryable_exceptions(cls) -> tuple[type[Exception], ...]:
        return (ModelFault,)

    def _prepare(self) -> None:
        self.formatter = DeepSeekChatFormatter()
        self.calls = 0
        self.failures_injected = 0

    # ---- 入口 -------------------------------------------------------------
    async def _call_api(self, model_name: str, messages: list[Msg],
                        tools: list[dict] | None = None, tool_choice: Any = None,
                        **kwargs: Any) -> ChatResponse:
        self.calls += 1
        user_text = _user_text(messages)
        sys_text = _system_text(messages)
        # 判定顺序很重要：「唯一看到的出口」也含 Router 字样，必须先判断合成
        is_synth = "唯一看到的出口" in sys_text
        is_explain = "决定**不为这条消息派任务**" in sys_text
        is_gate = ("语义层" in sys_text and "WORTH" in sys_text)
        is_router = ("你是常驻的 Router" in sys_text) and not is_synth and not is_explain

        if not is_router and not is_synth and not is_gate:
            if _FAIL_MARKER in user_text:
                self.failures_injected += 1
                raise ModelFault("injected model failure (<<FAIL>>)")
            if self.fault_rate and self.rng.random() < self.fault_rate:
                self.failures_injected += 1
                raise ModelFault("random model failure")

        lo, hi = self.latency
        delay = self.rng.uniform(lo, hi)
        if is_router and not is_synth:
            delay = min(delay, 0.2)
        async with meter_context(self.meter):
            await asyncio.sleep(delay)

        if is_synth:
            content: list[Any] = [_text(self._synthesize(user_text))]
        elif is_explain:
            content = [_text(self._explain(user_text))]
        elif is_gate:
            content = [_text("WORTH: 有明确工作诉求" if _worth(user_text) else "NOT_WORTH: 无需行动")]
        elif is_router:
            content = [_text(self._route(user_text))]
        else:
            content = self._worker_step(user_text, messages, tools)

        plain = "".join(getattr(b, "text", "") for b in content)
        return ChatResponse(
            content=content, is_last=True,
            usage=ChatUsage(input_tokens=max(1, len(user_text) // 2 + len(sys_text) // 6),
                            output_tokens=max(1, len(plain) // 2), time=delay),
            finished_reason=FinishedReason.COMPLETED,
        )

    # ---- Router：四选一 -----------------------------------------------
    def _route(self, user_text: str) -> str:
        """从 Router 的输入里读出决定。

        输入固定是 ``# 在管的事`` / ``# 已结束的事`` / ``# 新消息`` 三段。
        """
        new_msg = _section(user_text, "新消息")
        living = _section(user_text, "在管的事")
        done = _section(user_text, "已结束的事")

        # 1) 「刚才那个 / 前面那个 / 上面」= 召回已结束任务
        if _looks_like_recall(new_msg) and done.strip():
            first = done.strip().splitlines()[0]
            tid = _first_token(first)
            return json.dumps({"mode": "recall", "target": tid,
                               "why": "是已完成任务的延续", "tasks": []},
                              ensure_ascii=False)

        # 2) 主题与某个在管的任务吻合 = 直通
        target = _match_living(new_msg, living)
        if target:
            return json.dumps({"mode": "direct", "target": target,
                               "why": "同一件事已在处理中", "tasks": []},
                              ensure_ascii=False)

        if not _actionable(new_msg):
            return json.dumps({"mode": "ignore", "target": None,
                               "why": "不值得占用注意力", "tasks": []}, ensure_ascii=False)

        return json.dumps({"mode": "spawn", "target": None, "why": "新任务",
                           "tasks": self._plan_briefs(new_msg)}, ensure_ascii=False)

    def _plan_briefs(self, text: str) -> list[dict[str, str]]:
        text = text.strip()
        # 涉及不可逆动作的请求优先交给 planner：只有它挂着 ask_human 工具，
        # 能在真动手之前把决定权交回给人。
        wants_action = any(k in text for k in ("订", "购买", "下单", "支付", "发消息", "发邮件", "删除", "提交"))
        wants_plan = any(k in text for k in ("计划", "规划", "路线", "安排", "行程", "排版", "步骤", "日程"))
        wants_write = any(k in text for k in ("写", "总结", "摘要", "文案", "整理", "报告", "稿", "介绍"))
        wants_research = any(k in text for k in ("查", "搜索", "对比", "比较", "资料", "数据", "价格", "怎么样"))
        core = text[:110]
        topic = _topic_of(text)

        briefs: list[dict[str, str]] = []
        if wants_research or (not (wants_action or wants_plan or wants_write)):
            briefs.append({
                "worker_type": "researcher", "topic": topic + "查证",
                "prompt": f"查证并对比以下诉求所需的关键信息，给出可直接使用的事实要点：{core}",
            })
        if wants_action or wants_plan:
            briefs.append({
                "worker_type": "planner", "topic": topic + "排期",
                "prompt": f"为以下诉求排一份可执行步骤计划（含依赖、时间、验收标准）；"
                          f"涉及不可逆动作时先请人确认：{core}",
            })
        if wants_write:
            briefs.append({
                "worker_type": "writer", "topic": topic + "成文",
                "prompt": f"把以下诉求整理成可直接交付的文字（含结论和要点）：{core}",
            })
        if not briefs:
            briefs.append({"worker_type": "writer", "topic": topic + "成文",
                           "prompt": f"针对以下诉求给出简洁结论与要点：{core}"})
        return briefs[:3]

    # ---- Worker ------------------------------------------------------------
    def _worker_step(self, user_text: str, messages: list[Msg],
                     tools: list[dict] | None) -> list[Any]:
        sys_text = _system_text(messages)
        called = _tools_called(messages)
        available = [t["function"]["name"] for t in (tools or [])]
        self._pool_jobs = self._current_jobs()
        ctx = CURRENT_CONTEXT.get()
        # 已经请求挂起：这一轮不再调任何工具，直接收尾让运行时去处理挂起。
        # （否则"等结果→被拉起→又等"会把 max_iters 耗尽，最后报成失败。）
        if ctx is not None and ctx.wait_request:
            return [_text("（本轮挂起，等条件满足后继续）")]

        def has(name: str) -> bool:
            return name in available

        # 被追加指令（steer）后，模型先复述一次，让你在结果里看到它收到了
        if "【追加要求】" in user_text and called:
            return [_text(f"已按追加要求调整。{self._final_answer(user_text, called, messages)}")]

        # <<LOOP>>：永远调同一个工具，演示 Watchdog 的「在动但没进展」
        if _LOOP_MARKER in user_text and called:
            return [_tool_call(short_id("c"), "webfetch",
                            {"url": _search_url(user_text[:40])})]

        call_id = short_id("c")

        # planner：先排计划；含不可逆动作时必须挂起等人确认。
        # ⚠ 这一段要在通用的「调过工具就收尾」之前，否则永远走不到 ask_human。
        if "planner" in sys_text.lower() and has("ask_human"):
            done = set(called)
            # 已经有人批过的任务：不要再次请求同一个确认
            approved = "【人工批准】" in user_text
            if "make_plan" not in done:
                return [_tool_call(call_id + "p", "make_plan",
                                   {"topic": user_text[:50], "max_steps": 4})]
            # 先拿到外部结果，再决定要不要请人确认（顺序反了会变成"还没查就想订"）
            if has("start_job") and "航班" in user_text:
                if not self._pool_jobs:
                    return [_tool_call(call_id + "j", "start_job", {"job": "查航班"})]
                if not self._job_ready():
                    return [_tool_call(call_id + "s", "stop_job", {"job": "查航班"})]
            if _has_irreversible(user_text) and not approved and "ask_human" not in done:
                return [_tool_call(call_id + "h", "ask_human",
                                   {"question": "计划里包含不可逆动作（订票/下单），确认执行？"})]

        # 正常：调过一次就收尾
        if called:
            return [_text(self._final_answer(user_text, called, messages))]

        if "<<NO_TOOL>>" in user_text:
            return [_text("（没有调用工具）结论：直接给出结果。")]

        if "researcher" in sys_text.lower() and has("webfetch"):
            q = _query(user_text)
            return [
                _tool_call(call_id + "a", "webfetch", {"url": _search_url(q)}),
                _tool_call(call_id + "b", "webfetch", {"url": _search_url(q + " 新手")}),
            ]
        if has("draft"):
            return [_tool_call(call_id, "draft", {"outline": user_text[:110]})]
        return [_text("（没有可用工具）结论：直接作答。")]

    def _final_answer(self, user_text: str, called: list[str],
                      messages: list[Msg] | None = None) -> str:
        """Worker 收尾。

        要点：结论必须来自**工具返回的内容**，而不是把任务描述再念一遍。
        之前只回述 prompt，问"深圳"看到的是"帮我查个…"，等于没回答。
        """
        tools = "、".join(dict.fromkeys(called))
        ask = _clip(_strip_prompt_prefix(user_text), 26)
        results = _tool_outputs(messages or [])
        facts: list[str] = []
        for name, text in results:
            try:
                data = json.loads(text)
            except (json.JSONDecodeError, TypeError):
                data = None
            if isinstance(data, dict) and data.get("results"):
                facts.extend(str(x) for x in data["results"][:3])
            elif name == "draft":
                facts.append(f"草稿大纲：{_clip(text, 60)}")
            elif name == "make_plan":
                facts.append(f"计划：{_clip(text, 60)}")
            elif text.strip():
                facts.append(_clip(text.strip(), 80))
        if not facts:
            return (f"用了 {len(called)} 次工具（{tools}）。\n"
                    f"关于「{ask}」没有拿到可用结果。")
        lines = "\n".join(f"  · {fact}" for fact in facts[:4])
        return (f"关于「{ask}」，用了 {len(called)} 次工具（{tools}）。\n"
                f"{lines}\n"
                f"（以上是本地演示数据，未联网；接真实模型后会换成检索结果。）")

    # ---- 合成 --------------------------------------------------------------
    def _current_jobs(self) -> dict[str, Any]:
        """当前任务已经启动的外部作业（跨执行体存活，挂在池子上）。"""
        ctx = CURRENT_CONTEXT.get()
        if ctx is None or ctx.pool is None:
            return {}
        return {k: v for k, v in ctx.pool.jobs.items() if k.startswith(f"{ctx.task_id}:")}

    def _job_ready(self) -> bool:
        if not self._pool_jobs:
            return False
        return all(getattr(job, "done", lambda: True)() for job in self._pool_jobs.values())

    def _explain(self, user_text: str) -> str:
        """不派活时的回话。要点：说清没派、为什么、你可以怎么问。"""
        said = _section(user_text, "用户说") or user_text
        why = _section(user_text, "你的判断") or "没有可执行的事"

        # 问能力/自我介绍：直接答，别绕
        if any(k in said for k in ("你能做什么", "你是谁", "怎么用", "会什么", "能干嘛", "介绍一下")):
            return ("我是个常驻的任务调度器：你把事情说出来，我拆成任务派给 Worker 并行去做，"
                    "做完由我汇总回你。\n"
                    "试一句：「帮我查一下成都周边徒步路线，排个两天计划」。")

        if len(said) < 6 or not _worth(said):
            if _looks_like_recall(said):
                return "你想接之前那件事，但我没找到对应的任务。可以说得更具体些，比如「刚才那个路线计划改成三天」。"
            return (f"这条我没派任务：{_clip(why, 40)}。\n"
                    "要让我动手，可以说「帮我查一下…」「帮我写一版…」「帮我排个计划…」。")
        return f"这条我没派任务：{_clip(why, 40)}。"

    def _synthesize(self, digest: str) -> str:
        rows = _parse_digest(digest)
        if not rows:
            return "这一轮没有需要你处理的事。"
        done = [r for r in rows if r[1] == "SUCCEEDED"]
        blocked = [r for r in rows if r[1] in ("NEEDS_HUMAN", "WAITING")]
        failed = [r for r in rows if r[1] == "FAILED"]
        parts = [f"在做 {len(rows)} 件：完成 {len(done)}、卡住 {len(blocked)}、失败 {len(failed)}。"]
        for name, status, body in rows[:4]:
            if status == "NEEDS_HUMAN":
                parts.append(f"  · {name} 等你确认：{_clip(body, 40) or '（见任务表）'}")
            elif status == "WAITING":
                parts.append(f"  · {name} 在等外部结果：{_clip(body, 40)}")
            elif status == "SUCCEEDED":
                parts.append(f"  · {name}：{_clip(body, 60)}")
            else:
                parts.append(f"  · {name} [{status}]：{_clip(body, 50)}")
        if blocked:
            parts.append("需要你出手的只有上面标注的那几条。")
        return "\n".join(parts)

    def snapshot(self) -> dict[str, Any]:
        return {"calls": self.calls, "peak": self.meter.peak,
                "failures_injected": self.failures_injected}


def _section(text: str, title: str) -> str:
    """取 ``# title ...`` 那一行之后、到下一个 ``#`` 标题之间的内容。

    行首标题允许带后缀（例如 ``# 新消息（来自 console）``），所以匹配的是
    ``# title`` 开头、直到行尾，而不是要求标题后立刻换行。
    """
    m = re.search(rf"(?m)^#\s*{re.escape(title)}[^\n]*\n(.*?)(?=^#\s|\Z)", text, re.S)
    return m.group(1).strip() if m else ""


def _first_token(line: str) -> str:
    m = re.match(r"\s*-?\s*([0-9a-f]{6,8})", line)
    return m.group(1) if m else ""


def _looks_like_recall(text: str) -> bool:
    return any(k in text for k in ("刚才", "刚刚", "前面那个", "上面那个", "之前那个", "那个计划", "继续上次"))


def _match_living(new_msg: str, living: str) -> str | None:
    """在「在管的事」里找一个和消息主题明显吻合的任务。"""
    if not living.strip():
        return None
    lowered = new_msg
    best: tuple[int, str] | None = None
    for line in living.strip().splitlines():
        m = re.search(r"([0-9a-f]{6,8}).*?#(\S+).*?::\s*(.*)", line)
        if not m:
            continue
        tid, topic, prompt = m.group(1), m.group(2), m.group(3)
        score = 0
        for token in _keywords(prompt):
            if token in lowered:
                score += 2
        if topic and topic.rstrip("查证成文排期") and topic.rstrip("查证成文排期") in lowered:
            score += 3
        if score >= 2 and (best is None or score > best[0]):
            best = (score, tid)
    return best[1] if best else None


def _keywords(text: str) -> list[str]:
    """粗粒度关键词：取 2~4 字的中文片段与英文单词，够 Demo 用。"""
    words = re.findall(r"[A-Za-z]{3,}|[\u4e00-\u9fa5]{2,4}", text)
    stop = {"请帮我", "帮我", "查一下", "做一个", "并将", "以下", "的需求", "关键信息",
            "给出", "可以", "直接用", "使用", "事实要点", "结论", "要点"}
    return [w for w in words if w not in stop][:6]


def _topic_of(text: str) -> str:
    for kw in ("航班", "机票", "高铁", "路线", "徒步", "酒店", "预算", "周报", "竞品", "排版", "海报"):
        if kw in text:
            return kw
    m = re.search(r"[\u4e00-\u9fa5]{2,4}", text)
    return m.group(0) if m else "事项"


def _query(text: str) -> str:
    cleaned = re.sub(r"^.*?[:：]", "", text.strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return "关键词"
    for marker in (_EMPTY_MARKER, _SLOW_MARKER, _BLOCKED_MARKER, _LOOP_MARKER):
        if marker in cleaned:
            return f"{marker} {cleaned.replace(marker, '').strip()[:40]}".strip()
    return cleaned if len(cleaned) <= 48 else cleaned[:47] + "…"


_WORTH_KW = ("查", "写", "计划", "对比", "整理", "总结", "规划", "路线", "方案", "怎么做",
             "怎么", "如何", "改", "换", "追加", "补充", "航班", "高铁", "预算", "进度")
#: 动作类诉求：不是「查资料」，但恰恰最需要系统接管（因为要挂起等人确认）
_ACTION_KW = ("订", "购买", "下单", "支付", "删除", "提交", "发消息", "发邮件", "报销", "审批")


def _worth(text: str) -> bool:
    return any(k in text for k in _WORTH_KW)


def _actionable(text: str) -> bool:
    """「值得系统接住」的判定，语义层与 Router 共用同一套口径。"""
    return _has_irreversible(text) or _worth(text)


def _has_irreversible(text: str) -> bool:
    """是否包含需要人拍板的不可逆动作。"""
    return any(k in text for k in _ACTION_KW)


def _parse_digest(digest: str) -> list[tuple[str, str, str]]:
    rows = []
    for ln in digest.splitlines():
        s = ln.strip()
        if not s.startswith("- "):
            continue
        name, _, rest = s[2:].partition(" ")
        status, _, body = rest.strip().partition(" ")
        rows.append((name, status.strip("[]"), body.strip("（()） ")))
    if rows:
        return rows
    pat = re.compile(r"(?P<name>[\w-]+)\s*\[(?P<status>[A-Z_]+)\]")
    matches = list(pat.finditer(digest))
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(digest)
        rows.append((m.group("name"), m.group("status"),
                     digest[m.end():end].strip(" \n：:（）()")))
    return rows


def _clip(text: str, n: int) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= n else text[: n - 1] + "…"


def build_chat_model(provider: str = "fake", *, meter: ConcurrencyMeter | None = None,
                     config: Any = None, **kwargs: Any) -> ChatModelBase:
    """按配置构造模型。

    ``provider`` 只是快捷方式（fake / deepseek / openai）；真正要接任意
    OpenAI 兼容服务（例如华为云 MaaS）时，传 ``config=ModelConfig``。
    """
    provider = (provider or "fake").lower()
    if provider == "fake":
        return FakeChatModel(meter=meter, **kwargs)

    # 有显式配置就用它，否则回退到按 provider 猜
    if config is None:
        try:
            import config as config_module
            cfg = config_module.load()
            if not cfg.model.is_fake and cfg.model.provider == provider:
                config = cfg.model
        except Exception:  # pragma: no cover
            config = None

    if config is not None and getattr(config, "base_url", ""):
        ok, why = config.ready()
        if not ok:
            raise RuntimeError(why)
        from agentscope.credential import OpenAICredential  # type: ignore
        from agentscope.model import OpenAIChatModel  # type: ignore

        return OpenAIChatModel(
            # base_url 要挂在 credential 上：OpenAIChatModel 会自己把它传给
            # openai.AsyncClient，再放进 client_kwargs 会报重复参数。
            credential=OpenAICredential(api_key=config.api_key, base_url=config.base_url),
            model=config.model_name,
            stream=config.stream,
            context_size=config.context_size,
            max_retries=3,
            retry_delay=1.0,
            parameters=OpenAIChatModel.Parameters(
                temperature=config.temperature,
                max_tokens=config.max_tokens,
            ),
        )

    key = os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not key:
        raise RuntimeError(
            "没有可用的模型配置。运行 `python tools/setup_model.py` 填一份 config.json，"
            "或设置 DEEPSEEK_API_KEY / OPENAI_API_KEY，或使用 --model fake"
        )
    from agentscope.credential import DeepSeekCredential, OpenAICredential  # type: ignore
    from agentscope.model import DeepSeekChatModel, OpenAIChatModel  # type: ignore

    if provider == "deepseek":
        return DeepSeekChatModel(credential=DeepSeekCredential(api_key=key),
                                 model="deepseek-chat", max_retries=3, retry_delay=1.0)
    return OpenAIChatModel(credential=OpenAICredential(api_key=key),
                           model="gpt-4o-mini", max_retries=3, retry_delay=1.0)


# =============================================================================
# Worker 上下文与工具
# =============================================================================
class WorkerCancelled(Exception):
    """在安全点发现取消标记。"""


@dataclass
class AsyncHandle:
    """异步工具的句柄：``start`` 立刻返回它，结果由后台任务填进来。

    这个对象挂在**池子**上而不是 WorkerContext 上：任务挂起 → 执行体被丢弃 →
    恢复时换一个新的执行体，但同一个外部作业还得找得到。这也顺带说明了
    「执行体可丢弃，外部资源不能跟着丢」。
    """

    tool: str
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    value: str = ""

    async def stop(self, timeout: float = 30.0) -> str:
        await asyncio.wait_for(self.ready.wait(), timeout=timeout)
        return self.value


@dataclass
class WorkerContext:
    table: TaskTable
    trace: TraceStore
    task_id: str
    span_id: str
    rng: random.Random = field(default_factory=random.Random)
    call_counts: dict[str, int] = field(default_factory=dict)
    handles: dict[str, AsyncHandle] = field(default_factory=dict)
    gate: Any = None
    pool: Any = None
    #: 当前模型，供 draft/make_plan 这类"要生成"的工具使用
    model: Any = None
    #: 离线模式：用假模型跑演示/测试时不访问网络，工具直接报错而不是编数据
    offline: bool = False
    #: 工具要求挂起时填进来（"待批:..." / "等:..."），由运行时在 ReAct 结束后处理
    wait_request: str | None = None
    #: 挂起多久后自动恢复（模拟外部结果就绪）
    wait_seconds: float = 3.0

    def spawn(self, coro: Awaitable[Any]) -> asyncio.Task[Any]:
        return asyncio.ensure_future(coro)

    def task(self) -> TaskBrief:
        task = self.table.get(self.task_id)
        if task is None:
            raise WorkerCancelled(f"任务 {self.task_id} 已从表中移除")
        return task

    def checkpoint(self) -> None:
        """安全点：先看取消，再记进展。顺序反了会把 CANCELLING 刷掉。"""
        task = self.task()
        if task.status == CANCELLING:
            raise WorkerCancelled(f"任务 {self.task_id} 被取消")
        task.last_activity = time.monotonic()

    def note_tool(self, tool: str) -> None:
        n = self.call_counts.get(tool, 0) + 1
        self.call_counts[tool] = n


def _current() -> WorkerContext:
    ctx = CURRENT_CONTEXT.get()
    if ctx is None:
        raise RuntimeError("工具必须在 Worker 上下文中调用")
    return ctx


def _warn(ctx: WorkerContext, message: str) -> None:
    task = ctx.task()
    ctx.table.update(ctx.task_id, warning_count=task.warning_count + 1)
    ctx.trace.warn_span(ctx.span_id, message)


async def _tick_sleep(ctx: WorkerContext, seconds: float, *, step: float = 0.04) -> None:
    remaining = seconds
    while remaining > 0:
        ctx.checkpoint()
        await asyncio.sleep(min(step, remaining))
        remaining -= step


def _register_background(ctx: WorkerContext, handle: AsyncHandle, coro: Awaitable[str]) -> None:
    """把后台作业登记在池子上（跨执行体存活），而不是挂在执行体上。"""
    pool = ctx.pool

    async def runner() -> None:
        try:
            handle.value = await coro
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover
            handle.value = f"（失败：{exc}）"
        finally:
            handle.ready.set()

    task = asyncio.ensure_future(runner())
    if pool is not None:
        pool.jobs.setdefault(f"{ctx.task_id}:{len(pool.jobs)}", task)
        pool.handles.setdefault(ctx.task_id, []).append(handle)
    else:  # pragma: no cover - 单测里可能没有池子
        ctx.spawn(task)


# -- 工具（async def，全部可取消） ------------------------------------------
class ToolError(RuntimeError):
    """工具做不到就抛这个。

    宁可让模型看到一句明确的错误，也不要回一份编造的数据——编造的结果会被
    当成事实写进最终答复，比失败危险得多。
    """


async def _http_get(url: str, *, timeout: float = 20.0, limit: int = 400_000) -> str:
    """真发一个 GET。放线程里跑，避免阻塞事件循环。"""
    import urllib.error
    import urllib.parse
    import urllib.request

    # 模型给出的 URL 常带中文（尤其搜索页），必须先编码，
    # 否则 urllib 抛 UnicodeEncodeError('ascii')。
    parts = urllib.parse.urlsplit(url)
    url = urllib.parse.urlunsplit((
        parts.scheme, parts.netloc,
        urllib.parse.quote(parts.path, safe="/%"),
        urllib.parse.quote(parts.query, safe="=&%+"),
        urllib.parse.quote(parts.fragment),
    ))

    def fetch() -> str:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read(limit)
            charset = resp.headers.get_content_charset() or "utf-8"
        return raw.decode(charset, errors="replace")

    return await asyncio.to_thread(fetch)


def _html_to_text(html: str, limit: int = 12000) -> str:
    """粗暴去标签取正文。够用即可，不引解析库。"""
    html = re.sub(r"(?is)<(script|style|noscript|svg)[^>]*>.*?</\1>", " ", html)
    html = re.sub(r"(?is)<br\s*/?>|</p>|</div>|</li>|</h[1-6]>", "\n", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    text = html.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<")
    text = text.replace("&gt;", ">").replace("&quot;", '"').replace("&#39;", "'")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()[:limit]


async def webfetch(url: str) -> ToolChunk:
    """抓取一个网页，返回正文。检索只用这个，不内置搜索引擎。

    参数给完整 http(s) URL。如果只知道关键词，可以先抓一个搜索结果页
    （例如 https://cn.bing.com/search?q=... ），再从正文里找链接。

    Args:
        url (str): 要抓取的地址，必须带 http(s)。
    """
    ctx = _current()
    ctx.checkpoint()
    ctx.note_tool("webfetch")
    if ctx.offline:
        raise ToolError("离线演示模式不联网；接上真实模型后才会真正抓取")
    url = (url or "").strip()
    if not url.startswith(("http://", "https://")):
        raise ToolError(f"webfetch 需要完整 URL（http/https），收到 {url!r}")
    try:
        html = await _http_get(url)
    except Exception as exc:  # noqa: BLE001
        raise ToolError(f"抓取失败 {url}：{type(exc).__name__}: {exc}") from exc
    text = _html_to_text(html)
    if not text:
        raise ToolError(f"{url} 没有可读正文（可能是纯前端渲染页面）")
    return ToolChunk(content=[_text(json.dumps(
        {"url": url, "chars": len(text), "text": text}, ensure_ascii=False))])



async def _generate(ctx: "WorkerContext", prompt: str, *, limit: int = 4000) -> str:
    """用当前模型生成文本。没有模型就报错，而不是套模板。"""
    model = getattr(ctx, "model", None)
    if model is None:
        raise ToolError("当前上下文没有绑定模型，无法生成内容")
    result = model([UserMsg("tool", prompt)])
    if hasattr(result, "__await__"):
        result = await result
    text = ""
    if hasattr(result, "__aiter__"):
        async for chunk in result:
            for block in getattr(chunk, "content", None) or []:
                text += getattr(block, "text", "") or ""
    else:
        text = getattr(result, "get_text_content", lambda: "")() or ""
    text = text.strip()
    if not text:
        raise ToolError("模型返回了空内容")
    return text[:limit]


async def draft(outline: str) -> ToolChunk:
    """按提纲起草文字（由模型生成）。

    Args:
        outline (str): 写作提纲。
    """
    ctx = _current()
    ctx.checkpoint()
    ctx.note_tool("draft")
    text = await _generate(ctx, f"按下面的提纲写一版可直接使用的文字，不要解释过程：\n{outline}")
    return ToolChunk(content=[_text(text)])


async def make_plan(topic: str, max_steps: int = 4) -> ToolChunk:
    """生成分步计划（由模型生成）。

    Args:
        topic (str): 计划主题。
        max_steps (int): 最多几步。
    """
    ctx = _current()
    ctx.checkpoint()
    ctx.note_tool("make_plan")
    text = await _generate(
        ctx, f"为下面的诉求排一份不超过 {max_steps} 步的执行计划，只输出步骤：\n{topic}")
    return ToolChunk(content=[_text(text)])


async def start_job(job: str) -> ToolChunk:
    """后台抓取一个网址，立刻返回句柄（不阻塞对话）。

    这是真实能力：给一个 URL，它在后台抓取，稍后由 ``stop_job`` 取回。
    没有 URL 就报错——不编造"已完成"。

    Args:
        job (str): 要抓取的 URL，例如 https://example.com/page。
    """
    ctx = _current()
    ctx.checkpoint()
    ctx.note_tool("start_job")
    url = job.strip()
    if not url.startswith(("http://", "https://")):
        raise ToolError(f"start_job 需要一个 http(s) URL，收到 {job!r}")
    handle = AsyncHandle(tool="start_job")

    async def job_result() -> str:
        html = await _http_get(url)
        text = _html_to_text(html)
        return text or "（页面没有可读正文）"

    _register_background(ctx, handle, job_result())
    return ToolChunk(content=[_text(f"已在后台抓取 {url}，稍后用 stop_job 取回。")])


async def stop_job(job: str) -> ToolChunk:
    """取回异步作业的结果。

    Args:
        job (str): 作业名。
    """
    ctx = _current()
    ctx.checkpoint()
    ctx.note_tool("stop_job")
    handles = list(ctx.handles.values())
    if ctx.pool is not None:
        handles += ctx.pool.handles.get(ctx.task_id, [])
    if not handles:
        return ToolChunk(content=[_text("（没有在跑的作业）")])
    handle = handles[-1]
    if not handle.ready.is_set():
        # 结果还没好：把任务挂起（等外部条件），而不是在这里死等。
        # 等的时候人不被占住——这正是"异步全双工"的意义。
        ctx.wait_request = f"等:{job}结果"
        ctx.wait_seconds = 1.0
        return ToolChunk(content=[_text(f"（结果未就绪，先挂起等 {job}）")])
    return ToolChunk(content=[_text(handle.value)])


#: 挂起请求的两种前缀：``待批:`` 走人工确认，``等:`` 走外部条件。
#: 不能用抛异常表达挂起——AgentScope 会把工具异常当成普通 tool_result 回填给模型，
#: ReAct 会继续跑，"挂起"就丢了。
async def wait_for(thing: str) -> ToolChunk:
    """等一个尚未就绪的外部结果。工具立刻返回，由运行时把任务挂起。

    Args:
        thing (str): 在等什么，例如「政策发布」「审批结果」。
    """
    ctx = _current()
    ctx.checkpoint()
    ctx.note_tool("wait_for")
    ctx.wait_request = f"等:{thing}"
    return ToolChunk(content=[_text(f"（已挂起，等待：{thing}）")])


async def ask_human(question: str) -> ToolChunk:
    """不可逆动作前请人拍板。任务会挂起，直到在控制台上批准。

    Args:
        question (str): 要用户确认什么。
    """
    ctx = _current()
    ctx.checkpoint()
    ctx.note_tool("ask_human")
    ctx.wait_request = f"待批:{question}"
    return ToolChunk(content=[_text(f"（已挂起，等待人工确认：{question}）")])


WORKER_TOOLS: dict[str, list[Callable[..., Awaitable[ToolChunk]]]] = {
    "researcher": [webfetch, wait_for],
    "planner": [make_plan, ask_human, start_job, stop_job],
    "writer": [draft, webfetch],
}


def build_toolkit(worker_type: str, *, call_log: list[str] | None = None) -> Toolkit:
    """按角色建工具集。

    每个工具都包一层 trace：AgentScope 的 ToolCallStartEvent 只带工具名和
    call id，不带参数和返回值，所以参数/结果只能在这里自己记。
    """
    funcs = WORKER_TOOLS.get(worker_type, [draft])

    def instrument(fn: Callable[..., Awaitable[ToolChunk]]) -> FunctionTool:
        async def wrapped(*args: Any, **kwargs: Any) -> ToolChunk:
            if call_log is not None:
                call_log.append(fn.__name__)
            ctx = CURRENT_CONTEXT.get()
            if ctx is None:
                return await fn(*args, **kwargs)

            detail_in = {str(k): v for k, v in kwargs.items()}
            if args:  # 位置参数按签名补上名字
                try:
                    import inspect as _inspect
                    names = [p.name for p in _inspect.signature(fn).parameters.values()]
                    for name, value in zip(names, args):
                        detail_in[name] = value
                except Exception:  # pragma: no cover
                    detail_in["args"] = list(args)

            span = ctx.trace.start_span(
                KIND_TOOL, fn.__name__, parent_id=ctx.span_id, tool=fn.__name__,
                input=_clip(json.dumps(detail_in, ensure_ascii=False, default=str), 600))
            try:
                out = await fn(*args, **kwargs)
            except Exception as exc:
                span.end(ST_ERROR, error=f"{type(exc).__name__}: {exc}")
                ctx.trace.push_span(span)
                raise
            span.end(ST_OK, output=_clip(_toolchunk_text(out), 1200))
            ctx.trace.push_span(span)
            return out

        wrapped.__name__ = fn.__name__
        wrapped.__doc__ = fn.__doc__
        return FunctionTool(wrapped, name=fn.__name__, permission=AUTO_ALLOW)

    return Toolkit(tools=[instrument(f) for f in funcs])


def _toolchunk_text(chunk: Any) -> str:
    """把 ToolChunk 里的文字取出来，供 trace 展示。"""
    parts: list[str] = []
    for block in getattr(chunk, "content", None) or []:
        text = getattr(block, "text", "")
        if text:
            parts.append(str(text))
    if not parts:
        parts.append(str(chunk))
    return "\n".join(parts)


# =============================================================================
# 双层过滤
# =============================================================================
@dataclass
class GateResult:
    event_id: str
    raw: str
    program_pass: bool
    program_reason: str
    semantic_verdict: str
    semantic_reason: str
    publishable: bool
    """程序层是否放行。语义层的判断只作标注，不在这里拦人。"""

    source: str = "console"
    span_id: str = ""
    task_ids: list[str] = field(default_factory=list)
    route: str = ""
    route_note: str = ""

    @property
    def worthwhile(self) -> bool:
        """语义层认为值不值得行动（仅供参考，由 Router 最终决定）。"""
        return self.semantic_verdict == "WORTH"

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id, "raw": self.raw, "source": self.source,
            "program_pass": self.program_pass, "program_reason": self.program_reason,
            "semantic_verdict": self.semantic_verdict,
            "semantic_reason": self.semantic_reason,
            "task_ids": list(self.task_ids), "route": self.route,
            "route_note": self.route_note,
        }


class EventGate:
    """程序层（快）+ 语义层（慢）。

    程序层按来源、白名单、去重、限流做确定性筛选，零模型成本；
    语义层才让模型判断「值不值得占用注意力」。
    """

    def __init__(
        self,
        *,
        min_length: int = 2,
        allow_sources: Iterable[str] | None = None,
        deny_keywords: Iterable[str] = ("忽略我", "退订", "+1"),
        dedupe_window: float = 300.0,
        rate_limit: tuple[int, float] | None = (12, 60.0),
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.min_length = min_length
        self.allow_sources = set(allow_sources) if allow_sources else None
        self.deny_keywords = tuple(deny_keywords)
        self.dedupe_window = dedupe_window
        self.rate_limit = rate_limit
        self._clock = clock
        self._seen: dict[str, float] = {}     # 归一化文本 -> 首次出现时间
        self._recent: list[float] = []
        self._counter = 0
        self.semantic_llm = False

    def next_event_id(self) -> str:
        self._counter += 1
        return f"EV-{self._counter:03d}"

    def program_layer(self, raw: str, source: str = "console") -> tuple[bool, str]:
        text = raw.strip()
        if self.allow_sources is not None and source not in self.allow_sources:
            return False, f"程序层: REJECT source_not_allowed({source})"
        if not text:
            return False, "程序层: REJECT empty"
        if not re.search(r"[\w\u4e00-\u9fff]", text):
            return False, "程序层: REJECT punctuation_only"
        for kw in self.deny_keywords:
            if kw and kw in text:
                return False, f"程序层: REJECT deny_keyword({kw})"
        if len(text) < self.min_length:
            return False, f"程序层: REJECT too_short(len={len(text)})"
        norm = re.sub(r"\s+", "", text.lower())
        now = self._clock()
        # 去重窗口内的完全相同消息只放行第一次
        if norm in self._seen and now - self._seen[norm] < self.dedupe_window:
            return False, "程序层: REJECT duplicate(窗口内重复)"
        if self.rate_limit is not None:
            limit, window = self.rate_limit
            self._recent = [t for t in self._recent if now - t < window]
            if len(self._recent) >= limit:
                return False, f"程序层: REJECT rate_limited({limit}/{int(window)}s)"
            self._recent.append(now)
        self._seen[norm] = now
        return True, "程序层: PASS"

    def semantic_layer(self, raw: str) -> tuple[str, str]:
        """语义层的离线版：判断值不值得行动。

        注意这里要覆盖「订票 / 购买 / 删除」这类**动作类**请求——它们不是"查资料"，
        但恰恰最需要系统接管（因为要挂起等人确认）。
        """
        action_kw = _ACTION_KW
        if any(k in raw for k in action_kw):
            return "WORTH", "涉及不可逆动作，需要接管并请人确认"
        if _worth(raw):
            return "WORTH", "有明确工作诉求"
        if len(raw) < 8:
            return "NOT_WORTH", "信息不足以行动"
        return "NOT_WORTH", "没有可执行诉求"

    async def check(self, raw: str, *, source: str = "console",
                    semantic_fn: Callable[[str], Awaitable[tuple[str, str]]] | None = None) -> GateResult:
        event_id = self.next_event_id()
        ok, reason = self.program_layer(raw, source)
        if not ok:
            return GateResult(event_id, raw, False, reason, "SKIPPED",
                              "程序层已拒绝，语义层未执行", False, source)
        verdict, sreason = (await semantic_fn(raw)) if semantic_fn else self.semantic_layer(raw)
        # 语义层只做**标注**，不负责拦人：判断"要不要行动"是 Router 的事。
        # 早期版本在这里就把 NOT_WORTH 挡掉，于是用户问"你能做什么"这类话
        # 根本到不了 Router，也就永远收不到回复。
        return GateResult(event_id, raw, True, reason, verdict, sreason, True, source)


# =============================================================================
# Router
# =============================================================================
@dataclass
class Brief:
    worker_type: str
    prompt: str
    topic: str = ""

    def to_dict(self) -> dict[str, str]:
        return {"worker_type": self.worker_type, "prompt": self.prompt, "topic": self.topic}


@dataclass
class Decision:
    mode: str                      # direct / recall / spawn / ignore
    target: str | None = None
    why: str = ""
    briefs: list[Brief] = field(default_factory=list)

    @property
    def touched_router_only(self) -> bool:
        return self.mode == "spawn"


_JSON_OBJ_RE = re.compile(r"\{.*\}", re.S)


def parse_decision(text: str) -> Decision:
    """解析 Router 输出。模型爱加代码块和前后缀，这里做三级降级。"""
    text = (text or "").strip()
    candidates = [text]
    m = _JSON_OBJ_RE.search(text)
    if m:
        candidates.append(m.group(0))
    data: Any = None
    for cand in candidates:
        try:
            data = json.loads(cand)
            break
        except json.JSONDecodeError:
            continue
    if data is None:
        try:
            import json_repair  # type: ignore

            for cand in candidates:
                fixed = json_repair.loads(cand)
                if isinstance(fixed, dict):
                    data = fixed
                    break
        except Exception:
            data = None
    if not isinstance(data, dict):
        return Decision("spawn", why="Router 输出无法解析，按新任务处理",
                        briefs=[])

    mode = str(data.get("mode", "spawn")).lower()
    if mode not in ("direct", "recall", "spawn", "ignore"):
        mode = "spawn"
    briefs = []
    for item in data.get("tasks") or []:
        if not isinstance(item, dict):
            continue
        prompt = str(item.get("prompt") or "").strip()
        if not prompt:
            continue
        briefs.append(Brief(
            worker_type=prompts.normalize_worker_type(item.get("worker_type")),
            prompt=prompt,
            topic=prompts.clean_topic(item.get("topic")),
        ))
        if len(briefs) >= 3:
            break
    target = data.get("target")
    return Decision(mode=mode, target=str(target) if target else None,
                    why=str(data.get("why") or ""), briefs=briefs)


class Router:
    """常驻的调度层：没有工具，只做判断；合成答复也只在需要说话时说话。"""

    def __init__(self, model: ChatModelBase, *, max_briefs: int = 3) -> None:
        self.model = model
        self.max_briefs = max_briefs
        self.agent: Agent | None = None
        self.decision_count = 0
        self.skipped_decisions = 0   # 被直通/召回省掉的拆单次数

    def _ensure_agent(self) -> Agent:
        if self.agent is None:
            self.agent = Agent(name="router", system_prompt=prompts.ROUTER_SYSTEM_PROMPT,
                               model=self.model, toolkit=None,
                               react_config=ReActConfig(max_iters=2),
                               model_config=ModelConfig(max_retries=2))
        return self.agent

    async def decide(self, gate: GateResult, *, living: str, done: str,
                     global_context: str = "") -> Decision:
        content = (f"# 全局上下文\n{global_context or '（无）'}\n\n"
                   f"# 在管的事\n{living or '（无）'}\n\n"
                   f"# 已结束的事\n{done or '（无）'}\n\n"
                   f"# 新消息（来自 {gate.source}）\n{gate.raw}")
        self.decision_count += 1
        msg = await self._ensure_agent().reply(UserMsg("event", content))
        decision = parse_decision(msg.get_text_content() or "")
        decision.briefs = decision.briefs[: self.max_briefs]
        return decision

    async def synthesize(self, digest: str) -> str:
        agent = Agent(name="router-synth", system_prompt=prompts.ROUTER_SYNTHESIZE_PROMPT,
                      model=self.model, toolkit=None,
                      react_config=ReActConfig(max_iters=1),
                      model_config=ModelConfig(max_retries=2))
        msg = await agent.reply(UserMsg("tasks", f"任务表：\n{digest}"))
        return msg.get_text_content() or digest

    async def explain(self, text: str, why: str) -> str:
        """决定不派活时，回一句人话。不派活 ≠ 不理人。"""
        agent = Agent(name="router-explain", system_prompt=prompts.ROUTER_EXPLAIN_PROMPT,
                      model=self.model, toolkit=None,
                      react_config=ReActConfig(max_iters=1),
                      model_config=ModelConfig(max_retries=2))
        msg = await agent.reply(UserMsg("event", f"用户说：{text}\n你的判断：{why}"))
        return msg.get_text_content() or f"这条我先不派任务（{why}）。"

    async def answer(self, text: str) -> str:
        """不需要动手的问题，直接答。"""
        agent = Agent(name="router-answer", system_prompt=prompts.ROUTER_ANSWER_PROMPT,
                      model=self.model, toolkit=None,
                      react_config=ReActConfig(max_iters=1),
                      model_config=ModelConfig(max_retries=2))
        msg = await agent.reply(UserMsg("user", text))
        return msg.get_text_content() or ""


# =============================================================================
# WorkerPool
# =============================================================================
@dataclass
class PoolResult:
    task_ids: list[str]
    succeeded: int = 0
    failed: int = 0
    cancelled: int = 0
    blocked: int = 0
    injected: int = 0

    def summary(self) -> str:
        return (f"{len(self.task_ids)} 个任务：完成 {self.succeeded}、失败 {self.failed}、"
                f"取消 {self.cancelled}、等人/等结果 {self.blocked}、被注入要求 {self.injected}")

    def merge(self, other: "PoolResult") -> "PoolResult":
        return PoolResult(
            task_ids=self.task_ids + other.task_ids,
            succeeded=self.succeeded + other.succeeded,
            failed=self.failed + other.failed,
            cancelled=self.cancelled + other.cancelled,
            blocked=self.blocked + other.blocked,
            injected=self.injected + other.injected,
        )


class WorkerPool:
    def __init__(self, table: TaskTable, trace: TraceStore, model: ChatModelBase, *,
                 node_id: str | None = None, max_concurrency: int = 8,
                 call_log: list[str] | None = None, log: Any = None) -> None:
        self.table = table
        self.trace = trace
        self.model = model
        self.log = log
        self.node_id = node_id or f"node-{uuid.uuid4().hex[:5]}"
        self.max_concurrency = max_concurrency
        self.call_log = call_log if call_log is not None else []
        self._running: dict[str, asyncio.Task[None]] = {}
        self._background: dict[str, list[asyncio.Task[Any]]] = {}
        #: 任务 id -> 后台作业 asyncio.Task（挂在池子上，跨执行体存活）
        self.jobs: dict[str, asyncio.Task[Any]] = {}
        #: 任务 id -> 异步工具句柄
        self.handles: dict[str, list[AsyncHandle]] = {}
        self._semaphore = asyncio.Semaphore(max_concurrency)
        #: 注入给某条任务的新要求：task_id -> [text]
        self._steer: dict[str, list[str]] = {}
        #: 重跑触发（人工批准后继续）
        self._resume: dict[str, asyncio.Event] = {}
        self.meter = ConcurrencyMeter()
        self.worker_factory: Callable[[TaskBrief], Awaitable[Agent]] | None = None
        self.injected_total = 0

    # ---- 写表 -------------------------------------------------------------
    def enqueue(self, briefs: Iterable[Brief], *, event_id: str,
                parent_span_id: str, origin: TaskBrief | None = None) -> list[TaskBrief]:
        created: list[TaskBrief] = []
        for brief in briefs:
            task = self.table.add_pending(
                brief.worker_type, brief.prompt, event_id=event_id, topic=brief.topic,
                origin_task_id=origin.task_id if origin else None, owner_node=self.node_id,
            )
            chain = "→".join(task.trace_chain)
            span = self.trace.start_span(
                KIND_WORKER, task.name, parent_id=parent_span_id, status=ST_QUEUED,
                chain=chain, input=_clip(brief.prompt, 160), topic=brief.topic,
            )
            task.span_id = span.span_id
            self.table.update(task.task_id, span_id=span.span_id)
            self.trace.publish(TraceEvent(kind="task_upsert", task=task.to_dict()))
            created.append(task)
        return created

    # ---- 拉起 -------------------------------------------------------------
    async def run_tasks(self, tasks: Iterable[TaskBrief]) -> PoolResult:
        """并行拉起所有任务。

        刻意不按角色串行：串行会把并发峰值压到 1，并行才是这个运行时的意义。
        需要上游产出时用 ``handoff`` 显式传（召回/续做场景本来就有现成结果）。
        """
        tasks = list(tasks)
        return await self._gather(tasks)

    def handoff(self, tasks: Iterable[TaskBrief], upstream: TaskBrief) -> str:
        """把一条已完成任务的产出变成下游任务的前置资料。"""
        if upstream.status != SUCCEEDED or not upstream.result:
            return ""
        block = (f"【上游任务 {upstream.name} 的产出，直接使用，不要重复检索】\n"
                 f"{upstream.result.strip()}")
        for t in tasks:
            t.prompt = f"{block}\n\n你的任务：\n{t.prompt}"
            if t.span_id:
                span = self.trace.get(t.span_id)
                if span is not None:
                    span.detail["input"] = _clip(t.prompt, 400)
                    span.revision += 1
                    self.trace.push_span(span)
        return block

    async def _gather(self, tasks: list[TaskBrief]) -> PoolResult:
        coros = [self._run_worker_by_id(t.task_id) for t in tasks]
        await asyncio.gather(*coros, return_exceptions=True)
        return self.summarize([t.task_id for t in tasks])

    async def dispatch(self, briefs: Iterable[Brief], *, event_id: str,
                       parent_span_id: str, origin: TaskBrief | None = None) -> PoolResult:
        tasks = self.enqueue(briefs, event_id=event_id, parent_span_id=parent_span_id, origin=origin)
        return await self.run_tasks(tasks)

    def summarize(self, task_ids: list[str]) -> PoolResult:
        r = PoolResult(task_ids=list(task_ids))
        for tid in task_ids:
            t = self.table.get(tid)
            if t is None:
                continue
            if t.status == SUCCEEDED:
                r.succeeded += 1
            elif t.status == CANCELLED:
                r.cancelled += 1
            elif t.status in (NEEDS_HUMAN, WAITING):
                r.blocked += 1
            elif t.status == FAILED:
                r.failed += 1
            if t.interventions:
                r.injected += sum(1 for i in t.interventions if i["kind"] == "steer")
        return r

    # ---- 订阅直通 ---------------------------------------------------------
    def deliver(self, task: TaskBrief, text: str) -> bool:
        """把一条新消息直接投递给已在管这件事的任务（不经 Router 拆单）。

        如果那个任务已经结束，就把它的状态拉回 PENDING 重跑，并把新要求追加进去——
        「接着这个活干」在语义上仍然是对**同一条任务**的续写。
        """
        self._steer.setdefault(task.task_id, []).append(text)
        self.injected_total += 1
        was_finished = task.is_terminal
        task.add_intervention("steer", text)
        if was_finished:
            self.table.update(task.task_id, status=PENDING, result=None,
                              ended_at=None, attempt=task.attempt + 1)
            self.table.update(task.task_id, trace_chain=task.trace_chain)
        else:
            self.table.update(task.task_id, warning_count=task.warning_count)
        self.trace.publish(TraceEvent(kind="task_upsert", task=task.to_dict()))
        return was_finished

    def pending_steers(self, task_id: str) -> list[str]:
        return self._steer.pop(task_id, [])

    # ---- 取消 -------------------------------------------------------------
    def cancel(self, task_id: str) -> bool:
        if not self.table.cancel(task_id):
            return False
        task = self.table[task_id]
        span = self.trace.get(task.span_id)
        if span is not None and span.ended_at is None:
            span.note(cancel_requested=True)
            self.trace.push_span(span)
        self.trace.publish(TraceEvent(kind="alert", alert={
            "level": "INFO", "source": task.name, "span_id": task.span_id,
            "message": "取消请求（协作式），下一个安全点退出",
        }))
        self.trace.publish(TraceEvent(kind="task_upsert", task=task.to_dict()))
        return True

    def cancel_all(self) -> int:
        return sum(1 for t in self.table.active() if self.cancel(t.task_id))

    # ---- 人工批准 ---------------------------------------------------------
    async def approve(self, task_id: str, *, note: str = "批准") -> bool:
        task = self.table.get(task_id)
        if task is None or task.status != NEEDS_HUMAN:
            return False
        task.add_intervention("approve", note, actor="human")
        task.needs_human_reason = ""
        self._steer.setdefault(task_id, []).append(f"【人工批准】用户已确认，可以继续执行。{note}")
        self.table.update(task_id, status=PENDING, attempt=task.attempt + 1)
        self.trace.publish(TraceEvent(kind="alert", alert={
            "level": "INFO", "source": task.name, "span_id": task.span_id,
            "message": f"人工批准（{note}），任务继续",
        }))
        self.trace.publish(TraceEvent(kind="task_upsert", task=task.to_dict()))
        await self.run_tasks([task])
        return True

    def reject(self, task_id: str, *, note: str = "驳回") -> bool:
        task = self.table.get(task_id)
        if task is None:
            return False
        task.add_intervention("reject", note, actor="human")
        self.table.update(task_id, status=CANCELLED, ended_at=time.monotonic(),
                          result=f"人工驳回：{note}", needs_human_reason="")
        self.trace.publish(TraceEvent(kind="task_upsert", task=task.to_dict()))
        return True

    # ---- 恢复 -------------------------------------------------------------
    async def resume_orphans(self) -> list[TaskBrief]:
        orphans = self.table.requeue_orphans(self.node_id)
        for task in orphans:
            span = self.trace.start_span(
                KIND_WORKER, task.name, parent_id=self._event_span_for(task),
                status=ST_QUEUED, chain="→".join(task.trace_chain),
                input=_clip(task.prompt, 160), resumed=True, attempt=task.attempt,
            )
            self.table.update(task.task_id, span_id=span.span_id)
            self.trace.publish(TraceEvent(kind="alert", alert={
                "level": "INFO", "source": task.name, "span_id": span.span_id,
                "message": f"恢复：原执行体不在本节点，重新排队（attempt={task.attempt}）",
            }))
        if orphans:
            await self.run_tasks(orphans)
        return orphans

    def _event_span_for(self, task: TaskBrief) -> str | None:
        for root in self.trace.roots():
            if root.kind == KIND_EVENT and task.event_id and task.event_id in root.name:
                return root.span_id
        return None

    # ---- 单个 Worker ------------------------------------------------------
    async def _run_worker_by_id(self, task_id: str) -> None:
        task = self.table.get(task_id)
        if task is None or task.is_terminal:
            return
        span_id = task.span_id
        span = self.trace.get(span_id)
        # 同一任务第 2 次及以后执行（被追加要求 / 被批准 / 恢复）：
        # 旧的 span 已经是终态，直接复用会让时间轴错位，所以开一个新的执行段。
        if span is not None and span.ended_at is not None:
            parent = self._event_span_for(task) or (span.parent_id if span.parent_id else None)
            span = self.trace.start_span(
                KIND_WORKER, f"{task.name} (第{task.attempt}次)", parent_id=parent,
                status=ST_QUEUED, chain="→".join(task.trace_chain),
                input=_clip(task.prompt, 160), topic=task.topic, rerun=True,
            )
            span_id = span.span_id
            self.table.update(task_id, span_id=span_id)
        cur = asyncio.current_task()
        if cur is not None:
            self._running[task_id] = cur
        try:
            async with self._semaphore:
                now = time.monotonic()
                self.table.update(task_id, status=RUNNING, owner_node=self.node_id,
                                  started_at=task.started_at or now, last_activity=now)
                self.trace.publish(TraceEvent(kind="task_upsert", task=self.table[task_id].to_dict()))
                if span is not None:
                    span.status = ST_RUNNING
                    self.trace.push_span(span)
                await self._execute(self.table[task_id], span_id)
        finally:
            self._running.pop(task_id, None)
            # 1) 任务行落到终态（挂起不是终态）
            current_task = self.table.get(task_id)
            if current_task is not None and not current_task.is_terminal \
                    and current_task.status not in (NEEDS_HUMAN, WAITING):
                self.table.update(task_id, status=FAILED, ended_at=time.monotonic(),
                                  error="执行体异常结束（未捕获）")
            final = self.table[task_id]
            if final.is_terminal:
                self._cleanup_jobs(task_id)
            # 2) worker span 落到终态。
            #    挂起时**立即结束这一段执行**：那之后的时间是"停住等人"，
            #    留着 open 的 span 会让时间轴把它算成在跑（实测会把 34 秒的等待
            #    变成"并行收益"）。恢复执行时会另开一个新段。
            if span is not None and span.ended_at is None:
                if final.status in (NEEDS_HUMAN, WAITING):
                    span.end(ST_WAITING,
                             waiting_on=final.waiting_on or final.needs_human_reason,
                             note="停住等人（不计入执行时间）")
                else:
                    st = {SUCCEEDED: ST_OK, CANCELLED: ST_CANCELLED,
                          FAILED: ST_ERROR}.get(final.status, ST_ERROR)
                    if final.warning_count and st == ST_OK:
                        st = ST_WARN
                    span.end(st, result=_clip(final.result or "", 240),
                             chain="→".join(final.trace_chain))
                self.trace.push_span(span)
            self.trace.publish(TraceEvent(kind="task_upsert", task=final.to_dict()))

    async def _execute(self, task: TaskBrief, span_id: str) -> None:
        task_id = task.task_id
        ctx = WorkerContext(table=self.table, trace=self.trace, task_id=task_id,
                            span_id=span_id, rng=random.Random(hash(task_id) & 0xFFFF),
                            pool=self, model=self.model,
                            offline=isinstance(self.model, FakeChatModel))
        steers = self.pending_steers(task_id)

        token = CURRENT_CONTEXT.set(ctx)
        try:
            agent = await self._build_agent(task)
            prompt = task.prompt
            if steers:
                prompt += "\n\n【追加要求】" + "；".join(steers)
            stream = await stream_with_span_hooks(agent, self.trace, task, span_id,
                                                  table=self.table)
            ctx.checkpoint()
            msg = await run_agent_with_trace(agent, UserMsg("router", prompt), stream)
            ctx.checkpoint()
            text = msg.get_text_content() or ""

            # 挂起判定：工具调用了 wait_for / ask_human，说明这次停在"等人/等结果"上。
            # 工具把请求记在 ctx.wait_request 上（不靠异常，也不靠解析上下文）。
            wait_what = ctx.wait_request
            if wait_what:
                if wait_what.startswith("待批:"):
                    self.table.update(task_id, status=NEEDS_HUMAN,
                                      needs_human_reason=wait_what[3:])
                else:
                    self.table.update(task_id, status=WAITING, waiting_on=wait_what[3:],
                                      wait_seconds=ctx.wait_seconds,
                                      ended_at=time.monotonic())
                self.trace.publish(TraceEvent(kind="alert", alert={
                    "level": "INFO", "source": task.name, "span_id": span_id,
                    "message": ("需要你确认：" if wait_what.startswith("待批:") else "在等待：")
                               + wait_what[3:],
                }))
                return

            self.table.update(task_id, status=SUCCEEDED, result=text,
                              ended_at=time.monotonic(), last_activity=time.monotonic())
        except asyncio.CancelledError:
            raise
        except WorkerCancelled as exc:
            self.table.update(task_id, status=CANCELLED, result=f"已取消：{exc}",
                              ended_at=time.monotonic())
        except Exception as exc:  # noqa: BLE001
            self.table.update(task_id, status=FAILED, result=f"{type(exc).__name__}: {exc}",
                              error=f"{type(exc).__name__}: {exc}",
                              ended_at=time.monotonic())
            self._log_exception(f"worker:{task.name}", exc)
            span = self.trace.get(span_id)
            if span is not None:
                span.note(error=f"{type(exc).__name__}: {exc}")
                self.trace.push_span(span)
            self.trace.publish(TraceEvent(kind="alert", alert={
                "level": "ERROR", "source": task.name, "span_id": span_id,
                "message": f"Worker 失败：{type(exc).__name__}: {exc}",
            }))
        finally:
            CURRENT_CONTEXT.reset(token)
            self._drain_background(task_id)
            self.trace.publish(TraceEvent(kind="log",
                                          text=f"{task.name} → {self.table[task_id].status}"))

    def _log_exception(self, where: str, exc: BaseException) -> None:
        """记异常。日志不可用也不能影响失败处理本身。"""
        if self.log is None:
            return
        try:
            self.log.exception(where, exc)
        except Exception:  # pragma: no cover
            pass

    def _drain_background(self, task_id: str) -> None:
        """执行体结束时只清**执行体内**的后台协程。

        外部作业（``self.jobs``）不在这里取消：任务挂起后恢复时还要用它的结果。
        只有任务真正结束（完成 / 失败 / 取消 / 驳回）时才清理。
        """
        for bg in self._background.pop(task_id, []):
            if not bg.done():
                bg.cancel()

    def _cleanup_jobs(self, task_id: str) -> None:
        for key in [k for k in self.jobs if k.startswith(f"{task_id}:")]:
            job = self.jobs.pop(key)
            if not job.done():
                job.cancel()
        self.handles.pop(task_id, None)

    # ---- 等结果的恢复 -----------------------------------------------------
    def resume_waiting(self, task: TaskBrief) -> bool:
        """把「等结果」的任务拉回来继续跑。

        外部条件满足时（这里模拟为后台作业完成 / 时间到了），不需要重新理解上下文，
        同一条任务换一段新的执行继续即可。
        """
        if task.status != WAITING:
            return False
        task.add_intervention("resume", f"外部结果就绪（{task.waiting_on}）", actor="system")
        self.table.update(task.task_id, status=PENDING, waiting_on="",
                          attempt=task.attempt + 1)
        self.trace.publish(TraceEvent(kind="task_upsert", task=task.to_dict()))
        return True

    async def resume_waiting_all(self) -> list[TaskBrief]:
        """把所有「等结果」且外部条件确实满足的任务重新拉起。

        判断依据优先看**外部作业是否真的完成**，定时器只是兜底。否则会在作业没好的
        时候反复唤醒，时间轴上出现一串没意义的「第 2 次 / 第 3 次」。
        """
        ready = []
        now = time.monotonic()
        for task in self.table.all():
            if task.status != WAITING:
                continue
            jobs = [j for k, j in self.jobs.items() if k.startswith(f"{task.task_id}:")]
            if jobs:
                if not all(j.done() for j in jobs):
                    continue          # 作业还没好，别醒
            elif now - (task.ended_at or task.last_activity or now) < task.wait_seconds:
                continue              # 没有作业时按设定的等待时间
            if self.resume_waiting(task):
                ready.append(task)
        if ready:
            await self.run_tasks(ready)
        return ready

    async def _build_agent(self, task: TaskBrief) -> Agent:
        if self.worker_factory is not None:
            return await self.worker_factory(task)
        toolkit = build_toolkit(task.worker_type, call_log=self.call_log)
        return Agent(name=task.name, system_prompt=prompts.worker_prompt(task.worker_type),
                     model=self.model, toolkit=toolkit,
                     react_config=ReActConfig(max_iters=6),
                     model_config=ModelConfig(max_retries=2))


# =============================================================================
# 桥接：把 Agent 事件旁听成 Span
# =============================================================================
async def stream_with_span_hooks(agent: Agent, trace: TraceStore, task: TaskBrief,
                                 worker_span_id: str, *, table: TaskTable | None = None):
    original = agent.reply_stream

    async def stream(inputs_message: Any) -> Any:
        async for evt in original(inputs=inputs_message):
            if isinstance(evt, ToolCallStartEvent):
                # span 由 build_toolkit 里的包装器创建（那里才拿得到参数和返回值），
                # 这里只更新「最近调用了什么工具」给 Watchdog 用。
                if table is not None and task.task_id in table:
                    table.mark_progress(task.task_id, tool=evt.tool_call_name)
            elif isinstance(evt, ModelCallEndEvent):
                worker = trace.get(worker_span_id)
                if worker is not None:
                    prev = worker.detail.get("tokens") or {}
                    worker.detail["tokens"] = {
                        "in": int(prev.get("in", 0)) + int(evt.input_tokens),
                        "out": int(prev.get("out", 0)) + int(evt.output_tokens),
                    }
                    worker.revision += 1
                    trace.push_span(worker)
                if table is not None and task.task_id in table:
                    table.bump_tokens(task.task_id, tokens_in=int(evt.input_tokens),
                                      tokens_out=int(evt.output_tokens))
            yield evt

    return stream


async def run_agent_with_trace(agent: Agent, inputs_message: Msg, stream) -> Msg:
    """驱动一次完整 ReAct，返回最终消息。

    AgentScope 2.0 的 ``_reply_impl`` 正常结束只 yield ``ReplyEndEvent``，
    所以最终消息要从 ``agent.state.context`` 里取。
    """
    final: Msg | None = None
    async for item in stream(inputs_message):
        if isinstance(item, Msg):
            final = item
    if final is None:
        final = _last_assistant_msg(agent)
    if final is None:
        raise RuntimeError("Agent 没有产生最终消息")
    return final


def _last_assistant_msg(agent: Agent) -> Msg | None:
    try:
        for msg in reversed(list(agent.state.context)):
            if getattr(msg, "role", None) == "assistant":
                return msg
    except Exception:  # pragma: no cover
        return None
    return None


# =============================================================================
# Watchdog
# =============================================================================
class Watchdog:
    """外部看门狗：独立 task 巡检，判断「在动」还是「空转」，并且可以注入提示。

    三层：

    * **心跳**：长时间没活动 → SUSPECT（可能卡在长工具里）
    * **实质进展**：一直在调工具，但同一个工具连续 N 次 / token 不涨 → STALLED，
      并向 Worker 的上下文注入一条提示（这是「状态外置」才做得到的事：
      人能往一个正在跑的 Worker 里加话，而不需要打断它）
    * **自检**：Watchdog 自己卡死时，状态行会显示「上次扫描 Ns 前」
    """

    def __init__(self, table: TaskTable, trace: TraceStore, *,
                 interval: float = 3.0, suspect_after: float = 60.0,
                 stall_after: int = 3) -> None:
        self.table = table
        self.trace = trace
        self.interval = interval
        self.suspect_after = suspect_after
        self.stall_after = stall_after
        self._task: asyncio.Task[None] | None = None
        self.scans = 0
        self.last_scan_at: float | None = None
        self.injections: list[tuple[float, str, str]] = []
        self._alert_counts: dict[str, int] = {}

    # ---- 生命周期 ---------------------------------------------------------
    def start(self) -> "Watchdog":
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop(), name="watchdog")
        return self

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, RuntimeError):
                pass
            self._task = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def last_scan_age(self) -> float | None:
        return None if self.last_scan_at is None else time.monotonic() - self.last_scan_at

    async def _loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                return
            try:
                self.scan()
            except Exception:  # pragma: no cover
                pass

    # ---- 一次巡检 ---------------------------------------------------------
    def scan(self) -> dict[str, int]:
        self.scans += 1
        self.last_scan_at = time.monotonic()
        result = {"suspect": 0, "stalled": 0, "recovered": 0}
        for task in self.table.all():
            if task.is_terminal or task.is_blocked:
                continue   # 在等人 / 在等外部结果的任务不该被催
            if task.status in (RUNNING, CANCELLING):
                idle = task.idle_seconds
                if idle >= self.suspect_after:
                    if task.status == CANCELLING:
                        continue
                    self.table.update(task.task_id, status=SUSPECT,
                                      suspect_count=task.suspect_count + 1)
                    self._flag_span(task, f"无进展 {idle:.0f}s / 阈值 {self.suspect_after:.0f}s")
                    self.alert("WARN", task.name, task.span_id,
                               f"无进展 {idle:.0f}s → 疑似卡死")
                    result["suspect"] += 1
                elif task.repeat_streak >= self.stall_after:
                    # 在动，但原地打转：这才是更值得管的一种「忙」
                    streak = task.repeat_streak
                    self.table.update(task.task_id, status=STALLED)
                    self.inject(task, f"你已经连续 {streak} 次调用 {task.last_tool}，"
                                      f"请基于已有信息收尾，不要重复搜索")
                    self._flag_span(task, f"{task.last_tool} 连续 {streak} 次")
                    self.alert("WARN", task.name, task.span_id,
                               f"{task.last_tool} 连续调用 {streak} 次 → 无进展，已注入提示")
                    result["stalled"] += 1
            elif task.status in (SUSPECT, STALLED):
                if task.status == SUSPECT and task.last_activity and \
                        time.monotonic() - task.last_activity < self.suspect_after:
                    self.table.update(task.task_id, status=RUNNING)
                    self.alert("INFO", task.name, task.span_id, "恢复进展 → 回到执行中")
                    result["recovered"] += 1
        return result

    def inject(self, task: TaskBrief, message: str) -> None:
        """往任务的上下文里塞一条提示。Worker 下一个工具调用前会读到。"""
        self.injections.append((time.monotonic(), task.task_id, message))
        task.add_intervention("watchdog", message, actor="watchdog")
        repeat = task.repeat_streak
        task.repeat_streak = 0
        task.last_tool = ""          # 注入之后重新计数，否则下一轮又立刻触发
        self.table.update(task.task_id, warning_count=task.warning_count + 1)
        self.trace.publish(TraceEvent(kind="alert", alert={
            "level": "INFO", "source": task.name, "span_id": task.span_id,
            "message": f"注入提示（第 {repeat} 次连续调用）：{_clip(message, 50)}",
        }))

    def pending_hints(self, task_id: str) -> list[str]:
        return [m for _, tid, m in self.injections if tid == task_id]

    def _flag_span(self, task: TaskBrief, message: str) -> None:
        span = self.trace.get(task.span_id)
        if span is not None:
            span.add_warning(message)
            self.trace.push_span(span)

    def alert(self, level: str, source: str, span_id: str, message: str) -> None:
        self.trace.publish(TraceEvent(kind="alert", alert={
            "level": level, "source": source, "span_id": span_id, "message": message,
        }))


# =============================================================================
# Runtime
# =============================================================================
class Runtime:
    """把过滤、路由、执行、巡检串成一条链。UI 只调用 ``process()`` 并消费 ``events``。"""

    def __init__(
        self,
        *,
        model: ChatModelBase | None = None,
        router_model: ChatModelBase | None = None,
        gate: EventGate | None = None,
        node_id: str | None = None,
        persist: str | os.PathLike[str] | None = None,
        watchdog_interval: float = 3.0,
        suspect_after: float = 60.0,
        semantic_llm: bool = False,
        log: Any = None,
    ) -> None:
        self.events: asyncio.Queue[TraceEvent] = asyncio.Queue()
        self.log = log

        def publish(evt: TraceEvent) -> None:
            self.events.put_nowait(evt)

        self.trace = TraceStore(publish=publish)
        self.table = TaskTable(jsonl_path=persist)
        self.node_id = node_id or f"node-{uuid.uuid4().hex[:5]}"
        self.meter = ConcurrencyMeter()
        self.model = model or FakeChatModel(meter=self.meter)
        self.router = Router(router_model or self._default_router_model())
        self.pool = WorkerPool(self.table, self.trace, self.model, node_id=self.node_id,
                               log=self.log)
        self.gate = gate or EventGate()
        self.gate.semantic_llm = semantic_llm
        self.watchdog = Watchdog(self.table, self.trace, interval=watchdog_interval,
                                 suspect_after=suspect_after)
        self.history: list[GateResult] = []
        self.stats_counters = {
            "events": 0, "gate_reject": 0, "router_calls": 0,
            "direct": 0, "recall": 0, "spawn": 0, "ignore": 0,
            "tasks": 0, "needs_human": 0, "watchdog_inject": 0,
        }
        self._pending_recheck: list[str] = []
        self._resumer: asyncio.Task[None] | None = None
        if self.log is not None:
            self.trace.observe(self.log.event)
            self.log.write("runtime_ready", model=getattr(self.model, "model", "?"),
                           node=self.node_id, watchdog=watchdog_interval,
                           suspect_after=suspect_after,
                           router_model=getattr(self.router.model, "model", "?"))

    def _reject_reply(self, gate: GateResult) -> str:
        """被程序层挡下时也要给一句话，别让用户对着空屏幕。"""
        reason = gate.program_reason.replace("程序层: REJECT ", "")
        if "duplicate" in reason:
            return "这条和刚才那条一样，我不重复跑。要改条件的话，直接说改哪儿。"
        if "too_short" in reason:
            return "太短了，我没处理。多说一点，比如「帮我查一下成都周边的徒步路线」。"
        if "empty" in reason or "punctuation_only" in reason:
            return "没收到内容，再说一次？"
        if "deny_keyword" in reason:
            return "这条按规则跳过了。"
        if "rate_limited" in reason:
            return "短时间内消息太多，我先缓一下，几秒后再试。"
        return f"这条我没处理（{reason}）。"

    def _default_router_model(self) -> ChatModelBase:
        """Router 用哪份模型实例。

        假模型下单独造一个：否则 Router 自己的调用会把 Worker 的并发峰值抬上去。
        真实模型下默认复用同一个；想给 Router 配轻量模型时传 ``router_model=``。
        """
        if isinstance(self.model, FakeChatModel):
            return FakeChatModel(meter=ConcurrencyMeter())
        return self.model

    # ---- 生命周期 ---------------------------------------------------------
    async def start(self) -> None:
        self.watchdog.start()
        self._resumer = asyncio.create_task(self._resume_loop(), name="waiter")
        await self.pool.resume_orphans()

    async def stop(self) -> None:
        await self.watchdog.stop()
        if self._resumer is not None:
            self._resumer.cancel()
            try:
                await self._resumer
            except (asyncio.CancelledError, RuntimeError):
                pass
            self._resumer = None

    async def _resume_loop(self) -> None:
        """外面那个"外部条件满足"的模拟器：等结果到期的任务自动被拉回来跑。

        真实系统里这一步由 webhook / 轮询 / 订阅回调触发；Demo 里用一个定时循环代替。
        """
        while True:
            await asyncio.sleep(0.3)
            try:
                await self.pool.resume_waiting_all()
            except Exception:  # pragma: no cover
                pass

    # ---- 主链路 -----------------------------------------------------------
    async def process(self, raw: str, *, source: str = "console") -> "ProcessingResult":
        t0 = time.monotonic()
        self.stats_counters["events"] += 1
        self._log("input", raw=raw, source=source)

        gate = await self.gate.check(
            raw, source=source,
            semantic_fn=self._semantic if self.gate.semantic_llm else None,
        )
        self.history.append(gate)
        self._log("gate", event=gate.event_id, program=gate.program_pass,
                  program_reason=gate.program_reason,
                  semantic=gate.semantic_verdict, semantic_reason=gate.semantic_reason)

        if not gate.publishable:
            gate.route = ROUTE_GATE
            gate.route_note = gate.program_reason
            self.stats_counters["gate_reject"] += 1
            self._publish_gate(gate)
            return ProcessingResult(gate=gate, dispatched=[],
                                    final_text=self._reject_reply(gate),
                                    elapsed=time.monotonic() - t0, route=ROUTE_GATE)

        # 先尝试「订阅直通」：程序层就能判断的情况不必惊动 Router
        direct_target = self._find_subscriber(gate)
        if direct_target is not None:
            gate.route = ROUTE_DIRECT
            span = self.trace.start_span(KIND_EVENT, f"{gate.event_id} {ROUTE_DIRECT}",
                                         input=_clip(gate.raw, 120),
                                         route=ROUTE_DIRECT, source=source)
            gate.span_id = span.span_id
            restarted = self.pool.deliver(direct_target, gate.raw)
            gate.route_note = (f"直通 {direct_target.task_id}"
                               + ("（原任务已完成，重新拉起）" if restarted else "（追加到正在跑的 Worker）"))
            gate.task_ids = [direct_target.task_id]
            self.stats_counters["direct"] += 1
            self.stats_counters["router_skipped"] = self.stats_counters.get("router_skipped", 0) + 1
            span.end(ST_OK, chain="→".join(direct_target.trace_chain),
                     note=gate.route_note)
            self.trace.push_span(span)
            self._publish_gate(gate)
            if restarted:
                await self.pool.run_tasks([direct_target])
            result = ProcessingResult(gate=gate, dispatched=[direct_target],
                                      final_text=None, elapsed=time.monotonic() - t0,
                                      route=ROUTE_DIRECT)
            return result

        # 走到 Router：它会给出 spawn / recall / direct / ignore
        span = self.trace.start_span(KIND_EVENT, f"{gate.event_id}", input=_clip(gate.raw, 120),
                                     source=source)
        gate.span_id = span.span_id
        router_span = self.trace.start_span(KIND_ROUTER, "Router 判断",
                                            parent_id=span.span_id)
        living = self._living_digest()
        done = self._done_digest()
        try:
            decision = await self.router.decide(gate, living=living, done=done,
                                                global_context=self._context_digest())
        except Exception as exc:  # noqa: BLE001
            router_span.end(ST_ERROR, error=f"{type(exc).__name__}: {exc}")
            self.trace.push_span(router_span)
            span.end(ST_ERROR, error=str(exc))
            self.trace.push_span(span)
            self._publish_gate(gate)
            return ProcessingResult(gate=gate, dispatched=[], final_text=None,
                                    elapsed=time.monotonic() - t0, error=str(exc))
        self.stats_counters["router_calls"] += 1
        self._log("decision", event=gate.event_id, mode=decision.mode,
                  target=decision.target, why=decision.why,
                  briefs=[b.to_dict() for b in decision.briefs])

        if decision.mode == "answer":
            # 不需要动手的问题直接答，不要绕一圈说"我不派任务"
            gate.route = ROUTE_ANSWER
            gate.route_note = f"直接回答（{decision.why}）"
            self.stats_counters["answered"] = self.stats_counters.get("answered", 0) + 1
            try:
                reply = await self.router.answer(gate.raw)
            except Exception as exc:  # noqa: BLE001
                reply = f"这条我没能回答出来：{type(exc).__name__}"
                self._log_exception("router.answer", exc)
            if not reply:
                reply = await self.router.explain(gate.raw, decision.why)
            router_span.end(ST_OK, output="answer", why=decision.why)
            self.trace.push_span(router_span)
            reply_span = self.trace.start_span(KIND_ROUTER, "Router 回答",
                                               parent_id=span.span_id)
            reply_span.end(ST_OK, output=_clip(reply, 200))
            self.trace.push_span(reply_span)
            span.end(ST_OK, note=gate.route_note)
            self.trace.push_span(span)
            self._publish_gate(gate)
            return ProcessingResult(gate=gate, dispatched=[], final_text=reply,
                                    elapsed=time.monotonic() - t0, route=ROUTE_ANSWER)

        if decision.mode == "ignore":
            gate.route = ROUTE_IGNORE
            gate.route_note = f"Router 判断不用派活（{decision.why}）"
            self.stats_counters["ignore"] += 1
            router_span.end(ST_OK, output="ignore", why=decision.why)
            self.trace.push_span(router_span)
            # 不派活也要回话：用户问了一句，得有个交代。
            try:
                reply = await self.router.explain(gate.raw, decision.why)
            except Exception as exc:  # noqa: BLE001
                reply = f"这条我没派任务（{decision.why}）。"
                self._log_exception("router.explain", exc)
            reply_span = self.trace.start_span(KIND_ROUTER, "Router 回话",
                                               parent_id=span.span_id)
            reply_span.end(ST_OK, output=_clip(reply, 200))
            self.trace.push_span(reply_span)
            span.end(ST_OK, note=gate.route_note)
            self.trace.push_span(span)
            self._publish_gate(gate)
            return ProcessingResult(gate=gate, dispatched=[], final_text=reply,
                                    elapsed=time.monotonic() - t0, route=ROUTE_IGNORE)

        if decision.mode == "recall":
            router_span.end(ST_OK, output=f"recall → {decision.target}", why=decision.why)
            self.trace.push_span(router_span)
            return await self._recall(gate, decision, span, t0)

        if decision.mode == "direct":
            target = self.table.get(decision.target or "") if decision.target else None
            if target is None:
                # 模型给的 id 找不到：降级成新任务，不能静默丢事件
                decision.mode = "spawn"
            else:
                router_span.end(ST_OK, output=f"direct → {target.task_id}", why=decision.why)
                self.trace.push_span(router_span)
                self.pool.deliver(target, gate.raw)
                self.stats_counters["direct"] += 1
                gate.route = ROUTE_DIRECT
                gate.route_note = f"直通 {target.task_id}"
                gate.task_ids = [target.task_id]
                span.end(ST_OK, note=gate.route_note)
                self.trace.push_span(span)
                self._publish_gate(gate)
                if target.is_terminal:
                    await self.pool.run_tasks([target])
                return ProcessingResult(gate=gate, dispatched=[target], final_text=None,
                                        elapsed=time.monotonic() - t0, route=ROUTE_DIRECT)

        # spawn
        if not decision.briefs:
            # 模型说要开任务却没给任务单：退化成直接回答，
            # 否则用户会看到一句"任务表为空"，等于没答。
            gate.route = ROUTE_ANSWER
            gate.route_note = "直接回答（没拆出可执行的任务）"
            try:
                reply = await self.router.answer(gate.raw)
            except Exception as exc:  # noqa: BLE001
                reply = "这条我没能给出回答。"
                self._log_exception("router.answer", exc)
            router_span.end(ST_OK, output="answer-fallback", why=decision.why)
            self.trace.push_span(router_span)
            span.end(ST_OK, note=gate.route_note)
            self.trace.push_span(span)
            self._publish_gate(gate)
            return ProcessingResult(gate=gate, dispatched=[],
                                    final_text=reply or decision.why,
                                    elapsed=time.monotonic() - t0, route=ROUTE_ANSWER)

        self.stats_counters["spawn"] += 1
        gate.route = ROUTE_ROUTER
        router_span.end(ST_OK, output=f"{len(decision.briefs)} 张任务单", why=decision.why,
                        briefs=[b.to_dict() for b in decision.briefs])
        self.trace.push_span(router_span)
        tasks = self.pool.enqueue(decision.briefs, event_id=gate.event_id,
                                  parent_span_id=span.span_id)
        gate.task_ids = [t.task_id for t in tasks]
        gate.route_note = f"拆出 {len(tasks)} 个任务"
        self.stats_counters["tasks"] += len(tasks)
        result = await self.pool.run_tasks(tasks)

        digest = self._digest(tasks)
        synth_span = self.trace.start_span(KIND_ROUTER, "Router 合成",
                                           parent_id=span.span_id)
        try:
            final_text = await self.router.synthesize(digest)
            synth_span.end(ST_OK, output=_clip(final_text, 240))
        except Exception as exc:  # noqa: BLE001
            final_text = f"（合成失败：{exc}）"
            synth_span.end(ST_ERROR, error=str(exc))
        self.trace.push_span(synth_span)
        span.end(ST_ERROR if result.failed else ST_OK, summary=result.summary())
        self.trace.push_span(span)
        self._publish_gate(gate)
        return ProcessingResult(gate=gate, dispatched=tasks, final_text=final_text,
                                elapsed=time.monotonic() - t0, pool=result,
                                route=ROUTE_ROUTER)

    async def _recall(self, gate: GateResult, decision: Decision, span: Span, t0: float) -> "ProcessingResult":
        target = self._pick_recall_target(gate.raw, decision.target)
        if target is None:
            gate.route = ROUTE_GATE
            gate.route_note = "召回失败：没有可接续的任务"
            span.end(ST_WARN, note=gate.route_note)
            self.trace.push_span(span)
            self._publish_gate(gate)
            return ProcessingResult(gate=gate, dispatched=[], final_text=None,
                                    elapsed=time.monotonic() - t0, route=ROUTE_GATE)

        self.stats_counters["recall"] += 1
        chain = "→".join(target.trace_chain + ["新"])
        follow = Brief(worker_type=target.worker_type, topic=target.topic or "接续",
                       prompt=f"按以下新要求继续：{gate.raw}")
        tasks = self.pool.enqueue([follow], event_id=gate.event_id,
                                  parent_span_id=span.span_id, origin=target)
        # 上下文当缓存：把上一版的完整产出接给续做任务，而不是只留 80 字摘要
        self.pool.handoff(tasks, target)
        gate.route = ROUTE_RECALL
        gate.route_note = f"召回 {target.task_id} → 续做 {tasks[0].task_id if tasks else ''}"
        gate.task_ids = [t.task_id for t in tasks]
        span.note(chain=chain, recalled_from=target.task_id)
        for sp in self.trace.children_of(span.span_id):
            if sp.kind == KIND_ROUTER:
                sp.note(output=f"recall → {target.task_id}", chain=chain)
                self.trace.push_span(sp)
        # 召回是"接着已结束的活做"：先给它补一个 human span，让时间轴上看得出
        # 这次续写来自哪条任务
        human = self.trace.start_span(KIND_HUMAN, f"续写 {target.task_id}",
                                      parent_id=span.span_id, note=gate.raw)
        human.end(ST_OK)
        self.trace.push_span(human)
        result = await self.pool.run_tasks(tasks)
        span.end(ST_OK, chain=chain, note=gate.route_note)
        self.trace.push_span(span)
        self._publish_gate(gate)
        digest = self._digest(tasks)
        synth_span = self.trace.start_span(KIND_ROUTER, "Router 合成", parent_id=span.span_id)
        try:
            final_text = await self.router.synthesize(digest)
            synth_span.end(ST_OK, output=_clip(final_text, 240))
        except Exception as exc:  # noqa: BLE001
            final_text = f"（合成失败：{exc}）"
            synth_span.end(ST_ERROR, error=str(exc))
        self.trace.push_span(synth_span)
        return ProcessingResult(gate=gate, dispatched=tasks, final_text=final_text,
                                elapsed=time.monotonic() - t0, pool=result,
                                route=ROUTE_RECALL)

    # ---- 人工介入（给 UI 调用） -------------------------------------------
    async def approve(self, task_id: str, note: str = "批准") -> bool:
        ok = await self.pool.approve(task_id, note=note)
        if ok:
            self.stats_counters["needs_human"] = max(0, self.stats_counters["needs_human"] - 1)
        return ok

    def reject(self, task_id: str, note: str = "驳回") -> bool:
        ok = self.pool.reject(task_id, note=note)
        if ok:
            self.stats_counters["needs_human"] = max(0, self.stats_counters["needs_human"] - 1)
        return ok

    def needs_human(self) -> list[TaskBrief]:
        return [t for t in self.table.all() if t.status == NEEDS_HUMAN]

    # ---- Router 的输入摘要 -------------------------------------------------
    def _living_digest(self) -> str:
        rows = []
        for t in self.table.active():
            rows.append(f"- {t.task_id} {t.status} #{t.topic} :: {_clip(t.prompt, 60)}")
        return "\n".join(rows)

    def _done_digest(self) -> str:
        rows = []
        for t in sorted(self.table.all(), key=lambda x: x.ended_at or 0, reverse=True)[:8]:
            if t.is_terminal:
                rows.append(f"- {t.task_id} [{t.status}] #{t.topic} :: "
                            f"{_clip(t.result or t.prompt, 60)}")
        return "\n".join(rows)

    def _context_digest(self) -> str:
        recent = [f"- {g.event_id}: {'PASS' if g.program_pass else 'REJECT'} / "
                  f"{g.semantic_verdict} :: {_clip(g.raw, 50)}" for g in self.history[-5:]]
        return "\n".join(recent)

    def _digest(self, tasks: list[TaskBrief]) -> str:
        """给合成用的产出摘要。

        要带**实质内容**：只给状态的话，合成出来就是"已完成 1 项"这种废话，
        用户看不到他要的东西。
        """
        out = []
        for t in tasks:
            body = t.result or t.needs_human_reason or t.waiting_on or ""
            head = f"- {t.name} [{t.status}]"
            if t.status == "SUCCEEDED" and body:
                out.append(f"{head}\n  产出：{_clip(body, 1800)}")
            elif body:
                out.append(f"{head} {_clip(body, 200)}")
            else:
                out.append(head)
        return "\n".join(out)

    def _find_subscriber(self, gate: GateResult) -> TaskBrief | None:
        """在程序层判断：这条消息是否明显属于某条在管/刚结束的任务。

        这里刻意用**确定性规则**而不是模型：直通的意义就是省下 Router 这一次判断。
        匹配按「主题词的命中长度」打分，取最高分，避免多个任务共享一个字时误判。
        """
        text = gate.raw
        if _looks_like_recall(text):
            return None
        best: tuple[int, TaskBrief] | None = None
        for task in self.table.all():
            if not task.topic:
                continue
            topic = task.topic.rstrip("查证成文排期")
            if len(topic) < 2 or topic not in text:
                continue
            if not (task.is_active or (task.ended_at and time.monotonic() - task.ended_at < 300)):
                continue
            score = len(topic)
            if best is None or score > best[0]:
                best = (score, task)
        return best[1] if best else None

    def _pick_recall_target(self, text: str, hint: str | None) -> TaskBrief | None:
        """为「刚才那个 X」找到对应的旧任务。

        「刚才那个**文案**」应该命中成文任务，而不是最后结束的那条。
        按主题词 + 意图词打分，Router 给的目标只在明显吻合时才采信。
        """
        done = [t for t in self.table.all() if t.is_terminal]
        if not done:
            return None

        def score_of(task: TaskBrief) -> int:
            topic = task.topic.rstrip("查证成文排期")
            score = 0
            if topic and len(topic) >= 2 and topic in text:
                score += 3
            for kw, suffix in (("文案", "成文"), ("介绍", "成文"), ("文字", "成文"), ("稿", "成文"),
                               ("计划", "排期"), ("排期", "排期"), ("行程", "排期"),
                               ("查", "查证"), ("资料", "查证"), ("对比", "查证"), ("价格", "查证")):
                if kw in text and task.topic.endswith(suffix):
                    score += 2
            return score

        if hint:
            hinted = self.table.get(hint)
            if hinted is not None and (score_of(hinted) >= 2 or len(done) == 1):
                return hinted
        best = max(done, key=score_of)
        if score_of(best) > 0:
            return best
        return sorted(done, key=lambda t: t.ended_at or 0)[-1]

    async def _semantic(self, raw: str) -> tuple[str, str]:
        agent = Agent(name="semantic-gate", system_prompt=prompts.SEMANTIC_GATE_PROMPT,
                      model=self.router.model, react_config=ReActConfig(max_iters=1))
        msg = await agent.reply(UserMsg("event", raw))
        text = (msg.get_text_content() or "").strip()
        if text.upper().startswith("WORTH"):
            return "WORTH", re.sub(r"^WORTH\s*[:：]?\s*", "", text, flags=re.I)[:60] or "值得"
        return "NOT_WORTH", re.sub(r"^NOT_WORTH\s*[:：]?\s*", "", text, flags=re.I)[:60] or "不值得"

    # ---- 观测 -------------------------------------------------------------
    def _log(self, kind: str, **fields: Any) -> None:
        if self.log is not None:
            try:
                self.log.write(kind, **fields)
            except Exception:  # pragma: no cover - 日志不能影响主流程
                pass

    def _log_exception(self, where: str, exc: BaseException) -> None:
        if self.log is not None:
            try:
                self.log.exception(where, exc)
            except Exception:  # pragma: no cover
                pass

    def _publish_gate(self, gate: GateResult) -> None:
        self.trace.publish(TraceEvent(kind="event_gate",
                                      text=json.dumps(gate.to_dict(), ensure_ascii=False)))

    def stats(self) -> dict[str, Any]:
        out = dict(self.stats_counters)
        out.update({
            "tasks": len(self.table),
            "status": self.table.status_counts(),
            "attention": self.table.human_attention(),
            "router_skipped": out.get("router_skipped", 0),
            "peak": self.model.meter.peak if isinstance(self.model, FakeChatModel) else None,
        })
        return out


@dataclass
class ProcessingResult:
    gate: GateResult
    dispatched: list[TaskBrief]
    final_text: str | None
    elapsed: float
    pool: PoolResult | None = None
    error: str | None = None
    route: str = ""

    @property
    def accepted(self) -> bool:
        return self.gate.publishable

    def to_dict(self) -> dict[str, Any]:
        return {
            "event": self.gate.to_dict(),
            "dispatched": [t.task_id for t in self.dispatched],
            "final_text": self.final_text,
            "elapsed": round(self.elapsed, 3),
            "route": self.route,
            "finished": self.pool.summary() if self.pool else None,
            "error": self.error,
        }
