# 架构与实现

这篇是给"想知道它是怎么做的"的人看的。按面试讲项目的顺序组织：为什么这么设计、整体长什么样、每个模块怎么落地、当初踩了什么坑、哪些地方做了取舍。

代码量约 4000 行 Python，不含测试。技术栈：AgentScope 2.0（Agent 与工具调用）、标准库 `asyncio`（并发与调度）、标准库 `http.server`（浏览器控制台）、Rich（终端表格/文本渲染，仅用于日志和 CLI 输出）。

---

## 1. 问题：Agent 卡在人身上，不卡在模型上

单任务的模型能力已经够用。真正限制产出的是并发会话数：一个人同时能跟住的活跃上下文大约三到五个，超过之后所有成本都转移到人身上——自己开新会话、自己把补充条件贴回对应的会话、自己记住哪件事做到第几步、自己判断哪条通知值得打断手头的事。

所以这个项目的目标不是"让 Agent 更聪明"，而是**把会话管理从人脑搬到系统里**。

拆成四个具体动作：

1. 新事情来了，系统自己决定拆成几条任务，而不是让人开新窗口。
2. 补充条件来了，系统找到它属于哪件事，投过去，而不是让人复述上下文。
3. 用户说"刚才那个改一下"，系统找回原来的任务和它的产出，而不是让人重新描述。
4. 通知来了，系统先判断值不值得打扰人，而不是把一切都推给人。

这四条对应四个机制：**拆单 / 直通 / 召回 / 过滤**。整个架构都是围绕它们展开的。

---

## 2. 一个决定性的选择：状态离开调用栈

最常见的 Agent 写法是"一次请求 = 一个函数调用栈"：`handle(msg)` 里调模型、调工具、返回结果。栈一返回，这次会话的所有中间状态就没了。

这个项目把它反过来：**任务状态存在一张表里，而不是存在栈里**。

```
请求进来 → Router 判断 → 往任务表里写入 1~N 条任务 → Worker 并行执行 → 结果写回任务表
```

这个选择带来四个直接后果，也是整个设计的骨架：

| 因为状态在表里 | 所以能做到 |
| --- | --- |
| Worker 是临时执行体 | 跑完就丢，不占常驻内存；同一件事可以换执行体继续 |
| 任务有稳定 id | 补充条件能定位到"哪件事"，而不是"哪个会话" |
| 任务有状态机 | 挂起、等人、取消、失败都能表达，而不只是"返回了/没返回" |
| 任务有来源链 | "刚才那个"能顺着链找回去，把上一版产出当上下文接上 |

`state.py` 里 `TaskBrief` 就是这条记录，`TaskTable` 是它的容器。

---

## 3. 整体结构

```
                    ┌─────────────────────────────┐
   浏览器 ──HTTP──▶ │ web.py  (ThreadingHTTPServer)│
                    │  · 静态页 ui/console.html    │
                    │  · /api/state 增量轮询       │
                    │  · /api/message 提交         │
                    └──────────────┬──────────────┘
                                   │ run_coroutine_threadsafe
                    ┌──────────────▼──────────────┐
                    │ Runtime (asyncio 单事件循环) │
                    │                              │
  用户消息 ────────▶ │  EventGate  双层过滤         │
                    │      ↓                       │
                    │  Router     四选一判断       │
                    │   ├─ answer  直接回答        │
                    │   ├─ ignore  不派活但回话     │
                    │   ├─ direct  订阅直通        │
                    │   ├─ recall  链式召回        │
                    │   └─ spawn   拆单            │
                    │      ↓                       │
                    │  WorkerPool  asyncio.gather  │
                    │   ├─ Worker A (researcher)   │
                    │   ├─ Worker B (planner)      │
                    │   └─ Worker C (writer)       │
                    │      ↓                       │
                    │  TaskTable  ← 状态写回        │
                    │  TraceStore ← span 事件流     │
                    └──────────────┬──────────────┘
                                   │ asyncio.Queue
                    ┌──────────────▼──────────────┐
                    │ 观察者：Web 轮询 / 运行日志   │
                    └─────────────────────────────┘

   Watchdog：独立的 asyncio 任务，周期性扫描 TaskTable
```

三条依赖纪律：

- `trace.py` 不 import agentscope，也不 import web。
- `runtime.py` 不 import web。
- 界面只通过 `Runtime.process()` 和事件流交互，是可替换的观察者（之前有 Textual 版，现在只剩浏览器版，切换时 `runtime.py` 一行没改）。

---

## 4. 双层事件过滤（EventGate）

为什么要两层：**确定性的判断不该花模型调用**。

第一层是程序层，纯规则，零成本：

| 规则 | 拦什么 | 为什么放第一层 |
| --- | --- | --- |
| 长度 / 空白 / 纯标点 | `嗯`、`.` | 一眼可判 |
| 关键词黑名单 | `忽略我`、`退订` | 确定性规则，不该花钱 |
| 去重窗口 | 5 分钟内重复的同一句话 | 需要状态，但不需要模型 |
| 限流 | 每分钟超过 12 条 | 保护后端 |

第二层是语义层，判断"值不值得行动"，但它**只做标注，不做拦截**。

这一条是踩坑改出来的。最初语义层返回 `NOT_WORTH` 就直接丢弃消息，结果用户问"你能做什么"时，这句话根本到不了 Router，屏幕上什么都不会出现——用户以为系统死了。改法是把拦截权全部收归 Router：语义层只给一个 `worthwhile` 标记，Router 有 `answer`、`ignore`、`spawn` 等出口，**无论选哪个都必须产出回复**。

```python
# runtime.py · EventGate.check()
ok, reason = self.program_layer(raw, source)
if not ok:
    return GateResult(..., publishable=False, ...)   # 只有程序层能拦
verdict, sreason = await semantic_fn(raw) if semantic_fn else self.semantic_layer(raw)
return GateResult(..., publishable=True,              # 语义层只留标记
                  semantic_verdict=verdict, ...)
```

---

## 5. Router：只判断，不执行

Router 是一个 AgentScope `Agent`，但**不给它任何工具**（`toolkit=None`）。它唯一的输出是一段 JSON：

```json
{"mode": "direct|recall|spawn|answer|ignore",
 "target": "任务 id 或 null",
 "why": "一句话理由（显示在界面上）",
 "tasks": [{"worker_type": "researcher", "prompt": "...", "topic": "短主题"}]}
```

为什么强制 JSON：Router 的判断要能**被程序消费**——要投递就得知道投给谁，要拆单就得知道拆几张。自然语言在这里没法用。

为什么 `max_iters=1`：Router 不该有 ReAct 循环。它看不到 Worker 的中间过程，只根据任务表的摘要做决定。这样它也便宜——可以换更小的模型（`Runtime(router_model=...)` 支持单独指定）。

五种去向的语义：

| mode | 触发条件 | 后果 |
| --- | --- | --- |
| `spawn` | 需要新产出 | 拆 1~3 条任务，并行跑 |
| `direct` | 在给某件在办的事补条件 | 投递给那条任务，不动其他 |
| `recall` | 在说已结束的某件事 | 派生续做任务，接上原产出 |
| `answer` | 要解释，不要结果 | 直接答，不起任务 |
| `ignore` | 纯噪音 | 回一句"没派任务"，不装死 |

`answer` 是后来加的。之前只有 `ignore`，模型遇到"介绍一下 Raft 和 Paxos"会判成"不值得开任务"，然后只回一句"我没派任务"——技术上是自洽的，体验上是坏的。加了 `answer` 之后，概念性问题直接答，需要动手才拆单。

---

## 6. 直通（direct）：省掉一次模型调用

判断"这条消息属于哪件在办的事"，不一定要问模型。程序层用订阅键就能做：

```python
# state.TaskBrief.topic：拆单时由 Router 给出，2~6 字的短主题
# Runtime._find_subscriber()：按主题词命中长度打分，取最高分
```

命中就直接投递，`Router.decision_count` 不增加。界面上会显示"Router 判断 4 次，省下 1 次"——省下多少是可度量的，这是这个设计能被验证的地方。

投递分两种情况：

- 任务还在跑 → 追加进该 Worker 的上下文（steer），不打断它。
- 任务已结束 → 拉回来重跑一次，但**还是同一条 task**，不新建。

第二种情况体现了"状态在表里"的好处：执行体已经销毁了，任务还在，可以重新拉起。

---

## 7. 召回（recall）：把上下文当缓存

`TaskBrief` 里存了 `origin_task_id` 和完整的 `trace_chain`。遇到"刚才那个改一下"，系统在已结束的任务里按主题和意图词打分选目标，派生一条新任务挂在它后面。

关键细节是**交接**：续做任务不能只拿到一句"在上一版基础上改"，它需要上一版的内容。

```python
# runtime.py · WorkerPool.handoff()
block = f"【上游任务 {upstream.name} 的产出，直接使用，不要重复检索】\n{upstream.result}"
for t in tasks:
    t.prompt = f"{block}\n\n你的任务：\n{t.prompt}"
```

早期版本只把上一版截断到 80 字塞进提示，等于把缓存丢了。现在接全文，并且同步更新 trace 里该 span 的 `input`——trace 上能直接看到"这条任务拿到了什么上下文"。

> 一个失败的尝试：我试过让 researcher 先跑完再跑 planner/writer，用产出做交接。测试立刻抓到并发峰值从 2 掉到 1——那等于把并行架构改成串行，已回退。并行是底线，交接只放在本来就有现成结果的场景（召回/续做）。

---

## 8. 拆单与并行执行（WorkerPool）

`asyncio.gather` 并行拉起，`max_concurrency` 限制峰值。每个 Worker 是独立 `Agent` 实例，按角色挂不同工具集：

| 角色 | 工具 | 用途 |
| --- | --- | --- |
| researcher | `webfetch`、`wait_for` | 抓资料、等外部结果 |
| planner | `make_plan`、`ask_human`、`start_job`、`stop_job` | 排期、不可逆动作前请示 |
| writer | `draft`、`webfetch` | 成文 |

Worker 之间**上下文隔离**：各带各的 ReAct 循环，互相看不见，结果只有写回任务表的那部分会被 Router 看到。这样失败是局部的——一个 Worker 崩了不影响其他人。

---

## 9. 两个挂起态：这套设计里最关键的部分

Agent 系统最容易做坏的地方是"等"。等人工审批、等外部接口返回，如果用一个 `await` 卡在那里，整个执行体就废了。

这里把"等"建模成**任务状态**，而不是阻塞调用：

| 状态 | 等什么 | 谁恢复它 |
| --- | --- | --- |
| `WAITING` | 外部结果 | 调度器检测条件满足后自动拉起 |
| `NEEDS_HUMAN` | 人点头 | 用户在界面上点「批准」/「驳回」 |

两者都是**非终态**：不占执行体、不被 Watchdog 催、不计入"在做"。

实现上有个坑：**挂起不能用抛异常表达**。AgentScope 会把工具异常当成普通 `tool_result` 回填给模型，ReAct 继续跑，"挂起"就丢了。所以工具只是设一个哨兵：

```python
ctx.wait_request = f"等:{job}结果"     # 或 "待批:..."
```

运行时在 ReAct 结束之后统一检查这个字段，决定把任务置成 `WAITING` 还是 `NEEDS_HUMAN`。

第二个坑：**恢复时不能重新从头执行**。否则会"启动作业 → 挂起 → 恢复 → 又启动作业"地循环，直到 `max_iters` 耗尽，最后报失败。修法是让外部作业的句柄挂在池子上（跨执行体存活），恢复时直接取结果：

```python
# 执行体可以丢，外部资源不能跟着丢
self.jobs[task_id] = asyncio.ensure_future(runner())
```

第三个坑：**恢复条件要用事实判断，不要用定时器**。最初用 `waited >= wait_seconds` 唤醒，作业没好的时候会被反复唤醒，时间轴上出现一串没意义的"第 2 次 / 第 3 次"。改成先看作业是否 `done()`，定时器只做兜底。

---

## 10. Watchdog：独立于 Agent 之外

它不是一个 Agent，是一个普通 `asyncio` 任务，周期性扫任务表，判两件事：

1. **长时间没有活动** → 疑似卡死，标 `SUSPECT`。
2. **一直在调工具但同一个连续 N 次** → 无进展，标 `STALLED` 并注入提示。

第二种是真正有价值的：Agent 看起来在忙，实际上在原地打转。判定后它往 Worker 的上下文里塞一句话（"你已经连续 4 次调用 webfetch，基于已有信息收尾"），而不是直接 cancel。收不收尾由 Worker 在下一个安全点决定。

它**从不 cancel**，也不覆盖已经设置的取消标记——这是刻意的职责边界：Watchdog 负责发现和提示，负责终止的是人或者用户操作。

它自己也可能卡住，所以有一个 `last_scan_age` 自检，界面上能直接看到"上次扫描 N 秒前"。

注入算**系统替人做的干预**，记在"系统自己救 N 次"上；人点批准才算"你出手 N 次"。两个数分开，才看得出这个系统到底替人省了多少事。

---

## 11. 取消：协作式

`cancel_all()` 不杀协程，只设标记。Worker 在安全点检查：

```python
def checkpoint(self):
    if self.task.status in (CANCELLING, CANCELLED):
        raise WorkerCancelled(...)
```

安全点包括：每次工具调用前、每次模型调用前。这样取消不会留下"写了一半的状态"。

---

## 12. Trace：三层模型

参照 Langfuse / OpenTelemetry 的分层：**trace（一次请求）→ span（一步）→ 具体工具/模型调用**。每一层都有开始结束时间、状态、输入输出。

数据结构在 `trace.py`：

```python
@dataclass
class Span:
    span_id, parent_id, kind, name, status
    started_at, ended_at
    detail: dict          # input / output / error / tokens / warnings
    revision: int         # 增量渲染用
```

`kind` 分四种：`event`（一次用户请求）、`router`（一次判断或合成）、`worker`（一条任务）、`tool`（一次工具调用）。

三个视图回答三个不同的问题：

- **时间轴**：为什么是这个耗时。横轴是时间，每个 Worker 一条泳道，能看出谁和谁并行、关键路径是谁。
- **调用树**：谁拉起了谁。`EV-001 → Router 判断 → 各 Worker → 各自的工具`。
- **任务生命周期**：每条任务一行，来源链、订阅键、人出手和系统自愈次数。

### 工具参数是怎么进 trace 的

AgentScope 的 `ToolCallStartEvent` 只带 `tool_call_name` 和 `tool_call_id`，**不带参数和返回值**。所以之前 trace 里工具节点是空的。

解法是在注册工具时包一层（`runtime.build_toolkit`），在工具本体前后自己记：

```python
span = ctx.trace.start_span(KIND_TOOL, fn.__name__, parent_id=ctx.span_id,
                            input=json.dumps(kwargs))
try:
    out = await fn(*args, **kwargs)
except Exception as exc:
    span.end(ST_ERROR, error=f"{type(exc).__name__}: {exc}")
    raise
span.end(ST_OK, output=_clip(_toolchunk_text(out), 1200))
```

这也是"工具做不到就报错"的实现位置：`webfetch` 抓不到页面就抛 `ToolError`，trace 里立刻看到红节点和错误原因，模型也会收到这个错误而不是一份编造的摘要。

### 时间轴算法上的两个坑

1. **挂起的时间不能算成在执行**。早期版本里 `WAITING` 的 Worker span 一直不结束，于是"等人批"的 34 秒被算进执行时间，得出"并行省下 34 秒"的假结论。现在挂起时立即结束该段 span，恢复时另起一段；时间轴上单独标 `░`，不计入"同时忙"。
2. **关键路径只取最晚结束的执行链**，不建完整依赖图。Worker 之间没有显式依赖，这样够用，也避免假精确。

---

## 13. 浏览器控制台

没有用 Web 框架，只有标准库：

- `ThreadingHTTPServer` 处理 HTTP。
- Runtime 跑在另一个线程的 `asyncio` 事件循环里，提交消息用 `asyncio.run_coroutine_threadsafe`。
- 共享状态用一把 `threading.Lock` 保护，`SharedState` 负责跨线程传递。
- 前端是 `ui/console.html`，纯原生 JS，**500ms 轮询 `/api/state?since=N` 增量拉事件**。

为什么轮询而不是 WebSocket：Runtime 本身就是事件流（`asyncio.Queue`），轮询加游标 `seq` 已经能做到增量，省掉一个长连接和一套心跳逻辑。代价是延迟最多 500ms，对一个"看 Agent 干活"的界面够用。

前端有两个容易做错的点，都踩过：

1. **对话区不能整体重绘**。原来每次轮询都重建 `#chat.innerHTML`，用户正在选中的文字会被立刻清掉。改成**只追加**：记住已渲染条数，只 append 新消息。
2. **任务表和 Trace 各自算签名**，没变化就不碰 DOM，否则 500ms 一次的刷新会让页面一直闪。

`trace.py` 的增量渲染同理：树节点按指纹比对，只更新变化的行；终端版本会在状态行显示"重建 N / 累计跳过 M"，跳过数远大于重建数才说明增量生效。

---

## 14. 运行日志与问题定位

每次运行写一个目录：

```
logs/run-<时间>/
  events.jsonl   每个 TraceEvent 一行（带 seq、时间戳）
  session.log    人读的流水：输入、过滤判定、Router 决定、异常栈
  summary.json   退出时的统计快照
```

Runtime 里所有关键节点都会写一条结构化记录：

```python
log.write("input", raw=..., source=...)
log.write("gate", program=..., program_reason=..., semantic=..., semantic_reason=...)
log.write("decision", mode=..., target=..., why=..., briefs=[...])
```

配合 `tools/inspect_log.py` 直接给结论：

```
输入 3 条，模型判定 2 次，异常 1 条
── 每条输入的去向 ──
  1. 帮我查一下成都周边徒步路线…  spawn   程序层: PASS
  2. 嗯                        拦截    程序层: REJECT too_short(len=1)
── 失败的任务 ──
  writer-f37ca2  ModelFault: injected model failure
── 异常 ──
  [worker:writer-f37ca2] ModelFault … runtime.py line 195 raise ModelFault(...)
结论：1 条异常、1 个任务失败
```

这是"能定位"和"靠猜"的区别。用户说"上次它没回我"，跑一下这个脚本就能看到那次输入在哪一步断掉。

---

## 15. 模型接入

AgentScope 的 `OpenAIChatModel` 支持自定义 `base_url`，所以任何 OpenAI 兼容服务都能接。华为云 MaaS 的坑是 **`base_url` 必须挂在 credential 上**，放进 `client_kwargs` 会报重复参数：

```python
OpenAIChatModel(
    credential=OpenAICredential(api_key=..., base_url="https://api.modelarts-maas.com/openai/v1"),
    model="deepseek-v4.1-flash", ...
)
```

配置写在 `config.json`（进 `.gitignore`），支持预设、环境变量、命令行三级覆盖。设置页改完会重建模型实例换进正在跑的 Runtime，不用重启。

`FakeChatModel` 是离线用的确定性假模型：它按关键词决定路由、决定调几次工具、还能注入故障（`<<FAIL>>`、`<<LOOP>>`）。它的作用是让测试和 headless 冒烟不依赖网络和花钱——但**它不产出假数据给真实工具**，离线模式下 `webfetch` 直接报错。

---

## 16. 测试策略

96 个测试，重点不是覆盖界面细节，而是守住几条硬约束：

| 约束 | 怎么测 |
| --- | --- |
| Router 不挂工具 | 断言 `runtime.router.model` 的 toolkit 为 None、决策只产出 JSON |
| Worker 真并行 | 并发峰值 `meter.peak >= 2` |
| 直通不增加判断次数 | `decision_count` 前后不变，且任务 id 不变 |
| 召回选对目标 | 造多条已结束任务，断言召回命中预期那条 |
| 挂起可恢复 | 挂起 → 批准 → 断言继续跑完 |
| Watchdog 不越界 | 注入后任务状态不是 CANCELLED |
| 时间轴不算等待 | 构造等 30s 的场景，断言 `parallel_saved` 不虚高 |
| 工具不造假 | 离线模式下 `webfetch` 抛 `ToolError` |

跑法：

```bash
python -m pytest        # 96 passed
python headless.py      # 场景冒烟，断言结构
```

---

## 17. 取舍与没做的事

面试里最容易被追问的是"你这个方案的边界在哪"，这里是诚实的答案：

| 没做 | 为什么 | 真要做的话 |
| --- | --- | --- |
| 多渠道接入 | 需要各平台的鉴权与事件格式 | 抽象一个 `EventSource`，把 `process()` 的入口换掉 |
| 多副本调度 | 单进程够验证架构 | 任务表换 Redis/Postgres，`owner_node` 加租约 |
| 上下文容量管理 | Worker 跑完即弃，暂时不需要 | 任务间传摘要 + 溢写到磁盘 |
| 成本控制 | 没有预算维度 | 在 `ModelCallEndEvent` 上累计 token，超限降级 |
| 权限体系 | 工具全是本地只读 | 接 AgentScope 的 permission 机制做审批 |
| 专用搜索 | 按需求只保留 webfetch | 挂 Tavily/博查，把 `webfetch` 换成搜索接口 |

---

## 18. AgentScope 到底用在哪（以及为什么编排不用它）

这是最容易被追问的一题：**既然调度是 asyncio，那 AgentScope 是不是可有可无？**

诚实的答案是：**编排层一行 AgentScope 都没有，但执行单元完全依赖它**。

### 编排层：纯 asyncio

任务表、状态机、拆单、并行拉起、挂起恢复、巡检、取消，全部是标准库：

```python
# 并行拉起
await asyncio.gather(*coros, return_exceptions=True)

# 后台作业（跨执行体存活）
self.jobs[task_id] = asyncio.ensure_future(runner())

# Watchdog 是独立任务
self._task = asyncio.create_task(self._loop(), name="watchdog")

# 挂起后的自动恢复
self._resumer = asyncio.create_task(self._resume_loop(), name="waiter")
```

这一层没有任何框架介入。**这是刻意的**：如果让框架来管多 Agent 的生命周期，任务状态就会被绑在框架的会话对象上，也就没法做到"执行体销毁、任务还在表里、可以重新拉起"——而那正是整个设计的地基。

### 执行层：一个 Worker = 一个 AgentScope Agent

```python
# runtime.py · WorkerPool._build_agent()
return Agent(name=task.name,
             system_prompt=prompts.worker_prompt(task.worker_type),
             model=self.model,                        # ChatModelBase
             toolkit=toolkit,                         # Toolkit / FunctionTool
             react_config=ReActConfig(max_iters=6),   # ReAct 循环上限
             model_config=ModelConfig(max_retries=2)) # 重试策略
```

单个 Worker 内部要做的事——多轮「模型思考 → 调工具 → 看结果 → 再思考」——全部由 AgentScope 的 ReAct 循环完成。我提供的是工具集和消息，拿回来的是最终文本。

### 具体拆一下它提供了什么

| AgentScope 的能力 | 我在哪用 | 不用它会怎样 |
| --- | --- | --- |
| `Agent.reply_stream()` + ReAct 循环 | Worker 执行体 | 要自己写多轮工具调用循环、终止判断、上下文拼装 |
| `Toolkit` / `FunctionTool` | 按角色注册工具 | 要自己从函数签名和 docstring 生成 JSON Schema、解析模型返回的 tool_call |
| `ChatModelBase` | `FakeChatModel` 继承它；`OpenAIChatModel` 接华为云 | 要自己对每家 API 写一遍请求/流式解析 |
| `Msg` / `UserMsg` / `TextBlock` | 所有模型输入输出 | 要自己定一套消息协议 |
| `ToolCallStartEvent` / `ModelCallEndEvent` | trace 埋点、token 统计 | 拿不到执行中的事件，只能看最终结果 |
| `PermissionDecision` | 工具授权（现在是 ALLOW 桩） | 没有权限钩子 |

### 一句话总结

> **AgentScope 负责"一个 Agent 怎么想"，asyncio 负责"多个 Agent 怎么协作"。**

这两件事恰好是分层的：框架擅长的部分（ReAct、工具协议、模型适配）交给框架；框架不擅长的部分（跨会话的生命周期、状态外置、挂起恢复、可观测）自己写。

如果一个项目只用 AgentScope 的 `msghub` / pipeline 做多 Agent，那 Router 和 Worker 的关系就会被框架的会话模型框住，任务状态也难以外置。这个项目把编排抽出来的原因就在这里——**不是重复造轮子，是为了让任务状态能离开执行体**。

---

## 19. 如果只记三件事

1. **状态离开调用栈**，才有并行、追加、取消、恢复。这是整套设计的地基。
2. **"等"要建模成状态**，不能建模成阻塞。挂起和恢复是这套系统里最容易做错、也最能体现功底的地方。
3. **每个异步入口都必须有出口**。不派活也要回话，工具失败要报错不要编数据——用户感知到的"系统坏了"，往往是某条路径静默返回了空。
