# mini_super_agent

一个可运行、可阅读、可修改的 **Super Agent 运行时** mini 复刻。

它把 DSH（DeepSeek Harness）里「Router 常驻只分派、Worker 临时执行、任务状态放在调用栈之外」这套骨架单独抽出来实现，用来验证和演示多会话管理、事件过滤、订阅直通、链式召回、挂起与人工介入、外部 Watchdog，以及配套的 trace 与运行日志。

这不是 DSH 本身，也不是一个面向生产的完整 Agent 系统，只保留了架构主干。如果你想看设计取舍和实现细节，请读 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

- 技术栈：AgentScope 2.0（Agent 与工具调用）、Python 标准库 `asyncio`（并发与调度）、标准库 `http.server`（浏览器控制台）、原生 HTML/JS 前端。
- 规模：约 5,600 行 Python（不含测试），96 个测试。
- 形态：单进程、本地运行。

## 快速开始

```bash
pip install -r requirements.txt

# 首次使用先配置模型（也可以启动后在网页右上角「设置」里填）
python tools/setup_model.py --ping

# 启动浏览器控制台，打开 http://127.0.0.1:8080
python web.py
```

Windows 下可以直接双击 `start_web.bat`：它会自行定位 Python、检查依赖、启动服务并在端口绑定后打开浏览器。

不想启动界面、只想看完整链路时，用无头模式：

```bash
python headless.py                 # 跑主剧本「一个忙的上午」
python headless.py --list          # 列出全部场景
python headless.py --scenario stall
```

## 它怎么处理一句话

任务状态不放在函数调用栈里，而是放在一张任务表里。请求进来后：

```
请求 → 事件过滤 → Router 判断 → 写入 1~N 条任务 → Worker 并行执行 → 结果写回任务表
```

Router 只有五种去向，每种都必须在控制台上给出回应，不派活也要说话：

- `spawn`：新事情，拆成 1~3 条自包含的任务单并行执行；
- `direct`：是某件正在办或刚办完的事的补充要求，直接投递给那条任务，不再拆单；
- `recall`：是「刚才那个」的延续，顺着任务链找回旧任务派生续做；
- `answer`：不需要动手的概念解释、闲聊，直接回答；
- `ignore`：纯噪音，回一句说明为什么不处理。

任务有两种「等」的状态，都是非终态、不占执行体、不会被 Watchdog 催：

- `WAITING`：等外部结果（异步工具的作业还没回来）；
- `NEEDS_HUMAN`：不可逆动作（订票、下单、删除、发消息等）前停下来等用户批准。

在浏览器里，等待人工确认的任务会显示「批准 / 驳回」按钮。

## 核心机制

- **双层事件过滤**（`runtime.EventGate`）：程序层用确定性规则挡住空消息、纯标点、黑名单、去重和限流，零模型成本；语义层只做「值不值得行动」的标注，拦截权统一留给 Router。
- **订阅直通**（`Runtime._find_subscriber` / `WorkerPool.deliver`）：每条任务带一个 `topic` 作为订阅键，新消息命中在管任务的 topic 就直接投递，省掉一次 Router 判断；被追加的消息会写进任务的介入记录。
- **链式召回**（`Runtime._pick_recall_target` / `WorkerPool.handoff`）：任务记录 `origin_task_id` 和完整 `trace_chain`，召回时按主题与意图打分选目标，并把上一版的完整产出作为上下文交给续做任务。
- **并行执行**（`WorkerPool.run_tasks`）：用 `asyncio.gather` 并行拉起，`max_concurrency` 限制峰值；Worker 之间上下文隔离，一个失败不影响其他。
- **挂起与恢复**：工具不抛异常表达挂起，而是把请求写进 `ctx.wait_request`（`待批:` 或 `等:`），由运行时在 ReAct 结束后统一置状态；外部作业句柄挂在池子上，跨执行体存活，恢复时直接取结果而不是从头重跑。
- **Watchdog**（`runtime.Watchdog`）：独立于 Agent 的巡检任务，判断「长时间无活动」和「一直调同一个工具无进展」；只标记并向 Worker 上下文注入提示，从不取消任务。
- **协作式取消**（`WorkerContext.checkpoint`）：取消只设标记，Worker 在工具/模型调用前的安全点检查并退出，不会留下半截状态。
- **Trace 与日志**：`trace.py` 记录 span 树、时间轴与任务生命周期；每次运行落一份可检索的日志。

## 模型配置

配置写在 `config.json`（已加入 `.gitignore`，不会进版本库），支持供应商预设。优先级从高到低：命令行参数 > 环境变量 > `config.json` > 内置默认值。

| provider | 默认 base_url | 说明 |
| --- | --- | --- |
| `huawei-maas` | `https://api.modelarts-maas.com/openai/v1` | 华为云 MaaS，模型名如 `deepseek-v4.1-flash`、`qwen3-32b` |
| `deepseek` | `https://api.deepseek.com/v1` | DeepSeek 官方 |
| `openai` | `https://api.openai.com/v1` | OpenAI |
| `ollama` | `http://127.0.0.1:11434/v1` | 本地模型 |
| `fake` | — | 内置确定性假模型，不联网，仅用于测试和离线演示 |

API Key 也可以放在 `MAAS_API_KEY` / `DEEPSEEK_API_KEY` / `OPENAI_API_KEY` 等环境变量里。设置页改完会重建模型实例换进正在运行的 Runtime，无需重启。

`config.example.json` 是配置模板，真实 Key 只留在本机的 `config.json`。

## 工具与抓取

每个角色挂一套独立的工具集：

| 角色 | 工具 |
| --- | --- |
| researcher | `webfetch`、`wait_for` |
| planner | `make_plan`、`ask_human`、`start_job`、`stop_job` |
| writer | `draft`、`webfetch` |

`webfetch` 抓取指定的 http(s) 页面并抽取正文；`draft` / `make_plan` 由模型生成；`start_job` / `stop_job` 是后台抓取与取回结果。项目没有内置搜索引擎或第三方搜索 API：演示用的假模型会把关键词拼成一个搜索页 URL 交给 `webfetch` 抓取，效果取决于目标站点。

工具「做不到就报错」，不返回编造的数据：抓取失败会抛 `ToolError`，错误会作为工具结果回给模型，也会在 trace 里留下红色节点。离线假模型模式下 `webfetch` 直接报错并说明「不联网」。

## 日志与问题定位

每次运行写一个目录 `logs/run-<时间>/`：

| 文件 | 内容 |
| --- | --- |
| `events.jsonl` | 每个 TraceEvent 一行，含 seq、时间戳、span/任务/告警 |
| `session.log` | 人读的流水：输入、过滤判定、Router 决定、异常栈 |
| `summary.json` | 退出时的统计快照（崩溃也会尽量写入） |

`tools/inspect_log.py` 直接给出定位结论：

```bash
python tools/inspect_log.py            # 最近一次：输入去向、失败任务、慢步骤、异常
python tools/inspect_log.py --errors   # 只看异常栈
python tools/inspect_log.py --list     # 历史运行列表
```

日志只保留最近 20 次；启动时加 `--no-log` 可关闭。

## 代码结构

```
web.py            浏览器控制台：标准库 HTTP + 静态页，Runtime 跑在独立线程的事件循环里
ui/console.html   控制台前端（时间轴 / 调用树 / 详情），改样式不用动 Python
ui/settings.html  模型设置页
runtime.py        EventGate / Router / WorkerPool / Watchdog / 假模型 / 工具
trace.py          Span / TraceStore / 时间轴 / 生命周期 / 增量渲染
state.py          TaskBrief / TaskTable：状态、订阅、任务链、人工介入记录
config.py         模型配置：预设、读写、Key 掩码
runlog.py         运行日志：events.jsonl / session.log / summary.json
prompts.py        Router 判断、合成、直接回答与 Worker 的 prompt
headless.py       不启动界面的场景冒烟
scenarios.py      剧本数据，控制台与无头模式共用
tools/            日志定位与交互式配置
tests/            96 个测试
```

依赖方向是单向的：`trace.py` 不导入 AgentScope，`runtime.py` 不导入界面，运行时与界面之间只通过事件流交互，界面是可替换的观察者。

## 测试

```bash
python -m pytest        # 96 passed
python headless.py      # 场景冒烟，失败时非零退出
```

测试重点放在几条硬约束上，而不是界面细节：

- Router 不挂工具，只产出任务单；
- Worker 真正并行，并发峰值可断言；
- 直通不增加 Router 判断次数，且作用在同一条任务上；
- 召回能在多条已结束任务中选对目标；
- 挂起后批准能继续跑、驳回能终止；
- Watchdog 不覆盖取消标记，不催挂起任务；
- 时间轴不把等待时间算进并行收益；
- 工具在离线模式下报错而非造假。

## 目前的边界

这个项目覆盖的是架构主干，下面这些是它有意识没有做的部分，不是遗漏了却假装完成：

- **事件来源**：只有控制台和浏览器输入，没有接入群聊、GitHub、CI 等外部渠道。
- **部署形态**：单进程。`owner_node` 和重启重排只做到语义层面，没有真正的多副本调度。
- **上下文容量**：没有内联、溢写和压缩，Worker 的上下文跑完即弃。
- **成本控制**：没有 token 预算和配额。
- **权限体系**：工具都是本地桩并统一放行，没有审批策略；工具直接在本机执行，没有沙箱。
- **检索能力**：只有 `webfetch`，没有稳定的搜索后端，抓取质量取决于目标站点。
- **假模型演示**：`fake` 模型是确定性的离线桩，用于测试和演示结构，不产生真实检索结果；要看到真实内容需要接真实模型。
