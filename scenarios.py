"""剧本：一个「忙的上午」。

不是一条请求，而是一串陆续到来的人话。重点在于系统怎么接住它们：

1. 新需求 → Router 拆单 → 多个 Worker 并行
2. 追加条件 → 订阅直通，追加到正在跑的 Worker，不重新拆单
3. 等待外部结果 → 任务进入 WAITING，不占执行体
4. 不可逆动作 → 任务进入 NEEDS_HUMAN，等人批准
5. 「刚才那个怎样了」 → 链式召回
6. 一直打转 → Watchdog 判无进展并注入提示
7. 噪音 → 程序层直接挡掉

每个场景都是可执行的：Console 里按 F3 播放、headless.py 跑断言、
tests/ 里也复用同一份数据。
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Step:
    """剧本里的一步。"""

    say: str
    """用户说的话。"""

    note: str = ""
    """这一步演示的是哪个机制（显示在控制台/文档里）。"""

    wait_for: str = "idle"
    """提交后等什么：``idle``（所有任务落终态）、``blocked``（出现等人/等结果）、
    ``none``（不等）。"""

    hold: float = 0.8
    """提交后至少等多久（让 Watchdog / 并行有时间发生）。"""


@dataclass
class Scenario:
    id: str
    title: str
    steps: list[Step] = field(default_factory=list)
    expect: dict = field(default_factory=dict)
    observations: list[str] = field(default_factory=list)


#: 主剧本：一次「忙的上午」
MORNING: list[Step] = [
    Step("帮我查一下成都周边徒步路线，排个两天计划，再写一版介绍文案",
         note="新需求：Router 拆单，三个方向并行", wait_for="idle", hold=1.0),
    Step("路线那条再补一句：避开收费景区",
         note="追加条件：订阅直通，不重新拆单", wait_for="idle", hold=0.6),
    Step("另外帮我订一张下周三去成都的机票，先看看航班和价格",
         note="有外部依赖：等结果期间任务不占执行体，你还能继续说话", wait_for="blocked", hold=1.2),
    Step("排期里那步订票，帮我直接订了吧",
         note="不可逆动作：挂起等人批", wait_for="blocked", hold=1.0),
    Step("刚才那个介绍文案改成更正式的腔调",
         note="链式召回：接着已完成的任务改", wait_for="idle", hold=0.8),
    Step("忽略我",
         note="噪音：程序层直接挡掉", wait_for="none", hold=0.3),
]


SCENARIOS: list[Scenario] = [
    Scenario(
        id="morning",
        title="一个忙的上午",
        steps=MORNING,
        expect={
            "routes": ["ROUTER", "DIRECT", "RECALL", "GATE"],
            "min_tasks": 4,
            "needs_human_seen": True,
            "router_skipped_min": 1,
        },
        observations=[
            "Event Stream：每行标出这条消息走的是 拆单 / 直通 / 召回 / 拦截",
            "直通那一步，Router 决定数不增加——这就是少占用注意力的直接体现",
            "任务行：能看到「等结果」「等人批」，以及人插手了几次",
        ],
    ),
    Scenario(
        id="parallel",
        title="并行：四件事同时推进",
        steps=[
            Step("帮我查一下成都、西安、拉萨三条徒步路线各自两三天的行程",
                 note="一个需求，三个方向并行", wait_for="idle", hold=1.0),
        ],
        expect={"min_tasks": 2, "peak_min": 2},
        observations=["时间轴：各任务条重叠，总耗时 ≈ 最慢那条而不是相加"],
    ),
    Scenario(
        id="stall",
        title="打转：Watchdog 发现无进展并注入提示",
        steps=[
            Step("<<LOOP>> 帮我查一下竞品定价",
                 note="Worker 一直重复同一个工具", wait_for="idle", hold=3.0),
        ],
        expect={"watchdog_inject_min": 1},
        observations=["Watchdog 判「无进展」，往 Worker 里注入提示，而不是硬取消"],
    ),
    Scenario(
        id="approve",
        title="人工介入：挂起 → 批准 → 继续",
        steps=[
            Step("排期里那步订票，帮我直接订了吧", note="挂起等人批", wait_for="blocked", hold=1.0),
        ],
        expect={"needs_human_seen": True},
        observations=["任务停在「等人批」，控制台按 A 批准后继续"],
    ),
]

FAST_SCENARIOS = ("parallel", "approve", "stall")


def get(scenario_id: str) -> Scenario:
    for sc in SCENARIOS:
        if sc.id == scenario_id:
            return sc
    raise KeyError(scenario_id)


def morning_lines() -> list[str]:
    return [f"{i + 1}. {s.say}    —— {s.note}" for i, s in enumerate(MORNING)]
