"""回复文本：把一次处理结果翻译成人话。

和界面无关——浏览器、命令行、测试都用它，保证同一个操作在任何入口
看到的措辞一致。
"""

from __future__ import annotations

from typing import Any

from trace import ROUTE_ANSWER, ROUTE_DIRECT, ROUTE_GATE, ROUTE_IGNORE


def reply_text(result: Any) -> str:
    """每种去向都必须有回复：被过滤、被转交、挂起、失败都一样。

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
    if route == ROUTE_ANSWER:
        return "这条我直接答了，但没组织出内容。"
    if route == ROUTE_IGNORE:
        return "这条我没派任务。"

    if route == ROUTE_DIRECT:
        task = result.dispatched[0] if result.dispatched else None
        if task is None:
            return "已经转给在办的任务。"
        if task.status == "NEEDS_HUMAN":
            return f"转给 {task.name} 了。它需要你确认：{task.needs_human_reason}（点「批准」继续）"
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
            return f"派了 {len(result.dispatched)} 条，完成 {len(done)} 条，结果见任务表。"
    return "这条没有产生可执行的任务。"
