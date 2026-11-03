"""工具必须真做真报错：不允许返回占位数据。"""

from __future__ import annotations

import asyncio

import pytest

import runtime as R


def _ctx(*, offline: bool) -> tuple[R.WorkerContext, R.TaskTable]:
    """造一个带真实任务的上下文：工具的 checkpoint/note_tool 需要任务存在。"""
    table = R.TaskTable()
    task = R.TaskBrief(worker_type="researcher", prompt="测试")
    table.put(task)
    ctx = R.WorkerContext(table=table, trace=R.TraceStore(),
                          task_id=task.task_id, span_id="s-test",
                          offline=offline)
    return ctx, table


def test_tool_error_exists():
    assert issubclass(R.ToolError, RuntimeError)


def test_offline_mode_refuses_to_fake_search():
    """假模型模式下不允许编造搜索结果，必须报错。"""

    async def run():
        ctx, _ = _ctx(offline=True)
        token = R.CURRENT_CONTEXT.set(ctx)
        try:
            with pytest.raises(R.ToolError):
                await R.webfetch("https://cn.bing.com/search?q=shenzhen")
            with pytest.raises(R.ToolError):
                await R.webfetch("https://example.com")
        finally:
            R.CURRENT_CONTEXT.reset(token)

    asyncio.run(run())


def test_webfetch_rejects_non_http():
    async def run():
        ctx, _ = _ctx(offline=False)
        token = R.CURRENT_CONTEXT.set(ctx)
        try:
            with pytest.raises(R.ToolError):
                await R.webfetch("file:///etc/passwd")
        finally:
            R.CURRENT_CONTEXT.reset(token)

    asyncio.run(run())


def test_start_job_requires_url():
    async def run():
        ctx, _ = _ctx(offline=False)
        token = R.CURRENT_CONTEXT.set(ctx)
        try:
            with pytest.raises(R.ToolError):
                await R.start_job("查航班")
        finally:
            R.CURRENT_CONTEXT.reset(token)

    asyncio.run(run())


def test_html_to_text_strips_scripts():
    html = "<html><script>evil()</script><style>x{}</style><p>正文 &amp; 内容</p></html>"
    text = R._html_to_text(html)
    assert "evil" not in text and "正文" in text and "&" in text
