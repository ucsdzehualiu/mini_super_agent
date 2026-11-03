"""生成 docs/ 下的截图与演示输出。

    python tools/make_screenshots.py

产出：
    docs/screenshot-morning.svg   控制台（播放完剧本后的画面）
    docs/screenshot-timeline.svg  控制台（切到时间轴视图）
    docs/screenshot-morning.txt   同一画面的纯文本
    docs/headless-demo.txt         headless 跑主剧本的完整输出
"""

from __future__ import annotations

import asyncio
import io
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from rich.console import Console  # noqa: E402

import main as app_module  # noqa: E402

SIZE = (150, 48)


def _text(app) -> str:
    w, h = app.size
    buf = io.StringIO()
    Console(width=w, height=h, file=buf, force_terminal=False, color_system=None).print(
        app.screen._compositor)
    return buf.getvalue()


async def capture() -> None:
    docs = ROOT / "docs"
    docs.mkdir(exist_ok=True)
    app = app_module.ConsoleApp(watchdog_interval=0.5)
    async with app.run_test(size=SIZE) as pilot:
        await pilot.pause(0.3)
        await app._run_morning()
        await pilot.pause(0.5)
        pending = app.runtime.needs_human()
        if pending:
            await app.runtime.approve(pending[0].task_id)
            await pilot.pause(0.6)
        (docs / "screenshot-morning.svg").write_text(app.export_screenshot(), encoding="utf-8")
        (docs / "screenshot-morning.txt").write_text(_text(app), encoding="utf-8")
        app.action_toggle_view()
        await pilot.pause(0.6)
        (docs / "screenshot-timeline.svg").write_text(app.export_screenshot(), encoding="utf-8")
        print("已生成 screenshot-morning / screenshot-timeline")
        await app.export_debug_dump(docs / "console-dump.txt")


async def headless_demo() -> None:
    import headless

    buf = io.StringIO()
    real = sys.stdout
    sys.stdout = buf
    try:
        await headless.run_all(["morning", "stall"])
    finally:
        sys.stdout = real
    (ROOT / "docs" / "headless-demo.txt").write_text(buf.getvalue(), encoding="utf-8")
    print("已生成 headless-demo.txt")


def main() -> int:
    asyncio.run(capture())
    asyncio.run(headless_demo())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
