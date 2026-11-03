"""pytest 配置。

* 把项目根目录放进 ``sys.path``，这样测试可以直接 ``import runtime`` 等；
* 统一使用 ``asyncio`` 模式（pytest-asyncio），并给慢速用例留足时间。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture()
def fast_model():
    """一个"几乎不耗时"的假模型，测试用。"""
    from runtime import FakeChatModel

    return FakeChatModel(latency=(0.001, 0.003))


@pytest.fixture()
def runtime(fast_model):
    """构造一个不启动 Watchdog 循环的 Runtime（测试里手动调用 ``watchdog.scan()``）。"""
    from runtime import Runtime

    return Runtime(model=fast_model, watchdog_interval=3600, suspect_after=60)
