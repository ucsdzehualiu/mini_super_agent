"""setup_model.py —— 交互式配置模型，写到 config.json。

用法：
    python tools/setup_model.py            # 交互
    python tools/setup_model.py --ping     # 配完立刻试一次连通性
    python tools/setup_model.py --show     # 只看当前配置

也可以在浏览器里配：启动 web.py 后点右上角「设置模型」。
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import config as C  # noqa: E402


def show() -> None:
    cfg = C.load()
    print(f"配置文件  {C.CONFIG_PATH}")
    print(f"当前     {C.describe(cfg)}")
    m = cfg.model
    print(f"provider {m.provider}")
    print(f"base_url {m.base_url or '—'}")
    print(f"model    {m.model_name or '—'}")
    print(f"api_key  {m.masked_key() or '—'}")
    ok, why = m.ready()
    print(f"状态     {'可用' if ok else '不可用：' + why}")


async def ping(cfg: C.AppConfig) -> bool:
    from runtime import build_chat_model
    from agentscope.message import UserMsg

    try:
        model = build_chat_model(cfg.model.provider, config=cfg.model)
        result = model([UserMsg("user", "只回答两个字：收到")])
        if hasattr(result, "__await__"):
            result = await result
        text = ""
        if hasattr(result, "__aiter__"):
            async for chunk in result:
                for block in getattr(chunk, "content", None) or []:
                    text += getattr(block, "text", "") or ""
        else:
            text = getattr(result, "get_text_content", lambda: "")() or ""
    except Exception as exc:  # noqa: BLE001
        print(f"\n调用失败：{type(exc).__name__}: {exc}")
        return False
    print(f"\n模型回复：{text.strip()[:80]!r}")
    return bool(text.strip())


def interactive() -> C.AppConfig:
    cfg = C.load()
    print("按回车跳过 = 保留当前值\n")

    providers = list(C.PRESETS) + ["fake", "custom"]
    print("可选供应商：")
    for i, name in enumerate(providers, 1):
        preset = C.PRESETS.get(name, {})
        note = preset.get("model_name", "")
        print(f"  {i}. {name}{'  (' + note + ')' if note else ''}")

    raw = input(f"\n供应商 [{cfg.model.provider}]: ").strip()
    if raw.isdigit() and 1 <= int(raw) <= len(providers):
        cfg.model.provider = providers[int(raw) - 1]
    elif raw:
        cfg.model.provider = raw

    preset = C.PRESETS.get(cfg.model.provider, {})
    base = input(f"base_url [{cfg.model.base_url or preset.get('base_url','')}]: ").strip()
    if base:
        cfg.model.base_url = base
    elif preset:
        cfg.model.base_url = preset["base_url"]

    name = input(f"模型名 [{cfg.model.model_name or preset.get('model_name','')}]: ").strip()
    if name:
        cfg.model.model_name = name
    elif preset:
        cfg.model.model_name = preset["model_name"]

    if cfg.model.provider != "fake":
        key = input(f"API Key [{cfg.model.masked_key() or '未设置'}]: ").strip()
        if key:
            cfg.model.api_key = key

    path = C.save(cfg)
    print(f"\n已写入 {path}")
    return cfg


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="配置大模型")
    p.add_argument("--show", action="store_true")
    p.add_argument("--ping", action="store_true", help="保存后试一次调用")
    args = p.parse_args(argv)

    if args.show:
        show()
        return 0

    cfg = interactive()
    if args.ping:
        return 0 if asyncio.run(ping(cfg)) else 1
    print("下一步：python web.py  或  start.bat")
    return 0


if __name__ == "__main__":
    sys.exit(main())
