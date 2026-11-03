"""config.py —— 模型与运行参数配置。

优先级（高 → 低）：命令行参数 > 环境变量 > config.json > 内置默认值。

``config.json`` 里有 API Key，已被 .gitignore 排除。不要把它提交出去。

华为云 MaaS（ModelArts Studio）的 OpenAI 兼容地址：
    https://api.modelarts-maas.com/openai/v1
可用模型名见控制台「模型广场」，例如 deepseek-v4.1-flash / qwen3-32b / openpangu-2.0-pro。
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

CONFIG_PATH = Path(os.environ.get("SUPER_AGENT_CONFIG", "config.json"))

#: 常见供应商的默认值，省得每次手填
PRESETS: dict[str, dict[str, str]] = {
    "huawei-maas": {
        "base_url": "https://api.modelarts-maas.com/openai/v1",
        "model_name": "deepseek-v4.1-flash",
    },
    "deepseek": {
        "base_url": "https://api.deepseek.com/v1",
        "model_name": "deepseek-chat",
    },
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "model_name": "gpt-4o-mini",
    },
    "ollama": {
        "base_url": "http://127.0.0.1:11434/v1",
        "model_name": "qwen3",
    },
}

ENV_KEYS = {
    "huawei-maas": ("MAAS_API_KEY", "HUAWEI_MAAS_API_KEY"),
    "deepseek": ("DEEPSEEK_API_KEY",),
    "openai": ("OPENAI_API_KEY",),
    "ollama": ("OLLAMA_API_KEY",),
}


@dataclass
class ModelConfig:
    """一份模型配置。``provider`` 决定用哪个预设的 base_url。"""

    provider: str = "fake"          # fake | huawei-maas | deepseek | openai | ollama | custom
    base_url: str = ""
    api_key: str = ""
    model_name: str = ""
    temperature: float = 0.6
    max_tokens: int = 2048
    stream: bool = True
    context_size: int = 128000

    def resolved(self) -> "ModelConfig":
        """补齐预设里的字段（不覆盖已经显式填过的）。"""
        preset = PRESETS.get(self.provider, {})
        if not self.base_url:
            self.base_url = preset.get("base_url", "")
        if not self.model_name:
            self.model_name = preset.get("model_name", "")
        if not self.api_key:
            for env in ENV_KEYS.get(self.provider, ()):
                if os.environ.get(env):
                    self.api_key = os.environ[env]
                    break
        return self

    @property
    def is_fake(self) -> bool:
        return self.provider == "fake"

    def ready(self) -> tuple[bool, str]:
        """能不能真的发请求；不能就给出具体缺什么。"""
        if self.is_fake:
            return False, "当前是内置假数据，不联网"
        if not self.base_url:
            return False, "缺少 base_url"
        if not self.model_name:
            return False, "缺少 model_name"
        if not self.api_key:
            return False, f"缺少 API Key（填 api_key 或设置环境变量 {'/'.join(ENV_KEYS.get(self.provider, ('API_KEY',)))}）"
        return True, ""

    def masked_key(self) -> str:
        if not self.api_key:
            return ""
        if len(self.api_key) <= 10:
            return "*" * len(self.api_key)
        return f"{self.api_key[:4]}…{self.api_key[-4:]}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AppConfig:
    model: ModelConfig = field(default_factory=ModelConfig)
    watchdog: float = 3.0
    suspect: float = 60.0
    web_port: int = 8080
    log_dir: str = "logs"

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["model"]["api_key"] = self.model.api_key  # 落盘要真实值
        return data

    def public_dict(self) -> dict[str, Any]:
        """给前端/日志看：Key 只露头尾。"""
        data = asdict(self)
        data["model"]["api_key"] = self.model.masked_key()
        data["model"]["has_key"] = bool(self.model.api_key)
        return data


def load(path: str | os.PathLike[str] | None = None) -> AppConfig:
    """读配置；文件不存在就用默认值（假模型）。"""
    p = Path(path or CONFIG_PATH)
    cfg = AppConfig()
    if not p.exists():
        cfg.model.resolved()
        return cfg
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        cfg.model.resolved()
        return cfg
    m = raw.get("model") or {}
    cfg.model = ModelConfig(
        provider=m.get("provider", "fake"),
        base_url=m.get("base_url", ""),
        api_key=m.get("api_key", ""),
        model_name=m.get("model_name", ""),
        temperature=float(m.get("temperature", 0.6)),
        max_tokens=int(m.get("max_tokens", 2048)),
        stream=bool(m.get("stream", True)),
        context_size=int(m.get("context_size", 128000)),
    ).resolved()
    cfg.watchdog = float(raw.get("watchdog", cfg.watchdog))
    cfg.suspect = float(raw.get("suspect", cfg.suspect))
    cfg.web_port = int(raw.get("web_port", cfg.web_port))
    cfg.log_dir = str(raw.get("log_dir", cfg.log_dir))
    return cfg


def save(cfg: AppConfig, path: str | os.PathLike[str] | None = None) -> Path:
    p = Path(path or CONFIG_PATH)
    p.write_text(json.dumps(cfg.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return p


def update(**fields: Any) -> AppConfig:
    """改几个字段并保存，其余保持不变。"""
    cfg = load()
    for key, value in fields.items():
        if hasattr(cfg.model, key):
            setattr(cfg.model, key, value)
        elif hasattr(cfg, key):
            setattr(cfg, key, value)
    cfg.model.resolved()
    save(cfg)
    return cfg


def describe(cfg: AppConfig) -> str:
    """一句话说清现在用的是什么模型。"""
    m = cfg.model
    if m.is_fake:
        return "内置假数据（不联网，答案来自本地桩）"
    return f"{m.provider} · {m.model_name} · {m.base_url}"
