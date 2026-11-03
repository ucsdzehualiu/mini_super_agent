"""配置相关测试：读写、掩码、预设、以及 web 设置接口。"""

from __future__ import annotations

import json

import config as C


def test_default_is_fake_without_config(tmp_path, monkeypatch):
    monkeypatch.setattr(C, "CONFIG_PATH", tmp_path / "none.json")
    cfg = C.load()
    assert cfg.model.is_fake
    ok, why = cfg.model.ready()
    assert not ok and "假数据" in why


def test_load_and_save_roundtrip(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({
        "model": {"provider": "huawei-maas", "api_key": "sk-test-1234567890",
                  "base_url": "https://api.modelarts-maas.com/openai/v1",
                  "model_name": "deepseek-v4.1-flash"},
    }), encoding="utf-8")
    cfg = C.load(path)
    assert cfg.model.provider == "huawei-maas"
    assert cfg.model.model_name == "deepseek-v4.1-flash"
    assert cfg.model.ready()[0]

    C.save(cfg, path)
    again = C.load(path)
    assert again.model.api_key == "sk-test-1234567890"


def test_preset_fills_missing_fields(tmp_path, monkeypatch):
    monkeypatch.setattr(C, "CONFIG_PATH", tmp_path / "none.json")
    cfg = C.AppConfig()
    cfg.model.provider = "huawei-maas"
    cfg.model.api_key = "k" * 20
    cfg.model.resolved()
    assert cfg.model.base_url == C.PRESETS["huawei-maas"]["base_url"]
    assert cfg.model.model_name == C.PRESETS["huawei-maas"]["model_name"]
    assert cfg.model.ready()[0]


def test_key_is_masked(tmp_path):
    m = C.ModelConfig(provider="openai", api_key="sk-abcdefghijklmnop", model_name="x")
    masked = m.masked_key()
    assert masked.startswith("sk-a") and masked.endswith("mnop")
    assert "abcdefghij" not in masked
    public = C.AppConfig(model=m).public_dict()
    assert public["model"]["has_key"] is True
    assert "abcdefghij" not in json.dumps(public, ensure_ascii=False)


def test_missing_key_reports_actionable_reason():
    m = C.ModelConfig(provider="huawei-maas", api_key="").resolved()
    ok, why = m.ready()
    assert not ok and "API Key" in why


def test_describe(tmp_path, monkeypatch):
    monkeypatch.setattr(C, "CONFIG_PATH", tmp_path / "none.json")
    assert "假数据" in C.describe(C.load())
    cfg = C.AppConfig(model=C.ModelConfig(provider="deepseek", api_key="k" * 20,
                                         model_name="deepseek-chat").resolved())
    assert "deepseek-chat" in C.describe(cfg)


def test_web_config_endpoints(tmp_path, monkeypatch):
    """设置页要能读、能写。"""
    import threading
    import urllib.request
    from http.server import ThreadingHTTPServer

    import web

    # 别把测试写进真实的 config.json
    monkeypatch.setattr(C, "CONFIG_PATH", tmp_path / "config.json")
    service = web.Service(model_kind="fake", watchdog=3600, log=None, speed=4.0)
    service.start()
    server = ThreadingHTTPServer(("127.0.0.1", 0), web.make_handler(service))
    base = f"http://127.0.0.1:{server.server_address[1]}"
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        with urllib.request.urlopen(base + "/settings", timeout=5) as r:
            html = r.read().decode("utf-8")
        assert "模型设置" in html and "huawei-maas" in html

        with urllib.request.urlopen(base + "/api/config", timeout=5) as r:
            data = json.loads(r.read().decode())
        assert "presets" in data and "huawei-maas" in data["presets"]
        assert "api_key" in data["config"]["model"]

        # 写一份配置，确认真的落到了 config.json
        body = json.dumps({"provider": "deepseek", "model_name": "deepseek-chat",
                           "api_key": "sk-test-abcdefghij"}).encode()
        req = urllib.request.Request(base + "/api/config", data=body,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as r:
            assert json.loads(r.read().decode())["ok"]
        saved = json.loads((tmp_path / "config.json").read_text(encoding="utf-8"))
        assert saved["model"]["model_name"] == "deepseek-chat"
        # 响应里不能带出完整 Key
        with urllib.request.urlopen(base + "/api/config", timeout=5) as r:
            masked = r.read().decode()
        assert "abcdefghij" not in masked
    finally:
        server.shutdown()
