import sys
from pathlib import Path


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))


class DummyResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"message": {"content": '{"summary":"ok","tags":["ai"]}'}}


def test_analyze_requests_json_response_format(monkeypatch):
    from crawl_scheduler.utils import llm as llm_module
    from crawl_scheduler.utils.llm import LLM

    calls = []

    def fake_post(url, json, timeout):
        calls.append({"url": url, "json": json, "timeout": timeout})
        return DummyResponse()

    monkeypatch.setattr(llm_module.requests, "post", fake_post)

    result = LLM(base_url="http://llm.local", model="gemma4:e4b").analyze("title\nbody")

    assert result == {"summary": "ok", "tags": ["ai"]}
    assert calls[0]["json"]["format"] == "json"


def test_default_ollama_timeout_allows_slow_local_model(monkeypatch):
    from crawl_scheduler.utils.llm import LLM

    monkeypatch.delenv("OLLAMA_TIMEOUT_SECONDS", raising=False)

    assert LLM().timeout_seconds == 60.0
