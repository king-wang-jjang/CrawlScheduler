import sys
from pathlib import Path


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))


def test_once_flag_runs_crawl_and_exits(monkeypatch):
    from crawl_scheduler import main

    calls = []

    monkeypatch.setattr(main, "get_realtime_best", lambda: calls.append("crawl"))

    assert main.main(["--once"]) == 0
    assert calls == ["crawl"]


def test_seed_alias_runs_crawl_and_exits(monkeypatch):
    from crawl_scheduler import main

    calls = []

    monkeypatch.setattr(main, "get_realtime_best", lambda: calls.append("crawl"))

    assert main.main(["--seed"]) == 0
    assert calls == ["crawl"]
