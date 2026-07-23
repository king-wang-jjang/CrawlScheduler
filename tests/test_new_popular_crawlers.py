import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from bs4 import BeautifulSoup


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))


@pytest.mark.parametrize(
    ("module_name", "class_name", "html", "expected"),
    [
        (
            "fmkorea",
            "Fmkorea",
            """<li class="li"><a class="pc_voted_count"><span class="count">1,234</span></a>
            <h3 class="title"><a href="/?document_srl=9001"><span class="ellipsis-target">포텐 글</span>
            <span class="comment_count">[56]</span></a></h3><span class="regdate">1 시간 전<!--10:20--></span></li>""",
            (9001, "best", "포텐 글", 56, 1234, None),
        ),
        (
            "arca",
            "Arca",
            """<div class="vrow"><span class="badges"><a class="badge" href="/b/game">게임</a></span>
            <a class="title" href="/b/live/9002"><span>실베 글</span><span class="comment-count">12</span></a>
            <span class="col-time"><time datetime="2026-07-23T01:00:00.000Z"></time></span>
            <span class="col-view">3,456</span><span class="col-rate">78</span></div>""",
            (9002, "game", "실베 글", 12, 78, 3456),
        ),
        (
            "inven",
            "Inven",
            """<div id="hotven-list"><div class="list-common con"><div class="title">
            <a href="https://www.inven.co.kr/board/lostark/1234/9003"><span class="name">핫벤 글</span></a>
            </div><span class="comment">34</span><span class="date">07-22</span>
            <span class="hits">5,678</span><span class="reco">90</span></div></div>""",
            (9003, "lostark-1234", "핫벤 글", 34, 90, 5678),
        ),
    ],
)
def test_popular_feeds_parse_identity_and_metrics(
    monkeypatch, module_name, class_name, html, expected
):
    module = __import__(
        f"crawl_scheduler.community_website.{module_name}", fromlist=[class_name]
    )
    monkeypatch.setattr(module, "PostgresController", lambda: object())
    crawler = getattr(module, class_name)()
    monkeypatch.setattr(
        crawler,
        "soup_from_url",
        lambda url: BeautifulSoup(html, "html.parser"),
    )

    entries = crawler.get_board_entries()

    assert len(entries) == 1
    entry = entries[0]
    assert (
        entry.no,
        entry.category,
        entry.title,
        entry.native_comment_count,
        entry.native_like_count,
        entry.native_view_count,
    ) == expected
    assert entry.source_rank == 1
    assert entry.metrics_crawled_at is not None


@pytest.mark.parametrize(
    ("module_name", "class_name", "site", "category", "no"),
    [
        ("fmkorea", "Fmkorea", "fmkorea", "best", 9101),
        ("arca", "Arca", "arca", "game", 9102),
        ("inven", "Inven", "inven", "lostark-1234", 9103),
    ],
)
def test_new_popular_posts_enter_common_ai_queue(
    monkeypatch, module_name, class_name, site, category, no
):
    from crawl_scheduler.community_website.board_list_entry import BoardListEntry
    from crawl_scheduler.constants import DEFAULT_GPT_ANSWER

    module = __import__(
        f"crawl_scheduler.community_website.{module_name}", fromlist=[class_name]
    )

    class FakeDB:
        def __init__(self):
            self.documents = []

        def find(self, *args):
            return []

        def insert_one(self, collection, document):
            if collection == "Realtime":
                self.documents.append(document)
            return SimpleNamespace(inserted_id="id")

    fake_db = FakeDB()
    monkeypatch.setattr(module, "PostgresController", lambda: fake_db)
    crawler = getattr(module, class_name)()
    entry = BoardListEntry(
        url=f"https://example.com/{no}",
        category=category,
        no=no,
        title="new popular post",
        created_at=datetime.now(ZoneInfo("Asia/Seoul")),
        native_comment_count=10,
        native_like_count=20,
        native_view_count=30,
        source_rank=1,
    )
    monkeypatch.setattr(crawler, "get_board_entries", lambda: [entry])
    monkeypatch.setattr(crawler, "get_board_contents", lambda **kwargs: [])

    assert crawler.get_realtime_best() is True
    assert len(fake_db.documents) == 1
    assert fake_db.documents[0]["site"] == site
    assert fake_db.documents[0]["gpt_answer"] == DEFAULT_GPT_ANSWER


def test_optional_proxy_is_used_for_popular_site_requests(monkeypatch):
    from crawl_scheduler.community_website.popular_community import (
        PopularCommunityCrawler,
    )

    monkeypatch.setenv("CRAWLER_HTTP_PROXY", "http://100.64.0.1:3128")

    assert PopularCommunityCrawler.request_proxies() == {
        "http": "http://100.64.0.1:3128",
        "https": "http://100.64.0.1:3128",
    }
