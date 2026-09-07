from __future__ import annotations

from types import SimpleNamespace

import pytest


LIST_HTML = """
<table>
  <tr class="bbs_new1" data-bbs_id="freeboard" data-bbs_no="10013570">
    <td>
      <a class="baseList-title"
         href="/zboard/view.php?id=freeboard&amp;no=10013570">
        actual post title
      </a>
      <span class="list_comment2">52</span>
    </td>
    <td class="board_date">15:46:12</td>
    <td class="board_date">69 - 7</td>
    <td class="board_date">11,189</td>
  </tr>
</table>
"""

ARTICLE_HTML = """
<html>
  <head><meta property="og:image" content="/thumbnail.jpg"></head>
  <body>
    <table><tr><td class="board-contents">
      This is a sufficiently long article body for the analysis queue.
    </td></tr></table>
  </body>
</html>
"""


class FakeResponse:
    def __init__(self, html: str, status_code: int = 200):
        self.text = html
        self.content = html.encode("utf-8")
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class RecordingSession:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return next(self.responses)


class RecordingDB:
    def __init__(self):
        self.insertions = []

    def find(self, *args, **kwargs):
        return []

    def insert_one(self, collection, document):
        self.insertions.append((collection, document))
        return SimpleNamespace(inserted_id="gpt-result")


def build_crawler(monkeypatch, responses):
    from crawl_scheduler.community_website import ppomppu

    fake_db = RecordingDB()
    fake_session = RecordingSession(responses)
    monkeypatch.setattr(ppomppu, "PostgresController", lambda: fake_db)
    monkeypatch.setattr(ppomppu.requests, "Session", lambda: fake_session)
    crawler = ppomppu.Ppomppu()
    crawler.request_delay_seconds = 0
    return ppomppu, crawler, fake_db, fake_session


def test_list_and_article_share_session_with_browser_request_context(
    monkeypatch,
):
    proxy_url = "http://approved-proxy.test:3128"
    monkeypatch.setenv("CRAWLER_HTTP_PROXY", proxy_url)
    ppomppu, crawler, fake_db, fake_session = build_crawler(
        monkeypatch,
        [FakeResponse(LIST_HTML), FakeResponse(ARTICLE_HTML)],
    )

    assert crawler.get_realtime_best() is True

    assert [url for url, _kwargs in fake_session.calls] == [
        ppomppu.PPOMPPU_HOT_URL,
        "https://www.ppomppu.co.kr/zboard/view.php?id=freeboard&no=10013570",
    ]
    list_request = fake_session.calls[0][1]
    article_request = fake_session.calls[1][1]
    assert list_request["headers"]["Referer"] == "https://www.ppomppu.co.kr/"
    assert article_request["headers"]["Referer"] == ppomppu.PPOMPPU_HOT_URL
    assert list_request["headers"]["User-Agent"]
    assert list_request["headers"]["Accept-Language"].startswith("ko-KR")
    assert list_request["proxies"] == {"http": proxy_url, "https": proxy_url}
    assert article_request["proxies"] == {
        "http": proxy_url,
        "https": proxy_url,
    }
    assert list_request["timeout"] == 15
    assert article_request["timeout"] == 15

    realtime_documents = [
        document
        for collection, document in fake_db.insertions
        if collection == "Realtime"
    ]
    assert len(realtime_documents) == 1
    assert realtime_documents[0]["url"].startswith(
        "https://www.ppomppu.co.kr/"
    )


@pytest.mark.parametrize(
    "article_response",
    [
        FakeResponse("request blocked", status_code=403),
        FakeResponse("request blocked", status_code=430),
        FakeResponse("<html><body>access denied</body></html>"),
        FakeResponse(
            "<html><body><table><tr>"
            '<td class="board-contents"></td>'
            "</tr></table></body></html>"
        ),
        FakeResponse(
            '<html><head><meta property="og:image" content="/thumbnail.jpg">'
            '</head><body><table><tr><td class="board-contents"></td>'
            '</tr></table></body></html>'
        ),
    ],
    ids=["forbidden", "blocked-status", "soft-block", "empty-body", "metadata-only"],
)
def test_blocked_or_empty_article_is_not_inserted(
    monkeypatch, article_response
):
    _ppomppu, crawler, fake_db, _fake_session = build_crawler(
        monkeypatch,
        [FakeResponse(LIST_HTML), article_response],
    )

    assert crawler.get_realtime_best() is False
    assert not any(
        collection == "Realtime" for collection, _document in fake_db.insertions
    )


def test_invalid_request_delay_uses_default(monkeypatch):
    from crawl_scheduler.community_website import ppomppu

    monkeypatch.setenv("CRAWLER_PPOMPPU_REQUEST_DELAY_SECONDS", "invalid")
    fake_db = RecordingDB()
    monkeypatch.setattr(ppomppu, "PostgresController", lambda: fake_db)
    monkeypatch.setattr(
        ppomppu.requests,
        "Session",
        lambda: RecordingSession([]),
    )

    crawler = ppomppu.Ppomppu()

    assert (
        crawler.request_delay_seconds
        == ppomppu.DEFAULT_PPOMPPU_REQUEST_DELAY_SECONDS
    )
