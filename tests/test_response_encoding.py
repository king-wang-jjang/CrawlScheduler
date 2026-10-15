import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
from bs4 import BeautifulSoup


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))


def response_with_html(content, content_type):
    response = requests.Response()
    response.status_code = 200
    response._content = content
    response.headers["Content-Type"] = content_type
    response.encoding = requests.utils.get_encoding_from_headers(response.headers)
    return response


@pytest.mark.parametrize("page_kind", ["article", "feed"])
def test_inven_preserves_korean_when_utf8_metadata_is_truncated(monkeypatch, page_kind):
    from crawl_scheduler.community_website.inven import Inven

    title = "정상적인 한글 제목"
    body = "본문 첫 문장입니다.\n둘째 문장도 정상이어야 합니다."
    # Inven truncates description bytes inside a Korean character. The rest of
    # the real article is valid UTF-8, but byte detection can choose PTCP154.
    html = (
        b'<html><head><meta charset="utf-8"><meta name="description" content="'
        + "한글 설명".encode("utf-8")[:-1]
        + b'..."></head><body><div id="powerbbsContent">'
        + body.encode("utf-8").replace(b"\n", b"<br>")
        + b'</div><div id="hotven-list"><div class="list-common con">'
        + b'<div class="title"><a href="https://www.inven.co.kr/board/lostark/6271/123">'
        + b'<span class="name">'
        + title.encode("utf-8")
        + b'</span></a></div><span class="date">10-15</span></div></div></body></html>'
    )
    response = response_with_html(html, "text/html; charset=UTF-8")
    # Reproduce the observed detector result regardless of installed detector
    # version; the HTTP charset must preserve Korean even with this bad guess.
    monkeypatch.setattr(
        "bs4.dammit.chardet_module",
        SimpleNamespace(detect=lambda markup: {"encoding": "ptcp154"}),
    )
    assert BeautifulSoup(html, "html.parser").original_encoding == "ptcp154"
    monkeypatch.setattr(Inven, "get_response", classmethod(lambda cls, url: response))
    crawler = Inven.__new__(Inven)

    if page_kind == "article":
        contents = crawler.get_board_contents(
            url="https://www.inven.co.kr/board/lostark/6271/123"
        )
        assert contents == [{"type": "text", "text": body}]
    else:
        entries = crawler.get_board_entries()
        assert len(entries) == 1
        assert entries[0].title == title


@pytest.mark.parametrize(
    ("http_charset", "meta_charset"),
    [
        ("EUC-KR", "utf-8"),
        (None, "euc-kr"),
        ("unknown-test-charset", "euc-kr"),
    ],
)
def test_response_charset_preserves_legacy_and_detected_korean(
    monkeypatch, http_charset, meta_charset
):
    from crawl_scheduler.community_website.popular_community import PopularCommunityCrawler

    body = "이전 인코딩으로 제공되는 한국어 본문입니다."
    html = (
        f'<html><head><meta charset="{meta_charset}"></head>'
        f'<body><div id="article">{body}</div></body></html>'
    ).encode("euc-kr")
    content_type = "text/html"
    if http_charset:
        content_type += f'; charset="{http_charset}"'
    response = response_with_html(html, content_type)
    monkeypatch.setattr(
        PopularCommunityCrawler, "get_response", classmethod(lambda cls, url: response)
    )

    soup = PopularCommunityCrawler.soup_from_url("https://example.com/article")

    assert soup.select_one("#article").get_text() == body
