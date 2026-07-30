from __future__ import annotations

import importlib
from pathlib import Path
import sys

import pytest
from bs4 import BeautifulSoup


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))


from crawl_scheduler.community_website.article_content import (
    ordered_article_content_parts,
)
from crawl_scheduler.community_website.community_website import (
    AbstractCommunityWebsite,
)


class FakeResponse:
    def __init__(self, html: str):
        self.text = html
        self.content = html.encode()

    def raise_for_status(self) -> None:
        return None


def test_ordered_article_parts_keep_dom_order_without_recursive_duplicates():
    body = BeautifulSoup(
        """
        <div class="body">
          시작 <span>인라인</span>
          <div>중첩 <strong>본문</strong><script>명령 노이즈</script></div>
          <p>이미지 앞<img src="/image.jpg">이미지 뒤</p>
          <video><source src="/clip.mp4"></video>영상 뒤
          <style>스타일 노이즈</style><noscript>대체 노이즈</noscript>
        </div>
        """,
        "html.parser",
    ).div

    parts = ordered_article_content_parts(body)

    assert [part.kind for part in parts] == [
        "text",
        "image",
        "text",
        "video",
        "text",
    ]
    assert parts[0].text == "시작 인라인\n중첩 본문\n이미지 앞"
    assert parts[2].text == "이미지 뒤"
    assert parts[4].text == "영상 뒤"
    all_text = "\n".join(part.text or "" for part in parts)
    assert all_text.count("중첩 본문") == 1
    assert "명령 노이즈" not in all_text
    assert "스타일 노이즈" not in all_text
    assert "대체 노이즈" not in all_text


def test_adjacent_inline_elements_receive_a_readable_separator():
    body = BeautifulSoup(
        "<div><span>foo</span><span>bar</span></div>",
        "html.parser",
    ).div

    parts = ordered_article_content_parts(body)

    assert [(part.kind, part.text) for part in parts] == [("text", "foo bar")]


def test_common_popular_body_parser_interleaves_text_and_media(monkeypatch):
    from crawl_scheduler.community_website.popular_community import (
        PopularCommunityCrawler,
    )

    html = """
    <html>
      <head><meta property="og:image" content="/thumbnail.jpg"></head>
      <body>
        <div id="article">
          <div>첫 문장 <span>중첩</span></div>
          <p>그림 전<img data-src="/image.jpg">그림 후</p>
          <div>문단 밖</div>
          <video><source src="/clip.mp4"></video>
          <iframe src="https://www.youtube.com/embed/example"></iframe>
          <p>마지막<script>버릴 내용</script></p>
        </div>
      </body>
    </html>
    """
    crawler = PopularCommunityCrawler.__new__(PopularCommunityCrawler)
    crawler.site = "test"
    crawler.body_selectors = ("#article",)
    saved_urls = []
    monkeypatch.setattr(crawler, "get_response", lambda url: FakeResponse(html))
    monkeypatch.setattr(
        crawler,
        "save_file",
        lambda media_url, **kwargs: saved_urls.append(media_url)
        or f"test/{media_url.rsplit('/', 1)[-1]}",
    )
    monkeypatch.setattr(crawler, "img_to_text", lambda path: "이미지 OCR")

    contents = crawler.get_board_contents(
        category="best",
        no=1,
        url="https://example.com/posts/1",
    )

    assert [block["type"] for block in contents] == [
        "metadata",
        "text",
        "image",
        "text",
        "video",
        "video",
        "text",
    ]
    assert contents[1]["text"] == "첫 문장 중첩\n그림 전"
    assert contents[2]["text"] == "이미지 OCR"
    assert contents[3]["text"] == "그림 후\n문단 밖"
    assert contents[5] == {
        "type": "video",
        "source_url": "https://www.youtube.com/embed/example",
    }
    assert contents[6]["text"] == "마지막"
    assert saved_urls == [
        "https://example.com/image.jpg",
        "https://example.com/clip.mp4",
    ]

    saved_urls.clear()
    recovery_contents = crawler.get_board_contents(
        category="best",
        no=1,
        url="https://example.com/posts/1",
        save_videos=False,
    )

    assert saved_urls == ["https://example.com/image.jpg"]
    assert recovery_contents[4] == {
        "type": "video",
        "source_url": "https://example.com/clip.mp4",
    }


def test_filename_alt_does_not_prevent_ocr(monkeypatch):
    from crawl_scheduler.community_website.article_content import (
        build_ordered_content_blocks,
    )
    from crawl_scheduler.community_website.popular_community import (
        PopularCommunityCrawler,
    )

    body = BeautifulSoup(
        '<div><img src="/notice.webp" '
        'alt="Dcinside/humor/123/important_image_description.webp"></div>',
        "html.parser",
    ).div
    crawler = PopularCommunityCrawler.__new__(PopularCommunityCrawler)
    monkeypatch.setattr(
        crawler,
        "save_file",
        lambda *_args, **_kwargs: "Dcinside/humor/123/notice.webp",
    )
    monkeypatch.setattr(crawler, "img_to_text", lambda _path: "이미지에 적힌 실제 안내 문구")

    blocks = build_ordered_content_blocks(
        crawler,
        body,
        base_url="https://example.com/posts/1",
        category="humor",
        no=123,
    )

    assert blocks == [
        {
            "type": "image",
            "media_path": "Dcinside/humor/123/notice.webp",
            "source_url": "https://example.com/notice.webp",
            "text": "이미지에 적힌 실제 안내 문구",
            "alt_text": "Dcinside/humor/123/important_image_description.webp",
        }
    ]


@pytest.mark.parametrize(
    ("module_name", "class_name", "body_markup"),
    [
        ("ygosu", "Ygosu", '<div class="container">{body}</div>'),
        ("ppomppu", "Ppomppu", '<td class="board-contents">{body}</td>'),
        ("dcinside", "Dcinside", '<div class="write_div">{body}</div>'),
    ],
)
def test_legacy_body_parsers_capture_non_paragraph_text_in_order(
    monkeypatch, module_name, class_name, body_markup
):
    body = """
      머리말
      <div><span>중첩</span><span>텍스트</span></div>
      <p>이미지 앞<img src="/image.jpg">이미지 뒤</p>
      <div>문단 밖</div>
      <video><source src="/clip.mp4"></video>
      <p>꼬리말<style>스타일 노이즈</style></p>
    """
    html = f"<html><body>{body_markup.format(body=body)}</body></html>"
    module = importlib.import_module(
        f"crawl_scheduler.community_website.{module_name}"
    )
    crawler = getattr(module, class_name).__new__(getattr(module, class_name))
    saved_urls = []
    monkeypatch.setattr(
        module.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(html),
    )
    monkeypatch.setattr(
        AbstractCommunityWebsite,
        "save_file",
        lambda self, media_url, **kwargs: saved_urls.append(media_url)
        or f"test/{media_url.rsplit('/', 1)[-1]}",
    )
    monkeypatch.setattr(
        AbstractCommunityWebsite,
        "img_to_text",
        lambda self, path: "이미지 OCR",
    )

    contents = crawler.get_board_contents(
        category="best",
        no=1,
        url="https://example.com/posts/1",
    )

    assert [block["type"] for block in contents] == [
        "text",
        "image",
        "text",
        "video",
        "text",
    ]
    assert contents[0]["text"] == "머리말\n중첩 텍스트\n이미지 앞"
    assert contents[1]["text"] == "이미지 OCR"
    assert contents[2]["text"] == "이미지 뒤\n문단 밖"
    assert contents[4]["text"] == "꼬리말"
    assert "스타일 노이즈" not in "\n".join(
        block.get("text", "") for block in contents
    )
    assert saved_urls == [
        "https://example.com/image.jpg",
        "https://example.com/clip.mp4",
    ]
