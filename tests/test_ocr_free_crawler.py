import sys
from pathlib import Path
from types import SimpleNamespace

from bs4 import BeautifulSoup


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))


def test_community_website_delegates_image_text_to_ocr_extractor(monkeypatch):
    import crawl_scheduler.community_website.community_website as community_website

    class ConcreteCrawler(community_website.AbstractCommunityWebsite):
        def get_daily_best(self):
            pass

        def get_realtime_best(self):
            pass

        def get_board_contents(self, board_id):
            pass

        def is_ad(self, title) -> bool:
            return False

        def get_gpt_obj(self, url):
            pass

        def get_board_list(self):
            pass

    calls = []

    def fake_extract_text_from_image(img_path):
        calls.append(img_path)
        return "이미지 안의 한국어"

    monkeypatch.setattr(
        community_website,
        "extract_text_from_image",
        fake_extract_text_from_image,
    )

    crawler = ConcreteCrawler("20260428")

    assert crawler.img_to_text("image.jpg") == "이미지 안의 한국어"
    assert calls == ["image.jpg"]


def test_project_dependencies_use_paddle_ocr_not_tesseract():
    pyproject = (SERVICE_ROOT / "pyproject.toml").read_text(encoding="utf-8").lower()
    dockerfile = (SERVICE_ROOT / "Dockerfile").read_text(encoding="utf-8").lower()

    assert "paddleocr" in pyproject
    assert "paddlepaddle" in pyproject
    assert "libgl1" in dockerfile

    for dependency in ["pytesseract", "opencv-python"]:
        assert dependency not in pyproject

    for system_package in ["tesseract-ocr", "tesseract-ocr-kor", "libtesseract-dev"]:
        assert system_package not in dockerfile

    assert "tessdata_prefix" not in dockerfile


def test_save_file_sanitizes_query_only_media_urls(monkeypatch, tmp_path):
    import crawl_scheduler.community_website.community_website as community_website

    class ConcreteCrawler(community_website.AbstractCommunityWebsite):
        def get_daily_best(self):
            pass

        def get_realtime_best(self):
            pass

        def get_board_contents(self, board_id):
            pass

        def is_ad(self, title) -> bool:
            return False

        def get_gpt_obj(self, url):
            pass

        def get_board_list(self):
            pass

    monkeypatch.setenv("ROOT", str(tmp_path))
    monkeypatch.setattr(
        community_website.requests,
        "get",
        lambda *args, **kwargs: SimpleNamespace(
            status_code=200,
            headers={"Content-Type": "image/webp"},
            content=b"image-bytes",
        ),
    )

    crawler = ConcreteCrawler("20260428")

    relative_path = crawler.save_file(
        "https://storage2.ygosu.com/?code=S69ef94e374d2c6.49108564",
        category="yeobgi",
        no=2144113,
    )

    assert relative_path
    assert "?" not in Path(relative_path).name
    assert Path(relative_path).suffix == ".webp"


def test_save_file_does_not_write_failed_response(monkeypatch, tmp_path):
    import crawl_scheduler.community_website.community_website as community_website

    class ConcreteCrawler(community_website.AbstractCommunityWebsite):
        def get_daily_best(self):
            pass

        def get_realtime_best(self):
            pass

        def get_board_contents(self, board_id):
            pass

        def is_ad(self, title) -> bool:
            return False

        def get_gpt_obj(self, url):
            pass

        def get_board_list(self):
            pass

    monkeypatch.setenv("ROOT", str(tmp_path))
    monkeypatch.setattr(
        community_website.requests,
        "get",
        lambda *args, **kwargs: SimpleNamespace(
            status_code=404,
            headers={"Content-Type": "text/html"},
            content=b"not-found",
        ),
    )

    crawler = ConcreteCrawler("20260428")

    assert not crawler.save_file(
        "https://example.com/missing.jpg",
        category="humor",
        no=1,
    )
    assert list(tmp_path.rglob("*.*")) == []


def test_media_url_from_tag_prefers_real_lazy_image_over_placeholder():
    import crawl_scheduler.community_website.community_website as community_website

    soup = BeautifulSoup(
        '<img src="https://nstatic.dcinside.com/dc/m/img/gallview_loading_ori.gif" '
        'data-original="//dcimg6.dcinside.co.kr/viewimage.php?id=real">',
        "html.parser",
    )

    assert (
        community_website.AbstractCommunityWebsite.media_url_from_tag(soup.img)
        == "https://dcimg6.dcinside.co.kr/viewimage.php?id=real"
    )


def test_normalize_media_url_repairs_duplicate_scheme():
    import crawl_scheduler.community_website.community_website as community_website

    assert (
        community_website.AbstractCommunityWebsite.normalize_media_url(
            "https:https://dcimg6.dcinside.co.kr/viewimage.php?id=real"
        )
        == "https://dcimg6.dcinside.co.kr/viewimage.php?id=real"
    )


def test_metadata_image_url_from_soup_prefers_open_graph_image():
    import crawl_scheduler.community_website.community_website as community_website

    soup = BeautifulSoup(
        '<html><head>'
        '<meta property="og:image" content="//cdn.example.com/post.jpg">'
        '<meta name="twitter:image" content="https://cdn.example.com/twitter.jpg">'
        '</head></html>',
        "html.parser",
    )

    assert (
        community_website.AbstractCommunityWebsite.metadata_image_url_from_soup(
            soup,
            base_url="https://example.com/post/1",
        )
        == "https://cdn.example.com/post.jpg"
    )


def test_metadata_image_url_from_soup_supports_image_src_link():
    import crawl_scheduler.community_website.community_website as community_website

    soup = BeautifulSoup(
        '<html><head><link rel="image_src" href="/images/post.jpg"></head></html>',
        "html.parser",
    )

    assert (
        community_website.AbstractCommunityWebsite.metadata_image_url_from_soup(
            soup,
            base_url="https://example.com/post/1",
        )
        == "https://example.com/images/post.jpg"
    )


def test_metadata_image_url_skips_video_and_uses_later_image_candidate():
    import crawl_scheduler.community_website.community_website as community_website

    soup = BeautifulSoup(
        '<html><head>'
        '<meta property="og:image" content="https://cdn.example.com/post.mp4">'
        '<meta name="twitter:image" content="https://cdn.example.com/post.jpg">'
        '</head></html>',
        "html.parser",
    )

    assert (
        community_website.AbstractCommunityWebsite.metadata_image_url_from_soup(soup)
        == "https://cdn.example.com/post.jpg"
    )


def test_metadata_image_url_rejects_known_placeholder_images():
    import crawl_scheduler.community_website.community_website as community_website

    soup = BeautifulSoup(
        '<html><head>'
        '<meta property="og:image" '
        'content="https://cdn2.ppomppu.co.kr/images/icon_app_20160427.png">'
        '<meta name="twitter:image" '
        'content="https://cdn3.ppomppu.co.kr/cdn_img_404.jpg">'
        '</head></html>',
        "html.parser",
    )

    assert (
        community_website.AbstractCommunityWebsite.metadata_image_url_from_soup(soup)
        is None
    )
