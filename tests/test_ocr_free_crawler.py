import sys
from pathlib import Path
from types import SimpleNamespace


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))


def test_community_website_imports_without_ocr_dependencies():
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

    crawler = ConcreteCrawler("20260428")

    assert crawler.img_to_text("image.jpg") is None


def test_project_dependencies_do_not_include_ocr_stack():
    pyproject = (SERVICE_ROOT / "pyproject.toml").read_text(encoding="utf-8").lower()
    dockerfile = (SERVICE_ROOT / "Dockerfile").read_text(encoding="utf-8").lower()

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
