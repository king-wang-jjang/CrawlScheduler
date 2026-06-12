from datetime import datetime, timezone
import sys
from pathlib import Path


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))


class FakeAnalyzer:
    def analyze(self, content: str):
        assert "ai title" in content
        assert "body" in content
        return {"summary": "AI 요약", "tags": ["유머", "핫딜"]}


class UnexpectedAnalyzer:
    def __init__(self):
        self.calls = 0

    def analyze(self, content: str):
        self.calls += 1
        return {"summary": "should not be stored", "tags": ["unexpected"]}


def test_realtime_document_is_upserted_into_boards_table(tmp_path):
    from crawl_scheduler.db.postgres_controller import PostgresController

    controller = PostgresController(database_url=f"sqlite:///{tmp_path / 'crawler.db'}")
    created_at = datetime(2026, 4, 28, 10, 30, tzinfo=timezone.utc)

    result = controller.insert_one(
        "Realtime",
        {
            "site": "ygosu",
            "category": "humor",
            "no": 123,
            "title": "first title",
            "url": "https://example.com/post/123",
            "create_time": created_at,
            "gpt_answer": "default answer",
            "contents": [{"type": "text", "content": "body"}],
        },
    )
    controller.insert_one(
        "Realtime",
        {
            "site": "ygosu",
            "category": "humor",
            "no": 123,
            "title": "updated title",
            "url": "https://example.com/post/123",
            "create_time": created_at,
            "gpt_answer": "default answer",
            "contents": [{"type": "text", "content": "body"}],
        },
    )

    rows = controller.find("Realtime", {"site": "ygosu", "category": "humor", "no": 123})

    assert result.inserted_id
    assert len(rows) == 1
    assert rows[0]["_id"] == result.inserted_id
    assert rows[0]["source_id"] == "ygosu:humor:123"
    assert rows[0]["title"] == "updated title"
    assert rows[0]["gpt_answer"] == "default answer"
    assert rows[0]["contents"] == [{"type": "text", "text": "body"}]
    assert rows[0]["create_time"] == created_at


def test_realtime_document_stores_default_summary_without_ai_analysis(tmp_path):
    from crawl_scheduler.constants import DEFAULT_GPT_ANSWER
    from crawl_scheduler.db.postgres_controller import PostgresController

    analyzer = UnexpectedAnalyzer()
    controller = PostgresController(
        database_url=f"sqlite:///{tmp_path / 'crawler.db'}",
        analyzer=analyzer,
    )

    result = controller.insert_one(
        "Realtime",
        {
            "site": "ygosu",
            "category": "humor",
            "no": 124,
            "title": "ai title",
            "url": "https://example.com/post/124",
            "contents": [{"type": "text", "content": "body"}],
            "gpt_answer": DEFAULT_GPT_ANSWER,
            "tag": [],
        },
    )

    rows = controller.find("Realtime", {"_id": result.inserted_id})

    assert rows[0]["gpt_answer"] == DEFAULT_GPT_ANSWER
    assert rows[0]["tags"] == []
    assert analyzer.calls == 0


def test_gpt_and_tag_collections_are_virtual_defaults(tmp_path):
    from crawl_scheduler.db.postgres_controller import PostgresController

    controller = PostgresController(database_url=f"sqlite:///{tmp_path / 'crawler.db'}")

    gpt_result = controller.insert_one(
        "GPT",
        {"board_id": ("humor", 123), "site": "ygosu", "answer": "default answer"},
    )
    tag_result = controller.insert_one(
        "TAG",
        {"board_id": ("humor", 123), "site": "ygosu", "Tag": ["default"]},
    )

    assert gpt_result.inserted_id == "default answer"
    assert tag_result.inserted_id == ["default"]
    assert controller.find("GPT", {"site": "ygosu"}) == []
    assert controller.find("TAG", {"site": "ygosu"}) == []


def test_large_board_numbers_are_preserved(tmp_path):
    from crawl_scheduler.db.postgres_controller import PostgresController

    controller = PostgresController(database_url=f"sqlite:///{tmp_path / 'crawler.db'}")
    large_no = 4_190_928_872

    controller.insert_one(
        "Realtime",
        {
            "site": "theqoo",
            "category": "hot",
            "no": large_no,
            "title": "large no title",
            "url": "https://example.com/hot/4190928872",
        },
    )

    rows = controller.find("Realtime", {"site": "theqoo", "category": "hot", "no": large_no})

    assert len(rows) == 1
    assert rows[0]["source_id"] == f"theqoo:hot:{large_no}"
    assert rows[0]["no"] == large_no


def test_insert_normalizes_crawled_contents_and_extracts_thumbnail(tmp_path):
    from crawl_scheduler.db.postgres_controller import PostgresController

    controller = PostgresController(database_url=f"sqlite:///{tmp_path / 'crawler.db'}")

    controller.insert_one(
        "Realtime",
        {
            "site": "dcinside",
            "category": "humor",
            "no": 777,
            "title": "normalized title",
            "url": "https://example.com/post/777",
            "contents": [
                {"type": "image", "path": "Dcinside/humor/777/image.webp", "content": "ocr text"},
                {"type": "text", "content": "body text"},
            ],
        },
    )

    rows = controller.find("Realtime", {"site": "dcinside", "category": "humor", "no": 777})

    assert rows[0]["contents"] == [
        {"type": "image", "media_path": "Dcinside/humor/777/image.webp", "text": "ocr text"},
        {"type": "text", "text": "body text"},
    ]
    assert rows[0]["thumbnail"] == "Dcinside/humor/777/image.webp"
