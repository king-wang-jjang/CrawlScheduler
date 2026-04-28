from datetime import datetime, timezone
import sys
from pathlib import Path


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))


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
    assert rows[0]["contents"] == [{"type": "text", "content": "body"}]
    assert rows[0]["create_time"] == created_at


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
