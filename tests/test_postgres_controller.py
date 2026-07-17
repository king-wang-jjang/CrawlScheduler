from datetime import datetime, timedelta, timezone
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


def test_insert_uses_metadata_image_as_thumbnail_fallback(tmp_path):
    from crawl_scheduler.db.postgres_controller import PostgresController

    controller = PostgresController(database_url=f"sqlite:///{tmp_path / 'crawler.db'}")

    controller.insert_one(
        "Realtime",
        {
            "site": "theqoo",
            "category": "hot",
            "no": 778,
            "title": "metadata title",
            "url": "https://example.com/post/778",
            "contents": [
                {"type": "text", "content": "body text"},
                {"type": "metadata", "image_url": "https://cdn.example.com/og.jpg"},
            ],
        },
    )

    rows = controller.find("Realtime", {"site": "theqoo", "category": "hot", "no": 778})

    assert rows[0]["thumbnail"] == "https://cdn.example.com/og.jpg"


def test_insert_prefers_local_image_thumbnail_over_metadata_fallback(tmp_path):
    from crawl_scheduler.db.postgres_controller import PostgresController

    controller = PostgresController(database_url=f"sqlite:///{tmp_path / 'crawler.db'}")

    controller.insert_one(
        "Realtime",
        {
            "site": "dcinside",
            "category": "humor",
            "no": 779,
            "title": "local image title",
            "url": "https://example.com/post/779",
            "contents": [
                {"type": "metadata", "image_url": "https://cdn.example.com/og.jpg"},
                {"type": "image", "media_path": "Dcinside/humor/779/image.webp"},
            ],
        },
    )

    rows = controller.find("Realtime", {"site": "dcinside", "category": "humor", "no": 779})

    assert rows[0]["thumbnail"] == "Dcinside/humor/779/image.webp"


def test_upsert_records_native_metric_snapshots_and_scores(tmp_path):
    from crawl_scheduler.db.models import Board, BoardMetricSnapshot
    from crawl_scheduler.db.postgres import get_session_factory
    from crawl_scheduler.db.postgres_controller import PostgresController

    controller = PostgresController(database_url=f"sqlite:///{tmp_path / 'crawler.db'}")
    created_at = datetime(2026, 6, 23, 9, tzinfo=timezone.utc)
    first_capture = created_at + timedelta(minutes=5)
    second_capture = first_capture + timedelta(minutes=20)

    controller.insert_one(
        "Realtime",
        {
            "site": "dcinside",
            "category": "humor",
            "no": 900,
            "title": "metric title",
            "url": "https://example.com/post/900",
            "create_time": created_at,
            "comment_count": 2,
            "like_count": 1,
            "source_rank": 8,
            "metrics_crawled_at": first_capture,
        },
    )
    controller.insert_one(
        "Realtime",
        {
            "site": "dcinside",
            "category": "humor",
            "no": 900,
            "title": "metric title",
            "url": "https://example.com/post/900",
            "create_time": created_at,
            "comment_count": 9,
            "like_count": 5,
            "source_rank": 2,
            "metrics_crawled_at": second_capture,
        },
    )

    rows = controller.find("Realtime", {"site": "dcinside", "category": "humor", "no": 900})
    with get_session_factory(controller.database_url)() as session:
        board = session.query(Board).filter_by(source_id="dcinside:humor:900").one()
        snapshots = (
            session.query(BoardMetricSnapshot)
            .filter_by(board_id=board.id)
            .order_by(BoardMetricSnapshot.captured_at)
            .all()
        )

    assert rows[0]["native_comment_count"] == 9
    assert rows[0]["native_like_count"] == 5
    assert rows[0]["source_rank"] == 2
    assert rows[0]["hot_score"] > 0
    assert rows[0]["daily_score"] > 0
    assert rows[0]["score_breakdown"]["algorithm_version"] == 2
    assert rows[0]["score_breakdown"]["delta_comments_20m"] == 7
    assert rows[0]["score_breakdown"]["delta_likes_20m"] == 4
    assert len(snapshots) == 2
    assert snapshots[1].comment_count == 9


def test_refresh_native_metrics_preserves_crawled_and_local_board_data(tmp_path):
    from crawl_scheduler.db.models import Board, BoardMetricSnapshot
    from crawl_scheduler.db.postgres import get_session_factory
    from crawl_scheduler.db.postgres_controller import PostgresController

    controller = PostgresController(database_url=f"sqlite:///{tmp_path / 'crawler.db'}")
    created_at = datetime(2026, 6, 23, 9, tzinfo=timezone.utc)
    first_capture = created_at + timedelta(minutes=5)
    second_capture = first_capture + timedelta(minutes=5)
    result = controller.insert_one(
        "Realtime",
        {
            "site": "dcinside",
            "category": "dcbest",
            "no": 901,
            "title": "preserved title",
            "url": "https://example.com/post/901",
            "create_time": created_at,
            "contents": [{"type": "text", "text": "preserved body"}],
            "native_comment_count": 2,
            "native_like_count": 1,
            "native_view_count": 100,
            "source_rank": 8,
            "metrics_crawled_at": first_capture,
        },
    )
    with get_session_factory(controller.database_url)() as session:
        board = session.get(Board, result.inserted_id)
        board.comment_count = 4
        board.like_count = 3
        board.gpt_answer = "stored summary"
        board.tags = ["stored"]
        board.llm_engagement_score = 82
        board.llm_engagement_reason = "높은 토론 잠재력"
        session.commit()

    refreshed = controller.refresh_native_metrics(
        "Realtime",
        {"site": "dcinside", "category": "dcbest", "no": 901},
        {
            "native_comment_count": 7,
            "native_like_count": 5,
            "native_view_count": 250,
            "source_rank": 2,
            "metrics_crawled_at": second_capture,
        },
    )

    assert refreshed is not None
    assert refreshed["title"] == "preserved title"
    assert refreshed["contents"] == [{"type": "text", "text": "preserved body"}]
    assert refreshed["comment_count"] == 4
    assert refreshed["like_count"] == 3
    assert refreshed["gpt_answer"] == "stored summary"
    assert refreshed["tags"] == ["stored"]
    assert refreshed["llm_engagement_score"] == 82
    assert refreshed["llm_engagement_reason"] == "높은 토론 잠재력"
    assert refreshed["native_comment_count"] == 7
    assert refreshed["native_like_count"] == 5
    assert refreshed["native_view_count"] == 250
    assert refreshed["source_rank"] == 2
    assert refreshed["score_breakdown"]["algorithm_version"] == 2
    assert refreshed["score_breakdown"]["llm_engagement_score"] == 82

    with get_session_factory(controller.database_url)() as session:
        snapshots = session.query(BoardMetricSnapshot).filter_by(board_id=result.inserted_id).all()
        assert len(snapshots) == 2


def test_refresh_native_metrics_keeps_last_known_values_when_parsing_is_partial(tmp_path):
    from crawl_scheduler.db.postgres_controller import PostgresController

    controller = PostgresController(database_url=f"sqlite:///{tmp_path / 'crawler.db'}")
    result = controller.insert_one(
        "Realtime",
        {
            "site": "ppomppu",
            "category": "freeboard",
            "no": 902,
            "title": "partial metrics",
            "url": "https://example.com/post/902",
            "native_comment_count": 8,
            "native_like_count": 3,
            "native_view_count": 500,
            "source_rank": 4,
        },
    )

    refreshed = controller.refresh_native_metrics(
        "Realtime",
        {"_id": result.inserted_id},
        {
            "native_comment_count": 9,
            "native_like_count": None,
            "native_view_count": None,
            "source_rank": None,
        },
    )

    assert refreshed is not None
    assert refreshed["native_comment_count"] == 9
    assert refreshed["native_like_count"] == 3
    assert refreshed["native_view_count"] == 500
    assert refreshed["source_rank"] == 4


def test_metric_snapshots_are_indexed_and_pruned_after_retention_window(tmp_path):
    from sqlalchemy import inspect

    from crawl_scheduler.db.models import BoardMetricSnapshot
    from crawl_scheduler.db.postgres import get_engine, get_session_factory
    from crawl_scheduler.db.postgres_controller import PostgresController

    controller = PostgresController(database_url=f"sqlite:///{tmp_path / 'crawler.db'}")
    now = datetime.now(timezone.utc)
    stale_result = controller.insert_one(
        "Realtime",
        {
            "site": "dcinside",
            "category": "dcbest",
            "no": 903,
            "title": "dropped from feed",
            "url": "https://example.com/post/903",
            "native_comment_count": 1,
            "native_like_count": 1,
            "metrics_crawled_at": now - timedelta(days=8),
        },
    )

    active_result = controller.insert_one(
        "Realtime",
        {
            "site": "ppomppu",
            "category": "freeboard",
            "no": 904,
            "title": "active feed post",
            "url": "https://example.com/post/904",
            "native_comment_count": 5,
            "native_like_count": 3,
            "metrics_crawled_at": now,
        },
    )

    indexes = inspect(get_engine(controller.database_url)).get_indexes(
        "board_metric_snapshots"
    )
    assert any(
        index["name"] == "ix_board_metric_snapshots_board_captured_at"
        and index["column_names"] == ["board_id", "captured_at"]
        for index in indexes
    )
    assert any(
        index["name"] == "ix_board_metric_snapshots_captured_at"
        and index["column_names"] == ["captured_at"]
        for index in indexes
    )
    with get_session_factory(controller.database_url)() as session:
        assert (
            session.query(BoardMetricSnapshot)
            .filter_by(board_id=stale_result.inserted_id)
            .count()
            == 0
        )
        assert (
            session.query(BoardMetricSnapshot)
            .filter_by(board_id=active_result.inserted_id)
            .count()
            == 1
        )


def test_best_lists_order_by_popularity_scores(tmp_path):
    from crawl_scheduler.db.models import Board
    from crawl_scheduler.db.postgres import get_session_factory
    from crawl_scheduler.db.postgres_controller import PostgresController

    controller = PostgresController(database_url=f"sqlite:///{tmp_path / 'crawler.db'}")
    created_at = datetime(2026, 6, 23, 9, tzinfo=timezone.utc)
    with get_session_factory(controller.database_url)() as session:
        session.add_all(
            [
                Board(
                    id="cold-new",
                    category="humor",
                    no=1,
                    site="dcinside",
                    title="new but cold",
                    url="https://example.com/1",
                    created_at=created_at + timedelta(minutes=20),
                    hot_score=1,
                    daily_score=1,
                ),
                Board(
                    id="hot-older",
                    category="humor",
                    no=2,
                    site="dcinside",
                    title="older but hot",
                    url="https://example.com/2",
                    created_at=created_at,
                    hot_score=10,
                    daily_score=20,
                ),
            ]
        )
        session.commit()

    assert [row["id"] for row in controller.get_realtime_best(0, 10)] == ["hot-older", "cold-new"]
    assert [row["id"] for row in controller.get_daily_best(0, 10)] == ["hot-older", "cold-new"]


def test_best_lists_apply_decay_after_the_last_score_update(tmp_path):
    from crawl_scheduler.db.models import Board
    from crawl_scheduler.db.postgres import get_session_factory
    from crawl_scheduler.db.postgres_controller import PostgresController

    controller = PostgresController(database_url=f"sqlite:///{tmp_path / 'crawler.db'}")
    now = datetime.now(timezone.utc)
    with get_session_factory(controller.database_url)() as session:
        session.add_all(
            [
                Board(
                    id="stale-high-score",
                    category="humor",
                    no=10,
                    site="dcinside",
                    title="stale",
                    url="https://example.com/stale",
                    contents=[],
                    created_at=now - timedelta(days=10),
                    hot_score=10,
                    daily_score=10,
                    score_updated_at=now - timedelta(hours=120),
                ),
                Board(
                    id="fresh-lower-score",
                    category="humor",
                    no=11,
                    site="dcinside",
                    title="fresh",
                    url="https://example.com/fresh",
                    contents=[],
                    created_at=now,
                    hot_score=1,
                    daily_score=1,
                    score_updated_at=now,
                ),
            ]
        )
        session.commit()

    realtime = controller.get_realtime_best(0, 2)
    daily = controller.get_daily_best(0, 2)

    assert realtime[0]["id"] == "fresh-lower-score"
    assert realtime[0]["hot_score"] > realtime[1]["hot_score"]
    assert daily[0]["id"] == "fresh-lower-score"
    assert daily[0]["daily_score"] > daily[1]["daily_score"]
