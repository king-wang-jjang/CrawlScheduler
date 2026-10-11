import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from crawl_scheduler.db.models import Board, DailyTop10Snapshot, HourlyTop10Snapshot
from crawl_scheduler.db.postgres import get_session_factory
from crawl_scheduler.db.postgres_controller import PostgresController


FIRST = datetime(2026, 10, 11, 14, 5, tzinfo=timezone.utc)


@pytest.fixture
def controller(tmp_path):
    controller = PostgresController(database_url=f"sqlite:///{tmp_path / 'crawler.db'}")
    with get_session_factory(controller.database_url)() as session:
        session.add_all([
            Board(id="first", category="issue", no=1, site="dcinside", title="처음 제목",
                  url="https://example.com/first", created_at=FIRST, score_updated_at=FIRST, daily_score=100),
            Board(id="second", category="issue", no=2, site="dcinside", title="두 번째 글",
                  url="https://example.com/second", created_at=FIRST, score_updated_at=FIRST, daily_score=50),
        ])
        session.commit()
    return controller


def test_hourly_capture_replaces_only_its_hour_with_latest_observation(controller):
    factory = get_session_factory(controller.database_url)
    controller.record_daily_top10_snapshot(FIRST)
    latest = FIRST.replace(minute=55)
    with factory() as session:
        session.get(Board, "first").title = "바뀐 제목"
        session.get(Board, "second").daily_score = 200
        session.commit()
    controller.record_daily_top10_snapshot(latest)
    # A delayed earlier observation cannot overwrite the hourly capture.
    controller.record_daily_top10_snapshot(FIRST + timedelta(minutes=10))
    with factory() as session:
        snapshot = session.scalars(select(HourlyTop10Snapshot)).one()
        assert snapshot.started_at == FIRST.replace(minute=0, tzinfo=None)
        assert snapshot.captured_at == latest.replace(tzinfo=None)
        assert [post["board_id"] for post in snapshot.posts] == ["second", "first"]
        assert [post["rank"] for post in snapshot.posts] == [1, 2]
        assert snapshot.posts[1]["title"] == "바뀐 제목"
        assert snapshot.posts[1]["url"] == "https://example.com/first"
        saved_posts = snapshot.posts
        session.get(Board, "first").title = "다음 시간 제목"
        session.get(Board, "first").daily_score = 400
        session.commit()
    controller.record_daily_top10_snapshot(FIRST + timedelta(hours=1))
    with factory() as session:
        snapshots = session.scalars(select(HourlyTop10Snapshot).order_by(HourlyTop10Snapshot.started_at)).all()
        assert len(snapshots) == 2
        assert snapshots[0].posts == saved_posts
        assert snapshots[1].posts[0]["board_id"] == "first"
        assert snapshots[1].posts[0]["title"] == "다음 시간 제목"


def test_hourly_write_rolls_back_with_daily_snapshot_on_failure(controller, monkeypatch):
    controller.record_daily_top10_snapshot(FIRST)
    def fail(*args):
        raise RuntimeError("hourly capture failed")
    monkeypatch.setattr(controller, "_record_hourly_top10_snapshot", fail)
    with pytest.raises(RuntimeError, match="hourly capture failed"):
        controller.record_daily_top10_snapshot(FIRST + timedelta(hours=1))
    with get_session_factory(controller.database_url)() as session:
        assert len(session.scalars(select(HourlyTop10Snapshot)).all()) == 1
        assert all(snapshot.captured_at == FIRST.replace(tzinfo=None) for snapshot in session.scalars(select(DailyTop10Snapshot)).all())


def test_empty_hours_are_saved_and_old_hours_are_pruned(controller):
    controller.record_daily_top10_snapshot(FIRST)
    with get_session_factory(controller.database_url)() as session:
        session.query(DailyTop10Snapshot).delete()
        session.query(Board).delete()
        session.commit()
    after_retention = FIRST + timedelta(days=8)
    assert controller.record_daily_top10_snapshot(after_retention) == 0
    with get_session_factory(controller.database_url)() as session:
        snapshot = session.scalars(select(HourlyTop10Snapshot)).one()
        assert snapshot.started_at == after_retention.replace(minute=0, tzinfo=None)
        assert snapshot.posts == []


def test_hourly_top10_uses_live_coverage_and_inactive_fallback_independently_of_daily_limit(controller):
    factory = get_session_factory(controller.database_url)
    with factory() as session:
        old_time = FIRST - timedelta(days=8)
        session.add(Board(
            id="old-source", category="issue", no=3, site="ppomppu", title="오래된 인기글",
            url="https://example.com/old", created_at=old_time, score_updated_at=FIRST,
            metrics_crawled_at=old_time, daily_score=500,
        ))
        session.commit()
    # The inactive source gets no initial coverage, but it can fill a later slot.
    controller.record_daily_top10_snapshot(FIRST, limit=1)
    with factory() as session:
        snapshot = session.scalars(select(HourlyTop10Snapshot)).one()
        assert [post["board_id"] for post in snapshot.posts] == ["first", "old-source", "second"]
        assert len(session.scalars(select(DailyTop10Snapshot)).all()) == 1
