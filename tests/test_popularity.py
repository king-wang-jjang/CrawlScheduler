from datetime import datetime, timedelta, timezone

from crawl_scheduler.popularity import PopularityMetrics, calculate_popularity_scores


def test_calculate_popularity_scores_uses_recent_growth():
    captured_at = datetime(2026, 6, 23, 10, 20, tzinfo=timezone.utc)

    quiet = calculate_popularity_scores(
        PopularityMetrics(
            site="dcinside",
            created_at=captured_at - timedelta(hours=1),
            captured_at=captured_at,
            comment_count=100,
            like_count=50,
            previous_comment_count=100,
            previous_like_count=50,
        )
    )
    rising = calculate_popularity_scores(
        PopularityMetrics(
            site="dcinside",
            created_at=captured_at - timedelta(hours=1),
            captured_at=captured_at,
            comment_count=30,
            like_count=20,
            previous_comment_count=2,
            previous_like_count=1,
        )
    )

    assert rising.hot_score > quiet.hot_score
    assert rising.breakdown["delta_comments_20m"] == 28
    assert rising.breakdown["delta_likes_20m"] == 19
