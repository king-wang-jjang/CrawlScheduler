from datetime import datetime, timedelta, timezone

import pytest

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
    assert rising.breakdown["acceleration_component"] == 2.0
    assert rising.breakdown["algorithm_version"] == 2


def test_llm_engagement_score_is_centered_and_defaults_to_neutral():
    captured_at = datetime(2026, 6, 23, 10, 20, tzinfo=timezone.utc)
    common = {
        "site": "dcinside",
        "created_at": captured_at - timedelta(hours=1),
        "captured_at": captured_at,
        "comment_count": 20,
        "like_count": 10,
        "previous_comment_count": 20,
        "previous_like_count": 10,
    }

    low = calculate_popularity_scores(PopularityMetrics(**common, llm_engagement_score=0))
    neutral = calculate_popularity_scores(PopularityMetrics(**common))
    explicit_neutral = calculate_popularity_scores(
        PopularityMetrics(**common, llm_engagement_score=50)
    )
    high = calculate_popularity_scores(PopularityMetrics(**common, llm_engagement_score=100))

    assert high.hot_score > neutral.hot_score > low.hot_score
    assert high.daily_score > neutral.daily_score > low.daily_score
    assert neutral.hot_score == pytest.approx(explicit_neutral.hot_score)
    assert neutral.breakdown["llm_engagement_score"] == 50.0
    assert neutral.breakdown["llm_engagement_signal"] == 0.0
    assert low.breakdown["hot_llm_addend"] == -1.5
    assert high.breakdown["hot_llm_addend"] == 1.5


def test_llm_engagement_score_is_clamped_to_supported_range():
    captured_at = datetime(2026, 6, 23, 10, 20, tzinfo=timezone.utc)
    common = {
        "site": "theqoo",
        "created_at": captured_at - timedelta(hours=1),
        "captured_at": captured_at,
        "comment_count": 10,
        "like_count": 5,
    }

    above_maximum = calculate_popularity_scores(
        PopularityMetrics(**common, llm_engagement_score=150)
    )
    maximum = calculate_popularity_scores(
        PopularityMetrics(**common, llm_engagement_score=100)
    )
    below_minimum = calculate_popularity_scores(
        PopularityMetrics(**common, llm_engagement_score=-25)
    )
    minimum = calculate_popularity_scores(
        PopularityMetrics(**common, llm_engagement_score=0)
    )

    assert above_maximum.hot_score == pytest.approx(maximum.hot_score)
    assert below_minimum.hot_score == pytest.approx(minimum.hot_score)
    assert above_maximum.breakdown["llm_engagement_score"] == 100.0
    assert below_minimum.breakdown["llm_engagement_score"] == 0.0


def test_velocity_is_normalized_to_a_twenty_minute_rate():
    captured_at = datetime(2026, 6, 23, 10, 20, tzinfo=timezone.utc)
    common = {
        "site": "ppomppu",
        "created_at": captured_at - timedelta(hours=1),
        "captured_at": captured_at,
        "comment_count": 20,
        "like_count": 10,
    }

    five_minute_growth = calculate_popularity_scores(
        PopularityMetrics(
            **common,
            previous_comment_count=15,
            previous_like_count=8,
            previous_captured_at=captured_at - timedelta(minutes=5),
        )
    )
    twenty_minute_growth = calculate_popularity_scores(
        PopularityMetrics(
            **common,
            previous_comment_count=0,
            previous_like_count=2,
            previous_captured_at=captured_at - timedelta(minutes=20),
        )
    )

    assert five_minute_growth.breakdown["current_interval_minutes"] == 5.0
    assert five_minute_growth.breakdown["current_interval_scale"] == 4.0
    assert twenty_minute_growth.breakdown["current_interval_minutes"] == 20.0
    assert twenty_minute_growth.breakdown["current_interval_scale"] == 1.0
    assert five_minute_growth.breakdown["delta_comments_20m"] == 20.0
    assert five_minute_growth.breakdown["delta_likes_20m"] == 8.0
    assert five_minute_growth.breakdown["velocity_component"] == pytest.approx(
        twenty_minute_growth.breakdown["velocity_component"]
    )
    assert five_minute_growth.hot_score == pytest.approx(twenty_minute_growth.hot_score)


def test_interval_normalization_scale_is_bounded():
    captured_at = datetime(2026, 6, 23, 10, 20, tzinfo=timezone.utc)
    common = {
        "site": "ygosu",
        "created_at": captured_at - timedelta(hours=1),
        "captured_at": captured_at,
        "comment_count": 2,
        "previous_comment_count": 1,
    }

    too_short = calculate_popularity_scores(
        PopularityMetrics(
            **common,
            previous_captured_at=captured_at - timedelta(minutes=1),
        )
    )
    too_long = calculate_popularity_scores(
        PopularityMetrics(
            **common,
            previous_captured_at=captured_at - timedelta(minutes=200),
        )
    )

    assert too_short.breakdown["current_interval_scale"] == 4.0
    assert too_long.breakdown["current_interval_scale"] == 0.25


def test_source_rank_is_a_bounded_explicit_addend():
    captured_at = datetime(2026, 6, 23, 10, 20, tzinfo=timezone.utc)
    common = {
        "site": "dcinside",
        "created_at": captured_at - timedelta(hours=1),
        "captured_at": captured_at,
        "comment_count": 10,
        "like_count": 5,
    }

    first = calculate_popularity_scores(PopularityMetrics(**common, source_rank=1))
    fourth = calculate_popularity_scores(PopularityMetrics(**common, source_rank=4))
    unranked = calculate_popularity_scores(PopularityMetrics(**common))

    assert first.hot_score > fourth.hot_score > unranked.hot_score
    assert first.daily_score > fourth.daily_score > unranked.daily_score
    assert first.breakdown["hot_source_rank_addend"] == 2.0
    assert fourth.breakdown["hot_source_rank_addend"] == 1.0
    assert unranked.breakdown["hot_source_rank_addend"] == 0.0
