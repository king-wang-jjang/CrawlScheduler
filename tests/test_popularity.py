from datetime import datetime, timedelta, timezone

import pytest

from crawl_scheduler.popularity import (
    PopularityMetrics,
    RankingCandidate,
    balance_site_exposure,
    calculate_popularity_scores,
)


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
    assert rising.breakdown["algorithm_version"] == 3


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


def test_site_profiles_make_equivalent_relative_engagement_comparable():
    captured_at = datetime(2026, 6, 23, 10, 20, tzinfo=timezone.utc)
    common = {
        "created_at": captured_at - timedelta(hours=1),
        "captured_at": captured_at,
        "source_rank": 3,
    }

    dcinside = calculate_popularity_scores(
        PopularityMetrics(
            **common,
            site="dcinside",
            comment_count=60,
            like_count=100,
            view_count=12_000,
        )
    )
    ppomppu = calculate_popularity_scores(
        PopularityMetrics(
            **common,
            site="ppomppu",
            comment_count=25,
            like_count=20,
            view_count=4_000,
        )
    )

    assert dcinside.hot_score == pytest.approx(ppomppu.hot_score)
    assert dcinside.daily_score == pytest.approx(ppomppu.daily_score)
    assert dcinside.breakdown["comment_count"] == 60
    assert dcinside.breakdown["like_count"] == 100
    assert dcinside.breakdown["view_count"] == 12_000
    assert dcinside.breakdown["normalized_comment_count"] == pytest.approx(1.0)
    assert dcinside.breakdown["normalized_like_count"] == pytest.approx(1.0)
    assert dcinside.breakdown["normalized_view_count"] == pytest.approx(1.0)
    assert dcinside.breakdown["site_profile"] == "dcinside"
    assert ppomppu.breakdown["site_profile"] == "ppomppu"


def test_unavailable_source_metric_is_reweighted_instead_of_treated_as_zero():
    captured_at = datetime(2026, 6, 23, 10, 20, tzinfo=timezone.utc)
    common = {
        "site": "theqoo",
        "created_at": captured_at - timedelta(hours=1),
        "captured_at": captured_at,
        "comment_count": 120,
        "view_count": 30_000,
    }

    unavailable = calculate_popularity_scores(
        PopularityMetrics(**common, like_count=None)
    )
    known_zero = calculate_popularity_scores(
        PopularityMetrics(**common, like_count=0)
    )
    at_baseline = calculate_popularity_scores(
        PopularityMetrics(**common, like_count=50)
    )

    assert unavailable.breakdown["metric_availability"] == {
        "comments": True,
        "likes": False,
        "views": True,
    }
    assert unavailable.breakdown["total_available_weight"] == pytest.approx(1.4)
    assert unavailable.breakdown["total_component"] == pytest.approx(
        at_baseline.breakdown["total_component"]
    )
    assert unavailable.daily_score > known_zero.daily_score


def test_unknown_site_uses_documented_default_profile():
    captured_at = datetime(2026, 6, 23, 10, 20, tzinfo=timezone.utc)

    scores = calculate_popularity_scores(
        PopularityMetrics(
            site="new-community",
            created_at=captured_at,
            captured_at=captured_at,
            comment_count=30,
            like_count=30,
            view_count=5_000,
        )
    )

    assert scores.breakdown["site_profile"] == "default"
    assert scores.breakdown["site_baselines"]["comment_total"] == 30.0
    assert scores.breakdown["total_component"] == pytest.approx(3.2)


def test_site_balanced_selection_covers_each_site_before_exposure_adjusted_fill():
    created_at = datetime(2026, 8, 12, 6, tzinfo=timezone.utc)
    candidates = [
        RankingCandidate("dc-1", "dcinside", 100, created_at),
        RankingCandidate("dc-2", "dcinside", 90, created_at),
        RankingCandidate("dc-3", "dcinside", 80, created_at),
        RankingCandidate("pp-1", "ppomppu", 40, created_at),
        RankingCandidate("pp-2", "ppomppu", 30, created_at),
        RankingCandidate("tq-1", "theqoo", 20, created_at),
        RankingCandidate("low-1", "low-quality", 10, created_at),
    ]

    selected = balance_site_exposure(candidates, limit=5)

    assert [candidate.board_id for candidate in selected] == [
        "dc-1",
        "pp-1",
        "tq-1",
        "low-1",
        "dc-2",
    ]
    assert {candidate.site for candidate in selected} == {
        "dcinside",
        "ppomppu",
        "theqoo",
        "low-quality",
    }


def test_site_balanced_selection_uses_recency_for_equal_scores():
    created_at = datetime(2026, 8, 12, 6, tzinfo=timezone.utc)
    candidates = [
        RankingCandidate("older", "dcinside", 10, created_at),
        RankingCandidate(
            "newer-b",
            "ppomppu",
            10,
            created_at + timedelta(minutes=1),
        ),
        RankingCandidate(
            "newer-a",
            "ygosu",
            10,
            created_at + timedelta(minutes=1),
        ),
    ]

    assert [
        candidate.board_id
        for candidate in balance_site_exposure(candidates, limit=3)
    ] == ["newer-b", "newer-a", "older"]
