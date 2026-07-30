import importlib
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))


def test_time_only_source_value_rolls_back_across_midnight():
    from crawl_scheduler.community_website.board_list_entry import recent_source_datetime

    now = datetime(2026, 7, 17, 0, 3, tzinfo=ZoneInfo("Asia/Seoul"))

    previous_day = recent_source_datetime(23, 58, now=now)
    same_day = recent_source_datetime(0, 2, now=now)

    assert previous_day == datetime(2026, 7, 16, 23, 58, tzinfo=ZoneInfo("Asia/Seoul"))
    assert same_day == datetime(2026, 7, 17, 0, 2, tzinfo=ZoneInfo("Asia/Seoul"))


def test_once_flag_runs_crawl_and_exits(monkeypatch):
    from crawl_scheduler import main

    calls = []

    class FakeDB:
        def record_daily_top10_snapshot(self):
            calls.append("snapshot")

    monkeypatch.setattr(
        main,
        "get_realtime_best",
        lambda factories: calls.append(("crawl", tuple(factories))),
    )
    monkeypatch.setattr(main, "PostgresController", FakeDB)

    assert main.main(["--once"]) == 0
    assert calls == [
        ("crawl", main.DEFAULT_CRAWLER_FACTORIES),
        "snapshot",
    ]


def test_seed_alias_runs_crawl_and_exits(monkeypatch):
    from crawl_scheduler import main

    calls = []

    class FakeDB:
        def record_daily_top10_snapshot(self):
            calls.append("snapshot")

    monkeypatch.setattr(
        main,
        "get_realtime_best",
        lambda factories: calls.append(("crawl", tuple(factories))),
    )
    monkeypatch.setattr(main, "PostgresController", FakeDB)

    assert main.main(["--seed"]) == 0
    assert calls == [
        ("crawl", main.DEFAULT_CRAWLER_FACTORIES),
        "snapshot",
    ]


def test_disabled_sites_are_excluded_from_once_and_repeating_jobs(monkeypatch):
    import schedule

    from crawl_scheduler import main

    monkeypatch.setenv("CRAWLER_DISABLED_SITES", "fmkorea, theqoo, arca")
    args = main.parse_args([])
    enabled_sites = {spec.site for spec in args.crawler_specs}

    assert args.disabled_sites == frozenset({"fmkorea", "theqoo", "arca"})
    assert enabled_sites == {"ygosu", "ppomppu", "dcinside", "inven"}

    once_calls = []

    class FakeDB:
        def record_daily_top10_snapshot(self):
            once_calls.append("snapshot")

    monkeypatch.setattr(
        main,
        "get_realtime_best",
        lambda factories: once_calls.append(
            tuple(factory.__name__ for factory in factories)
        ),
    )
    monkeypatch.setattr(main, "PostgresController", FakeDB)

    assert main.main(["--once"]) == 0
    assert once_calls == [
        ("Ygosu", "Ppomppu", "Dcinside", "Inven"),
        "snapshot",
    ]

    scheduler = schedule.Scheduler()
    main.configure_schedule(
        scheduler,
        args.crawler_intervals,
        crawler_specs=args.crawler_specs,
    )

    assert {
        next(iter(job.tags))
        for job in scheduler.jobs
        if any(tag.startswith("crawler:") for tag in job.tags)
    } == {
        "crawler:ygosu",
        "crawler:ppomppu",
        "crawler:dcinside",
        "crawler:inven",
    }


def test_unknown_disabled_site_is_rejected(monkeypatch):
    from crawl_scheduler import main

    monkeypatch.setenv("CRAWLER_DISABLED_SITES", "fmkorea,typo")

    with pytest.raises(SystemExit):
        main.parse_args([])


def test_recommended_site_intervals_are_used_by_default(monkeypatch):
    from crawl_scheduler import main

    for name in (
        "CRAWLER_INTERVAL_MINUTES",
        "CRAWLER_ARCA_INTERVAL_MINUTES",
        "CRAWLER_THEQOO_INTERVAL_MINUTES",
        "CRAWLER_FMKOREA_INTERVAL_MINUTES",
    ):
        monkeypatch.delenv(name, raising=False)

    args = main.parse_args([])

    assert args.crawler_intervals == {
        "ygosu": 5,
        "ppomppu": 5,
        "theqoo": 15,
        "dcinside": 5,
        "fmkorea": 30,
        "arca": 10,
        "inven": 5,
    }


def test_global_interval_environment_overrides_recommended_defaults(monkeypatch):
    from crawl_scheduler import main

    monkeypatch.setenv("CRAWLER_INTERVAL_MINUTES", "12")

    assert set(main.parse_args([]).crawler_intervals.values()) == {12}


def test_site_interval_environment_only_overrides_that_site(monkeypatch):
    from crawl_scheduler import main

    monkeypatch.setenv("CRAWLER_ARCA_INTERVAL_MINUTES", "20")

    intervals = main.parse_args([]).crawler_intervals

    assert intervals["arca"] == 20
    assert intervals["theqoo"] == 15
    assert intervals["fmkorea"] == 30


def test_interval_minutes_cli_option_forces_every_site(monkeypatch):
    from crawl_scheduler import main

    monkeypatch.setenv("CRAWLER_INTERVAL_MINUTES", "15")
    monkeypatch.setenv("CRAWLER_FMKOREA_INTERVAL_MINUTES", "60")

    args = main.parse_args(["--interval-minutes", "30"])

    assert args.interval_minutes == 30
    assert set(args.crawler_intervals.values()) == {30}


@pytest.mark.parametrize(
    ("environment_name", "value"),
    [
        ("CRAWLER_INTERVAL_MINUTES", "0"),
        ("CRAWLER_INTERVAL_MINUTES", "-1"),
        ("CRAWLER_INTERVAL_MINUTES", "invalid"),
        ("CRAWLER_THEQOO_INTERVAL_MINUTES", "1.5"),
    ],
)
def test_interval_minutes_rejects_invalid_environment_values(
    monkeypatch, environment_name, value
):
    from crawl_scheduler import main

    monkeypatch.setenv(environment_name, value)

    with pytest.raises(SystemExit):
        main.parse_args([])


def test_repeating_scheduler_registers_each_site_and_one_snapshot_job():
    import schedule

    from crawl_scheduler import main

    scheduler = schedule.Scheduler()
    intervals = {
        spec.site: spec.default_interval_minutes for spec in main.CRAWLER_SPECS
    }

    main.configure_schedule(scheduler, intervals)

    jobs_by_tag = {
        next(iter(job.tags)): job
        for job in scheduler.jobs
    }
    assert len(jobs_by_tag) == len(main.CRAWLER_SPECS) + 1
    assert jobs_by_tag["crawler:arca"].interval == 10
    assert jobs_by_tag["crawler:theqoo"].interval == 15
    assert jobs_by_tag["crawler:fmkorea"].interval == 30
    assert jobs_by_tag["snapshot"].interval == 5


def test_scheduled_site_job_runs_only_its_factory(monkeypatch):
    import schedule

    from crawl_scheduler import main

    calls = []

    class FirstCrawler:
        pass

    class SecondCrawler:
        pass

    specs = (
        main.CrawlerSpec("first", FirstCrawler, 5),
        main.CrawlerSpec("second", SecondCrawler, 10),
    )
    scheduler = schedule.Scheduler()
    monkeypatch.setattr(
        main,
        "get_realtime_best",
        lambda factories: calls.append(tuple(factories)),
    )

    main.configure_schedule(
        scheduler,
        {"first": 5, "second": 10},
        crawler_specs=specs,
    )
    next(job for job in scheduler.jobs if "crawler:second" in job.tags).run()

    assert calls == [(SecondCrawler,)]


def test_snapshot_failure_is_logged_without_stopping_the_job(monkeypatch):
    from crawl_scheduler import main

    calls = []

    class FailingDB:
        def record_daily_top10_snapshot(self):
            calls.append("snapshot")
            raise RuntimeError("database unavailable")

    class FakeLogger:
        def error(self, message, exc_info=False):
            calls.append((message, exc_info))

    monkeypatch.setattr(
        main,
        "get_realtime_best",
        lambda factories: calls.append(("crawl", tuple(factories))),
    )
    monkeypatch.setattr(main, "PostgresController", FailingDB)
    monkeypatch.setattr(main, "logger", FakeLogger())

    main.job()

    assert calls == [
        ("crawl", main.DEFAULT_CRAWLER_FACTORIES),
        "snapshot",
        ("Error - daily Top10 snapshot: database unavailable", True),
    ]


def test_crawler_initialization_failure_does_not_stop_other_sites(monkeypatch):
    from crawl_scheduler import main

    calls = []

    class FailingCrawler:
        def __init__(self):
            raise RuntimeError("database DNS unavailable")

    class HealthyCrawler:
        def get_realtime_best(self):
            calls.append("healthy crawl")
            return True

    class FakeLogger:
        def error(self, message, exc_info=False):
            calls.append((message, exc_info))

        def info(self, message):
            calls.append(message)

    monkeypatch.setattr(main, "logger", FakeLogger())

    result = main.get_realtime_best((FailingCrawler, HealthyCrawler))

    assert result == {"FailingCrawler": "Fail", "HealthyCrawler": "Success"}
    assert (
        "Error - initializing real-time FailingCrawler: database DNS unavailable",
        True,
    ) in calls
    assert "healthy crawl" in calls


def test_theqoo_board_list_skips_notice_rows_before_hot_posts(monkeypatch):
    from crawl_scheduler.community_website import theqoo

    class FakeDB:
        def find(self, *args, **kwargs):
            return []

    class FakeResponse:
        text = """
        <table class="hide_notice">
          <tr>
            <td>공지</td><td></td>
            <td><a href="/hot/3516074637">공지글</a></td>
            <td>24.12.06</td><td>1,000</td>
          </tr>
          <tr>
            <td class="no">155609</td><td class="cate">이슈</td>
            <td class="title">
              <a href="/hot/4230320581">현재 한국 넷플릭스 top 10.jpg</a>
              <a class="replyNum" href="/hot/4230320581#4230320581_comment">321</a>
            </td>
            <td class="time">15:46</td><td class="m_no">8,769</td>
          </tr>
        </table>
        """

        def raise_for_status(self):
            return None

    monkeypatch.setattr(theqoo, "PostgresController", lambda: FakeDB())
    monkeypatch.setattr(
        theqoo.Theqoo,
        "get_response",
        lambda *args, **kwargs: FakeResponse(),
    )

    crawler = theqoo.Theqoo()
    entries = crawler.get_board_entries()
    rows = crawler.get_board_list()

    assert len(rows) == 1
    assert rows[0][1] == "4230320581"
    assert rows[0][3] == "현재 한국 넷플릭스 top 10.jpg"
    assert entries[0].title == "현재 한국 넷플릭스 top 10.jpg"
    assert entries[0].native_comment_count == 321
    assert entries[0].native_like_count is None
    assert entries[0].native_view_count == 8769
    assert entries[0].source_rank == 1
    assert entries[0].created_at.utcoffset().total_seconds() == 9 * 60 * 60


def test_dcinside_board_list_preserves_absolute_links(monkeypatch):
    from crawl_scheduler.community_website import dcinside

    class FakeDB:
        def find(self, *args, **kwargs):
            return []

    class FakeResponse:
        text = """
        <table>
          <tr class="ub-content">
            <td class="gall_num">공지</td>
            <td class="gall_tit"><a href="/board/view/?id=dcbest&no=1">공지글</a></td>
            <td class="gall_date">15:31</td>
            <td class="gall_count">999</td>
            <td class="gall_recommend">99</td>
          </tr>
          <tr class="ub-content">
            <td class="gall_num">1</td>
            <td class="gall_tit">
              <a href="http://gall.dcinside.com/list.php?id=dcinterview&no=28987">
                인터뷰 글
              </a>
              <a><span class="reply_num">[6,214/2]</span></a>
            </td>
            <td class="gall_date" title="2026-07-17 15:30:00">15:30</td>
            <td class="gall_count">12,345</td>
            <td class="gall_recommend">870</td>
          </tr>
        </table>
        """

        def raise_for_status(self):
            return None

    monkeypatch.setattr(dcinside, "PostgresController", lambda: FakeDB())
    monkeypatch.setattr(dcinside.requests, "get", lambda *args, **kwargs: FakeResponse())

    crawler = dcinside.Dcinside()
    entries = crawler.get_board_entries()
    rows = crawler.get_board_list()

    assert len(rows) == 1
    assert rows[0][0] == "http://gall.dcinside.com/list.php?id=dcinterview&no=28987"
    assert rows[0][1] == "dcinterview"
    assert rows[0][2] == "28987"
    assert entries[0].native_comment_count == 6214
    assert entries[0].native_like_count == 870
    assert entries[0].native_view_count == 12345
    assert entries[0].source_rank == 1
    assert entries[0].created_at.tzinfo == ZoneInfo("Asia/Seoul")


def test_ppomppu_board_list_omits_reply_count_from_title(monkeypatch):
    from crawl_scheduler.community_website import ppomppu

    class FakeDB:
        def find(self, *args, **kwargs):
            return []

    class FakeResponse:
        text = """
        <table>
          <tr class="bbs_new1">
            <td>
              <a class="baseList-title" href="/zboard/zboard.php?id=freeboard&no=10013570">
                <a href="/zboard/zboard.php?id=freeboard&no=10013570" class="baseList-title">
                  <img src="/images/menu/hot_icon2.jpg" alt="hot" />
                  actual post title
                </a>
                &nbsp;<span class="list_comment2">52</span>
              </a>
            </td>
            <td class="board_date">15:46:12</td>
            <td class="board_date">69 - 7</td>
            <td class="board_date">11,189</td>
          </tr>
        </table>
        """

        def raise_for_status(self):
            return None

    monkeypatch.setattr(ppomppu, "PostgresController", lambda: FakeDB())
    monkeypatch.setattr(ppomppu.requests, "get", lambda *args, **kwargs: FakeResponse())

    crawler = ppomppu.Ppomppu()
    entries = crawler.get_board_entries()
    rows = crawler.get_board_list()

    assert len(rows) == 1
    assert rows[0][4] == "actual post title"
    assert entries[0].native_comment_count == 52
    assert entries[0].native_like_count == 69
    assert entries[0].native_view_count == 11189
    assert entries[0].source_rank == 1
    assert entries[0].created_at.tzinfo == ZoneInfo("Asia/Seoul")


def test_ygosu_board_entries_parse_metrics_and_contiguous_rank(monkeypatch):
    from crawl_scheduler.community_website import ygosu

    class FakeDB:
        def find(self, *args, **kwargs):
            return []

    class FakeResponse:
        text = """
        <table class="bd_list"><tbody>
          <tr>
            <td class="tit"><a href="/board/real_article/notice/1/">공지</a></td>
            <td class="read"></td><td class="date"></td><td class="vote"></td>
          </tr>
          <tr>
            <td class="tit"><a href="/board/real_article/yeobgi/2147373/">실시간 글</a>
              <span class="reply_cnt">(1,234)</span></td>
            <td class="read">12,345</td><td class="date">21:25</td><td class="vote">67</td>
          </tr>
          <tr>
            <td class="tit"><a href="/board/real_article/yeobgi/2147374/">댓글 없는 글</a></td>
            <td class="read">90</td><td class="date">21:20</td><td class="vote">2</td>
          </tr>
        </tbody></table>
        """

        content = text.encode()

        def raise_for_status(self):
            return None

    monkeypatch.setattr(ygosu, "PostgresController", lambda: FakeDB())
    monkeypatch.setattr(ygosu.requests, "get", lambda *args, **kwargs: FakeResponse())

    crawler = ygosu.Ygosu()
    entries = crawler.get_board_entries()
    rows = crawler.get_board_list()

    assert [entry.source_rank for entry in entries] == [1, 2]
    assert entries[0].native_comment_count == 1234
    assert entries[0].native_like_count == 67
    assert entries[0].native_view_count == 12345
    assert entries[1].native_comment_count == 0
    assert entries[0].created_at.tzinfo == ZoneInfo("Asia/Seoul")
    assert entries[0].metrics_crawled_at is not None
    assert entries[0].metrics_crawled_at == entries[1].metrics_crawled_at
    assert entries[0].metrics_dict()["metrics_crawled_at"].utcoffset().total_seconds() == 0
    assert len(rows[0]) == 5
    assert rows[0][4] == "실시간 글"


@pytest.mark.parametrize(
    ("module_name", "class_name", "site", "category", "no"),
    [
        ("ygosu", "Ygosu", "ygosu", "yeobgi", 101),
        ("ppomppu", "Ppomppu", "ppomppu", "freeboard", 102),
        ("theqoo", "Theqoo", "theqoo", "hot", 103),
        ("dcinside", "Dcinside", "dcinside", "dcbest", 104),
    ],
)
def test_existing_posts_refresh_metrics_without_fetching_body(
    monkeypatch, module_name, class_name, site, category, no
):
    from crawl_scheduler.community_website.board_list_entry import BoardListEntry

    module = importlib.import_module(
        f"crawl_scheduler.community_website.{module_name}"
    )

    class FakeDB:
        def __init__(self):
            self.refresh_calls = []

        def find(self, collection, query):
            return [{"id": "existing"}] if collection == "Realtime" else []

        def refresh_native_metrics(self, *args):
            self.refresh_calls.append(args)

        def needs_content_refresh(self, *args):
            return False

        def insert_one(self, *args, **kwargs):
            pytest.fail("existing posts must not be inserted again")

    fake_db = FakeDB()
    monkeypatch.setattr(module, "PostgresController", lambda: fake_db)
    crawler = getattr(module, class_name)()
    metrics = {
        "native_comment_count": 12,
        "native_like_count": None if site == "theqoo" else 8,
        "native_view_count": 345,
        "source_rank": 2,
    }
    entry = BoardListEntry(
        url=f"https://example.com/{no}",
        category=category,
        no=no,
        title="existing title",
        created_at=datetime.now(ZoneInfo("Asia/Seoul")),
        **metrics,
    )
    monkeypatch.setattr(crawler, "get_board_entries", lambda: [entry])
    monkeypatch.setattr(
        crawler,
        "get_board_contents",
        lambda *args, **kwargs: pytest.fail("body/OCR must not run for existing posts"),
    )

    assert crawler.get_realtime_best() is True
    assert fake_db.refresh_calls == [
        (
            "Realtime",
            {"site": site, "category": category, "no": no},
            metrics,
        )
    ]


@pytest.mark.parametrize(
    ("module_name", "class_name", "site", "category", "no"),
    [
        ("ygosu", "Ygosu", "ygosu", "yeobgi", 151),
        ("ppomppu", "Ppomppu", "ppomppu", "freeboard", 152),
        ("theqoo", "Theqoo", "theqoo", "hot", 153),
        ("dcinside", "Dcinside", "dcinside", "dcbest", 154),
    ],
)
def test_existing_posts_with_insufficient_body_are_refetched(
    monkeypatch, module_name, class_name, site, category, no
):
    from crawl_scheduler.community_website.board_list_entry import BoardListEntry

    module = importlib.import_module(
        f"crawl_scheduler.community_website.{module_name}"
    )

    class FakeDB:
        def __init__(self):
            self.metric_refreshes = []
            self.content_refreshes = []

        def find(self, collection, query):
            return [{"id": "existing", "contents": []}]

        def refresh_native_metrics(self, *args):
            self.metric_refreshes.append(args)

        def needs_content_refresh(self, *args):
            return True

        def refresh_crawled_content(self, *args, **kwargs):
            self.content_refreshes.append((args, kwargs))
            return {"analysis_status": "pending"}

        def insert_one(self, *args, **kwargs):
            pytest.fail("content recovery must update the existing post")

    fake_db = FakeDB()
    monkeypatch.setattr(module, "PostgresController", lambda: fake_db)
    crawler = getattr(module, class_name)()
    if hasattr(crawler, "request_delay_seconds"):
        crawler.request_delay_seconds = 0

    relative_url = f"/zboard/view.php?id={category}&no={no}"
    entry_url = (
        relative_url
        if site == "ppomppu"
        else f"https://example.com/{no}"
    )
    entry = BoardListEntry(
        url=entry_url,
        category=category,
        no=no,
        title="본문을 다시 수집할 글",
        created_at=datetime.now(ZoneInfo("Asia/Seoul")),
        native_comment_count=7,
        native_like_count=3,
        native_view_count=120,
        source_rank=2,
    )
    recovered_contents = [
        {
            "type": "text",
            "text": "재수집한 본문에는 요약에 필요한 배경과 맥락이 충분히 포함되어 있습니다.",
        }
    ]
    body_calls = []
    monkeypatch.setattr(crawler, "get_board_entries", lambda: [entry])
    monkeypatch.setattr(
        crawler,
        "get_board_contents",
        lambda *args, **kwargs: body_calls.append((args, kwargs))
        or recovered_contents,
    )

    assert crawler.get_realtime_best() is True

    query = {"site": site, "category": category, "no": no}
    expected_url = (
        f"https://ppomppu.co.kr{relative_url}"
        if site == "ppomppu"
        else entry_url
    )
    assert len(body_calls) == 1
    assert body_calls[0][1]["save_videos"] is False
    assert fake_db.metric_refreshes == [
        ("Realtime", query, entry.metrics_dict())
    ]
    assert fake_db.content_refreshes == [
        (
            ("Realtime", query, recovered_contents),
            {"title": entry.title, "url": expected_url},
        )
    ]


@pytest.mark.parametrize(
    ("module_name", "class_name", "site", "category", "no"),
    [
        ("ygosu", "Ygosu", "ygosu", "yeobgi", 201),
        ("ppomppu", "Ppomppu", "ppomppu", "freeboard", 202),
        ("theqoo", "Theqoo", "theqoo", "hot", 203),
        ("dcinside", "Dcinside", "dcinside", "dcbest", 204),
    ],
)
def test_new_posts_include_native_metrics_in_insert_payload(
    monkeypatch, module_name, class_name, site, category, no
):
    from crawl_scheduler.community_website.board_list_entry import BoardListEntry

    module = importlib.import_module(
        f"crawl_scheduler.community_website.{module_name}"
    )

    class FakeDB:
        def __init__(self):
            self.realtime_documents = []

        def find(self, *args, **kwargs):
            return []

        def insert_one(self, collection, document):
            if collection == "Realtime":
                self.realtime_documents.append(document)
            return SimpleNamespace(inserted_id="gpt-result")

    fake_db = FakeDB()
    monkeypatch.setattr(module, "PostgresController", lambda: fake_db)
    crawler = getattr(module, class_name)()
    metrics = {
        "native_comment_count": 21,
        "native_like_count": None if site == "theqoo" else 13,
        "native_view_count": 987,
        "source_rank": 3,
    }
    entry = BoardListEntry(
        url=(
            f"/zboard/view.php?id={category}&no={no}"
            if site == "ppomppu"
            else f"https://example.com/{no}"
        ),
        category=category,
        no=no,
        title="new title",
        created_at=datetime.now(ZoneInfo("Asia/Seoul")),
        **metrics,
    )
    monkeypatch.setattr(crawler, "get_board_entries", lambda: [entry])
    monkeypatch.setattr(crawler, "get_board_contents", lambda *args, **kwargs: [])

    assert crawler.get_realtime_best() is True
    assert len(fake_db.realtime_documents) == 1
    document = fake_db.realtime_documents[0]
    assert {key: document[key] for key in metrics} == metrics
