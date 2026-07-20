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

    monkeypatch.setattr(main, "get_realtime_best", lambda: calls.append("crawl"))
    monkeypatch.setattr(main, "PostgresController", FakeDB)

    assert main.main(["--once"]) == 0
    assert calls == ["crawl", "snapshot"]


def test_seed_alias_runs_crawl_and_exits(monkeypatch):
    from crawl_scheduler import main

    calls = []

    class FakeDB:
        def record_daily_top10_snapshot(self):
            calls.append("snapshot")

    monkeypatch.setattr(main, "get_realtime_best", lambda: calls.append("crawl"))
    monkeypatch.setattr(main, "PostgresController", FakeDB)

    assert main.main(["--seed"]) == 0
    assert calls == ["crawl", "snapshot"]


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

    monkeypatch.setattr(main, "get_realtime_best", lambda: calls.append("crawl"))
    monkeypatch.setattr(main, "PostgresController", FailingDB)
    monkeypatch.setattr(main, "logger", FakeLogger())

    main.job()

    assert calls == [
        "crawl",
        "snapshot",
        ("Error - daily Top10 snapshot: database unavailable", True),
    ]


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
    monkeypatch.setattr(theqoo.requests, "get", lambda *args, **kwargs: FakeResponse())

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
