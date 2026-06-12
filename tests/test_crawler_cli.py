import sys
from pathlib import Path


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))


def test_once_flag_runs_crawl_and_exits(monkeypatch):
    from crawl_scheduler import main

    calls = []

    monkeypatch.setattr(main, "get_realtime_best", lambda: calls.append("crawl"))

    assert main.main(["--once"]) == 0
    assert calls == ["crawl"]


def test_seed_alias_runs_crawl_and_exits(monkeypatch):
    from crawl_scheduler import main

    calls = []

    monkeypatch.setattr(main, "get_realtime_best", lambda: calls.append("crawl"))

    assert main.main(["--seed"]) == 0
    assert calls == ["crawl"]


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
            <td>155609</td><td>이슈</td>
            <td><a href="/hot/4230320581">현재 한국 넷플릭스 top 10.jpg</a></td>
            <td>15:46</td><td>8,769</td>
          </tr>
        </table>
        """

        def raise_for_status(self):
            return None

    monkeypatch.setattr(theqoo, "PostgresController", lambda: FakeDB())
    monkeypatch.setattr(theqoo.requests, "get", lambda *args, **kwargs: FakeResponse())

    rows = theqoo.Theqoo().get_board_list()

    assert len(rows) == 1
    assert rows[0][1] == "4230320581"
    assert rows[0][3] == "현재 한국 넷플릭스 top 10.jpg"


def test_dcinside_board_list_preserves_absolute_links(monkeypatch):
    from crawl_scheduler.community_website import dcinside

    class FakeDB:
        def find(self, *args, **kwargs):
            return []

    class FakeResponse:
        text = """
        <table>
          <tr class="ub-content">
            <td class="gall_num">1</td>
            <td>
              <a href="http://gall.dcinside.com/list.php?id=dcinterview&no=28987">
                인터뷰 글
              </a>
            </td>
            <td class="gall_date">15:30</td>
          </tr>
        </table>
        """

        def raise_for_status(self):
            return None

    monkeypatch.setattr(dcinside, "PostgresController", lambda: FakeDB())
    monkeypatch.setattr(dcinside.requests, "get", lambda *args, **kwargs: FakeResponse())

    rows = dcinside.Dcinside().get_board_list()

    assert len(rows) == 1
    assert rows[0][0] == "http://gall.dcinside.com/list.php?id=dcinterview&no=28987"
    assert rows[0][1] == "dcinterview"
    assert rows[0][2] == "28987"
