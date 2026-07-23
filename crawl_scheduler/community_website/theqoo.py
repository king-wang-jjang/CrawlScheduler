from datetime import datetime
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from crawl_scheduler.community_website.board_list_entry import (
    BoardListEntry,
    parse_native_count,
    recent_source_datetime,
)
from crawl_scheduler.community_website.popular_community import (
    BROWSER_HEADERS,
    PopularCommunityCrawler,
)
from crawl_scheduler.constants import SITE_THEQOO
from crawl_scheduler.db.postgres_controller import PostgresController
from crawl_scheduler.utils.loghandler import logger


class Theqoo(PopularCommunityCrawler):
    site = SITE_THEQOO
    list_url = "https://theqoo.net/hot?filter_mode=normal"
    body_selectors = (".xe_content", ".rd_body")

    def __init__(self):
        self.db_controller = PostgresController()

    def get_board_entries(self):
        try:
            response = requests.get(
                self.list_url,
                headers=BROWSER_HEADERS,
                proxies=self.request_proxies(),
                timeout=15,
            )
            response.raise_for_status()
            html = getattr(response, "content", None) or response.text
            soup = BeautifulSoup(html, "html.parser")
        except Exception as exc:
            logger.error("Get Theqoo HOT list error: %s", exc)
            return []

        entries = []
        crawled_at = self.utc_now()
        now = datetime.now(ZoneInfo("Asia/Seoul"))
        for row in soup.select("tr[data-document_srl], .hide_notice tr"):
            try:
                if "notice" in (row.get("class") or []):
                    continue
                title_cell = row.select_one("td.title")
                anchor = title_cell.select_one("a[href]") if title_cell else None
                if not anchor:
                    continue
                url = urljoin("https://theqoo.net", anchor["href"])
                path = [part for part in urlparse(url).path.split("/") if part]
                if len(path) < 2 or path[0] != "hot" or not path[1].isdigit():
                    continue
                no = int(path[1])
                regdate = row.get("data-regdate")
                time_node = row.select_one("td.time")
                if regdate and len(regdate) == 14:
                    created_at = datetime.strptime(regdate, "%Y%m%d%H%M%S").replace(
                        tzinfo=ZoneInfo("Asia/Seoul")
                    )
                else:
                    time_text = time_node.get_text(strip=True) if time_node else ""
                    if ":" not in time_text:
                        continue
                    hour, minute = map(int, time_text.split(":")[:2])
                    created_at = recent_source_datetime(hour, minute, now=now)
                entries.append(
                    BoardListEntry(
                        url=f"https://theqoo.net/hot/{no}",
                        category="hot",
                        no=no,
                        title=anchor.get_text(" ", strip=True),
                        created_at=created_at,
                        native_comment_count=parse_native_count(title_cell.select_one(".replyNum")) or 0,
                        native_like_count=None,
                        native_view_count=parse_native_count(row.select_one("td.m_no")),
                        source_rank=len(entries) + 1,
                        metrics_crawled_at=crawled_at,
                    )
                )
            except Exception as exc:
                logger.error("Error parsing Theqoo HOT post: %s", exc)
        return entries

    def get_board_list(self):
        return [
            (entry.url, str(entry.no), entry.created_at, entry.title)
            for entry in self.get_board_entries()
        ]
