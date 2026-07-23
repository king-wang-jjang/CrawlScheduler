import re
from datetime import datetime, timedelta
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from crawl_scheduler.community_website.board_list_entry import BoardListEntry, parse_native_count
from crawl_scheduler.community_website.popular_community import PopularCommunityCrawler
from crawl_scheduler.constants import SITE_INVEN
from crawl_scheduler.db.postgres_controller import PostgresController
from crawl_scheduler.utils.loghandler import logger


class Inven(PopularCommunityCrawler):
    site = SITE_INVEN
    list_url = "https://hot.inven.co.kr/"
    body_selectors = ("#powerbbsContent", ".contentBody")

    def __init__(self):
        self.db_controller = PostgresController()

    def get_board_entries(self):
        try:
            soup = self.soup_from_url(self.list_url)
        except Exception as exc:
            logger.error("Get Inven Hotven list error: %s", exc)
            return []

        entries = []
        crawled_at = self.utc_now()
        for row in soup.select("#hotven-list .list-common.con"):
            try:
                anchor = row.select_one(".title > a[href]")
                if not anchor:
                    continue
                url = anchor["href"]
                path = [part for part in urlparse(url).path.split("/") if part]
                if len(path) < 4 or path[0] != "board" or not path[-1].isdigit():
                    continue
                category = f"{path[1]}-{path[2]}"
                title_node = anchor.select_one(".name") or anchor
                title_copy = title_node.__copy__()
                for extra in title_copy.select(".num, .cate, .comment"):
                    extra.decompose()
                date_text = (row.select_one(".date") or row).get_text(" ", strip=True)
                created_at = self._recent_month_day(date_text)
                entries.append(
                    BoardListEntry(
                        url=url,
                        category=category,
                        no=int(path[-1]),
                        title=title_copy.get_text(" ", strip=True),
                        created_at=created_at,
                        native_comment_count=parse_native_count(row.select_one(".comment")) or 0,
                        native_like_count=parse_native_count(row.select_one(".reco")),
                        native_view_count=parse_native_count(row.select_one(".hits")),
                        source_rank=len(entries) + 1,
                        metrics_crawled_at=crawled_at,
                    )
                )
            except Exception as exc:
                logger.error("Error parsing Inven post: %s", exc)
        return entries

    @staticmethod
    def _recent_month_day(value, now=None):
        match = re.search(r"(\d{1,2})[-/.](\d{1,2})", value)
        current = now or datetime.now(ZoneInfo("Asia/Seoul"))
        if not match:
            return current
        candidate = current.replace(
            month=int(match.group(1)), day=int(match.group(2)), microsecond=0
        )
        if candidate > current + timedelta(days=1):
            candidate = candidate.replace(year=candidate.year - 1)
        return candidate
