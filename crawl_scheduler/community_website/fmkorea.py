import re
from datetime import datetime
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from crawl_scheduler.community_website.board_list_entry import (
    BoardListEntry,
    parse_native_count,
    recent_source_datetime,
)
from crawl_scheduler.community_website.popular_community import PopularCommunityCrawler
from crawl_scheduler.constants import SITE_FMKOREA
from crawl_scheduler.db.postgres_controller import PostgresController
from crawl_scheduler.utils.loghandler import logger


class Fmkorea(PopularCommunityCrawler):
    site = SITE_FMKOREA
    list_url = "https://www.fmkorea.com/best"
    body_selectors = (".xe_content", ".rd_body")

    def __init__(self):
        self.db_controller = PostgresController()

    def get_board_entries(self):
        try:
            soup = self.soup_from_url(self.list_url)
        except Exception as exc:
            logger.error("Get FMKorea list error: %s", exc)
            return []

        entries = []
        crawled_at = self.utc_now()
        for row in soup.select("li.li"):
            try:
                anchor = row.select_one("h3.title a[href]")
                if not anchor:
                    continue
                query = parse_qs(urlparse(anchor.get("href", "")).query)
                no = (query.get("document_srl") or [None])[0]
                if not no:
                    match = re.search(r"/(\d+)(?:\?|$)", anchor.get("href", ""))
                    no = match.group(1) if match else None
                if not no:
                    continue
                title_node = anchor.select_one(".ellipsis-target") or anchor
                time_node = row.select_one(".regdate") or row
                time_match = re.search(r"(\d{1,2}):(\d{2})", str(time_node))
                created_at = (
                    recent_source_datetime(int(time_match.group(1)), int(time_match.group(2)))
                    if time_match
                    else datetime.now(ZoneInfo("Asia/Seoul"))
                )
                entries.append(
                    BoardListEntry(
                        url=f"https://www.fmkorea.com/best/{no}",
                        category="best",
                        no=int(no),
                        title=title_node.get_text(" ", strip=True),
                        created_at=created_at,
                        native_comment_count=parse_native_count(row.select_one(".comment_count")) or 0,
                        native_like_count=parse_native_count(row.select_one(".pc_voted_count .count")),
                        native_view_count=None,
                        source_rank=len(entries) + 1,
                        metrics_crawled_at=crawled_at,
                    )
                )
            except Exception as exc:
                logger.error("Error parsing FMKorea post: %s", exc)
        return entries
