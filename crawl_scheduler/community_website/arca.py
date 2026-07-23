from datetime import datetime
from urllib.parse import urljoin, urlparse

from crawl_scheduler.community_website.board_list_entry import BoardListEntry, parse_native_count
from crawl_scheduler.community_website.popular_community import PopularCommunityCrawler
from crawl_scheduler.constants import SITE_ARCA
from crawl_scheduler.db.postgres_controller import PostgresController
from crawl_scheduler.utils.loghandler import logger


class Arca(PopularCommunityCrawler):
    site = SITE_ARCA
    list_url = "https://arca.live/b/live"
    body_selectors = (".article-content", ".fr-view")

    def __init__(self):
        self.db_controller = PostgresController()

    def get_board_entries(self):
        try:
            soup = self.soup_from_url(self.list_url)
        except Exception as exc:
            logger.error("Get Arca Live list error: %s", exc)
            return []

        entries = []
        crawled_at = self.utc_now()
        for row in soup.select("div.vrow:not(.notice)"):
            try:
                anchor = row.select_one("a.title[href]")
                time_node = row.select_one(".col-time time[datetime]")
                if not anchor or not time_node:
                    continue
                url = urljoin("https://arca.live", anchor["href"])
                path = [part for part in urlparse(url).path.split("/") if part]
                if len(path) < 3 or not path[-1].isdigit():
                    continue
                badge = row.select_one(".badges a.badge[href^='/b/']")
                category = urlparse(badge["href"]).path.split("/")[-1] if badge else "live"
                title_node = anchor.__copy__()
                for extra in title_node.select(".comment-count, .badges"):
                    extra.decompose()
                entries.append(
                    BoardListEntry(
                        url=url.split("?", 1)[0],
                        category=category,
                        no=int(path[-1]),
                        title=title_node.get_text(" ", strip=True),
                        created_at=datetime.fromisoformat(time_node["datetime"].replace("Z", "+00:00")),
                        native_comment_count=parse_native_count(row.select_one(".comment-count")) or 0,
                        native_like_count=parse_native_count(row.select_one(".col-rate")),
                        native_view_count=parse_native_count(row.select_one(".col-view")),
                        source_rank=len(entries) + 1,
                        metrics_crawled_at=crawled_at,
                    )
                )
            except Exception as exc:
                logger.error("Error parsing Arca Live post: %s", exc)
        return entries
