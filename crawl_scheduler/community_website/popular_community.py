from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from math import ceil
import os
import time

import requests
from bs4 import BeautifulSoup

from crawl_scheduler.community_website.article_content import (
    build_ordered_content_blocks,
)
from crawl_scheduler.community_website.community_website import AbstractCommunityWebsite
from crawl_scheduler.constants import DEFAULT_GPT_ANSWER
from crawl_scheduler.crawled_content import metadata_image_block
from crawl_scheduler.db.postgres_controller import PostgresController
from crawl_scheduler.utils.loghandler import logger


BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6",
}
DEFAULT_RETRY_AFTER_SECONDS = 300


class CrawlerThrottledError(RuntimeError):
    def __init__(self, site, url, status_code, retry_after_seconds):
        self.site = site
        self.url = url
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            f"{site} throttled HTTP {status_code}; "
            f"retry after {retry_after_seconds}s: {url}"
        )


def retry_after_seconds(response):
    value = (getattr(response, "headers", {}) or {}).get("Retry-After")
    if value:
        try:
            return max(int(value), 1)
        except (TypeError, ValueError):
            try:
                retry_at = parsedate_to_datetime(value)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                seconds = (retry_at - datetime.now(timezone.utc)).total_seconds()
                return max(ceil(seconds), 1)
            except (AttributeError, TypeError, ValueError, OverflowError):
                pass
    return DEFAULT_RETRY_AFTER_SECONDS


class PopularCommunityCrawler(AbstractCommunityWebsite):
    """Common persistence and article-body handling for popular feeds."""

    site = ""
    body_selectors = ()
    request_delay_seconds = 0.0
    _cooldown_until_by_site = {}

    def __init__(self):
        self.db_controller = PostgresController()

    def get_daily_best(self):
        return None

    def get_realtime_best(self):
        existing_posts = []
        try:
            board_entries = self.get_board_entries()
        except CrawlerThrottledError as exc:
            logger.warning("%s; stopping this crawl cycle", exc)
            return False
        if not board_entries:
            logger.error("%s popular feed returned no posts", self.site)
            return False

        for entry in board_entries:
            query = {"site": self.site, "category": entry.category, "no": int(entry.no)}
            try:
                if self.db_controller.find("Realtime", query):
                    self.db_controller.refresh_native_metrics(
                        "Realtime", query, entry.metrics_dict()
                    )
                    if self.db_controller.needs_content_refresh("Realtime", query):
                        if self.request_delay_seconds:
                            time.sleep(self.request_delay_seconds)
                        contents = self.get_board_contents(
                            url=entry.url,
                            category=entry.category,
                            no=entry.no,
                            created_at=entry.created_at,
                            save_videos=False,
                        )
                        self.db_controller.refresh_crawled_content(
                            "Realtime",
                            query,
                            contents,
                            title=entry.title,
                            url=entry.url,
                        )
                    existing_posts.append((entry.category, entry.no))
                    continue

                if self.request_delay_seconds:
                    time.sleep(self.request_delay_seconds)
                contents = self.get_board_contents(
                    url=entry.url,
                    category=entry.category,
                    no=entry.no,
                    created_at=entry.created_at,
                )
                self.db_controller.insert_one(
                    "Realtime",
                    {
                        **query,
                        "title": entry.title,
                        "url": entry.url,
                        "create_time": entry.created_at,
                        "gpt_answer": DEFAULT_GPT_ANSWER,
                        "contents": contents,
                        **entry.metrics_dict(),
                    },
                )
                logger.info(
                    "Post %s/%s/%s inserted successfully",
                    self.site,
                    entry.category,
                    entry.no,
                )
            except CrawlerThrottledError as exc:
                logger.warning("%s; stopping this crawl cycle", exc)
                return False
            except Exception as exc:
                logger.error(
                    "Error saving %s/%s/%s: %s", self.site, entry.category, entry.no, exc
                )
        logger.info({f"{self.site} already exists": existing_posts})
        return True

    def get_board_list(self):
        return [
            (entry.url, entry.category, entry.no, entry.created_at, entry.title)
            for entry in self.get_board_entries()
        ]

    def get_board_contents(
        self,
        category=None,
        no=None,
        url=None,
        created_at=None,
        save_videos=True,
    ):
        if not url:
            return []
        try:
            response = self.get_response(url)
            response.raise_for_status()
            html = getattr(response, "content", None) or response.text
            soup = BeautifulSoup(html, "html.parser")
        except CrawlerThrottledError:
            raise
        except Exception as exc:
            logger.error("Error fetching %s body %s: %s", self.site, url, exc)
            return []

        body = next(
            (
                selected_body
                for selector in self.body_selectors
                if (selected_body := soup.select_one(selector)) is not None
            ),
            None,
        )
        if body is None:
            logger.warning("Could not find %s article body: %s", self.site, url)
            return []

        contents = []
        metadata = metadata_image_block(self.metadata_image_url_from_soup(soup, base_url=url))
        if metadata:
            contents.append(metadata)
        contents.extend(
            build_ordered_content_blocks(
                self,
                body,
                base_url=url,
                category=category,
                no=no,
                created_at=created_at,
                headers={**BROWSER_HEADERS, "Referer": url},
                proxies=self.request_proxies(),
                save_videos=save_videos,
            )
        )
        return contents

    def get_gpt_obj(self, board_id):
        return DEFAULT_GPT_ANSWER

    def is_ad(self, title=None):
        return False

    @classmethod
    def get_response(cls, url):
        cooldown_remaining = cls.cooldown_remaining_seconds()
        if cooldown_remaining:
            raise CrawlerThrottledError(
                cls.site,
                url,
                "cooldown",
                cooldown_remaining,
            )
        response = requests.get(
            url,
            headers=BROWSER_HEADERS,
            proxies=cls.request_proxies(),
            timeout=15,
        )
        if getattr(response, "status_code", None) in {429, 430}:
            retry_seconds = retry_after_seconds(response)
            cls._cooldown_until_by_site[cls.site] = time.monotonic() + retry_seconds
            raise CrawlerThrottledError(
                cls.site,
                url,
                response.status_code,
                retry_seconds,
            )
        return response

    @classmethod
    def soup_from_url(cls, url):
        response = cls.get_response(url)
        response.raise_for_status()
        html = getattr(response, "content", None) or response.text
        return BeautifulSoup(html, "html.parser")

    @classmethod
    def cooldown_remaining_seconds(cls):
        cooldown_until = cls._cooldown_until_by_site.get(cls.site, 0)
        remaining = ceil(cooldown_until - time.monotonic())
        if remaining <= 0:
            cls._cooldown_until_by_site.pop(cls.site, None)
            return 0
        return remaining

    @staticmethod
    def utc_now():
        return datetime.now(timezone.utc)

    @staticmethod
    def request_proxies():
        proxy_url = os.getenv("CRAWLER_HTTP_PROXY", "").strip()
        if not proxy_url:
            return None
        return {"http": proxy_url, "https": proxy_url}
