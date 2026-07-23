import os
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from crawl_scheduler.community_website.community_website import AbstractCommunityWebsite
from crawl_scheduler.config import Config
from crawl_scheduler.constants import DEFAULT_GPT_ANSWER
from crawl_scheduler.crawled_content import (
    image_block,
    metadata_image_block,
    text_block,
    video_block,
)
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


class PopularCommunityCrawler(AbstractCommunityWebsite):
    """Common persistence and article-body handling for popular feeds."""

    site = ""
    body_selectors = ()

    def __init__(self):
        self.db_controller = PostgresController()

    def get_daily_best(self):
        return None

    def get_realtime_best(self):
        existing_posts = []
        for entry in self.get_board_entries():
            query = {"site": self.site, "category": entry.category, "no": int(entry.no)}
            try:
                if self.db_controller.find("Realtime", query):
                    self.db_controller.refresh_native_metrics(
                        "Realtime", query, entry.metrics_dict()
                    )
                    existing_posts.append((entry.category, entry.no))
                    continue

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
                logger.info("Post %s/%s/%s inserted successfully", self.site, entry.category, entry.no)
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

    def get_board_contents(self, category=None, no=None, url=None, created_at=None):
        if not url:
            return []
        try:
            response = requests.get(url, headers=BROWSER_HEADERS, timeout=15)
            response.raise_for_status()
            html = getattr(response, "content", None) or response.text
            soup = BeautifulSoup(html, "html.parser")
        except Exception as exc:
            logger.error("Error fetching %s body %s: %s", self.site, url, exc)
            return []

        body = next((soup.select_one(selector) for selector in self.body_selectors if soup.select_one(selector)), None)
        if body is None:
            logger.warning("Could not find %s article body: %s", self.site, url)
            return []

        contents = []
        metadata = metadata_image_block(self.metadata_image_url_from_soup(soup, base_url=url))
        if metadata:
            contents.append(metadata)

        body_copy = BeautifulSoup(str(body), "html.parser")
        for unwanted in body_copy.select("script, style, noscript"):
            unwanted.decompose()
        block = text_block(body_copy.get_text("\n", strip=True))
        if block:
            contents.append(block)

        seen_urls = set()
        for tag in body.select("img, video, video source"):
            media_url = self.media_url_from_tag(tag, base_url=url)
            if not media_url or media_url in seen_urls:
                continue
            seen_urls.add(media_url)
            media_type = "video" if tag.name in {"video", "source"} else "image"
            file_path = self.save_file(
                media_url,
                category=category,
                no=no,
                alt_text=tag.get("alt"),
                headers={**BROWSER_HEADERS, "Referer": url},
                created_at=created_at,
            )
            if not file_path:
                continue
            if media_type == "video":
                media_block = video_block(media_path=file_path, source_url=media_url)
            else:
                alt_text = tag.get("alt")
                ocr_text = alt_text
                if not ocr_text:
                    try:
                        ocr_text = self.img_to_text(
                            os.path.join(Config().get_env("ROOT") or "./media", file_path)
                        )
                    except Exception as exc:
                        logger.warning("OCR skipped for %s: %s", media_url, exc)
                media_block = image_block(
                    media_path=file_path,
                    source_url=media_url,
                    text=ocr_text,
                    alt_text=alt_text,
                )
            if media_block:
                contents.append(media_block)
        return contents

    def get_gpt_obj(self, board_id):
        return DEFAULT_GPT_ANSWER

    def is_ad(self, title=None):
        return False

    @staticmethod
    def soup_from_url(url):
        response = requests.get(url, headers=BROWSER_HEADERS, timeout=15)
        response.raise_for_status()
        html = getattr(response, "content", None) or response.text
        return BeautifulSoup(html, "html.parser")

    @staticmethod
    def utc_now():
        return datetime.now(timezone.utc)
