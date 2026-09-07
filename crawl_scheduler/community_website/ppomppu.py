from datetime import datetime, timezone
import os
import re
import time
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
import requests

from crawl_scheduler.crawled_content import metadata_image_block
from crawl_scheduler.db.postgres_controller import PostgresController
from crawl_scheduler.community_website.article_content import build_ordered_content_blocks
from crawl_scheduler.community_website.community_website import AbstractCommunityWebsite
from crawl_scheduler.community_website.board_list_entry import BoardListEntry, parse_native_count, recent_source_datetime
from crawl_scheduler.constants import DEFAULT_GPT_ANSWER, SITE_PPOMPPU, DEFAULT_TAG
from crawl_scheduler.utils.loghandler import logger


PPOMPPU_BASE_URL = "https://www.ppomppu.co.kr"
PPOMPPU_HOT_URL = f"{PPOMPPU_BASE_URL}/hot.php?id=&page=1&category=999"
PPOMPPU_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6",
}
PPOMPPU_BLOCKED_STATUS_CODES = {403, 429, 430}
DEFAULT_PPOMPPU_REQUEST_DELAY_SECONDS = 1.0
SOFT_BLOCK_MAX_RESPONSE_BYTES = 10_000


class PpomppuCrawlBlockedError(RuntimeError):
    pass


class PpomppuArticleUnavailableError(RuntimeError):
    pass


class Ppomppu(AbstractCommunityWebsite):
    def __init__(self):
        self.db_controller = PostgresController()
        self.debugging_mode = False
        self._http_session = requests.Session()
        self._last_request_at = None
        self.request_delay_seconds = self._configured_request_delay_seconds()

    def get_daily_best(self):
        pass

    def get_realtime_best(self, category=None, no=None):
        already_exists_post = []
        board_entries = self.get_board_entries()
        
        if category and no:
            self.debugging_mode = True
            url = f"/zboard/view.php?id={category}&no={no}"
            board_entries = [
                BoardListEntry(
                    url=url,
                    category=category,
                    no=int(no),
                    title="Debbugging Mode",
                    created_at=datetime.now(ZoneInfo('Asia/Seoul')),
                    native_comment_count=None,
                    native_like_count=None,
                    native_view_count=None,
                    source_rank=None,
                )
            ]

        if not board_entries:
            logger.error("Ppomppu HOT feed returned no posts")
            return False

        crawl_succeeded = True

        for entry in board_entries:
            try:
                article_url = urljoin(f"{PPOMPPU_BASE_URL}/", entry.url)
                query = {
                    'site': SITE_PPOMPPU,
                    'category': entry.category,
                    'no': int(entry.no),
                }
                if self._post_already_exists(entry.category, entry.no) and self.debugging_mode == False:
                    self.db_controller.refresh_native_metrics(
                        'Realtime', query, entry.metrics_dict()
                    )
                    if self.db_controller.needs_content_refresh('Realtime', query):
                        contents = self.get_board_contents(
                            url=article_url,
                            category=entry.category,
                            no=entry.no,
                            save_videos=False,
                        )
                        if not any(block.get("type") != "metadata" for block in contents):
                            raise PpomppuArticleUnavailableError(
                                f"empty article contents: {article_url}"
                            )
                        self.db_controller.refresh_crawled_content(
                            'Realtime',
                            query,
                            contents,
                            title=entry.title,
                            url=article_url,
                        )
                    already_exists_post.append((entry.category, entry.no))
                    continue
                
                # if category == "freeboard":
                #     logger.warn("Freeboard ========================================")
                    
                gpt_obj_id = self.get_gpt_obj((entry.category, entry.no))
                contents = self.get_board_contents(
                    url=article_url,
                    category=entry.category,
                    no=entry.no,
                )
                if not any(block.get("type") != "metadata" for block in contents):
                    raise PpomppuArticleUnavailableError(
                        f"empty article contents: {article_url}"
                    )

                document = {
                    'site': SITE_PPOMPPU,
                    'category': entry.category,
                    'no': int(entry.no),
                    'title': entry.title,
                    'url': article_url,
                    'create_time': entry.created_at,
                    'gpt_answer': gpt_obj_id,
                    'contents': contents,
                    **entry.metrics_dict(),
                }
                self.db_controller.insert_one('Realtime', document)
                logger.info(f"Post {(entry.category, entry.no)} inserted successfully")
            except PpomppuCrawlBlockedError as e:
                logger.warning("Ppomppu crawl blocked; stopping this cycle: %s", e)
                return False
            except PpomppuArticleUnavailableError as e:
                crawl_succeeded = False
                logger.warning("Skipping unavailable Ppomppu article: %s", e)
            except Exception as e:
                crawl_succeeded = False
                logger.error(f"Error Save To DB {entry.category, entry.no}: {e}")

        logger.info({"already exists post": already_exists_post})
        return crawl_succeeded

    def get_board_list(self):
        """ 게시판에서 URL, 카테고리, 게시글 번호, 생성 시간, 제목 추출 """
        return [
            (entry.url, entry.category, entry.no, entry.created_at, entry.title)
            for entry in self.get_board_entries()
        ]

    def get_board_entries(self):
        now = datetime.now(ZoneInfo('Asia/Seoul'))
        try:
            response = self._get_response(
                PPOMPPU_HOT_URL,
                referer=f"{PPOMPPU_BASE_URL}/",
            )
            html = getattr(response, "content", None) or response.text
            soup = BeautifulSoup(html, 'html.parser')
        except PpomppuCrawlBlockedError as e:
            logger.warning("Get Ppomppu list blocked: %s", e)
            return []
        except Exception as e:
            logger.error(f"Get List Error: {e}")
            return []

        board_list = []
        metrics_crawled_at = datetime.now(timezone.utc)

        for tr in soup.find_all('tr', class_='bbs_new1'):
            try:
                title_element = tr.find('a', class_='baseList-title')
                metric_cells = tr.select('td.board_date')
                create_time_element = metric_cells[0] if metric_cells else None

                if not title_element or not create_time_element:
                    continue

                reply_element = tr.find(class_=re.compile(r'\blist_comment'))
                native_comment_count = (
                    parse_native_count(reply_element)
                    if reply_element is not None
                    else 0
                )
                title = self._extract_title(title_element)
                create_time = create_time_element.get_text(strip=True)
                url = title_element['href']

                # 광고인지 확인
                if self.is_ad(title=title):
                    continue

                category = tr.get('data-bbs_id')
                no = tr.get('data-bbs_no')
                if not category or not no:
                    category, no = self.get_category_and_no(url)
                url = f"/zboard/view.php?id={category}&no={no}"
                no = int(no)

                if "/" in create_time:
                    logger.debug(f"Skipping older post: {create_time}")
                    break

                hour, minute, second = map(int, create_time.split(":"))
                target_datetime = recent_source_datetime(
                    hour,
                    minute,
                    second,
                    now=now,
                )

                board_list.append(
                    BoardListEntry(
                        url=url,
                        category=category,
                        no=no,
                        title=title,
                        created_at=target_datetime,
                        native_comment_count=native_comment_count,
                        native_like_count=(
                            parse_native_count(metric_cells[1])
                            if len(metric_cells) > 1
                            else None
                        ),
                        native_view_count=(
                            parse_native_count(metric_cells[2])
                            if len(metric_cells) > 2
                            else None
                        ),
                        source_rank=len(board_list) + 1,
                        metrics_crawled_at=metrics_crawled_at,
                    )
                )

            except Exception as e:
                logger.error(f"Error parsing post: {e}")

        if not board_list:
            logger.error("Ppomppu HOT page contained no supported post rows")
        return board_list
    
    def is_ad(self, title) -> bool:
        if not title.startswith("AD"):
            return False
        return True

    @staticmethod
    def _extract_title(title_element):
        title_link = title_element.find('a', class_='baseList-title') or title_element
        for reply_count in title_link.find_all(class_=re.compile(r'\blist_comment')):
            reply_count.decompose()
        return title_link.get_text(" ", strip=True)

    def get_category_and_no(self, url):
        pattern = r"id=([^&]*)&no=([^&]*)"
        match = re.search(pattern, url)
        if match:
            return match.group(1), match.group(2)
        else:
            logger.warning(f"Could not extract board id and no from URL: {url}")
            return None, None

    def get_board_contents(
        self,
        category=None,
        no=None,
        url=None,
        save_videos=True,
    ):
        content_list = []
        if url:
            try:
                response = self._get_response(url, referer=PPOMPPU_HOT_URL)
                html = getattr(response, "content", None) or response.text
                soup = BeautifulSoup(html, 'lxml')
                metadata_block = metadata_image_block(
                    super().metadata_image_url_from_soup(soup, base_url=url)
                )
                if metadata_block:
                    content_list.append(metadata_block)
                board_body = soup.find('td', class_='board-contents')
                if not board_body:
                    response_size = len(html) if html is not None else 0
                    if response_size <= SOFT_BLOCK_MAX_RESPONSE_BYTES:
                        raise PpomppuCrawlBlockedError(
                            "possible soft block: article response has no body "
                            f"({response_size} bytes): {url}"
                        )
                    raise PpomppuArticleUnavailableError(
                        f"article body selector is missing: {url}"
                    )
                content_list.extend(
                    build_ordered_content_blocks(
                        self,
                        board_body,
                        base_url=url,
                        category=category,
                        no=no,
                        headers={**PPOMPPU_BROWSER_HEADERS, "Referer": url},
                        proxies=self.request_proxies(),
                        save_file=super().save_file,
                        save_videos=save_videos,
                    )
                )
            except (PpomppuCrawlBlockedError, PpomppuArticleUnavailableError):
                raise
            except Exception as e:
                logger.error(
                    f"Error fetching board contents for {no if no else url}: {e}"
                )
                raise PpomppuArticleUnavailableError(
                    f"could not fetch article {no if no else url}: {e}"
                ) from e

        return content_list

    def _get_response(self, url, *, referer=None):
        self._respect_request_delay()
        headers = dict(PPOMPPU_BROWSER_HEADERS)
        if referer:
            headers["Referer"] = referer

        session = getattr(self, "_http_session", None)
        request = session.get if session is not None else requests.get
        try:
            response = request(
                url,
                headers=headers,
                proxies=self.request_proxies(),
                timeout=15,
            )
        finally:
            self._last_request_at = time.monotonic()

        status_code = getattr(response, "status_code", None)
        if status_code in PPOMPPU_BLOCKED_STATUS_CODES:
            raise PpomppuCrawlBlockedError(f"HTTP {status_code}: {url}")
        response.raise_for_status()
        return response

    def _respect_request_delay(self):
        last_request_at = getattr(self, "_last_request_at", None)
        if last_request_at is None:
            return
        delay_seconds = max(
            float(
                getattr(
                    self,
                    "request_delay_seconds",
                    DEFAULT_PPOMPPU_REQUEST_DELAY_SECONDS,
                )
            ),
            0.0,
        )
        remaining = delay_seconds - (time.monotonic() - last_request_at)
        if remaining > 0:
            time.sleep(remaining)

    @staticmethod
    def _configured_request_delay_seconds():
        raw_value = os.getenv(
            "CRAWLER_PPOMPPU_REQUEST_DELAY_SECONDS",
            str(DEFAULT_PPOMPPU_REQUEST_DELAY_SECONDS),
        )
        try:
            return max(float(raw_value), 0.0)
        except ValueError:
            logger.warning(
                "Invalid CRAWLER_PPOMPPU_REQUEST_DELAY_SECONDS; using %.1f",
                DEFAULT_PPOMPPU_REQUEST_DELAY_SECONDS,
            )
            return DEFAULT_PPOMPPU_REQUEST_DELAY_SECONDS

    @staticmethod
    def request_proxies():
        proxy_url = os.getenv("CRAWLER_HTTP_PROXY", "").strip()
        if not proxy_url:
            return None
        return {"http": proxy_url, "https": proxy_url}

    def _post_already_exists(self, category, no):
        existing_instance = self.db_controller.find('Realtime', {'site': SITE_PPOMPPU, 'category': category, 'no': int(no)})
        return existing_instance

    def get_gpt_obj(self, board_id):
        gpt_exists = self.db_controller.find('GPT', {'board_id': board_id, 'site': SITE_PPOMPPU})
        if gpt_exists:
            return gpt_exists[0]['_id']
        else:
            gpt_obj = self.db_controller.insert_one('GPT', {
                'board_id': board_id,
                'site': SITE_PPOMPPU,
                'answer': DEFAULT_GPT_ANSWER,
                'tag': DEFAULT_TAG
            })
            return gpt_obj.inserted_id
