import re
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from crawl_scheduler.config import Config
from crawl_scheduler.crawled_content import image_block, metadata_image_block, text_block, video_block
from crawl_scheduler.db.postgres_controller import PostgresController
from crawl_scheduler.community_website.community_website import AbstractCommunityWebsite
from crawl_scheduler.community_website.board_list_entry import BoardListEntry, parse_native_count, recent_source_datetime
from crawl_scheduler.constants import DEFAULT_GPT_ANSWER, SITE_PPOMPPU, DEFAULT_TAG
import os
from crawl_scheduler.utils.loghandler import logger

class Ppomppu(AbstractCommunityWebsite):
    def __init__(self):
        self.db_controller = PostgresController()
        self.debugging_mode = False

    def get_daily_best(self):
        pass

    def get_realtime_best(self, category=None, no=None):
        domain = "https://ppomppu.co.kr"
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

        for entry in board_entries:
            try:
                query = {
                    'site': SITE_PPOMPPU,
                    'category': entry.category,
                    'no': int(entry.no),
                }
                if self._post_already_exists(entry.category, entry.no) and self.debugging_mode == False:
                    self.db_controller.refresh_native_metrics(
                        'Realtime', query, entry.metrics_dict()
                    )
                    already_exists_post.append((entry.category, entry.no))
                    continue
                
                # if category == "freeboard":
                #     logger.warn("Freeboard ========================================")
                    
                gpt_obj_id = self.get_gpt_obj((entry.category, entry.no))
                contents = self.get_board_contents(
                    url=domain + entry.url,
                    category=entry.category,
                    no=entry.no,
                )

                document = {
                    'site': SITE_PPOMPPU,
                    'category': entry.category,
                    'no': int(entry.no),
                    'title': entry.title,
                    'url': domain + entry.url,
                    'create_time': entry.created_at,
                    'gpt_answer': gpt_obj_id,
                    'contents': contents,
                    **entry.metrics_dict(),
                }
                self.db_controller.insert_one('Realtime', document)
                logger.info(f"Post {(entry.category, entry.no)} inserted successfully")
            except Exception as e:
                logger.error(f"Error Save To DB {entry.category, entry.no}: {e}")

        logger.info({"already exists post": already_exists_post})
        return True

    def get_board_list(self):
        """ 게시판에서 URL, 카테고리, 게시글 번호, 생성 시간, 제목 추출 """
        return [
            (entry.url, entry.category, entry.no, entry.created_at, entry.title)
            for entry in self.get_board_entries()
        ]

    def get_board_entries(self):
        url = "https://www.ppomppu.co.kr/hot.php?id=&page=1&category=999"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'}
        now = datetime.now(ZoneInfo('Asia/Seoul'))
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
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

    def get_board_contents(self, category= None, no=None, url=None):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'}
        content_list = []
        if url:
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'lxml')
                metadata_block = metadata_image_block(
                    super().metadata_image_url_from_soup(soup, base_url=url)
                )
                if metadata_block:
                    content_list.append(metadata_block)
                board_body = soup.find('td', class_='board-contents')
                # if category == "freeboard":
                #     logger.info(soup)
                #     logger.info("=========================")
                #     logger.info(board_body)
                paragraphs = board_body.find_all('p')
                    
                for p in paragraphs:
                    if p.find('img'):
                        img_url = super().media_url_from_tag(p.find('img'), base_url=url)
                        if not img_url:
                            continue
                        try:
                            file_path = super().save_file(img_url, category=category, no=no)
                            if not file_path:
                                continue
                            img_txt = super().img_to_text(os.path.join(Config().get_env('ROOT') or './media', file_path))
                            block = image_block(media_path=file_path, source_url=img_url, text=img_txt)
                            if block:
                                content_list.append(block)
                        except Exception as e:
                            logger.error(f"Error processing image: {url} {e}")
                    elif p.find('video'):
                        video_url = super().media_url_from_tag(p.find('video').find('source'), base_url=url)
                        if not video_url:
                            continue
                        try:
                            file_path = super().save_file(video_url, category=category, no=no)
                            if file_path:
                                block = video_block(media_path=file_path, source_url=video_url)
                                if block:
                                    content_list.append(block)
                        except Exception as e:
                            logger.error(f"Error saving video: {e}")
                    else:
                        block = text_block(p.text)
                        if block:
                            content_list.append(block)
            except Exception as e:
                logger.error(f"Error fetching board contents for {no if no else url}: {e}")

        return content_list

    def save_file(self, url):
        pass

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
