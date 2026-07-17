from datetime import datetime, timezone
import os
from typing import Tuple
from bs4 import BeautifulSoup
import requests
from crawl_scheduler.config import Config
from crawl_scheduler.crawled_content import image_block, metadata_image_block, text_block, video_block
from crawl_scheduler.db.postgres_controller import PostgresController
from crawl_scheduler.community_website.community_website import AbstractCommunityWebsite
from crawl_scheduler.community_website.board_list_entry import BoardListEntry, parse_native_count, recent_source_datetime
from crawl_scheduler.constants import DEFAULT_GPT_ANSWER, SITE_YGOSU, DEFAULT_TAG
from crawl_scheduler.utils.loghandler import logger
import sys
import uuid

class Ygosu(AbstractCommunityWebsite):
    def __init__(self):
        self.db_controller = PostgresController()

    def get_daily_best(self):
        logger.info("Fetching daily best posts from Ygosu")
        try:
            req = requests.get('https://ygosu.com/board/best_article/?type=daily')
            req.raise_for_status()
            soup = BeautifulSoup(req.text, 'html.parser')
        except Exception as e:
            logger.error(f"Error fetching Ygosu daily best: {e}")
            return

        already_exists_post = []

        for tr in soup.find_all('tr'):
            try:
                tit_element = tr.select_one('.tit a')
                create_time_element = tr.select_one('.day')
                rank_element = tr.select_one('.num')

                if tit_element and create_time_element and rank_element:
                    title = tit_element.get_text(strip=True)
                    rank = rank_element.get_text(strip=True)
                    create_time = create_time_element.get_text(strip=True)

                    if not create_time:  # 광고 및 공지 제외
                        continue

                    url = tit_element['href']
                    year, month, day = map(int, create_time.split('-'))
                    target_datetime = datetime(year, month, day)

                    board_id = self.get_category_and_no(url)
                    if self._post_already_exists(board_id, 'Daily'):
                        already_exists_post.append(board_id)
                        continue

                    gpt_obj_id = self.get_gpt_obj(board_id)
                    tag_obj_id = self._get_or_create_tag_object(board_id)

                    self.db_controller.insert_one('Daily', {
                        'board_id': board_id,
                        'site': SITE_YGOSU,
                        'rank': rank,
                        'title': title,
                        'url': url,
                        'create_time': target_datetime,
                        'gpt_answer': gpt_obj_id,
                        'tag': tag_obj_id
                    })
                    logger.info(f"Post {board_id} inserted successfully")
            except Exception as e:
                logger.error(f"Error processing post: {e}")

        logger.info({"already exists post": already_exists_post})

    def get_realtime_best(self):
        already_exists_post = []
        board_entries = self.get_board_entries()
        for entry in board_entries:
            try:
                query = {
                    'site': SITE_YGOSU,
                    'category': entry.category,
                    'no': int(entry.no),
                }
                if self._post_already_exists(entry.category, entry.no, 'Realtime'):
                    self.db_controller.refresh_native_metrics(
                        'Realtime', query, entry.metrics_dict()
                    )
                    already_exists_post.append((entry.category, entry.no))
                    continue

                gpt_obj_id = self.get_gpt_obj((entry.category, entry.no))
                contents = self.get_board_contents(
                    url=entry.url, category=entry.category, no=entry.no
                )
                document = {
                    'site': SITE_YGOSU,
                    'category': entry.category,
                    'no': int(entry.no),
                    'title': entry.title,
                    'url': entry.url,
                    'create_time': entry.created_at,
                    'gpt_answer': gpt_obj_id,
                    'contents': contents,
                    **entry.metrics_dict(),
                }
                self.db_controller.insert_one('Realtime', document)
                logger.info(f"Inserted Success: {(entry.category, entry.no)} ")
            except Exception as e:
                logger.error(f"Error Save To DB {entry.category, entry.no}: {e}")
                return False

        logger.info({"already exists post": already_exists_post})

        return True

    def get_board_contents(self, category=None, no=None, url=None):
        content_list = []
        if url:
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                metadata_block = metadata_image_block(
                    super().metadata_image_url_from_soup(soup, base_url=url)
                )
                if metadata_block:
                    content_list.append(metadata_block)
                board_body = soup.find('div', class_='container')
                if not board_body:
                    return content_list
                seen_media_urls = set()

                for element in board_body.find_all(['p', 'div'], recursive=True):  # <p>와 <div> 순회
                    img = element.find('img')
                    if img and 'src' in img.attrs:  # 이미지 처리
                        img_url = super().media_url_from_tag(img, base_url=url)
                        if not img_url or img_url in seen_media_urls:
                            continue
                        seen_media_urls.add(img_url)
                        try:
                            filename = str(uuid.uuid4())
                            file_path = super().save_file(img_url, category=category, no=no, alt_text=filename)
                            if not file_path:
                                continue
                            img_txt = super().img_to_text(os.path.join(Config().get_env('ROOT') or './media', file_path))
                            block = image_block(media_path=file_path, source_url=img_url, text=img_txt)
                            if block:
                                content_list.append(block)
                        except Exception as e:
                            logger.error(f"Error processing image {img_url}: {e}")
                    video = element.find('video')
                    if video:  # 비디오 처리
                        source = video.find('source')
                        if source and 'src' in source.attrs:
                            video_url = super().media_url_from_tag(source, base_url=url)
                            if not video_url or video_url in seen_media_urls:
                                continue
                            seen_media_urls.add(video_url)
                            try:
                                file_path = super().save_file(video_url, category=category, no=no)  # 비디오 저장
                                if file_path:
                                    block = video_block(media_path=file_path, source_url=video_url)
                                    if block:
                                        content_list.append(block)
                            except Exception as e:
                                logger.error(f"Error processing video {video_url}: {e}")
                    text = element.text.strip()
                    block = text_block(text)
                    if block:
                        content_list.append(block)
            
            except Exception as e:
                logger.error(f"Error fetching board contents for {no}: {e}")

        return content_list
    
    def save_file(self, url):
        pass
    
    def get_category_and_no(self, url) -> Tuple[str, int]:
        parts = url.split('/')
        no = parts[-2]
        category = parts[-3]
        
        return category, int(no)
    
    def get_board_list(self):
        """ 게시판에서 URL, 날짜, 카테고리, 게시글 번호, 제목 추출 """
        return [
            (entry.url, entry.created_at, entry.category, entry.no, entry.title)
            for entry in self.get_board_entries()
        ]

    def get_board_entries(self):
        board_list = []
        try:
            req = requests.get('https://ygosu.com/board/real_article', timeout=10)
            req.raise_for_status()
            soup = BeautifulSoup(req.text, 'html.parser')
        except Exception as e:
            logger.error(f"Get List Error: {e}")
            return []

        metrics_crawled_at = datetime.now(timezone.utc)
        for tr in soup.find_all('tr'):
            tit_element = tr.select_one('.tit a')
            create_time_element = tr.select_one('.date')

            if tit_element and create_time_element:
                title = tit_element.get_text(strip=True)
                create_time = create_time_element.get_text(strip=True)

                if self.is_ad(create_time):
                    continue

                url = tit_element['href']
                hour, minute = map(int, create_time.split(':'))
                target_datetime = recent_source_datetime(hour, minute)

                category, no = self.get_category_and_no(url)
                reply_element = tr.select_one('.reply_cnt')
                board_list.append(
                    BoardListEntry(
                        url=url,
                        category=category,
                        no=no,
                        title=title,
                        created_at=target_datetime,
                        native_comment_count=(
                            parse_native_count(reply_element)
                            if reply_element is not None
                            else 0
                        ),
                        native_like_count=parse_native_count(tr.select_one('.vote')),
                        native_view_count=parse_native_count(tr.select_one('.read')),
                        source_rank=len(board_list) + 1,
                        metrics_crawled_at=metrics_crawled_at,
                    )
                )

        return board_list
            
    def _post_already_exists(self, arg1, arg2, arg3=None):
        # 호환성 유지: (board_id, 'Daily') 또는 (category, no, 'Realtime') 모두 지원
        if arg3 is None:
            board_id, collection = arg1, arg2
            existing_instance = self.db_controller.find(collection, {'board_id': board_id, 'site': SITE_YGOSU})
        else:
            category, no, collection = arg1, arg2, arg3
            existing_instance = self.db_controller.find(collection, {'site': SITE_YGOSU, 'category': category, 'no': int(no)})
        return existing_instance

    def get_gpt_obj(self, board_id):
        gpt_exists = self.db_controller.find('GPT', {'board_id': board_id, 'site': SITE_YGOSU})
        if gpt_exists:
            return gpt_exists[0]['_id']
        else:
            gpt_obj = self.db_controller.insert_one('GPT', {
                'board_id': board_id,
                'site': SITE_YGOSU,
                'answer': DEFAULT_GPT_ANSWER,
                'tag': DEFAULT_TAG
            })
            return gpt_obj.inserted_id
            
    def is_ad(self, title) -> bool:
        if not title or ':' not in title:  # 광고 및 공지 제외
            return True
        return False
