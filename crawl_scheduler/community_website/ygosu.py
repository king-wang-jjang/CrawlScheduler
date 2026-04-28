from datetime import datetime
import os
from typing import Tuple
from bs4 import BeautifulSoup
import requests
from crawl_scheduler.config import Config
from crawl_scheduler.db.postgres_controller import PostgresController
from crawl_scheduler.community_website.community_website import AbstractCommunityWebsite
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
        board_list = self.get_board_list()  # 🔹 분리한 함수 호출
        for url, target_datetime, category, no, title in board_list:
            try:
                if self._post_already_exists(category, no, 'Realtime'):
                    already_exists_post.append((category, no))
                    continue

                gpt_obj_id = self.get_gpt_obj((category, no))
                contents = self.get_board_contents(url=url, category=category, no=no)
                self.db_controller.insert_one('Realtime', {
                    'site': SITE_YGOSU,
                    'category': category,
                    'no': int(no),
                    'title': title,
                    'url': url,
                    'create_time': target_datetime,
                    'gpt_answer': gpt_obj_id,
                    'contents': contents
                })
                logger.info(f"Inserted Success: {(category, no)} ")
            except Exception as e:
                logger.error(f"Error Save To DB {category, no}: {e}")
                return False

        logger.info({"already exists post": already_exists_post})

        return True

    def get_board_contents(self, category=None, no=None, url=None):
        content_list = []
        if url:
            try:
                response = requests.get(url)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                board_body = soup.find('div', class_='container')
                paragraphs = board_body.find_all('p')

                for element in board_body.find_all(['p', 'div'], recursive=True):  # <p>와 <div> 순회
                    img = element.find('img')
                    if img and 'src' in img.attrs:  # 이미지 처리
                        img_url = img['src']
                        img_response = requests.get(img_url)
                        # Content-Type 확인
                        content_type = img_response.headers.get('Content-Type')
                        # 파일 저장 및 확장자 추가
                        if content_type == "image/jpeg":
                            ext = ".jpg"
                        elif content_type == "image/png":
                            ext = ".png"
                        elif content_type == "image/webp":
                            ext = ".webp"  
                            
                        unique_id = uuid.uuid4()
                        filename = f"{unique_id}{ext}"
                        try:
                            file_path = super().save_file(img_url, category=category, no=no, alt_text=filename)
                            img_txt = super().img_to_text(os.path.join(Config().get_env('ROOT'), file_path))
                            content_list.append({'type': 'image', 'path': file_path, 'content': img_txt})
                        except Exception as e:
                            logger.error(f"Error processing image {img_url}: {e}")
                    video = element.find('video')
                    if video:  # 비디오 처리
                        source = video.find('source')
                        if source and 'src' in source.attrs:
                            video_url = source['src']
                            try:
                                file_path = super().save_file(video_url, category=category, no=no)  # 비디오 저장
                                content_list.append({'type': 'video', 'path': file_path})
                            except Exception as e:
                                logger.error(f"Error processing video {video_url}: {e}")
                    text = element.text.strip()
                    if text:
                        content_list.append({'type': 'text', 'content': text})
            
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
        board_list = []
        try:
            req = requests.get('https://ygosu.com/board/real_article')
            req.raise_for_status()
            soup = BeautifulSoup(req.text, 'html.parser')
        except Exception as e:
            logger.error(f"Get List Error: {e}")
            return

        for tr in soup.find_all('tr'):
            tit_element = tr.select_one('.tit a')
            create_time_element = tr.select_one('.date')

            if tit_element and create_time_element:
                title = tit_element.get_text(strip=True)
                create_time = create_time_element.get_text(strip=True)

                if self.is_ad(create_time):
                    continue

                url = tit_element['href']
                now = datetime.now()
                hour, minute = map(int, create_time.split(':'))
                target_datetime = datetime(now.year, now.month, now.day, hour, minute)

                category, no = self.get_category_and_no(url)
                board_list.append((url, target_datetime, category, no, title))

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
