from datetime import datetime
import os
from bs4 import BeautifulSoup
import requests
from crawl_scheduler.db.mongo_controller import MongoController
from crawl_scheduler.community_website.community_website import AbstractCommunityWebsite
from crawl_scheduler.constants import DEFAULT_GPT_ANSWER, SITE_YGOSU, DEFAULT_TAG
from crawl_scheduler.utils.loghandler import logger
import sys

class Ygosu(AbstractCommunityWebsite):
    def __init__(self):
        self.db_controller = MongoController()

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

                    board_id = self._extract_board_id(url)
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

    def get_real_time_best(self):
        try:
            req = requests.get('https://ygosu.com/board/real_article')
            req.raise_for_status()
            soup = BeautifulSoup(req.text, 'html.parser')
        except Exception as e:
            logger.error(f"Error fetching Ygosu real-time best: {e}")
            return

        already_exists_post = []

        for tr in soup.find_all('tr'):
            try:
                tit_element = tr.select_one('.tit a')
                create_time_element = tr.select_one('.date')

                if tit_element and create_time_element:
                    title = tit_element.get_text(strip=True)
                    create_time = create_time_element.get_text(strip=True)

                    if not create_time or ':' not in create_time:  # 광고 및 공지 제외
                        continue

                    url = tit_element['href']
                    now = datetime.now()
                    hour, minute = map(int, create_time.split(':'))
                    target_datetime = datetime(now.year, now.month, now.day, hour, minute)

                    board_id = int(self._extract_board_id(url))
                    if self._post_already_exists(board_id, 'RealTime'):
                        already_exists_post.append(board_id)
                        continue

                    gpt_obj_id = self.get_gpt_obj(board_id)
                    contents = self.get_board_contents(url=url)
                    self.db_controller.insert_one('RealTime', {
                        'board_id': board_id,
                        'site': SITE_YGOSU,
                        'title': title,
                        'url': url,
                        'create_time': target_datetime,
                        'gpt_answer': gpt_obj_id,
                        'contents': contents
                    })
                    logger.info(f"Inserted Success: {board_id} ")
            except Exception as e:
                logger.error(f"Error processing real-time post: {e}")
                return False

        logger.info({"already exists post": already_exists_post})
        return True

    def get_board_contents(self, board_id=None, url=None):
        if board_id:
            url = self.db_controller.find('RealTime', {'board_id': board_id, 'site': SITE_YGOSU})[0]['url']
        elif url:
            url = url
        else:
            logger.error('No Url')
            
        content_list = []
        if url:
            try:
                response = requests.get(url)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                board_body = soup.find('div', class_='container')
                paragraphs = board_body.find_all('p')

                for element in board_body.find_all(['p', 'div'], recursive=True):  # <p>와 <div> 순회
                    if element.name == 'p':  # 텍스트 추출
                        text = element.text.strip()
                        if text:
                            content_list.append({'type': 'text', 'content': text})
                    elif element.name == 'div':  # 이미지 또는 비디오 추출
                        img = element.find('img')
                        if img and 'src' in img.attrs:  # 이미지 처리
                            img_url = img['src']
                            try:
                                file_path = super().save_file(img_url)
                                img_txt = super().img_to_text(file_path)
                                content_list.append({'type': 'image', 'path': file_path, 'content': img_txt})
                            except Exception as e:
                                logger.error(f"Error processing image {img_url}: {e}")
                        video = element.find('video')
                        if video:  # 비디오 처리
                            source = video.find('source')
                            if source and 'src' in source.attrs:
                                video_url = source['src']
                                try:
                                    file_path = super().save_file(video_url)  # 비디오 저장
                                    content_list.append({'type': 'video', 'path': file_path})
                                except Exception as e:
                                    logger.error(f"Error processing video {video_url}: {e}")
            except Exception as e:
                logger.error(f"Error fetching board contents for {board_id}: {e}")

        return content_list
    
    def save_file(self, url):
        pass

    def _extract_board_id(self, url):
        for part in url.split('/'):
            if part.isdigit():
                return part
        return None

    def _post_already_exists(self, board_id, collection):
        existing_instance = self.db_controller.find(collection, {'board_id': board_id, 'site': SITE_YGOSU})
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