import re
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from crawl_scheduler.db.mongo_controller import MongoController
from crawl_scheduler.community_website.community_website import AbstractCommunityWebsite
from crawl_scheduler.constants import DEFAULT_GPT_ANSWER, SITE_PPOMPPU, DEFAULT_TAG
import os
from crawl_scheduler.utils.loghandler import logger

class Ppomppu(AbstractCommunityWebsite):
    def __init__(self):
        self.db_controller = MongoController()

    def get_daily_best(self):
        pass

    def get_real_time_best(self):
        _url = f"https://www.ppomppu.co.kr/hot.php?id=&page=1&category=999"
        try:
            response = requests.get(_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            logger.error(f"fetching page: {_url}, error: {e}")
            return {}

        now = datetime.now()
        already_exists_post = []

        result = []
        for tr in soup.find_all('tr', class_='bbs_new1'):
            try:
                title_element = tr.find('a', class_='baseList-title')
                create_time_element = tr.find('td', class_='board_date')
                create_time = create_time_element.get_text(strip=True)

                if title_element:
                    title = title_element.get_text(strip=True)
                    domain = "https://ppomppu.co.kr"
                    url = title_element['href']
                    board_id = int(self.extract_id_and_no_from_url(url))

                    hour, minute, second = map(int, create_time.split(":"))
                    target_datetime = datetime(now.year, now.month, now.day, hour, minute)

                    if "/" in create_time:
                        logger.debug(f"Skipping older post: {create_time}")
                        break

                    # Check if the post already exists
                    if self._post_already_exists(board_id):
                        already_exists_post.append(board_id)
                        continue

                    gpt_obj_id = self.get_gpt_obj(board_id)
                    contents = self.get_board_contents(url=domain+url)
                    self.db_controller.insert_one('RealTime', {
                        'board_id': board_id,
                        'site': SITE_PPOMPPU,
                        'title': title,
                        'url': domain + url,
                        'create_time': target_datetime,
                        'gpt_answer': gpt_obj_id,
                        'contents': contents
                    })
                    logger.info(f"Post {board_id} inserted successfully")
            except Exception as e:
                logger.error(f"Error processing post: {e}")

        logger.info({"already exists post": already_exists_post})

        data = {"rank": {i + 1: item for i, item in enumerate(result)}}
        return data

    def extract_id_and_no_from_url(self, url):
        pattern = r"id=([^&]*)&no=([^&]*)"
        match = re.search(pattern, url)
        if match:
            return match.group(2)
        else:
            logger.warning(f"Could not extract board id from URL: {url}")
            return None

    def get_board_contents(self, board_id=None, url=None):
        if board_id:
            url = self.db_controller.find('RealTime', {'board_id': board_id, 'site': SITE_PPOMPPU})[0]['url']
        elif url:
            url = url
        else:
            logger.error('No Url')

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'}
        content_list = []
        if url:
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'lxml')
                board_body = soup.find('td', class_='board-contents')
                paragraphs = board_body.find_all('p')

                for p in paragraphs:
                    if p.find('img'):
                        img_url = "https:" + p.find('img')['src']
                        try:
                            file_path = super().save_file(img_url)
                            img_txt = super().img_to_text(file_path)
                            content_list.append({'type': 'image', 'path': file_path, 'content': img_txt})
                        except Exception as e:
                            logger.error(f"Error processing image: {e}")
                    elif p.find('video'):
                        video_url = "https:" + p.find('video').find('source')['src']
                        try:
                            file_path = super().save_file(video_url)
                            content_list.append({'type': 'video', 'path': file_path})
                        except Exception as e:
                            logger.error(f"Error saving video: {e}")
                    else:
                        content_list.append({'type': 'text', 'content': p.text.strip()})
            except Exception as e:
                logger.error(f"Error fetching board contents for {board_id if board_id else url}: {e}")

        return content_list

    def save_file(self, url):
        pass

    def _post_already_exists(self, board_id):
        existing_instance = self.db_controller.find('RealTime', {'board_id': board_id, 'site': SITE_PPOMPPU})
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