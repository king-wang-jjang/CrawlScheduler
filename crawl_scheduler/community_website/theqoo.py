import re
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from crawl_scheduler.config import Config
from crawl_scheduler.db.mongo_controller import MongoController
from crawl_scheduler.community_website.community_website import AbstractCommunityWebsite
from crawl_scheduler.constants import DEFAULT_GPT_ANSWER, SITE_THEQOO, DEFAULT_TAG
import os
from crawl_scheduler.utils.loghandler import logger

class Theqoo(AbstractCommunityWebsite):
    g_headers = [
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'},
    ]

    def __init__(self):
        self.db_controller = MongoController()
   
    def get_daily_best(self):
        pass
    
    def get_real_time_best(self):
        category = 'hot'  # theqoo는 카테고리를 hot으로 단일 고정 (20250214)
        try:
            req = requests.get('https://theqoo.net/hot', headers=self.g_headers[0])
            req.raise_for_status()
            html_content = req.text
            soup = BeautifulSoup(html_content, 'html.parser')
            li_elements = soup.select('.hide_notice tr')
        except Exception as e:
            logger.error(f"Error fetching Theqoo hot page: {e}")
            return
   
        already_exists_post = []
        result = []

        for li in li_elements:
            elements = li.find_all('td')
            if len(elements) > 1:
                try:
                    title = elements[2].get_text(strip=True)
                    url = "https://theqoo.net" + elements[2].find('a')['href']
                    no = url.split('hot/')[-1]
                    time_text = elements[3].get_text(strip=True)

                    if '-' in time_text:
                        break  # Skip older posts

                    now = datetime.now()
                    hour, minute = map(int, time_text.split(':'))
                    target_datetime = datetime(now.year, now.month, now.day, hour, minute)

                    # Check if post already exists in DB
                    if self._post_already_exists(no, already_exists_post):
                        continue

                    gpt_obj_id = self.get_gpt_obj(no)
                    
                    contents = self.get_board_contents(url=url, category=category, no=no)
                    self.db_controller.insert_one('Realtime', {
                        'board_id': (category, no),
                        'site': SITE_THEQOO,
                        'title': title,
                        'url': url,
                        'create_time': target_datetime,
                        'GPTAnswer': gpt_obj_id,
                        'contents': contents
                    })
                    logger.info(f"Post {no} inserted successfully")
                except Exception as e:
                    logger.error(f"Error processing post {no}: {e}")
        
        logger.info({"already exists post": already_exists_post})
        return True

    def get_board_contents(self, category= None, no=None, url=None):
        _url = "https://theqoo.net/hot/" + no
        content_list = []
        try:
            req = requests.get(_url, headers=self.g_headers[0])
            req.raise_for_status()
            html_content = req.text
            soup = BeautifulSoup(html_content, 'html.parser')
            content_list = []
            write_div = soup.find('div', class_='rd_body clear')

            if write_div:
                find_all = write_div.find_all(['p', 'div'])
                for p in find_all:
                    if p.find('img'):
                        img_url = p.find('img')['src']
                        try:
                            file_path = super().save_file(img_url, category=category, no=no)
                            img_txt = super().img_to_text(os.path.join(Config().get_env('ROOT')), file_path)
                            content_list.append({'type': 'image', 'path': file_path, 'content': img_txt})
                        except Exception as e:
                            logger.error(f"Error processing image: {url} {e}")
                    elif p.find('video'):
                        video_url = "https:" + p.find('video').find('source')['src']
                        try:
                            file_path = super().save_file(video_url, category=category, no=no)
                            content_list.append({'type': 'video', 'path': file_path})
                        except Exception as e:
                            logger.error(f"Error saving video: {e}")
                    else:
                        content_list.append({'type': 'text', 'content': p.text.strip()})
            return content_list
        except Exception as e:
            logger.error(f"Error fetching board contents for {no}: {e}")
            return []

    def save_file(self, url):
        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)

        initial_file_count = len(os.listdir(self.download_path))
        try:
            script = f'''
                var link = document.createElement('a');
                link.href = "{url}";
                link.target = "_blank";
                link.click();
            '''
            self.driver.execute_script(script)


            newest_file = max(os.listdir(self.download_path),
                              key=lambda x: os.path.getctime(os.path.join(self.download_path, x)))
            return os.path.join(self.download_path, newest_file)
        except Exception as e:
            logger.error(f"Error saving image from {url}: {e}")
            return None

    def _post_already_exists(self, board_id, already_exists_post):
        existing_instance = self.db_controller.find('Realtime', {'board_id': board_id, 'site': SITE_THEQOO})
        if existing_instance:
            already_exists_post.append(board_id)
            return True
        return False

    def get_gpt_obj(self, board_id):
        gpt_exists = self.db_controller.find('GPT', {'board_id': board_id, 'site': SITE_THEQOO})
        if gpt_exists:
            return gpt_exists[0]['_id']
        else:
            gpt_obj = self.db_controller.insert_one('GPT', {
                'board_id': board_id,
                'site': SITE_THEQOO,
                'answer': DEFAULT_GPT_ANSWER
            })
            return gpt_obj.inserted_id

    def _get_or_create_tag_object(self, board_id):
        tag_exists = self.db_controller.find('TAG', {'board_id': board_id, 'site': SITE_THEQOO})
        if tag_exists:
            return tag_exists[0]['_id']
        else:
            tag_obj = self.db_controller.insert_one('TAG', {
                'board_id': board_id,
                'site': SITE_THEQOO,
                'Tag': DEFAULT_TAG
            })
            return tag_obj.inserted_id

    def is_ad():
        pass