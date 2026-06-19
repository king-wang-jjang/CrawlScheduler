import re
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from urllib.parse import parse_qs, urljoin, urlparse
from crawl_scheduler.config import Config
from crawl_scheduler.crawled_content import image_block, metadata_image_block, text_block, video_block
from crawl_scheduler.db.postgres_controller import PostgresController
from crawl_scheduler.community_website.community_website import AbstractCommunityWebsite
from crawl_scheduler.constants import DEFAULT_GPT_ANSWER, SITE_DCINSIDE, DEFAULT_TAG
import os
from crawl_scheduler.utils.loghandler import logger


class Dcinside(AbstractCommunityWebsite):
    g_headers = [
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'referer': 'https://www.dcinside.com/'
        },
    ]

    def __init__(self):
        self.db_controller = PostgresController()
 
    def get_daily_best(self):
        pass

    def get_realtime_best(self):
        already_exists_post = []
        board_list = self.get_board_list()  # 🔹 분리한 함수 호출

        for url, category, no, title, time_obj in board_list:  # ✅ 튜플 언패킹 활용
            try:
                if self._post_already_exists(category, no):
                    already_exists_post.append((category, no))
                    continue

                gpt_obj_id = self.get_gpt_obj((category, no))
                contents = self.get_board_contents(url=url, category=category, no=no)

                self.db_controller.insert_one('Realtime', {
                    'site': SITE_DCINSIDE,
                    'category': category,
                    'no': int(no),
                    'title': title,
                    'url': url,
                    'create_time': time_obj,
                    'gpt_answer': gpt_obj_id,
                    'contents': contents
                })
                logger.info(f"Post {(category, no)} inserted successfully")
            except Exception as e:
                logger.error(f"Error Save To DB {category, no}: {e}")

        logger.info("Already exists post: %s", already_exists_post)
        return True

    def get_board_list(self):
        """ 게시판에서 URL, 카테고리, 게시글 번호, 생성 시간, 제목 추출 """
        try:
            req = requests.get('https://gall.dcinside.com/board/lists/?id=dcbest', headers=self.g_headers[0])
            req.raise_for_status()  # Check for HTTP errors
            html_content = req.text
            soup = BeautifulSoup(html_content, 'html.parser')
        except Exception as e:
            logger.error(f"Get List Error: {e}")
            return []

        board_list = []
        tr_elements = soup.select('tr.ub-content')

        for tr in tr_elements:
            try:
                # URL 추출
                a_tag = tr.find('a', href=True)
                if not a_tag:
                    continue

                gall_num_td = tr.find('td', class_='gall_num')
                if self.is_ad(gall_num_td):
                    continue

                url = urljoin("https://gall.dcinside.com", a_tag['href'])
                query = parse_qs(urlparse(url).query)
                category = query.get("id", [""])[0]
                no = query.get("no", [""])[0]
                if not category or not no:
                    continue
                title = a_tag.get_text(strip=True)

                # 시간 처리
                time_obj = self.parse_time(tr.find('td', class_='gall_date'))

                # ✅ 튜플로 반환
                board_list.append((url, category, no, title, time_obj))

            except Exception as e:
                logger.error(f"Error parsing post: {e}")

        return board_list

    def parse_time(self, time_tag):
        """ 시간 문자열을 datetime 객체로 변환 """
        if not time_tag:
            return None
        
        time_str = time_tag.get_text(strip=True)

        # 만약 이미 날짜가 포함되어 있다면(예: '2025-02-17 02.16')
        if '-' in time_str:
            parts = time_str.split()
            if len(parts) == 2:
                date_part, time_part = parts
                if '.' in time_part and ':' not in time_part:
                    time_part = time_part.replace('.', ':')
                datetime_str = f"{date_part} {time_part}"
            else:
                datetime_str = time_str
        else:
            # 시간만 있다면 오늘 날짜와 결합 (예: '16:55' 또는 '02.16')
            if '.' in time_str and ':' not in time_str:
                time_str = time_str.replace('.', ':')
            today_date = datetime.today().strftime('%Y-%m-%d')
            datetime_str = f"{today_date} {time_str}"

        try:
            return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
        except ValueError:
            return None  # 파싱 실패 시 None 반환

    def get_board_contents(self, category= None, no=None, url=None):
        content_list = []
        try:
            respone = requests.get(url, headers=self.g_headers[0])
            respone.raise_for_status()
            soup = BeautifulSoup(respone.text, 'html.parser')
            metadata_block = metadata_image_block(
                super().metadata_image_url_from_soup(soup, base_url=url)
            )
            if metadata_block:
                content_list.append(metadata_block)
            board_body = soup.find('div', class_='write_div')
            paragraphs = board_body.find_all('p')

            for p in paragraphs:
                if p.find('img'):
                    img_url = super().media_url_from_tag(p.find('img'), base_url=url)
                    if not img_url:
                        continue
                    try:
                        file_path = super().save_file(img_url, category=category, no=no, headers=self.g_headers[0])
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
        
    def set_driver_options(self):
        logger.info("Setting up Chrome driver options for Selenium")
        import importlib
        webdriver_mod = importlib.import_module('selenium')
        options_mod = importlib.import_module('selenium.webdriver.chrome.options')
        common_by = importlib.import_module('selenium.webdriver.common.by')
        support_ui = importlib.import_module('selenium.webdriver.support.ui')
        support_ec = importlib.import_module('selenium.webdriver.support.expected_conditions')
        Options = getattr(options_mod, 'Options')
        webdriver = getattr(webdriver_mod, 'webdriver')
        By = getattr(common_by, 'By')
        WebDriverWait = getattr(support_ui, 'WebDriverWait')
        EC = support_ec
        chrome_options = Options()
        prefs = {"download.default_directory": self.download_path}
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-setuid-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

        os.makedirs(self.download_path, exist_ok=True)

        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.get("https://www.dcinside.com/")
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.XPATH, '//body'))
            )
            logger.info("Selenium driver initialized and page loaded")
            return True
        except Exception as e:
            logger.error('Error initializing Selenium driver: %s', e)
            return False

    def save_img(self, url):
        logger.info(f"Saving image from URL: {url}")
        os.makedirs(self.download_path, exist_ok=True)

        initial_file_count = len(os.listdir(self.download_path))
        import time
        script = f'''
            var link = document.createElement('a');
            link.href = "{url}";
            link.target = "_blank";
            link.click();
        '''
        self.driver.execute_script(script)

        try:
            start_time = time.time()
            timeout_sec = 5
            while time.time() - start_time < timeout_sec:
                if len(os.listdir(self.download_path)) > initial_file_count:
                    break
                time.sleep(0.1)
            newest_file = self._get_newest_file(self.download_path)
            logger.info(f"Image saved successfully at {newest_file}")
            return os.path.join(self.download_path, newest_file)
        except Exception as e:
            logger.error(f"Error saving image from {url}: %s", e)
            return None

    def _get_target_datetime(self, time_text):
        logger.debug(f"Parsing time_text {time_text}")
        now = datetime.now()
        hour, minute = map(int, time_text.split(':'))
        return datetime(now.year, now.month, now.day, hour, minute)

    def _post_exists(self, board_id):
        logger.debug(f"Checking if post {board_id} exists in DB")
        existing_instance = self.db_controller.find('Realtime', {'board_id': board_id, 'site': SITE_DCINSIDE})
        return existing_instance is not None

    def _get_or_create_gpt_obj_id(self, board_id):
        logger.debug(f"Fetching or creating GPT object for post {board_id}")
        gpt_exists = self.db_controller.find('GPT', {'board_id': board_id, 'site': SITE_DCINSIDE})
        if gpt_exists:
            return gpt_exists[0]['_id']
        else:
            gpt_obj = self.db_controller.insert_one('GPT', {
                'board_id': board_id,
                'site': SITE_DCINSIDE,
                'answer': DEFAULT_GPT_ANSWER
            })
            return gpt_obj.inserted_id

    def _get_newest_file(self, directory):
        logger.debug(f"Finding newest file in {directory}")
        files = os.listdir(directory)
        newest_file = max(files, key=lambda x: os.path.getctime(os.path.join(directory, x)))
        return newest_file

    def get_gpt_obj(self, board_id):
        gpt_exists = self.db_controller.find('GPT', {'board_id': board_id, 'site': SITE_DCINSIDE})
        if gpt_exists:
            return gpt_exists[0]['_id']
        else:
            gpt_obj = self.db_controller.insert_one('GPT', {
                'board_id': board_id,
                'site': SITE_DCINSIDE,
                'answer': DEFAULT_GPT_ANSWER,
                'tag': DEFAULT_TAG
            })
            return gpt_obj.inserted_id
        
    def is_ad(self, title) -> bool:
        if title and title.get_text(strip=True) in ['공지', '설문']:
            return True
        return False

    def save_file(self, url, category, no, alt_text=None):
        pass

    def _post_already_exists(self, category, no):
        existing_instance = self.db_controller.find('Realtime', {'site': SITE_DCINSIDE, 'category': category, 'no': int(no)})
        return existing_instance
