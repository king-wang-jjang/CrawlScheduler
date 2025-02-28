import re
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from crawl_scheduler.db.mongo_controller import MongoController
from crawl_scheduler.community_website.community_website import AbstractCommunityWebsite
from crawl_scheduler.constants import DEFAULT_GPT_ANSWER, SITE_DCINSIDE, DEFAULT_TAG
import os
from crawl_scheduler.utils.loghandler import logger


class Dcinside(AbstractCommunityWebsite):
    g_headers = [
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'},
    ]

    def __init__(self):
        self.db_controller = MongoController()
 
    def get_daily_best(self):
        pass

    def get_realtime_best(self):
        try:
            req = requests.get('https://gall.dcinside.com/board/lists/?id=dcbest', headers=self.g_headers[0])
            req.raise_for_status()  # Check for HTTP errors
            html_content = req.text
            soup = BeautifulSoup(html_content, 'html.parser')
            tr_elements = soup.select('tr.ub-content') # 첫번 째는 쓰레기 값
            already_exists_post = []

        except Exception as e:
            logger.error("fetching real-time best posts: %s", e)

        for tr in tr_elements:
            try:
                # URL 추출 (url)
                a_tag = tr.find('a', href=True)
                if a_tag:
                    gall_num_td = tr.find('td', class_='gall_num')
                    if (self.is_ad(gall_num_td)):
                        continue

                    url = "https://gall.dcinside.com" + a_tag['href']
                    url_parts = url.split('?id=')[1].split('&no=')
                    category = url_parts[0]
                    no = url_parts[1].split('&')[0]
                    title = a_tag.get_text(strip=True)
                    time_tag = tr.find('td', class_='gall_date')
                    if time_tag:
                        time_str = time_tag.get_text(strip=True)
                        # 만약 이미 날짜가 포함되어 있다면(예: '2025-02-17 02.16')
                        if '-' in time_str:
                            # 시간 부분에 콜론 대신 점이 사용되었다면 변환
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
                            time_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
                        except ValueError:
                            time_obj = None
                    else:
                        time_obj = None  # 시간을 찾지 못한 경우 None

                    if self._post_already_exists((category, no)):
                        already_exists_post.append((category, no))
                        continue

                    gpt_obj_id = self.get_gpt_obj((category, no))
                    contents = self.get_board_contents(url=url, category=category, no= no)
                    self.db_controller.insert_one('Realtime', {
                        'board_id': (category, no),
                        'site': SITE_DCINSIDE,
                        'title': title,
                        'url': url,
                        'create_time': time_obj,
                        'gpt_answer': gpt_obj_id,
                        'contents': contents
                    })
                    logger.info(f"Post {(category, no)} inserted successfully")
            except Exception as e:
                logger.error(f"Error processing post{(no, category)}{url}: {e}")

        logger.info("Already exists post: %s", already_exists_post)


    def get_board_contents(self, category= None, no=None, url=None):
        # try:
        #     respone = requests.get(url, headers=self.g_headers[0])
        #     respone.raise_for_status()
        #     soup = BeautifulSoup(respone.text, 'html.parser')
        #     board_body = soup.find('div', class_='write_div')
        #     paragraphs = board_body.find_all('p')

            # content_list = self._parse_content(soup)
            # return content_list
        
        # except Exception as e:
            # logger.error(f"Error fetching board contents for {no if no else url}: {e}")
        return {"text": "dcinside는 지원하지 않습니다."}

    def set_driver_options(self):
        logger.info("Setting up Chrome driver options for Selenium")
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
        script = f'''
            var link = document.createElement('a');
            link.href = "{url}";
            link.target = "_blank";
            link.click();
        '''
        self.driver.execute_script(script)

        try:
            WebDriverWait(self.driver, 5).until(
                lambda x: len(os.listdir(self.download_path)) > initial_file_count
            )
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

    def _parse_content(self, soup):
        logger.debug("Parsing content from the page")
        content_list = []
        write_div = soup.find('div', class_='write_div')
        find_all = write_div.find_all(['p']) if len(write_div.find_all(['p'])) > len(
            write_div.find_all(['div'])) else write_div.find_all(['div'])

        for element in find_all:
            text_content = element.text.strip()
            content_list.append({'type': 'text', 'content': text_content})
        return content_list

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

    def _post_already_exists(self, board_id):
        existing_instance = self.db_controller.find('Realtime', {'board_id': board_id, 'site': SITE_DCINSIDE})
        return existing_instance
