import sys
import os
from datetime import datetime

import requests
from crawl_scheduler.config import Config
from crawl_scheduler.utils.loghandler import logger
class FileManager:
    def __init__(self):
        
        self.yyyymmdd = datetime.today().strftime('%Y/%m/%d')
        self.directory_path = f'{Config().get_env("root")}/{self.yyyymmdd}'
        print(self.directory_path)

        if not os.path.exists(self.directory_path):
            os.makedirs(self.directory_path)

    def save_img(self, board_id):
        logger.info(f"Saving image from URL: {url}")
        # if not os.path.exists(self.yyyymmdd + '/' + board_id):
        #     os.makedirs(self.download_path)

        try:
            response = requests.get(url)
            response.raise_for_status()
            img_name = os.path.basename(url)

            with open(os.path.join(self.download_path, img_name), 'wb') as f:
                f.write(response.content)

            logger.info(f"Image saved successfully at {self.download_path}/{img_name}")
            return os.path.join(self.download_path, img_name)
        except Exception as e:
            logger.error(f"Error saving image {url}: {e}")
            return None
