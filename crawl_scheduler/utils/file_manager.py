import sys
import os
from datetime import datetime
from crawl_scheduler.config import Config

class FileManager:
    def __init__(self):
        
        self.yyyymmdd = datetime.today().strftime('%Y/%m/%d')
        self.directory_path = f'{Config().get_env("root")}/{self.yyyymmdd}'
        print(self.directory_path)

        if not os.path.exists(self.directory_path):
            os.makedirs(self.directory_path)

