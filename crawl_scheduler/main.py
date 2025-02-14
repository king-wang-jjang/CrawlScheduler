import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from crawl_scheduler.community_website.dcinside import Dcinside
from crawl_scheduler.community_website.ppomppu import Ppomppu
from crawl_scheduler.community_website.theqoo import Theqoo
from crawl_scheduler.community_website.ygosu import Ygosu
from crawl_scheduler.utils.loghandler import logger

# board_semaphores = {}

def get_real_time_best():
    crawl_List = [Dcinside()]
    # crawl_List = [ Theqoo()]
    # crawl_List = [Ygosu(), Ppomppu(), Theqoo()]
    success_status = {}

    for crawl in crawl_List:
        if crawl is None:
            logger.warning(f"Skipping null crawler in list.")
            continue
        try:
            current_site = crawl.__class__.__name__
            logger.info(f"Start - real-time {current_site}")
            if (crawl.get_real_time_best()):
                logger.info(f"Success: {current_site}")
                success_status[current_site] = "Success"
            else:
                logger.info(f"Fail: {current_site}")
                success_status[current_site] = "Fail"
        except Exception as e:
            logger.error(f"Error - real-time {crawl.__class__.__name__}: {str(e)}", exc_info=True)
        
    logger.info(f"\n{success_status}")

import schedule
import time

def job():
    get_real_time_best()

if __name__ == "__main__":
    Dcinside().get_real_time_best()
    schedule.every(1).minutes.do(job)  # 5분마다 실행

    while True:
        schedule.run_pending()
        
        time.sleep(1)