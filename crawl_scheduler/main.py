import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
import threading

from fastapi import FastAPI, HTTPException, APIRouter
from fastapi.responses import JSONResponse

from db.mongo_controller import MongoController

from constants import DEFAULT_GPT_ANSWER, SITE_DCINSIDE, SITE_YGOSU, SITE_PPOMPPU, SITE_THEQOO, SITE_INSTIZ, SITE_RULIWEB
from utils.loghandler import setup_logger
from utils.loghandler import catch_exception
import sys

# from crawl_scheduler.community_website.instiz import Instiz
# from crawl_scheduler.community_website.ppomppu import Ppomppu
# from crawl_scheduler.community_website.ruliweb import Ruliweb
# from crawl_scheduler.community_website.theqoo import Theqoo
from crawl_scheduler.community_website.ygosu import Ygosu
sys.excepthook = catch_exception
logger = setup_logger()

board_semaphores = {}
db_controller = MongoController()

def get_real_time_best():
    # CrawllerList = [Ygosu(), Ppomppu(), Theqoo(), Instiz(), Ruliweb()]
    CrawllerList = [Ygosu()]

    for crawler in CrawllerList:
        if crawler is None:
            logger.warning(f"Skipping null crawler in list.")
            continue

        try:
            logger.info(f"Starting real-time best fetch from {crawler.__class__.__name__}")
            crawler.get_real_time_best()
            logger.info(f"Successfully fetched real-time best from {crawler.__class__.__name__}")
        except Exception as e:
            logger.error(f"Error fetching real-time best from {crawler.__class__.__name__}: {str(e)}", exc_info=True)

    return JSONResponse(content={'response': "실시간 베스트 가져오기 완료"})

get_real_time_best()