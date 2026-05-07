import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import argparse
import schedule
import time

from crawl_scheduler.community_website.dcinside import Dcinside
from crawl_scheduler.community_website.ppomppu import Ppomppu
from crawl_scheduler.community_website.theqoo import Theqoo
from crawl_scheduler.community_website.ygosu import Ygosu
from crawl_scheduler.utils.loghandler import logger

DEFAULT_CRAWLER_FACTORIES = (Ygosu, Ppomppu, Theqoo, Dcinside)


def get_realtime_best(crawler_factories=DEFAULT_CRAWLER_FACTORIES):
    crawl_list = [factory() for factory in crawler_factories]
    success_status = {}

    for crawl in crawl_list:
        if crawl is None:
            logger.warning("Skipping null crawler in list.")
            continue
        try:
            current_site = crawl.__class__.__name__
            logger.info(f"Start - real-time {current_site}")
            if (crawl.get_realtime_best()):
                logger.info(f"Success: {current_site}")
                success_status[current_site] = "Success"
            else:
                logger.info(f"Fail: {current_site}")
                success_status[current_site] = "Fail"
        except Exception as e:
            logger.error(f"Error - real-time {crawl.__class__.__name__}: {str(e)}", exc_info=True)
        
    logger.info(f"\n{success_status}")
    return success_status


def job():
    get_realtime_best()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run the community crawl scheduler.")
    parser.add_argument(
        "--once",
        "--seed",
        action="store_true",
        help="Run one crawl immediately, write results to DATABASE_URL, and exit.",
    )
    parser.add_argument(
        "--run-on-start",
        action="store_true",
        help="Run one crawl immediately before starting the repeating scheduler.",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=5,
        help="Scheduler interval in minutes.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    if args.once:
        job()
        return 0

    if args.run_on_start:
        job()

    interval_minutes = max(args.interval_minutes, 1)
    schedule.every(interval_minutes).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    raise SystemExit(main())
