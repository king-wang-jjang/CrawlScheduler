import argparse
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import schedule

from crawl_scheduler.community_website.arca import Arca
from crawl_scheduler.community_website.dcinside import Dcinside
from crawl_scheduler.community_website.fmkorea import Fmkorea
from crawl_scheduler.community_website.inven import Inven
from crawl_scheduler.community_website.ppomppu import Ppomppu
from crawl_scheduler.community_website.theqoo import Theqoo
from crawl_scheduler.community_website.ygosu import Ygosu
from crawl_scheduler.constants import (
    SITE_ARCA,
    SITE_DCINSIDE,
    SITE_FMKOREA,
    SITE_INVEN,
    SITE_PPOMPPU,
    SITE_THEQOO,
    SITE_YGOSU,
)
from crawl_scheduler.db.postgres_controller import PostgresController
from crawl_scheduler.utils.loghandler import logger

DEFAULT_INTERVAL_MINUTES = 5
SNAPSHOT_INTERVAL_MINUTES = 5


@dataclass(frozen=True)
class CrawlerSpec:
    site: str
    factory: type
    default_interval_minutes: int = DEFAULT_INTERVAL_MINUTES


CRAWLER_SPECS = (
    CrawlerSpec(SITE_YGOSU, Ygosu),
    CrawlerSpec(SITE_PPOMPPU, Ppomppu),
    CrawlerSpec(SITE_THEQOO, Theqoo, 15),
    CrawlerSpec(SITE_DCINSIDE, Dcinside),
    CrawlerSpec(SITE_FMKOREA, Fmkorea, 30),
    CrawlerSpec(SITE_ARCA, Arca, 10),
    CrawlerSpec(SITE_INVEN, Inven),
)
DEFAULT_CRAWLER_FACTORIES = tuple(spec.factory for spec in CRAWLER_SPECS)


def positive_minutes(value):
    try:
        minutes = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if minutes < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return minutes


def resolve_crawler_intervals(forced_interval=None, crawler_specs=CRAWLER_SPECS):
    if forced_interval is not None:
        return {spec.site: forced_interval for spec in crawler_specs}

    global_value = os.getenv("CRAWLER_INTERVAL_MINUTES")
    intervals = {}
    for spec in crawler_specs:
        environment_name = f"CRAWLER_{spec.site.upper()}_INTERVAL_MINUTES"
        raw_value = os.getenv(environment_name)
        if raw_value is None:
            raw_value = global_value
        if raw_value is None:
            raw_value = spec.default_interval_minutes
        try:
            intervals[spec.site] = positive_minutes(raw_value)
        except argparse.ArgumentTypeError as exc:
            source_name = (
                environment_name
                if os.getenv(environment_name) is not None
                else "CRAWLER_INTERVAL_MINUTES"
            )
            raise argparse.ArgumentTypeError(f"{source_name} {exc}") from exc
    return intervals


def get_realtime_best(crawler_factories=DEFAULT_CRAWLER_FACTORIES):
    success_status = {}

    for factory in crawler_factories:
        current_site = getattr(factory, "__name__", factory.__class__.__name__)
        try:
            crawl = factory()
        except Exception as e:
            logger.error(
                f"Error - initializing real-time {current_site}: {str(e)}",
                exc_info=True,
            )
            success_status[current_site] = "Fail"
            continue

        if crawl is None:
            logger.warning(f"Skipping null crawler: {current_site}.")
            success_status[current_site] = "Fail"
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
            logger.error(
                f"Error - real-time {crawl.__class__.__name__}: {str(e)}",
                exc_info=True,
            )
            success_status[current_site] = "Fail"

    logger.info(f"\n{success_status}")
    return success_status


def record_daily_top10_snapshot():
    try:
        PostgresController().record_daily_top10_snapshot()
    except Exception as e:
        logger.error(f"Error - daily Top10 snapshot: {str(e)}", exc_info=True)


def job():
    get_realtime_best()
    record_daily_top10_snapshot()


def configure_schedule(
    scheduler,
    crawler_intervals,
    crawler_specs=CRAWLER_SPECS,
):
    for spec in crawler_specs:
        interval_minutes = crawler_intervals[spec.site]
        scheduler.every(interval_minutes).minutes.do(
            get_realtime_best,
            (spec.factory,),
        ).tag(f"crawler:{spec.site}")
        logger.info(
            "Scheduled %s every %d minutes",
            spec.site,
            interval_minutes,
        )
    scheduler.every(SNAPSHOT_INTERVAL_MINUTES).minutes.do(
        record_daily_top10_snapshot
    ).tag("snapshot")


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
        type=positive_minutes,
        default=None,
        help=(
            "Force one scheduler interval for every site, overriding environment "
            "and recommended site defaults."
        ),
    )
    args = parser.parse_args(argv)
    try:
        args.crawler_intervals = resolve_crawler_intervals(args.interval_minutes)
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
    return args


def main(argv=None):
    args = parse_args(argv)

    if args.once:
        job()
        return 0

    if args.run_on_start:
        job()

    configure_schedule(schedule, args.crawler_intervals)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    raise SystemExit(main())
