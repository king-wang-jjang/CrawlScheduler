"""Report whether stored crawler content is ready for AI analysis.

The audit only selects the columns needed for classification.  PostgreSQL
transactions are explicitly marked read-only, and the script never constructs
``PostgresController`` because that controller also performs schema
compatibility writes during initialization.
"""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Iterable
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any

from sqlalchemy import Engine, select
from sqlalchemy.orm import Session


SERVICE_ROOT = Path(__file__).resolve().parents[1]
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))

from crawl_scheduler.crawled_content import (  # noqa: E402
    analysis_min_body_characters,
    analysis_min_language_characters,
    has_sufficient_body,
    normalize_contents,
)
from crawl_scheduler.db.models import Board  # noqa: E402
from crawl_scheduler.db.postgres import get_engine  # noqa: E402


KNOWN_ANALYSIS_STATUSES = ("pending", "processing", "done", "failed")
UNKNOWN_SITE = "unknown"
UNKNOWN_STATUS = "unknown"


def _empty_counts() -> dict[str, Any]:
    return {
        "total": 0,
        "sufficient_body": 0,
        "needs_refresh": 0,
        "image_recoverable": 0,
        "title_only_or_insufficient": 0,
        "analysis_status": Counter(),
    }


def _normalized_label(value: object, fallback: str) -> str:
    label = str(value or "").strip().lower()
    return label or fallback


def _without_local_image_paths(contents: object) -> list[dict[str, str]]:
    """Remove the signal that makes a persisted image locally recoverable."""
    blocks = []
    for block in normalize_contents(contents):
        copied = dict(block)
        if copied.get("type") == "image":
            copied.pop("media_path", None)
        blocks.append(copied)
    return blocks


def _is_image_recoverable(
    title: object,
    contents: object,
    *,
    media_root: object = None,
) -> bool:
    """Return whether a local image is the only sufficient-body signal."""
    if not has_sufficient_body(title, contents, media_root=media_root):
        return False
    return not has_sufficient_body(
        title,
        _without_local_image_paths(contents),
        media_root=media_root,
    )


def _finalize_counts(counts: dict[str, Any]) -> dict[str, Any]:
    statuses = counts["analysis_status"]
    return {
        "total": counts["total"],
        "sufficient_body": counts["sufficient_body"],
        "needs_refresh": counts["needs_refresh"],
        "image_recoverable": counts["image_recoverable"],
        "title_only_or_insufficient": counts["title_only_or_insufficient"],
        "analysis_status": {
            status: statuses.get(status, 0)
            for status in (
                *KNOWN_ANALYSIS_STATUSES,
                *sorted(set(statuses) - set(KNOWN_ANALYSIS_STATUSES)),
            )
        },
    }


def summarize_rows(
    rows: Iterable[tuple[object, object, object, object]],
    *,
    generated_at: datetime | None = None,
    media_root: object = None,
) -> dict[str, Any]:
    """Build deterministic per-site and overall content-quality counters."""
    overall = _empty_counts()
    sites: dict[str, dict[str, Any]] = {}

    for site_value, title, contents, status_value in rows:
        site = _normalized_label(site_value, UNKNOWN_SITE)
        status = _normalized_label(status_value, UNKNOWN_STATUS)
        sufficient = has_sufficient_body(
            title,
            contents,
            media_root=media_root,
        )
        image_recoverable = _is_image_recoverable(
            title,
            contents,
            media_root=media_root,
        )
        site_counts = sites.setdefault(site, _empty_counts())

        for counts in (overall, site_counts):
            counts["total"] += 1
            counts["analysis_status"][status] += 1
            if sufficient:
                counts["sufficient_body"] += 1
            else:
                counts["needs_refresh"] += 1
                counts["title_only_or_insufficient"] += 1
            if image_recoverable:
                counts["image_recoverable"] += 1

    timestamp = generated_at or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    return {
        "generated_at": timestamp.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "policy": {
            "minimum_body_text_characters": analysis_min_body_characters(),
            "minimum_body_language_characters": (
                analysis_min_language_characters()
            ),
            "sufficient_body": "has_sufficient_body(title, contents)",
            "needs_refresh": "not sufficient_body",
            "image_recoverable": (
                "sufficient only because a persisted local image can be enriched"
            ),
            "title_only_or_insufficient": "same rows as needs_refresh",
        },
        "totals": _finalize_counts(overall),
        "sites": {
            site: _finalize_counts(sites[site])
            for site in sorted(sites)
        },
    }


def audit_database(
    engine: Engine,
    *,
    generated_at: datetime | None = None,
    media_root: object = None,
) -> dict[str, Any]:
    """Read board quality through a transaction that cannot write on PostgreSQL."""
    statement = select(
        Board.site,
        Board.title,
        Board.contents,
        Board.analysis_status,
    )

    with engine.connect() as connection:
        if engine.dialect.name == "postgresql":
            connection.exec_driver_sql("SET TRANSACTION READ ONLY")
        with Session(bind=connection) as session:
            rows = session.execute(
                statement.execution_options(yield_per=500)
            )
            return summarize_rows(
                rows,
                generated_at=generated_at,
                media_root=media_root,
            )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Emit a read-only JSON audit of stored crawler content quality.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Indent JSON for humans; compact JSON is the default for CI and ops.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not os.getenv("DATABASE_URL"):
        raise SystemExit("DATABASE_URL is required")

    report = audit_database(
        get_engine(),
        media_root=os.getenv("ROOT") or "./media",
    )
    json.dump(
        report,
        sys.stdout,
        ensure_ascii=False,
        indent=2 if args.pretty else None,
        sort_keys=args.pretty,
    )
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
