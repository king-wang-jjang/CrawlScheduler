"""Move legacy Dcinside/dcbest/<no> directories into Seoul-date shards.

The crawler must be stopped while applying the migration. Dry-run is the default;
pass --apply to move directories and update PostgreSQL paths.
"""

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg


SEOUL_TIMEZONE = ZoneInfo("Asia/Seoul")
OLD_BASE = Path("Dcinside/dcbest")


def psycopg_database_url(database_url: str) -> str:
    return database_url.replace("postgresql+psycopg://", "postgresql://", 1)


def rewrite_content_paths(value, old_prefix: str, new_prefix: str):
    if isinstance(value, list):
        return [rewrite_content_paths(item, old_prefix, new_prefix) for item in value]
    if isinstance(value, dict):
        rewritten = {}
        for key, item in value.items():
            if key in {"media_path", "path"} and isinstance(item, str):
                rewritten[key] = replace_prefix(item, old_prefix, new_prefix)
            else:
                rewritten[key] = rewrite_content_paths(item, old_prefix, new_prefix)
        return rewritten
    return value


def replace_prefix(value: str | None, old_prefix: str, new_prefix: str):
    if not value:
        return value
    if value == old_prefix:
        return new_prefix
    if value.startswith(f"{old_prefix}/"):
        return f"{new_prefix}{value[len(old_prefix):]}"
    return value


def date_parts(created_at: datetime) -> tuple[str, str, str]:
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=SEOUL_TIMEZONE)
    source_time = created_at.astimezone(SEOUL_TIMEZONE)
    return source_time.strftime("%Y"), source_time.strftime("%m"), source_time.strftime("%d")


def move_directory(source: Path, target: Path):
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise FileExistsError(f"target already exists: {target}")
    source.rename(target)


def load_legacy_dates(path: str | None) -> dict[str, datetime]:
    if not path:
        return {}
    dates = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        post_no, iso_date = line.split("\t", 1)
        dates[post_no] = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
    return dates


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=os.getenv("ROOT") or "/app/public")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--legacy-date-map")
    parser.add_argument("--moved-map")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    if not args.database_url:
        parser.error("DATABASE_URL or --database-url is required")

    root = Path(args.root).resolve()
    legacy_root = (root / OLD_BASE).resolve()
    if root not in legacy_root.parents or legacy_root.name != "dcbest":
        raise RuntimeError(f"unsafe migration root: {legacy_root}")
    if not legacy_root.is_dir():
        raise RuntimeError(f"media directory does not exist: {legacy_root}")

    legacy_dates = load_legacy_dates(args.legacy_date_map)
    moved_records = []
    counters = {
        "matched_postgres": 0,
        "matched_legacy": 0,
        "unmatched": 0,
        "conflicts": 0,
        "moved": 0,
    }
    with psycopg.connect(psycopg_database_url(args.database_url)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT no, created_at, contents, thumbnail
                FROM boards
                WHERE lower(site) = 'dcinside' AND category = 'dcbest'
                """
            )
            boards = {str(row[0]): row[1:] for row in cursor.fetchall()}

        for source in sorted(legacy_root.iterdir(), key=lambda path: path.name):
            if not source.is_dir() or not source.name.isdigit():
                continue
            board = boards.get(source.name)
            is_postgres_board = board is not None
            if is_postgres_board:
                created_at, contents, thumbnail = board
                counters["matched_postgres"] += 1
            elif source.name in legacy_dates:
                created_at = legacy_dates[source.name]
                contents = None
                thumbnail = None
                counters["matched_legacy"] += 1
            else:
                counters["unmatched"] += 1
                continue

            year, month, day = date_parts(created_at)
            target = legacy_root / year / month / day / source.name
            if target.exists():
                counters["conflicts"] += 1
                continue

            if not args.apply:
                continue

            old_prefix = (OLD_BASE / source.name).as_posix()
            new_prefix = (OLD_BASE / year / month / day / source.name).as_posix()
            decoded_contents = json.loads(contents) if isinstance(contents, str) else contents
            rewritten_contents = rewrite_content_paths(
                decoded_contents, old_prefix, new_prefix
            )
            rewritten_thumbnail = replace_prefix(thumbnail, old_prefix, new_prefix)

            move_directory(source, target)
            try:
                if is_postgres_board:
                    with connection.transaction():
                        with connection.cursor() as cursor:
                            cursor.execute(
                                """
                                UPDATE boards
                                SET contents = %s::json, thumbnail = %s
                                WHERE lower(site) = 'dcinside'
                                  AND category = 'dcbest'
                                  AND no = %s
                                """,
                                (
                                    json.dumps(rewritten_contents, ensure_ascii=False),
                                    rewritten_thumbnail,
                                    int(source.name),
                                ),
                            )
                            if cursor.rowcount != 1:
                                raise RuntimeError(
                                    f"expected one board row for {source.name}, got {cursor.rowcount}"
                                )
            except Exception:
                source.parent.mkdir(parents=True, exist_ok=True)
                target.rename(source)
                raise
            moved_records.append(f"{source.name}\t{new_prefix}")
            counters["moved"] += 1

    if args.moved_map and args.apply:
        Path(args.moved_map).write_text(
            "\n".join(moved_records) + ("\n" if moved_records else ""),
            encoding="utf-8",
        )

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"{mode}: " + " ".join(f"{key}={value}" for key, value in counters.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
