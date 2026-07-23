import sys
from datetime import datetime, timezone
from pathlib import Path


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))
sys.path.insert(0, str(SERVICE_ROOT / "scripts"))


def test_dated_post_directory_uses_seoul_source_date():
    from crawl_scheduler.media_paths import dated_post_directory

    created_at = datetime(2026, 7, 22, 15, 30, tzinfo=timezone.utc)

    assert dated_post_directory(
        "/media", "Dcinside", "dcbest", 447775, created_at
    ) == Path("/media/Dcinside/dcbest/2026/07/23/447775")


def test_dated_post_directory_keeps_legacy_shape_without_date():
    from crawl_scheduler.media_paths import dated_post_directory

    assert dated_post_directory(
        "/media", "Ppomppu", "humor", 123
    ) == Path("/media/Ppomppu/humor/123")


def test_migration_rewrites_only_local_media_paths():
    from migrate_dcinside_media_layout import rewrite_content_paths

    contents = [
        {
            "type": "image",
            "media_path": "Dcinside/dcbest/447775/image.jpg",
            "source_url": "https://example.com/Dcinside/dcbest/447775/image.jpg",
        },
        {"type": "text", "text": "Dcinside/dcbest/447775 must remain text"},
    ]

    rewritten = rewrite_content_paths(
        contents,
        "Dcinside/dcbest/447775",
        "Dcinside/dcbest/2026/07/23/447775",
    )

    assert rewritten[0]["media_path"] == (
        "Dcinside/dcbest/2026/07/23/447775/image.jpg"
    )
    assert rewritten[0]["source_url"] == contents[0]["source_url"]
    assert rewritten[1]["text"] == contents[1]["text"]


def test_migration_normalizes_sqlalchemy_postgres_url():
    from migrate_dcinside_media_layout import psycopg_database_url

    assert psycopg_database_url(
        "postgresql+psycopg://user:password@db/database"
    ) == "postgresql://user:password@db/database"


def test_migration_loads_legacy_iso_dates(tmp_path):
    from migrate_dcinside_media_layout import load_legacy_dates

    date_map = tmp_path / "dates.tsv"
    date_map.write_text("359240\t2025-08-28T16:30:00.000Z\n", encoding="utf-8")

    loaded = load_legacy_dates(str(date_map))

    assert loaded["359240"].astimezone(timezone.utc) == datetime(
        2025, 8, 28, 16, 30, tzinfo=timezone.utc
    )
