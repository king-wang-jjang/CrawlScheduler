from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


SEOUL_TIMEZONE = ZoneInfo("Asia/Seoul")


def dated_post_directory(
    root: str | Path,
    site: str,
    category: str,
    post_no: int | str,
    created_at: datetime | None = None,
) -> Path:
    """Return the media directory, sharded by the Seoul source date when known."""
    path = Path(root) / site / str(category)
    if created_at is not None:
        if created_at.tzinfo is None:
            source_time = created_at.replace(tzinfo=SEOUL_TIMEZONE)
        else:
            source_time = created_at.astimezone(SEOUL_TIMEZONE)
        path = path / source_time.strftime("%Y") / source_time.strftime("%m") / source_time.strftime("%d")
    return path / str(post_no)
