import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


_COUNT_PATTERN = re.compile(r"\d[\d,]*")
SEOUL_TIMEZONE = ZoneInfo("Asia/Seoul")


def parse_native_count(value) -> int | None:
    """Return the first non-negative integer found in a source metric label."""
    if value is None:
        return None

    text = value.get_text(" ", strip=True) if hasattr(value, "get_text") else str(value)
    match = _COUNT_PATTERN.search(text)
    if not match:
        return None
    return int(match.group(0).replace(",", ""))


def recent_source_datetime(
    hour: int,
    minute: int,
    second: int = 0,
    *,
    now: datetime | None = None,
) -> datetime:
    """Combine a time-only source value with the most plausible Seoul date."""
    current = now or datetime.now(SEOUL_TIMEZONE)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SEOUL_TIMEZONE)
    else:
        current = current.astimezone(SEOUL_TIMEZONE)
    candidate = current.replace(
        hour=hour,
        minute=minute,
        second=second,
        microsecond=0,
    )
    if candidate > current + timedelta(minutes=5):
        candidate -= timedelta(days=1)
    return candidate


@dataclass(frozen=True)
class BoardListEntry:
    url: str
    category: str
    no: int | str
    title: str
    created_at: datetime
    native_comment_count: int | None
    native_like_count: int | None
    native_view_count: int | None
    source_rank: int | None
    metrics_crawled_at: datetime | None = None

    def metrics_dict(self) -> dict:
        metrics = {
            "native_comment_count": self.native_comment_count,
            "native_like_count": self.native_like_count,
            "native_view_count": self.native_view_count,
            "source_rank": self.source_rank,
        }
        if self.metrics_crawled_at is not None:
            metrics["metrics_crawled_at"] = self.metrics_crawled_at
        return metrics
