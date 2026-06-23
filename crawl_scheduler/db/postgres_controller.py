from dataclasses import dataclass
from datetime import datetime, timezone
import json
from uuid import uuid4

from sqlalchemy import desc, inspect, nullslast, select, text

from crawl_scheduler.constants import DEFAULT_GPT_ANSWER, DEFAULT_TAG
from crawl_scheduler.crawled_content import (
    extract_llm_text,
    first_thumbnail_path,
    normalize_contents,
)
from crawl_scheduler.db.models import Board, BoardMetricSnapshot, CrawlerLog
from crawl_scheduler.db.postgres import Base, get_engine, get_session_factory
from crawl_scheduler.popularity import PopularityMetrics, calculate_popularity_scores
from crawl_scheduler.utils.llm import LLM


BOARD_COLLECTIONS = {"realtime", "daily"}


@dataclass(frozen=True)
class InsertOneResult:
    inserted_id: object


class DeleteResult:
    def __init__(self, deleted_count: int):
        self.deleted_count = deleted_count


class UpdateResult:
    def __init__(self, matched_count: int, modified_count: int):
        self.matched_count = matched_count
        self.modified_count = modified_count


class PostgresController:
    def __init__(self, database_url: str | None = None, analyzer: LLM | None = None):
        self.database_url = database_url
        self.analyzer = analyzer or LLM()
        engine = get_engine(database_url)
        Base.metadata.create_all(bind=engine)
        self._ensure_board_columns(engine)
        self._ensure_schema_compatibility(engine)

    def find(self, collection_name: str, query: dict) -> list[dict]:
        collection = collection_name.lower()
        if collection not in BOARD_COLLECTIONS:
            return []

        with get_session_factory(self.database_url)() as session:
            stmt = select(Board)
            stmt = self._apply_board_query(stmt, query)
            return [self._board_to_document(board) for board in session.scalars(stmt).all()]

    def insert_one(self, collection_name: str, document: dict) -> InsertOneResult:
        collection = collection_name.lower()
        if collection in BOARD_COLLECTIONS:
            board = self._upsert_board(collection, document)
            return InsertOneResult(board.id)
        if collection == "gpt":
            return InsertOneResult(document.get("answer") or DEFAULT_GPT_ANSWER)
        if collection == "tag":
            return InsertOneResult(document.get("Tag") or document.get("tag") or DEFAULT_TAG)
        if collection == "log":
            return self._insert_log(document)
        return InsertOneResult(str(uuid4()))

    def update_one(self, collection_name: str, query: dict, update: dict) -> UpdateResult:
        collection = collection_name.lower()
        if collection not in BOARD_COLLECTIONS:
            return UpdateResult(0, 0)

        with get_session_factory(self.database_url)() as session:
            stmt = self._apply_board_query(select(Board), query)
            board = session.scalars(stmt).first()
            if board is None:
                return UpdateResult(0, 0)

            values = update.get("$set", update)
            for key, value in values.items():
                attr = self._board_attr_name(key)
                if hasattr(board, attr):
                    setattr(board, attr, value)
            session.commit()
            return UpdateResult(1, 1)

    def delete_one(self, collection_name: str, query: dict) -> DeleteResult:
        collection = collection_name.lower()
        if collection not in BOARD_COLLECTIONS:
            return DeleteResult(0)

        with get_session_factory(self.database_url)() as session:
            stmt = self._apply_board_query(select(Board), query)
            board = session.scalars(stmt).first()
            if board is None:
                return DeleteResult(0)
            session.delete(board)
            session.commit()
            return DeleteResult(1)

    def get_realtime_best(self, index: int, limit: int) -> list[dict]:
        return self._list_boards(index=index, limit=limit, daily=False)

    def get_daily_best(self, index: int, limit: int) -> list[dict]:
        return self._list_boards(index=index, limit=limit, daily=True)

    def _upsert_board(self, collection_name: str, document: dict) -> Board:
        source_id = self._source_id_from_document(collection_name, document)
        with get_session_factory(self.database_url)() as session:
            board = session.scalar(select(Board).where(Board.source_id == source_id))
            values = self._board_values(collection_name, document, board)
            if board is None:
                board = Board(**values)
                session.add(board)
                session.flush()
            else:
                for key, value in values.items():
                    if key in {"id", "like_count"}:
                        continue
                    setattr(board, key, value)

            self._record_metric_snapshot(session, board, document)
            session.commit()
            session.refresh(board)
            return board

    def _board_values(self, collection_name: str, document: dict, existing_board: Board | None = None) -> dict:
        site = str(document.get("site") or "unknown")
        category, no = self._category_and_no(collection_name, document)
        contents = normalize_contents(document.get("contents"))
        summary, tags = self._analysis_values(document, existing_board)
        analysis_status, analysis_updated_at = self._analysis_queue_values(
            document,
            existing_board,
            summary,
        )
        return {
            "source_id": self._source_id(site, category, no, document),
            "category": category,
            "no": no,
            "site": site,
            "title": str(document.get("title") or ""),
            "url": str(document.get("url") or ""),
            "contents": self._json_safe(contents),
            "gpt_answer": summary,
            "tags": tags,
            "analysis_status": analysis_status,
            "analysis_priority": int(document.get("analysis_priority") or getattr(existing_board, "analysis_priority", 0) or 0),
            "analysis_requested_at": document.get("analysis_requested_at") or getattr(existing_board, "analysis_requested_at", None),
            "analysis_started_at": getattr(existing_board, "analysis_started_at", None),
            "analysis_updated_at": analysis_updated_at,
            "analysis_retry_count": int(document.get("analysis_retry_count") or getattr(existing_board, "analysis_retry_count", 0) or 0),
            "analysis_error": getattr(existing_board, "analysis_error", None),
            "thumbnail": document.get("thumbnail") or first_thumbnail_path(contents),
            "comment_count": int(document.get("comment_count") or 0),
            "like_count": int(document.get("like_count") or 0),
            "native_comment_count": self._optional_int(
                document.get("native_comment_count", document.get("comment_count"))
            ),
            "native_like_count": self._optional_int(
                document.get("native_like_count", document.get("like_count"))
            ),
            "native_view_count": self._optional_int(
                document.get("native_view_count", document.get("view_count"))
            ),
            "source_rank": self._optional_int(document.get("source_rank")),
            "metrics_crawled_at": document.get("metrics_crawled_at"),
            "next_metrics_crawl_at": document.get("next_metrics_crawl_at"),
            "created_at": self._coerce_datetime(document.get("create_time") or document.get("created_at")),
        }

    def _category_and_no(self, collection_name: str, document: dict) -> tuple[str, int]:
        category = document.get("category")
        no = document.get("no")
        board_id = document.get("board_id")

        if (category is None or no is None) and isinstance(board_id, (tuple, list)) and len(board_id) == 2:
            category, no = board_id

        if category is None:
            category = collection_name
        return str(category), self._coerce_int(no)

    def _source_id_from_document(self, collection_name: str, document: dict) -> str:
        site = str(document.get("site") or "unknown")
        category, no = self._category_and_no(collection_name, document)
        return self._source_id(site, category, no, document)

    def _source_id(self, site: str, category: str, no: int, document: dict) -> str:
        if category and no:
            return f"{site}:{category}:{no}"
        board_id = document.get("board_id")
        if board_id is not None:
            return f"{site}:{self._source_token(board_id)}"
        url = document.get("url")
        if url:
            return f"{site}:{url}"
        return f"{site}:{category}:{no}"

    def _apply_board_query(self, stmt, query: dict):
        source_id = self._source_id_from_query(query)
        if source_id is not None:
            stmt = stmt.where(Board.source_id == source_id)

        for key, value in query.items():
            if key == "_id":
                stmt = stmt.where(Board.id == str(value))
            elif key == "site":
                stmt = stmt.where(Board.site == str(value))
            elif key == "category":
                stmt = stmt.where(Board.category == str(value))
            elif key == "no":
                stmt = stmt.where(Board.no == self._coerce_int(value))
            elif key in {"source_id", "url", "title"}:
                stmt = stmt.where(getattr(Board, key) == str(value))
        return stmt

    def _source_id_from_query(self, query: dict) -> str | None:
        site = query.get("site")
        if not site:
            return None

        if "category" in query and "no" in query:
            return f"{site}:{query['category']}:{self._coerce_int(query['no'])}"

        board_id = query.get("board_id")
        if isinstance(board_id, (tuple, list)) and len(board_id) == 2:
            return f"{site}:{board_id[0]}:{self._coerce_int(board_id[1])}"
        if board_id is not None:
            return f"{site}:{self._source_token(board_id)}"
        return None

    def _list_boards(self, index: int, limit: int, daily: bool) -> list[dict]:
        offset = max(index, 0) * max(limit, 1)
        ordering = nullslast(desc(Board.daily_score)) if daily else nullslast(desc(Board.hot_score))
        secondary_ordering = desc(Board.like_count) if daily else desc(Board.created_at)
        with get_session_factory(self.database_url)() as session:
            boards = session.scalars(
                select(Board)
                .order_by(ordering, secondary_ordering, desc(Board.created_at))
                .offset(offset)
                .limit(limit)
            ).all()
            return [self._board_to_document(board) for board in boards]

    def _insert_log(self, document: dict) -> InsertOneResult:
        payload = self._json_safe(document)
        log = CrawlerLog(
            level=payload.get("levelname"),
            message=payload.get("message") or payload.get("msg"),
            logger_name=payload.get("name"),
            server=payload.get("server"),
            payload=payload,
        )
        with get_session_factory(self.database_url)() as session:
            session.add(log)
            session.commit()
            session.refresh(log)
            return InsertOneResult(log.id)

    @staticmethod
    def _ensure_schema_compatibility(engine) -> None:
        if engine.dialect.name != "postgresql":
            return

        with engine.begin() as connection:
            connection.execute(text("ALTER TABLE boards ALTER COLUMN no TYPE BIGINT"))

    def _board_to_document(self, board: Board) -> dict:
        return {
            "_id": board.id,
            "id": board.id,
            "source_id": board.source_id,
            "category": board.category,
            "no": board.no,
            "site": board.site,
            "title": board.title,
            "url": board.url,
            "contents": board.contents,
            "gpt_answer": board.gpt_answer,
            "tags": board.tags or [],
            "analysis_status": board.analysis_status,
            "analysis_priority": int(board.analysis_priority or 0),
            "analysis_retry_count": int(board.analysis_retry_count or 0),
            "analysis_error": board.analysis_error,
            "analysis_requested_at": board.analysis_requested_at,
            "analysis_started_at": board.analysis_started_at,
            "analysis_updated_at": board.analysis_updated_at,
            "create_time": self._coerce_datetime(board.created_at),
            "thumbnail": board.thumbnail,
            "comment_count": int(board.comment_count or 0),
            "like_count": int(board.like_count or 0),
            "native_comment_count": board.native_comment_count,
            "native_like_count": board.native_like_count,
            "native_view_count": board.native_view_count,
            "source_rank": board.source_rank,
            "hot_score": board.hot_score,
            "daily_score": board.daily_score,
            "score_breakdown": board.score_breakdown or {},
            "metrics_crawled_at": board.metrics_crawled_at,
            "score_updated_at": board.score_updated_at,
        }

    @staticmethod
    def _board_attr_name(key: str) -> str:
        return "created_at" if key == "create_time" else key

    def _analysis_values(self, document: dict, existing_board: Board | None) -> tuple[str | None, list]:
        incoming_summary = self._coerce_text(
            document.get("gpt_answer") or document.get("GPTAnswer") or DEFAULT_GPT_ANSWER
        )
        incoming_tags = self._normalize_tags(
            document.get("tags") or document.get("Tag") or document.get("tag") or DEFAULT_TAG
        )

        if incoming_summary and incoming_summary != DEFAULT_GPT_ANSWER:
            return incoming_summary, incoming_tags

        if existing_board and existing_board.gpt_answer and existing_board.gpt_answer != DEFAULT_GPT_ANSWER:
            return existing_board.gpt_answer, existing_board.tags or incoming_tags

        return incoming_summary, incoming_tags

    @staticmethod
    def _analysis_queue_values(document: dict, existing_board: Board | None, summary: str | None) -> tuple[str, datetime | None]:
        incoming_status = document.get("analysis_status")
        existing_status = getattr(existing_board, "analysis_status", None)
        existing_updated_at = getattr(existing_board, "analysis_updated_at", None)

        if summary and summary != DEFAULT_GPT_ANSWER:
            return "done", datetime.now(timezone.utc)

        if incoming_status in {"pending", "processing", "done", "failed"}:
            return incoming_status, datetime.now(timezone.utc)

        if existing_status:
            return existing_status, existing_updated_at

        return "pending", None

    @staticmethod
    def _analysis_text(document: dict, contents: object) -> str:
        return extract_llm_text(document.get("title"), contents)

    @staticmethod
    def _normalize_tags(tags: object) -> list:
        if tags is None:
            return []
        if isinstance(tags, str):
            return [tags] if tags.strip() else []
        if not isinstance(tags, list):
            return []

        normalized = []
        for tag in tags:
            if not isinstance(tag, str):
                continue
            value = tag.strip()
            if value and value not in normalized:
                normalized.append(value)
            if len(normalized) >= 5:
                break
        return normalized

    @staticmethod
    def _ensure_board_columns(engine) -> None:
        inspector = inspect(engine)
        if not inspector.has_table("boards"):
            return

        existing_columns = {column["name"] for column in inspector.get_columns("boards")}
        column_definitions = {
            "tags": "JSON",
            "analysis_status": "VARCHAR(32) NOT NULL DEFAULT 'pending'",
            "analysis_priority": "INTEGER NOT NULL DEFAULT 0",
            "analysis_requested_at": "TIMESTAMP",
            "analysis_started_at": "TIMESTAMP",
            "analysis_updated_at": "TIMESTAMP",
            "analysis_retry_count": "INTEGER NOT NULL DEFAULT 0",
            "analysis_error": "TEXT",
            "native_comment_count": "INTEGER",
            "native_like_count": "INTEGER",
            "native_view_count": "INTEGER",
            "source_rank": "INTEGER",
            "metrics_crawled_at": "TIMESTAMP",
            "next_metrics_crawl_at": "TIMESTAMP",
            "hot_score": "FLOAT",
            "daily_score": "FLOAT",
            "score_updated_at": "TIMESTAMP",
            "score_breakdown": "JSON",
        }
        missing_columns = {
            column_name: definition
            for column_name, definition in column_definitions.items()
            if column_name not in existing_columns
        }
        if not missing_columns:
            return

        with engine.begin() as connection:
            for column_name, definition in missing_columns.items():
                connection.execute(text(f"ALTER TABLE boards ADD COLUMN {column_name} {definition}"))

    @staticmethod
    def _source_token(value: object) -> str:
        if isinstance(value, (tuple, list)):
            return ":".join(str(item) for item in value)
        return str(value)

    @staticmethod
    def _coerce_int(value: object) -> int:
        if value is None or value == "":
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return abs(hash(str(value))) % 2_147_483_647

    @staticmethod
    def _optional_int(value: object) -> int | None:
        if value is None or value == "":
            return None
        try:
            return max(int(value), 0)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_text(value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, default=str)

    @staticmethod
    def _coerce_datetime(value: object) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        return datetime.now(timezone.utc)

    @staticmethod
    def _extract_thumbnail(contents: object) -> str | None:
        return first_thumbnail_path(contents)

    @staticmethod
    def _json_safe(value: object):
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))

    def _record_metric_snapshot(self, session, board: Board, document: dict) -> None:
        comment_count = self._optional_int(
            document.get("native_comment_count", document.get("comment_count"))
        )
        like_count = self._optional_int(document.get("native_like_count", document.get("like_count")))
        view_count = self._optional_int(document.get("native_view_count", document.get("view_count")))
        source_rank = self._optional_int(document.get("source_rank"))
        captured_at = document.get("metrics_crawled_at") or datetime.now(timezone.utc)
        if not isinstance(captured_at, datetime):
            captured_at = datetime.now(timezone.utc)
        if captured_at.tzinfo is None:
            captured_at = captured_at.replace(tzinfo=timezone.utc)

        previous_snapshots = session.scalars(
            select(BoardMetricSnapshot)
            .where(BoardMetricSnapshot.board_id == board.id)
            .order_by(desc(BoardMetricSnapshot.captured_at))
            .limit(2)
        ).all()
        previous_snapshot = previous_snapshots[0] if previous_snapshots else None
        previous_previous_snapshot = previous_snapshots[1] if len(previous_snapshots) > 1 else None
        scores = calculate_popularity_scores(
            PopularityMetrics(
                site=board.site,
                created_at=board.created_at,
                captured_at=captured_at,
                comment_count=comment_count or 0,
                like_count=like_count or 0,
                view_count=view_count,
                source_rank=source_rank,
                previous_comment_count=getattr(previous_snapshot, "comment_count", None),
                previous_like_count=getattr(previous_snapshot, "like_count", None),
                previous_view_count=getattr(previous_snapshot, "view_count", None),
                previous_delta_comments=self._snapshot_delta(
                    previous_snapshot,
                    previous_previous_snapshot,
                    "comment_count",
                ),
                previous_delta_likes=self._snapshot_delta(
                    previous_snapshot,
                    previous_previous_snapshot,
                    "like_count",
                ),
                previous_delta_views=self._snapshot_delta(
                    previous_snapshot,
                    previous_previous_snapshot,
                    "view_count",
                ),
            )
        )

        session.add(
            BoardMetricSnapshot(
                board_id=board.id,
                captured_at=captured_at,
                comment_count=comment_count or 0,
                like_count=like_count or 0,
                view_count=view_count,
                source_rank=source_rank,
            )
        )
        board.native_comment_count = comment_count
        board.native_like_count = like_count
        board.native_view_count = view_count
        board.source_rank = source_rank
        board.metrics_crawled_at = captured_at
        board.hot_score = scores.hot_score
        board.daily_score = scores.daily_score
        board.score_breakdown = scores.breakdown
        board.score_updated_at = captured_at

    @staticmethod
    def _snapshot_delta(
        current: BoardMetricSnapshot | None,
        previous: BoardMetricSnapshot | None,
        attr: str,
    ) -> int:
        if current is None or previous is None:
            return 0
        current_value = getattr(current, attr) or 0
        previous_value = getattr(previous, attr) or 0
        return max(int(current_value) - int(previous_value), 0)
