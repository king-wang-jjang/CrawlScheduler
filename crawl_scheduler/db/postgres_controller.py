from dataclasses import dataclass
from datetime import datetime, timezone
import json
from uuid import uuid4

from sqlalchemy import desc, select

from crawl_scheduler.constants import DEFAULT_GPT_ANSWER, DEFAULT_TAG
from crawl_scheduler.db.models import Board, CrawlerLog
from crawl_scheduler.db.postgres import Base, get_engine, get_session_factory


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
    def __init__(self, database_url: str | None = None):
        self.database_url = database_url
        Base.metadata.create_all(bind=get_engine(database_url))

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
        values = self._board_values(collection_name, document)
        with get_session_factory(self.database_url)() as session:
            board = session.scalar(select(Board).where(Board.source_id == values["source_id"]))
            if board is None:
                board = Board(**values)
                session.add(board)
            else:
                for key, value in values.items():
                    if key in {"id", "comment_count", "like_count"}:
                        continue
                    setattr(board, key, value)

            session.commit()
            session.refresh(board)
            return board

    def _board_values(self, collection_name: str, document: dict) -> dict:
        site = str(document.get("site") or "unknown")
        category, no = self._category_and_no(collection_name, document)
        contents = document.get("contents")
        return {
            "source_id": self._source_id(site, category, no, document),
            "category": category,
            "no": no,
            "site": site,
            "title": str(document.get("title") or ""),
            "url": str(document.get("url") or ""),
            "contents": self._json_safe(contents),
            "gpt_answer": self._coerce_text(
                document.get("gpt_answer") or document.get("GPTAnswer") or DEFAULT_GPT_ANSWER
            ),
            "thumbnail": document.get("thumbnail") or self._extract_thumbnail(contents),
            "comment_count": int(document.get("comment_count") or 0),
            "like_count": int(document.get("like_count") or 0),
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
        ordering = desc(Board.like_count) if daily else desc(Board.created_at)
        with get_session_factory(self.database_url)() as session:
            boards = session.scalars(
                select(Board).order_by(ordering, desc(Board.created_at)).offset(offset).limit(limit)
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
            "create_time": self._coerce_datetime(board.created_at),
            "thumbnail": board.thumbnail,
            "comment_count": int(board.comment_count or 0),
            "like_count": int(board.like_count or 0),
        }

    @staticmethod
    def _board_attr_name(key: str) -> str:
        return "created_at" if key == "create_time" else key

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
        if isinstance(contents, list):
            for item in contents:
                if isinstance(item, dict) and item.get("type") == "image":
                    return item.get("path")
        return None

    @staticmethod
    def _json_safe(value: object):
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))
