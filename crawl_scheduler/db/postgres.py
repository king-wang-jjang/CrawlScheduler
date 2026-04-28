import os
from collections.abc import Generator
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


load_dotenv(Path(__file__).resolve().parents[2] / ".env")


def _database_url(database_url: str | None = None) -> str:
    value = database_url or os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError("DATABASE_URL is required")
    return value


class Base(DeclarativeBase):
    pass


@lru_cache(maxsize=None)
def get_engine(database_url: str | None = None) -> Engine:
    url = _database_url(database_url)
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    return create_engine(url, pool_pre_ping=True, connect_args=connect_args)


@lru_cache(maxsize=None)
def get_session_factory(database_url: str | None = None) -> sessionmaker[Session]:
    return sessionmaker(bind=get_engine(database_url), autoflush=False, autocommit=False)


def get_session(database_url: str | None = None) -> Generator[Session, None, None]:
    session = get_session_factory(database_url)()
    try:
        yield session
    finally:
        session.close()
