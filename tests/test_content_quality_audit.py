import json
from datetime import datetime, timezone
from pathlib import Path
import sys

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))

from crawl_scheduler.db.models import Board
from crawl_scheduler.db.postgres import Base
from scripts import audit_content_quality


GENERATED_AT = datetime(2026, 7, 30, 3, 4, 5, tzinfo=timezone.utc)


def test_summary_reports_quality_and_analysis_status_per_site(tmp_path):
    image_path = tmp_path / "Dcinside" / "dcbest" / "123" / "image.webp"
    image_path.parent.mkdir(parents=True)
    image_path.write_bytes(b"image")
    rows = [
        (
            "dcinside",
            "충분한 글",
            [
                {
                    "type": "text",
                    "text": "사건의 배경과 진행 상황을 충분히 설명하는 게시물 본문입니다.",
                }
            ],
            "done",
        ),
        (
            "dcinside",
            "이미지 글",
            [
                {
                    "type": "image",
                    "media_path": "Dcinside/dcbest/123/image.webp",
                }
            ],
            "pending",
        ),
        (
            "ppomppu",
            "제목만 있는 글",
            [],
            "failed",
        ),
        (
            "PPOMPPU",
            "원격 이미지만 있는 글",
            [{"type": "image", "source_url": "https://example.com/image.jpg"}],
            None,
        ),
    ]

    report = audit_content_quality.summarize_rows(
        rows,
        generated_at=GENERATED_AT,
        media_root=tmp_path,
    )

    assert report["generated_at"] == "2026-07-30T03:04:05Z"
    assert report["totals"] == {
        "total": 4,
        "sufficient_body": 2,
        "needs_refresh": 2,
        "image_recoverable": 1,
        "title_only_or_insufficient": 2,
        "analysis_status": {
            "pending": 1,
            "processing": 0,
            "done": 1,
            "failed": 1,
            "unknown": 1,
        },
    }
    assert report["sites"]["dcinside"]["sufficient_body"] == 2
    assert report["sites"]["dcinside"]["image_recoverable"] == 1
    assert report["sites"]["ppomppu"]["total"] == 2
    assert report["sites"]["ppomppu"]["needs_refresh"] == 2


def test_audit_database_only_executes_selects(tmp_path):
    database_path = tmp_path / "boards.db"
    engine = create_engine(f"sqlite:///{database_path}")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(
            Board(
                source_id="dcinside:dcbest:1",
                category="dcbest",
                no=1,
                site="dcinside",
                title="본문 있는 글",
                url="https://example.com/1",
                contents=[
                    {
                        "type": "text",
                        "text": "요약에 사용할 수 있도록 충분히 긴 게시물 본문입니다.",
                    }
                ],
                analysis_status="done",
            )
        )
        session.commit()

    statements = []

    def record_statement(
        _connection,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ):
        statements.append(statement.strip())

    event.listen(engine, "before_cursor_execute", record_statement)
    try:
        report = audit_content_quality.audit_database(
            engine,
            generated_at=GENERATED_AT,
        )
    finally:
        event.remove(engine, "before_cursor_execute", record_statement)
        engine.dispose()

    assert report["totals"]["total"] == 1
    assert statements
    assert all(statement.upper().startswith("SELECT") for statement in statements)


def test_main_emits_machine_readable_json(monkeypatch, capsys):
    class FakeEngine:
        pass

    expected = audit_content_quality.summarize_rows([], generated_at=GENERATED_AT)
    monkeypatch.setenv("DATABASE_URL", "sqlite:///unused.db")
    monkeypatch.setattr(audit_content_quality, "get_engine", FakeEngine)
    monkeypatch.setattr(
        audit_content_quality,
        "audit_database",
        lambda _engine, **_kwargs: expected,
    )

    assert audit_content_quality.main([]) == 0
    assert json.loads(capsys.readouterr().out) == expected


def test_audit_script_does_not_use_schema_mutating_controller():
    source = (
        SERVICE_ROOT / "scripts" / "audit_content_quality.py"
    ).read_text(encoding="utf-8")

    assert "from crawl_scheduler.db.postgres_controller" not in source
    assert "PostgresController(" not in source
