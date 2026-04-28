from pathlib import Path


SERVICE_ROOT = Path(__file__).resolve().parents[1]


def test_runtime_crawler_code_does_not_import_legacy_document_controller():
    runtime_paths = [
        *sorted((SERVICE_ROOT / "crawl_scheduler" / "community_website").glob("*.py")),
        SERVICE_ROOT / "crawl_scheduler" / "utils" / "loghandler.py",
    ]

    for path in runtime_paths:
        content = path.read_text(encoding="utf-8")
        assert "document_controller" not in content, path


def test_crawler_dependencies_stay_postgres_only():
    pyproject = (SERVICE_ROOT / "pyproject.toml").read_text(encoding="utf-8")

    assert "psycopg" in pyproject.lower()
