from pathlib import Path


SERVICE_ROOT = Path(__file__).resolve().parents[1]


def test_runtime_crawler_code_does_not_import_mongo_controller():
    runtime_paths = [
        *sorted((SERVICE_ROOT / "crawl_scheduler" / "community_website").glob("*.py")),
        SERVICE_ROOT / "crawl_scheduler" / "utils" / "loghandler.py",
    ]

    for path in runtime_paths:
        content = path.read_text(encoding="utf-8")
        assert "mongo_controller" not in content, path
        assert "MongoController" not in content, path


def test_crawler_dependencies_do_not_include_pymongo():
    pyproject = (SERVICE_ROOT / "pyproject.toml").read_text(encoding="utf-8")

    assert "pymongo" not in pyproject.lower()
