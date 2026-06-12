import sys
from pathlib import Path


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))


def test_normalize_contents_converts_legacy_blocks_to_contract_shape():
    from crawl_scheduler.crawled_content import normalize_contents

    contents = [
        {"type": "text", "content": "  real body  "},
        {
            "type": "image",
            "path": "Dcinside/humor/123/image.webp",
            "url": "https://example.com/image.webp",
            "content": "ocr text",
            "alt": "image alt",
        },
        {"type": "video", "path": "Dcinside/humor/123/video.mp4"},
        "",
        None,
    ]

    assert normalize_contents(contents) == [
        {"type": "text", "text": "real body"},
        {
            "type": "image",
            "media_path": "Dcinside/humor/123/image.webp",
            "source_url": "https://example.com/image.webp",
            "text": "ocr text",
            "alt_text": "image alt",
        },
        {"type": "video", "media_path": "Dcinside/humor/123/video.mp4"},
    ]


def test_extract_llm_text_uses_ordered_text_and_ocr_without_media_paths():
    from crawl_scheduler.crawled_content import extract_llm_text

    text = extract_llm_text(
        "seed title",
        [
            {"type": "image", "media_path": "Dcinside/humor/123/image.webp", "text": "ocr text"},
            {"type": "text", "text": "real body"},
            {"type": "video", "media_path": "Dcinside/humor/123/video.mp4"},
        ],
    )

    assert text == "seed title\n[image] ocr text\nreal body"
    assert "Dcinside/humor/123" not in text
