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


def test_metadata_image_block_can_supply_thumbnail_fallback():
    from crawl_scheduler.crawled_content import first_thumbnail_path, normalize_contents

    contents = [
        {"type": "text", "text": "body only"},
        {"type": "metadata", "image_url": "https://example.com/og.jpg"},
    ]

    assert normalize_contents(contents) == [
        {"type": "text", "text": "body only"},
        {"type": "metadata", "image_url": "https://example.com/og.jpg"},
    ]
    assert first_thumbnail_path(contents) == "https://example.com/og.jpg"


def test_local_image_thumbnail_takes_precedence_over_metadata_image():
    from crawl_scheduler.crawled_content import first_thumbnail_path

    contents = [
        {"type": "metadata", "image_url": "https://example.com/og.jpg"},
        {"type": "image", "media_path": "Dcinside/humor/123/image.webp"},
    ]

    assert first_thumbnail_path(contents) == "Dcinside/humor/123/image.webp"


def test_invalid_metadata_images_cannot_supply_thumbnail_fallback():
    from crawl_scheduler.crawled_content import metadata_image_block, first_thumbnail_path

    invalid_urls = [
        "https://cdn.example.com/post.mp4?autoplay=1",
        "https://cdn2.ppomppu.co.kr/images/icon_app_20160427.png",
        "https://cdn3.ppomppu.co.kr/cdn_img_404.jpg",
    ]

    for image_url in invalid_urls:
        assert metadata_image_block(image_url) is None
        assert first_thumbnail_path(
            [{"type": "metadata", "image_url": image_url}]
        ) is None
