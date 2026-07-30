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


def test_body_quality_rejects_empty_title_duplicates_and_media_references():
    from crawl_scheduler.crawled_content import has_sufficient_body

    title = "오늘 올라온 인기 게시물 제목"

    assert not has_sufficient_body(title, [])
    assert not has_sufficient_body(
        title,
        [
            {"type": "text", "text": title},
            {"type": "metadata", "image_url": "https://example.com/preview.jpg"},
            {"type": "video", "media_path": "Dcinside/humor/123/clip.mp4"},
            {
                "type": "image",
                "source_url": "https://example.com/source.jpg",
            },
        ],
    )
    assert not has_sufficient_body(
        title,
        [{"type": "text", "text": "Dcinside/humor/123/image.webp"}],
    )
    assert not has_sufficient_body(
        title,
        [{"type": "text", "text": "2026073012345678901234567890"}],
    )


def test_body_quality_accepts_meaningful_text_ocr_and_alt_text():
    from crawl_scheduler.crawled_content import has_sufficient_body

    title = "짧은 제목"

    assert has_sufficient_body(
        title,
        [{"type": "text", "text": "본문에는 사건의 배경과 이후 반응이 자세하게 설명되어 있습니다."}],
    )
    assert has_sufficient_body(
        title,
        [
            {
                "type": "image",
                "source_url": "https://example.com/source.jpg",
                "text": "안내문에는 오늘 오후부터 출입이 제한된다고 적혀 있습니다.",
            }
        ],
    )
    assert has_sufficient_body(
        title,
        [
            {
                "type": "image",
                "source_url": "https://example.com/source.jpg",
                "alt_text": "고양이가 작은 상자 안에서 편안하게 잠든 모습입니다.",
            }
        ],
    )


def test_body_quality_counts_local_image_as_recoverable_without_using_its_path_as_text(
    tmp_path,
):
    from crawl_scheduler.crawled_content import has_sufficient_body

    assert has_sufficient_body(
        "이미지 게시물",
        [{"type": "image", "media_path": "Dcinside/humor/123/image.webp"}],
    )
    assert not has_sufficient_body(
        "이미지 게시물",
        [{"type": "image", "media_path": "Dcinside/humor/123/loading.gif"}],
    )
    assert not has_sufficient_body(
        "이미지 게시물",
        [{"type": "image", "media_path": "https://example.com/image.webp"}],
    )
    assert not has_sufficient_body(
        "이미지 게시물",
        [{"type": "image", "media_path": "Dcinside/humor/123/missing.webp"}],
        media_root=tmp_path,
    )

    image_path = tmp_path / "Dcinside" / "humor" / "123" / "image.webp"
    image_path.parent.mkdir(parents=True)
    image_path.write_bytes(b"image")
    assert has_sufficient_body(
        "이미지 게시물",
        [{"type": "image", "media_path": "Dcinside/humor/123/image.webp"}],
        media_root=tmp_path,
    )


def test_body_quality_uses_unicode_text_and_does_not_strip_short_title_everywhere():
    from crawl_scheduler.crawled_content import has_sufficient_body

    assert has_sufficient_body(
        "이",
        [{"type": "text", "text": "日本語の本文には出来事の背景と反応が詳しく書かれています"}],
    )
    assert has_sufficient_body(
        "a",
        [{"type": "text", "text": "a detailed account explains what happened and why it matters"}],
    )


def test_body_quality_limits_use_shared_bounded_environment_values(monkeypatch):
    from crawl_scheduler.crawled_content import (
        analysis_min_body_characters,
        analysis_min_language_characters,
    )

    monkeypatch.setenv("AI_ANALYSIS_MIN_BODY_CHARS", "999999")
    monkeypatch.setenv("AI_ANALYSIS_MIN_LANGUAGE_CHARS", "0")

    assert analysis_min_body_characters() == 10_000
    assert analysis_min_language_characters() == 1


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
