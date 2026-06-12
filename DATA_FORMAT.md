# Crawl Data Format

Crawler output is normalized before it is stored in `boards.contents`.

## Board Identity

Each board row is identified by:

```text
source_id = "{site}:{category}:{no}"
```

If a crawler only has a legacy `board_id`, the storage layer still accepts it and derives a stable `source_id`.

## Content Blocks

`contents` is a list of ordered content blocks. The canonical shape is:

```json
[
  { "type": "text", "text": "본문 문장" },
  {
    "type": "image",
    "media_path": "Dcinside/humor/123/image.webp",
    "source_url": "https://example.com/image.webp",
    "text": "OCR 또는 이미지 안의 읽을 수 있는 텍스트",
    "alt_text": "이미지 대체 텍스트"
  },
  {
    "type": "video",
    "media_path": "Dcinside/humor/123/video.mp4",
    "source_url": "https://example.com/video.mp4"
  }
]
```

Rules:

- `type` is required and is normally `text`, `image`, or `video`.
- `text` is the only field used as primary natural-language input.
- `media_path` is the local relative media path under `ROOT`.
- `source_url` is optional provenance for the original remote media URL.
- `alt_text` is optional fallback text for media blocks.
- Empty text blocks and empty media blocks are removed.
- Legacy blocks using `content`, `path`, `url`, or `alt` are accepted and normalized at insert time.

## LLM Input

LLM analysis receives a plain text projection of the ordered content:

```text
게시글 제목
본문 문장
[image] OCR 또는 이미지 안의 읽을 수 있는 텍스트
```

Media paths and raw JSON dictionaries are never included in the LLM prompt text.
