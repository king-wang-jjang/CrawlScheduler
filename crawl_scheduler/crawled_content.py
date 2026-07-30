from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


MEDIA_TYPES = {"image", "video"}
METADATA_TYPE = "metadata"
MIN_BODY_TEXT_CHARACTERS = 20
MIN_BODY_LANGUAGE_CHARACTERS = 4
MAX_BODY_TEXT_CHARACTERS = 10_000
MAX_BODY_LANGUAGE_CHARACTERS = 1_000
MIN_TITLE_SIGNAL_FOR_PARTIAL_REMOVAL = 4
RECOVERABLE_IMAGE_EXTENSIONS = {".gif", ".jpeg", ".jpg", ".png", ".webp"}
UNUSABLE_THUMBNAIL_NAMES = {
    "blank.gif",
    "cdn_img_404.jpg",
    "gallview_loading_ori.gif",
    "icon_app_20160427.png",
    "loading.gif",
}
VIDEO_THUMBNAIL_EXTENSIONS = {".m3u8", ".m4v", ".mov", ".mp4", ".webm"}
MEDIA_REFERENCE_EXTENSIONS = RECOVERABLE_IMAGE_EXTENSIONS | VIDEO_THUMBNAIL_EXTENSIONS


def text_block(text: object) -> dict[str, str] | None:
    text_value = _clean_text(text)
    if text_value is None:
        return None
    return {"type": "text", "text": text_value}


def image_block(
    *,
    media_path: object = None,
    source_url: object = None,
    text: object = None,
    alt_text: object = None,
) -> dict[str, str] | None:
    return _media_block(
        "image",
        media_path=media_path,
        source_url=source_url,
        text=text,
        alt_text=alt_text,
    )


def video_block(
    *,
    media_path: object = None,
    source_url: object = None,
    text: object = None,
    alt_text: object = None,
) -> dict[str, str] | None:
    return _media_block(
        "video",
        media_path=media_path,
        source_url=source_url,
        text=text,
        alt_text=alt_text,
    )


def metadata_image_block(image_url: object = None) -> dict[str, str] | None:
    image_url_value = _clean_text(image_url)
    if not is_usable_thumbnail_url(image_url_value):
        return None
    return {"type": METADATA_TYPE, "image_url": image_url_value}


def is_usable_thumbnail_url(image_url: object = None) -> bool:
    image_url_value = _clean_text(image_url)
    if image_url_value is None:
        return False

    path = urlparse(image_url_value).path.lower()
    basename = path.rstrip("/").rsplit("/", 1)[-1]
    if basename in UNUSABLE_THUMBNAIL_NAMES:
        return False

    return not any(path.endswith(extension) for extension in VIDEO_THUMBNAIL_EXTENSIONS)


def normalize_contents(contents: object) -> list[dict[str, str]]:
    if contents is None:
        return []

    if isinstance(contents, list):
        normalized = []
        for item in contents:
            block = normalize_content_block(item)
            if block:
                normalized.append(block)
        return normalized

    if isinstance(contents, dict) and "type" not in contents:
        normalized = []
        for value in contents.values():
            block = normalize_content_block(value)
            if block:
                normalized.append(block)
        return normalized

    block = normalize_content_block(contents)
    return [block] if block else []


def normalize_content_block(value: object) -> dict[str, str] | None:
    if value is None:
        return None

    if isinstance(value, str):
        return text_block(value)

    if not isinstance(value, dict):
        return text_block(value)

    block_type = _clean_text(value.get("type")) or "text"
    block_type = block_type.lower()

    if block_type == "text":
        return text_block(_first_text(value, "text", "content", "alt_text", "alt"))

    if block_type in MEDIA_TYPES:
        return _media_block(
            block_type,
            media_path=value.get("media_path") or value.get("path"),
            source_url=value.get("source_url") or value.get("url"),
            text=_first_text(value, "text", "content"),
            alt_text=value.get("alt_text") or value.get("alt"),
        )

    if block_type == METADATA_TYPE:
        return metadata_image_block(
            value.get("image_url") or value.get("thumbnail") or value.get("url")
        )

    return text_block(_first_text(value, "text", "content", "alt_text", "alt"))


def extract_llm_text(title: object, contents: object) -> str:
    parts = []
    title_text = _clean_text(title)
    if title_text:
        parts.append(title_text)

    for block in normalize_contents(contents):
        block_type = block.get("type", "text")
        if block_type == METADATA_TYPE:
            continue
        block_text = _clean_text(block.get("text")) or _clean_text(block.get("alt_text"))
        if not block_text:
            continue
        if block_type in MEDIA_TYPES:
            parts.append(f"[{block_type}] {block_text}")
        else:
            parts.append(block_text)

    return "\n".join(parts)


def has_sufficient_body(
    title: object,
    contents: object,
    *,
    minimum_text_characters: int | None = None,
    minimum_language_characters: int | None = None,
    media_root: object = None,
) -> bool:
    """Return whether crawled contents can support analysis beyond the title.

    A locally persisted image is considered recoverable because the analysis
    service can enrich it with vision later.  Its path is deliberately not
    counted as natural-language text.  Otherwise, only body text and image
    OCR/alt text count, after title duplicates and resource references are
    removed.
    """
    normalized = normalize_contents(contents)
    if any(
        _has_recoverable_image(block, media_root=media_root)
        for block in normalized
    ):
        return True

    title_signal = _text_signal(title)
    seen_signals = set()
    signal_character_count = 0
    language_character_count = 0
    required_characters = (
        analysis_min_body_characters()
        if minimum_text_characters is None
        else max(int(minimum_text_characters), 1)
    )
    required_language_characters = (
        analysis_min_language_characters()
        if minimum_language_characters is None
        else max(int(minimum_language_characters), 1)
    )
    required_language_characters = min(
        required_language_characters,
        required_characters,
    )

    for block in normalized:
        block_type = block.get("type", "text")
        if block_type == "text":
            candidates = (block.get("text"),)
        elif block_type == "image":
            candidates = (block.get("text"), block.get("alt_text"))
        else:
            continue

        for candidate in candidates:
            candidate_text = _clean_text(candidate)
            if candidate_text is None or _looks_like_media_reference(candidate_text):
                continue

            signal = _without_title_signal(
                _text_signal(candidate_text),
                title_signal,
            )
            if not signal or signal in seen_signals:
                continue

            seen_signals.add(signal)
            signal_character_count += len(signal)
            language_character_count += sum(
                character.isalpha() for character in signal
            )
            if (
                signal_character_count >= required_characters
                and language_character_count >= required_language_characters
            ):
                return True

    return False


def has_sufficient_text_signal(value: object) -> bool:
    """Return whether text alone is descriptive enough to skip local OCR."""
    text = _clean_text(value)
    if text is None or _looks_like_media_reference(text):
        return False
    signal = _text_signal(text)
    return (
        len(signal) >= analysis_min_body_characters()
        and sum(character.isalpha() for character in signal)
        >= analysis_min_language_characters()
    )


def analysis_min_body_characters() -> int:
    return _bounded_env_int(
        "AI_ANALYSIS_MIN_BODY_CHARS",
        default=MIN_BODY_TEXT_CHARACTERS,
        minimum=1,
        maximum=MAX_BODY_TEXT_CHARACTERS,
    )


def analysis_min_language_characters() -> int:
    return _bounded_env_int(
        "AI_ANALYSIS_MIN_LANGUAGE_CHARS",
        default=MIN_BODY_LANGUAGE_CHARACTERS,
        minimum=1,
        maximum=MAX_BODY_LANGUAGE_CHARACTERS,
    )


def first_thumbnail_path(contents: object) -> str | None:
    metadata_fallback = None
    for block in normalize_contents(contents):
        if block.get("type") == "image":
            thumbnail = block.get("media_path") or block.get("path")
            if thumbnail:
                return thumbnail
        if block.get("type") == METADATA_TYPE and metadata_fallback is None:
            metadata_fallback = block.get("image_url")
    return metadata_fallback


def _has_recoverable_image(
    block: dict[str, str],
    *,
    media_root: object = None,
) -> bool:
    if block.get("type") != "image":
        return False

    media_path = _clean_text(block.get("media_path"))
    if media_path is None or not is_usable_thumbnail_url(media_path):
        return False

    parsed = urlparse(media_path)
    if parsed.scheme or parsed.netloc:
        return False

    normalized_path = parsed.path.replace("\\", "/").rstrip("/")
    extension = (
        f".{normalized_path.rsplit('.', 1)[-1].lower()}"
        if "." in normalized_path.rsplit("/", 1)[-1]
        else ""
    )
    if extension not in RECOVERABLE_IMAGE_EXTENSIONS:
        return False
    if media_root is None:
        return True

    root = Path(str(media_root)).resolve()
    candidate = (root / normalized_path).resolve()
    if candidate != root and root not in candidate.parents:
        return False
    return candidate.is_file() and os.access(candidate, os.R_OK)


def _looks_like_media_reference(value: str) -> bool:
    parsed = urlparse(value)
    if parsed.scheme or parsed.netloc:
        return True

    normalized_path = parsed.path.replace("\\", "/").lower()
    return any(normalized_path.endswith(extension) for extension in MEDIA_REFERENCE_EXTENSIONS)


def _text_signal(value: object) -> str:
    text = _clean_text(value)
    if text is None:
        return ""
    return "".join(character for character in text if character.isalnum()).casefold()


def _without_title_signal(signal: str, title_signal: str) -> str:
    if not title_signal:
        return signal
    if signal == title_signal:
        return ""
    if len(title_signal) < MIN_TITLE_SIGNAL_FOR_PARTIAL_REMOVAL:
        return signal
    if signal.startswith(title_signal):
        return signal[len(title_signal):]
    if signal.endswith(title_signal):
        return signal[:-len(title_signal)]
    return signal


def _media_block(
    block_type: str,
    *,
    media_path: object = None,
    source_url: object = None,
    text: object = None,
    alt_text: object = None,
) -> dict[str, str] | None:
    block: dict[str, str] = {"type": block_type}
    _put_clean(block, "media_path", media_path)
    _put_clean(block, "source_url", source_url)
    _put_clean(block, "text", text)
    _put_clean(block, "alt_text", alt_text)

    return block if len(block) > 1 else None


def _put_clean(target: dict[str, str], key: str, value: object) -> None:
    clean_value = _clean_text(value)
    if clean_value is not None:
        target[key] = clean_value


def _first_text(source: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = _clean_text(source.get(key))
        if value is not None:
            return value
    return None


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _bounded_env_int(
    name: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw_value = os.getenv(name)
    try:
        value = int(raw_value) if raw_value is not None else default
    except ValueError:
        value = default
    return min(max(value, minimum), maximum)
