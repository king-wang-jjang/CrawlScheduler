from __future__ import annotations

from dataclasses import dataclass
import os
import re
from typing import Callable, Literal, Mapping

from bs4 import Comment, NavigableString, Tag

from crawl_scheduler.config import Config
from crawl_scheduler.crawled_content import (
    has_sufficient_text_signal,
    image_block,
    text_block,
    video_block,
)
from crawl_scheduler.utils.loghandler import logger


ContentKind = Literal["text", "image", "video"]
BLOCK_TAG_NAMES = {
    "address",
    "article",
    "aside",
    "blockquote",
    "dd",
    "details",
    "div",
    "dl",
    "dt",
    "figcaption",
    "figure",
    "footer",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "li",
    "main",
    "ol",
    "p",
    "pre",
    "section",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "tr",
    "ul",
}
IGNORED_TAG_NAMES = {"script", "style", "noscript", "template"}


@dataclass(frozen=True)
class ArticleContentPart:
    kind: ContentKind
    text: str | None = None
    tag: Tag | None = None


def ordered_article_content_parts(body: Tag | None) -> list[ArticleContentPart]:
    """Return visible text and media once each, in their DOM order."""
    if body is None:
        return []

    parts: list[ArticleContentPart] = []
    text_fragments: list[str] = []

    def flush_text() -> None:
        text = "".join(text_fragments)
        text_fragments.clear()
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{2,}", "\n", text)
        text = re.sub(r" {2,}", " ", text).strip()
        text = re.sub(r" +([,.;:!?%)}\]…。、！？])", r"\1", text)
        text = re.sub(r"([({\[]) +", r"\1", text)
        if text:
            parts.append(ArticleContentPart(kind="text", text=text))

    def add_boundary() -> None:
        if text_fragments and text_fragments[-1] != "\n":
            text_fragments.append("\n")

    def add_inline_space() -> None:
        if text_fragments and not text_fragments[-1].endswith((" ", "\n")):
            text_fragments.append(" ")

    def visit(node: object) -> None:
        if isinstance(node, Comment):
            return
        if isinstance(node, NavigableString):
            text_fragments.append(re.sub(r"\s+", " ", str(node)))
            return
        if not isinstance(node, Tag):
            return

        tag_name = node.name.lower()
        if tag_name in IGNORED_TAG_NAMES:
            return
        if tag_name == "img":
            flush_text()
            parts.append(ArticleContentPart(kind="image", tag=node))
            return
        if tag_name in {"iframe", "video"}:
            flush_text()
            parts.append(ArticleContentPart(kind="video", tag=node))
            return
        if tag_name == "br":
            add_boundary()
            return

        is_block = tag_name in BLOCK_TAG_NAMES
        if is_block:
            add_boundary()
        else:
            add_inline_space()
        for child in node.children:
            visit(child)
        if is_block:
            add_boundary()
        else:
            add_inline_space()

    visit(body)
    flush_text()
    return parts


def build_ordered_content_blocks(
    crawler: object,
    body: Tag | None,
    *,
    base_url: str,
    category: object = None,
    no: object = None,
    created_at: object = None,
    headers: Mapping[str, str] | None = None,
    proxies: Mapping[str, str] | None = None,
    save_file: Callable[..., object] | None = None,
    save_videos: bool = True,
) -> list[dict[str, str]]:
    """Materialize ordered article parts into the crawler content contract."""
    blocks: list[dict[str, str]] = []
    seen_media_urls: set[str] = set()
    save_media = save_file or getattr(crawler, "save_file")

    for part in ordered_article_content_parts(body):
        if part.kind == "text":
            block = text_block(part.text)
            if block:
                _append_text_block(blocks, block)
            continue

        media_tag = _source_tag(crawler, part, base_url=base_url)
        media_url = (
            getattr(crawler, "media_url_from_tag")(media_tag, base_url=base_url)
            if media_tag is not None
            else None
        )
        if not media_url or media_url in seen_media_urls:
            continue
        seen_media_urls.add(media_url)

        file_path = None
        is_remote_embed = part.tag is not None and part.tag.name.lower() == "iframe"
        should_save_media = (
            not is_remote_embed
            and (part.kind != "video" or save_videos)
        )
        if should_save_media:
            try:
                saved_path = save_media(
                    media_url,
                    category=category,
                    no=no,
                    headers=dict(headers) if headers else None,
                    created_at=created_at,
                    proxies=dict(proxies) if proxies else None,
                )
                if saved_path:
                    file_path = str(saved_path)
            except Exception as exc:
                logger.warning("Could not save article media %s: %s", media_url, exc)

        if part.kind == "video":
            block = video_block(media_path=file_path, source_url=media_url)
        else:
            alt_text = _media_description(part.tag)
            ocr_text = None
            if file_path and not has_sufficient_text_signal(alt_text):
                try:
                    media_root = Config().get_env("ROOT") or "./media"
                    ocr_text = getattr(crawler, "img_to_text")(
                        os.path.join(media_root, file_path)
                    )
                except Exception as exc:
                    logger.warning("OCR skipped for %s: %s", media_url, exc)
            block = image_block(
                media_path=file_path,
                source_url=media_url,
                text=ocr_text,
                alt_text=alt_text,
            )

        if block:
            blocks.append(block)

    return blocks


def _source_tag(
    crawler: object, part: ArticleContentPart, *, base_url: str
) -> Tag | None:
    if part.tag is None or part.kind != "video":
        return part.tag

    media_url_from_tag = getattr(crawler, "media_url_from_tag")
    if media_url_from_tag(part.tag, base_url=base_url):
        return part.tag
    return next(
        (
            source
            for source in part.tag.find_all("source")
            if media_url_from_tag(source, base_url=base_url)
        ),
        None,
    )


def _media_description(tag: Tag | None) -> str | None:
    if tag is None:
        return None
    for attribute in ("alt", "title", "aria-label"):
        value = tag.get(attribute)
        if value and str(value).strip():
            return str(value).strip()
    return None


def _append_text_block(
    blocks: list[dict[str, str]], block: dict[str, str]
) -> None:
    if blocks and blocks[-1].get("type") == "text":
        blocks[-1]["text"] = f"{blocks[-1]['text']}\n{block['text']}"
        return
    blocks.append(block)
