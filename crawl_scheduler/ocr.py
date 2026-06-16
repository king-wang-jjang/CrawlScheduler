from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

from crawl_scheduler.utils.loghandler import logger


PADDLE_OCR_KWARGS = {
    "lang": "korean",
    "use_doc_orientation_classify": False,
    "use_doc_unwarping": False,
    "use_textline_orientation": False,
}
DEFAULT_MIN_SCORE = 0.35


class PaddleOcrExtractor:
    def __init__(
        self,
        *,
        factory: Callable[..., Any] | None = None,
        min_score: float = DEFAULT_MIN_SCORE,
    ) -> None:
        self.factory = factory or self._create_paddle_ocr
        self.min_score = min_score
        self._ocr: Any | None = None

    def extract(self, image_path: str) -> str | None:
        if not image_path or not os.path.exists(image_path):
            logger.debug("OCR skipped because image path does not exist: %s", image_path)
            return None

        try:
            ocr = self._get_ocr()
            result = self._predict(ocr, image_path)
            text = "\n".join(self._extract_texts(result))
            return text or None
        except Exception as exc:
            logger.exception("OCR failed for %s: %s", image_path, exc)
            return None

    def _get_ocr(self) -> Any:
        if self._ocr is None:
            self._ocr = self.factory(**PADDLE_OCR_KWARGS)
        return self._ocr

    @staticmethod
    def _create_paddle_ocr(**kwargs: Any) -> Any:
        from paddleocr import PaddleOCR

        try:
            return PaddleOCR(**kwargs)
        except TypeError:
            return PaddleOCR(
                lang=kwargs["lang"],
                use_angle_cls=True,
                show_log=False,
            )

    @staticmethod
    def _predict(ocr: Any, image_path: str) -> Any:
        if hasattr(ocr, "predict"):
            return ocr.predict(image_path)
        return ocr.ocr(image_path, cls=True)

    def _extract_texts(self, result: Any) -> list[str]:
        texts: list[str] = []
        self._collect_texts(result, texts)
        return texts

    def _collect_texts(self, value: Any, texts: list[str]) -> None:
        legacy_text = self._legacy_text_result(value)
        if legacy_text is not None:
            texts.append(legacy_text)
            return

        if hasattr(value, "res"):
            self._collect_texts(value.res, texts)
            return

        if isinstance(value, dict):
            if "rec_texts" in value:
                self._collect_rec_texts(value, texts)
                return
            if "res" in value:
                self._collect_texts(value["res"], texts)
                return
            for item in value.values():
                self._collect_texts(item, texts)
            return

        if isinstance(value, (list, tuple)):
            for item in value:
                self._collect_texts(item, texts)

    def _collect_rec_texts(self, value: dict[str, Any], texts: list[str]) -> None:
        rec_texts = value.get("rec_texts") or []
        rec_scores = value.get("rec_scores") or []

        for index, text in enumerate(rec_texts):
            score = rec_scores[index] if index < len(rec_scores) else None
            clean_text = self._clean_text(text)
            if clean_text and self._score_is_usable(score):
                texts.append(clean_text)

    def _legacy_text_result(self, value: Any) -> str | None:
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            return None

        candidate = value[1]
        if not isinstance(candidate, (list, tuple)) or len(candidate) < 2:
            return None

        text, score = candidate[0], candidate[1]
        clean_text = self._clean_text(text)
        if clean_text and self._score_is_usable(score):
            return clean_text
        return None

    def _score_is_usable(self, score: Any) -> bool:
        if score is None:
            return True
        try:
            return float(score) >= self.min_score
        except (TypeError, ValueError):
            return True

    @staticmethod
    def _clean_text(text: Any) -> str | None:
        if text is None:
            return None
        clean = str(text).strip()
        return clean or None


_DEFAULT_EXTRACTOR: PaddleOcrExtractor | None = None


def extract_text_from_image(image_path: str) -> str | None:
    global _DEFAULT_EXTRACTOR

    if _DEFAULT_EXTRACTOR is None:
        _DEFAULT_EXTRACTOR = PaddleOcrExtractor(min_score=_ocr_min_score())
    return _DEFAULT_EXTRACTOR.extract(image_path)


def _ocr_min_score() -> float:
    try:
        return float(os.getenv("OCR_MIN_SCORE", DEFAULT_MIN_SCORE))
    except (TypeError, ValueError):
        return DEFAULT_MIN_SCORE
