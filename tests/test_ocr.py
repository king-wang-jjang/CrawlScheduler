import sys
from pathlib import Path


SERVICE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_ROOT))


class FakePaddleResult:
    def __init__(self, texts, scores):
        self.res = {"rec_texts": texts, "rec_scores": scores}


class FakePaddleOcr:
    def __init__(self, result):
        self.result = result
        self.predicted_paths = []

    def predict(self, image_path):
        self.predicted_paths.append(image_path)
        return self.result


def test_paddle_ocr_extractor_uses_korean_predict_api_and_filters_low_confidence(tmp_path):
    from crawl_scheduler.ocr import PaddleOcrExtractor

    image_path = tmp_path / "sample.webp"
    image_path.write_bytes(b"fake image")
    fake_ocr = FakePaddleOcr(
        [
            FakePaddleResult(
                texts=["첫 줄", "불확실한 줄", "둘째 줄"],
                scores=[0.98, 0.2, 0.81],
            )
        ]
    )
    created_kwargs = []

    def fake_factory(**kwargs):
        created_kwargs.append(kwargs)
        return fake_ocr

    extractor = PaddleOcrExtractor(factory=fake_factory, min_score=0.5)

    assert extractor.extract(str(image_path)) == "첫 줄\n둘째 줄"
    assert fake_ocr.predicted_paths == [str(image_path)]
    assert created_kwargs == [
        {
            "lang": "korean",
            "use_doc_orientation_classify": False,
            "use_doc_unwarping": False,
            "use_textline_orientation": False,
        }
    ]


def test_paddle_ocr_extractor_supports_legacy_ocr_result_shape(tmp_path):
    from crawl_scheduler.ocr import PaddleOcrExtractor

    image_path = tmp_path / "sample.jpg"
    image_path.write_bytes(b"fake image")

    class LegacyFakePaddleOcr:
        def ocr(self, image_path, cls=True):
            return [
                [
                    [[[0, 0], [1, 0], [1, 1], [0, 1]], ("안녕하세요", 0.92)],
                    [[[0, 2], [1, 2], [1, 3], [0, 3]], ("낮은 점수", 0.1)],
                    [[[0, 4], [1, 4], [1, 5], [0, 5]], ("반갑습니다", 0.75)],
                ]
            ]

    extractor = PaddleOcrExtractor(factory=lambda **kwargs: LegacyFakePaddleOcr(), min_score=0.5)

    assert extractor.extract(str(image_path)) == "안녕하세요\n반갑습니다"


def test_paddle_ocr_extractor_returns_none_when_dependency_is_missing(tmp_path):
    from crawl_scheduler.ocr import PaddleOcrExtractor

    image_path = tmp_path / "sample.png"
    image_path.write_bytes(b"fake image")

    def missing_factory(**kwargs):
        raise ImportError("paddleocr is not installed")

    extractor = PaddleOcrExtractor(factory=missing_factory)

    assert extractor.extract(str(image_path)) is None
