import os
import re
from abc import ABC, abstractmethod
from urllib.parse import urljoin, urlparse

import requests

from crawl_scheduler.config import Config
from crawl_scheduler.utils.loghandler import logger


class AbstractCommunityWebsite(ABC):
    dayBestUrl = ""
    realtimeBestUrl = ""

    def __init__(self, yyyymmdd) -> None:
        logger.info("Initializing AbstractCommunityWebsite with date %s", yyyymmdd)

    @abstractmethod
    def get_daily_best(self):
        logger.info("Getting daily best content.")
        pass

    @abstractmethod
    def get_realtime_best(self):
        logger.info("Getting real-time best content.")
        pass

    @abstractmethod
    def get_board_contents(self, board_id):
        logger.info("Fetching board contents for board_id: %s", board_id)
        pass

    @abstractmethod
    def is_ad(self, title) -> bool:
        pass

    @abstractmethod
    def get_gpt_obj(self, url):
        logger.info("Saving image from URL: %s", url)
        pass

    @abstractmethod
    def get_board_list(self):
        pass

    def save_file(self, url, category, no, alt_text=None, headers=None):
        try:
            media_url = self.normalize_media_url(url)
            if not media_url:
                logger.error("Invalid media URL for %s/%s: %s", category, no, url)
                return False

            if not headers:
                headers = {"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"}

            response = requests.get(media_url, headers=headers, stream=True, timeout=10)
            child_class_name = self.__class__.__name__
            root_path = Config().get_env("ROOT") or "./media"
            path = os.path.join(root_path, child_class_name, str(category), str(no))

            logger.info("Saving media from URL: %s", media_url)
            os.makedirs(path, exist_ok=True)

            file_name = alt_text or self._file_name_from_url(media_url)
            detected_extension = None

            if response.status_code == 200:
                content_disposition = response.headers.get("content-Disposition", "")
                match = re.search(r'filename="?([^";]+)"?', content_disposition)
                if match:
                    file_name = match.group(1)
                detected_extension = self._extension_from_content_type(
                    response.headers.get("Content-Type")
                )
            else:
                logger.error("Failed to fetch media from %s: %s", media_url, response.status_code)
                return False

            file_name = self._safe_file_name(file_name)
            file_name_without_extension, file_extension = os.path.splitext(file_name)
            if detected_extension and not self._is_known_media_extension(file_extension):
                file_extension = detected_extension
                file_name = f"{file_name_without_extension}{file_extension}"
                file_name_without_extension = os.path.splitext(file_name)[0]

            file_path = os.path.join(path, file_name)

            index = 1
            while os.path.exists(file_path):
                file_path = os.path.join(
                    path, f"{file_name_without_extension}_{index}{file_extension}"
                )
                index += 1

            with open(file_path, "wb") as f:
                f.write(response.content)

            return os.path.relpath(file_path, root_path)

        except Exception as e:
            logger.error("Failed to save media for %s/%s: %s", category, no, e)
            return False

    def img_to_text(self, img_path, *unused_args):
        logger.debug("OCR disabled; skipping image text extraction for %s", img_path)
        return None

    @staticmethod
    def _extension_from_content_type(content_type):
        if not content_type:
            return None

        media_type = content_type.split(";", 1)[0].strip().lower()
        return {
            "image/jpeg": ".jpg",
            "image/png": ".png",
            "image/webp": ".webp",
            "image/gif": ".gif",
            "video/mp4": ".mp4",
            "video/webm": ".webm",
        }.get(media_type)

    @classmethod
    def media_url_from_tag(cls, tag, base_url=None):
        if not tag:
            return None

        candidates = []
        for attr in ("data-original", "data-src", "data-lazy-src", "data-url", "src"):
            value = tag.get(attr)
            if value:
                candidates.append(value)

        for candidate in candidates:
            media_url = cls.normalize_media_url(candidate, base_url=base_url)
            if media_url and not cls._is_placeholder_media_url(media_url):
                return media_url

        return None

    @staticmethod
    def normalize_media_url(url, base_url=None):
        if not url:
            return None

        media_url = str(url).strip()
        if not media_url:
            return None

        media_url = re.sub(r"^https?://(https?://)", r"\1", media_url, flags=re.IGNORECASE)
        media_url = re.sub(r"^(https?:)(https?://)", r"\2", media_url, flags=re.IGNORECASE)

        if media_url.startswith("//"):
            return f"https:{media_url}"

        parsed_url = urlparse(media_url)
        if parsed_url.scheme and parsed_url.netloc:
            return media_url

        if base_url:
            return urljoin(base_url, media_url)

        return None

    @staticmethod
    def _file_name_from_url(url):
        parsed_url = urlparse(url)
        return os.path.basename(parsed_url.path) or parsed_url.query or "download"

    @staticmethod
    def _is_placeholder_media_url(url):
        parsed_url = urlparse(url)
        basename = os.path.basename(parsed_url.path).lower()
        return basename in {
            "blank.gif",
            "loading.gif",
            "gallview_loading_ori.gif",
        }

    @staticmethod
    def _safe_file_name(file_name):
        sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", file_name).strip()
        return sanitized.strip(". ") or "download"

    @staticmethod
    def _is_known_media_extension(extension):
        return extension.lower() in {
            ".jpg",
            ".jpeg",
            ".png",
            ".webp",
            ".gif",
            ".mp4",
            ".webm",
        }
