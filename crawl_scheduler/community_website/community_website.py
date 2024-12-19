import os
from abc import ABC, abstractmethod  # abc 모듈 추가
# img to text
import pytesseract
from PIL import Image
from crawl_scheduler.utils.loghandler import logger
import cv2



class AbstractCommunityWebsite(ABC):  # ABC 클래스 상속 추가
    dayBestUrl = ''
    realtimeBestUrl = ''

    def __init__(self, yyyymmdd) -> None:
        logger.info(f"Initializing AbstractCommunityWebsite with date {yyyymmdd}")

    @abstractmethod
    def get_daily_best(self):
        logger.info("Getting daily best content.")
        return {}

    @abstractmethod
    def get_real_time_best(self):
        logger.info("Getting real-time best content.")
        return {}

    @abstractmethod
    def get_board_contents(self, board_id):
        logger.info(f"Fetching board contents for board_id: {board_id}")
        return {}

    @abstractmethod
    def save_img(self, url):
        logger.info(f"Saving image from URL: {url}")
        return {}

    def img_to_text(self, img_path):
        logger.info(f"Converting image to text from path: {img_path}")
        custom_config = r'--oem 3 --psm 6 -l kor'
        allowed_extensions = ['jpg', 'png', 'jpeg']

        try:
            # Check file extension and apply OCR accordingly
            if any(ext in img_path for ext in allowed_extensions):
                logger.info(f"Valid image extension detected for {img_path}. Proceeding with OCR.")

                # Load the image using OpenCV
                image = cv2.imread(img_path)
                if image is None:
                    logger.error(f"Failed to load image: {img_path}")
                    raise ValueError(f"Invalid image path or format: {img_path}")

                # Convert the image to grayscale
                gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
                logger.debug(f"Image converted to grayscale.")

                # Apply thresholding to improve OCR accuracy
                threshold_image = cv2.threshold(gray_image, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]
                logger.debug("Thresholding applied to the image.")

                # Extract text using Tesseract OCR
                text = pytesseract.image_to_string(threshold_image, config=custom_config)
                logger.info(f"OCR completed successfully on image {img_path}.")
            else:
                # If not an image file, directly process the file path
                logger.warning(f"No valid image extension detected for {img_path}. Attempting direct OCR.")
                text = pytesseract.image_to_string(img_path, config=custom_config)

            logger.info(f"Text extraction successful for {img_path}.")
        except Exception as e:
            logger.exception(f"Error during image-to-text conversion for {img_path}: {str(e)}")
            raise e

        return text
