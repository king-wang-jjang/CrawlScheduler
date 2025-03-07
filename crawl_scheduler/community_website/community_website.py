from io import BytesIO
import os
from abc import ABC, abstractmethod
import re
from PIL import Image
# img to text
import pytesseract
import requests
from crawl_scheduler.config import Config
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
        pass 

    @abstractmethod
    def get_realtime_best(self):
        logger.info("Getting real-time best content.")
        pass

    @abstractmethod
    def get_board_contents(self, board_id):
        logger.info(f"Fetching board contents for board_id: {board_id}")
        pass

    @abstractmethod
    def is_ad(self, title) -> bool:
        pass

    @abstractmethod
    def get_gpt_obj(self, url):
        logger.info(f"Saving image from URL: {url}")
        pass
    
    @abstractmethod
    def get_board_list(self):
        pass

    def save_file(self, url, category, no, alt_text=None, headers=None):
        if not headers:
            headers = {"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"}

        response = requests.get(url, headers=headers, stream=True)
        child_class_name = self.__class__.__name__
        root_path = Config().get_env('ROOT')
        path = os.path.join(root_path, child_class_name, str(category), str(no))
        
        logger.info(f"Saving image from URL: {url}")
        os.makedirs(path, exist_ok=True)

        if alt_text:
            file_name = alt_text
        else:
            file_name = os.path.basename(url)

        if response.status_code == 200:
            match = re.search(r'filename="?([^";]+)"?', response.headers.get("content-Disposition"))
            if match:
                file_name = match.group(1)
            img = Image.open(BytesIO(response.content))
            img_format = img.format
        else:
            logger.error(f"이미지를 가져오는 데 실패했습니다.: {response.status_code}") 
                
        file_name_without_extension, file_extension = os.path.splitext(file_name)
        file_path = os.path.join(path, file_name)
        
        # 중복검사
        index = 1
        while os.path.exists(file_path):
            if img_format:
                file_extension = f".{img_format}"
            file_path = os.path.join(path, f"{file_name_without_extension}_{index}{file_extension}")
            index += 1
        
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        # ROOT 경로를 제거하여 상대 경로 반환
        relative_path = os.path.relpath(file_path, root_path)
        
        return relative_path

    def img_to_text(self, img_path):
        # pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
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
            return None

        return text
