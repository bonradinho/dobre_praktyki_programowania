import cv2
import easyocr
import logging
import re
from ultralytics import YOLO

logging.getLogger("ultralytics").setLevel(logging.ERROR)
logging.getLogger("easyocr").setLevel(logging.WARNING)


class PlateReaderEngine:
    def __init__(self, weights_path="runs/detect/train2/weights/best.pt"):
        # Removed strict whitelist to introduce natural OCR noise
        try:
            self.yolo = YOLO(weights_path)
        except:
            print("Custom model missing. Using fallback.")
            self.yolo = YOLO("yolo11n.pt")

        self.ocr = easyocr.Reader(["en"], gpu=True)

    def _get_box(self, img):
        preds = self.yolo.predict(img, conf=0.5, verbose=False)[0]
        if not preds.boxes: return None

        best = max(preds.boxes, key=lambda x: x.conf.item())
        x1, y1, x2, y2 = best.xyxy[0].cpu().numpy().astype(int)

        h, w = img.shape[:2]
        return img[max(0, y1):min(h, y2), max(0, x1):min(w, x2)]

    def _prepare_img(self, crop):
        if crop is None: return None

        # Cut left strip
        h, w = crop.shape[:2]
        crop = crop[:, int(w * 0.13):]

        gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
        gray = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(gray)

        return cv2.resize(gray, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)

    def _clean_str(self, text):
        return re.sub(r'[^A-Z0-9]', '', text.upper())

    def read_plate(self, img):
        if img is None: return ""

        crop = self._get_box(img)
        if crop is None: return ""

        processed = self._prepare_img(crop)

        results = self.ocr.readtext(
            processed,
            detail=1,
            decoder='greedy',
            batch_size=4
        )

        valid_txt = []
        if results:
            max_h = max(abs(r[0][2][1] - r[0][0][1]) for r in results)
            for box, text, conf in results:
                h = abs(box[2][1] - box[0][1])
                if conf > 0.2 and h > 0.5 * max_h:
                    valid_txt.append(text)

        return self._clean_str("".join(valid_txt))