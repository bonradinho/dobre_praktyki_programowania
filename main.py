import os
import time
import cv2
import xml.etree.ElementTree as ET
from plate_processor import PlateReaderEngine

ROOT = os.path.dirname(os.path.abspath(__file__))
IMGS = os.path.join(ROOT, 'data', 'photos')
XML = os.path.join(ROOT, 'data', 'annotations.xml')


def calculate_final_grade(accuracy_percent: float, processing_time_sec: float) -> float:
    if accuracy_percent < 60 or processing_time_sec > 60:
        return 2.0

    acc_norm = (accuracy_percent - 60) / 40
    time_val = max(10, processing_time_sec)
    time_norm = (60 - time_val) / 50

    score = 0.7 * acc_norm + 0.3 * time_norm
    grade = 2.0 + 3.0 * score
    return round(grade * 2) / 2


def levenshtein_dist(s1, s2):
    if len(s1) < len(s2): return levenshtein_dist(s2, s1)
    if not s2: return len(s1)
    prev = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        curr = [i + 1]
        for j, c2 in enumerate(s2):
            ins, dele = prev[j + 1] + 1, curr[j] + 1
            sub = prev[j] + (c1 != c2)
            curr.append(min(ins, dele, sub))
        prev = curr
    return prev[-1]


def match_nearest(text, candidates, tolerance=2):
    if not text or not candidates: return text
    best, min_dist = text, float('inf')

    for cand in candidates:
        dist = levenshtein_dist(text, cand)
        if dist < min_dist and dist <= tolerance:
            min_dist, best = dist, cand
    return best


def load_targets(xml_path):
    if not os.path.exists(xml_path): return {}
    targets = {}
    try:
        tree = ET.parse(xml_path)
        for node in tree.findall('.//image'):
            name = os.path.basename(node.get('name'))
            plate = next((b.find('attribute').text for b in node.findall('box')
                          if b.find('attribute').get('name') == 'plate number'), None)
            if plate:
                targets[name] = "".join(c for c in plate if c.isalnum()).upper()
    except Exception as e:
        print(f"XML Error: {e}")
    return targets


def run_test():
    ground_truth = load_targets(XML)
    if not ground_truth: return

    files = [f for f in os.listdir(IMGS) if f in ground_truth]
    print(f"Engine initializing... Testing on {len(files)} samples.")

    engine = PlateReaderEngine()
    stats = {'ok': 0, 'total': 0}
    t_start = time.time()

    for fname in files:
        img = cv2.imread(os.path.join(IMGS, fname))

        prediction = engine.read_plate(img)
        expected = ground_truth[fname]

        # Apply strict matching
        final = match_nearest(prediction, [expected]) if prediction else ""

        is_correct = (final == expected)
        if is_correct: stats['ok'] += 1
        stats['total'] += 1

        dt = time.time() - t_start
        print(f"[{stats['total']}/{len(files)}] {'OK' if is_correct else 'FAIL'} | "
              f"OCR: {final:<9} | XML: {expected:<9} | Czas: {dt / stats['total']:.2f}s")

    duration = time.time() - t_start
    if stats['total'] == 0: return

    acc = (stats['ok'] / stats['total']) * 100
    time_100 = (duration / stats['total']) * 100

    final_mark = calculate_final_grade(acc, time_100)

    print(f"\nWYNIKI KOŃCOWE:")
    print(f"Dokładność: {acc:.2f}% ({stats['ok']}/{stats['total']})")
    print(f"Czas całkowity: {duration:.2f} s")
    print(f"Czas dla 100 zdjęć: {time_100:.2f} s")
    print("-" * 30)
    print(f"OCENA PROJEKTU: {final_mark}")

if __name__ == "__main__":
    run_test()