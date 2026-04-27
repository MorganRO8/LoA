import base64
import json
import multiprocessing as mp
import sys
from io import BytesIO

mp.set_start_method("spawn", force=True)

try:
    from decimer_segmentation import (
        segment_chemical_structures,
        segment_chemical_structures_from_file,
        get_mrcnn_results,
        apply_masks,
        sort_segments_bboxes,
    )
    HAS_INTERNAL_SEGMENT_API = True
except Exception:
    from decimer_segmentation import (
        segment_chemical_structures,
        segment_chemical_structures_from_file,
    )
    HAS_INTERNAL_SEGMENT_API = False

try:
    from pdf2image import convert_from_path
    from PIL import Image
    import numpy as np
except Exception as err:
    print(f"Required DECIMER segmentation packages not available: {err}", file=sys.stderr)
    sys.exit(1)


def _encode_segment_image(segment):
    with BytesIO() as buf:
        Image.fromarray(segment).save(buf, format="PNG")
        return base64.b64encode(buf.getvalue()).decode("utf-8")


def _shape_to_bbox(box, h, w):
    if not box:
        return None, None
    y0, x0, y1, x1 = [int(x) for x in box]
    return [y0, x0, y1, x1], [y0 / h, x0 / w, y1 / h, x1 / w]


def _segment_array(arr):
    if HAS_INTERNAL_SEGMENT_API:
        masks, _, _ = get_mrcnn_results(arr)
        segments, boxes = apply_masks(arr, masks)
        if len(segments) > 0:
            segments, boxes = sort_segments_bboxes(segments, boxes)
        return segments, boxes
    segments = segment_chemical_structures(arr, expand=True)
    return segments, [None] * len(segments)


def _segment_file(path):
    if HAS_INTERNAL_SEGMENT_API:
        arr = np.array(Image.open(path).convert("RGB"))
        segments, boxes = _segment_array(arr)
        h, w = arr.shape[:2]
    else:
        segments = segment_chemical_structures_from_file(path, expand=True)
        boxes = [None] * len(segments)
        arr = np.array(Image.open(path).convert("RGB"))
        h, w = arr.shape[:2]

    out = []
    for seg_idx, (seg, box) in enumerate(zip(segments, boxes), start=1):
        bbox, bbox_relative = _shape_to_bbox(box, h, w)
        out.append(
            {
                "segment_index": seg_idx,
                "bbox": bbox,
                "bbox_relative": bbox_relative,
                "segment_image_base64": _encode_segment_image(seg),
            }
        )
    return out


def run_segmentation(path):
    results = []
    if path.lower().endswith(".pdf"):
        pages = convert_from_path(path, 300)
        for page_idx, page in enumerate(pages, start=1):
            arr = np.array(page)
            segments, boxes = _segment_array(arr)
            h, w = arr.shape[:2]
            for seg_idx, (seg, box) in enumerate(zip(segments, boxes), start=1):
                bbox, bbox_relative = _shape_to_bbox(box, h, w)
                results.append(
                    {
                        "page": page_idx,
                        "segment_index": seg_idx,
                        "bbox": bbox,
                        "bbox_relative": bbox_relative,
                        "segment_image_base64": _encode_segment_image(seg),
                    }
                )
    else:
        results = _segment_file(path)
    print(json.dumps(results))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: decimer_segment_runner.py <input_path>", file=sys.stderr)
        sys.exit(1)
    run_segmentation(sys.argv[1])
