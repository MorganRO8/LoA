import argparse
import base64
import json
import multiprocessing as mp
import sys
from io import BytesIO

mp.set_start_method("spawn", force=True)

try:
    from DECIMER import predict_SMILES
    from PIL import Image
    import numpy as np
except Exception as err:
    print(f"Required DECIMER SMILES packages not available: {err}", file=sys.stderr)
    sys.exit(1)


def _decode_segment_image(encoded):
    raw = base64.b64decode(encoded)
    with Image.open(BytesIO(raw)) as img:
        return np.array(img.convert("RGB"))


def run_smiles_prediction(segments_json_path):
    with open(segments_json_path, "r") as f:
        encoded_segments = json.load(f)

    results = []
    for encoded in encoded_segments:
        try:
            arr = _decode_segment_image(encoded)
            smi = predict_SMILES(arr)
            results.append(smi if smi else None)
        except Exception as err:
            print(f"SMILES prediction failed: {err}", file=sys.stderr)
            results.append(None)
    print(json.dumps(results))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Predict SMILES from DECIMER segment images.")
    parser.add_argument("--segments-json", required=True, help="JSON file with list of segment image base64 values.")
    args = parser.parse_args()
    run_smiles_prediction(args.segments_json)
