import json
import sys
import multiprocessing as mp

mp.set_start_method("spawn", force=True)

try:
    from decimer_segmentation import segment_chemical_structures_from_file
    from DECIMER import predict_SMILES
except Exception as err:
    print(f"Required DECIMER packages not available: {err}", file=sys.stderr)
    sys.exit(1)


def run_decimer(path: str):
    segments = segment_chemical_structures_from_file(path)
    smiles = []
    for seg in segments:
        try:
            smi = predict_SMILES(seg)
            if smi:
                smiles.append(smi)
        except Exception as e:
            print(f"SMILES prediction failed: {e}", file=sys.stderr)
    print(json.dumps(smiles))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: decimer_runner.py <input_path>", file=sys.stderr)
        sys.exit(1)
    run_decimer(sys.argv[1])
