import os
import sys
import json
import hashlib
import io
import shutil
from datetime import datetime, timezone


if sys.platform.startswith("win"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')



def compute_file_hash(filepath):
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def run_ingestion(source_file_path):

    if not os.path.exists(source_file_path):
        print(f"[ERROR] Khong tim thay file nguon tai: {source_file_path}")
        print("Vui long tai file creditcard.csv tu Kaggle va dat vao data/source_input/")
        sys.exit(1)
    now = datetime.now(timezone.utc)
    ingest_date = now.strftime("%Y-%m-%d")
    run_timestamp = now.strftime("%Y%m%d_%H%M%S")
    run_id = f"run_{run_timestamp}"

    print(f"[INGESTION] Dang nap du lieu tu nguon: {source_file_path}")


    raw_dest_dir = f"data/raw/credit_card_transactions/ingest_date={ingest_date}/{run_id}"
    os.makedirs(raw_dest_dir, exist_ok=True)
    raw_output_file = os.path.join(raw_dest_dir, "raw_transactions.csv")

    shutil.copyfile(source_file_path, raw_output_file)

    with open(source_file_path, "r", encoding="utf-8") as f:
        record_count = max(0, sum(1 for _ in f) - 1)

    file_bytes = os.path.getsize(raw_output_file)
    file_hash = compute_file_hash(source_file_path)

    manifest_data = {
        "dataset_name": "credit_card_transactions",
        "ingest_timestamp_utc": now.isoformat(),
        "run_id": run_id,
        "source_path": source_file_path,
        "destination_path": raw_output_file,
        "record_count": record_count,
        "file_size_bytes": file_bytes,
        "sha256_hash": file_hash,
        "status": "SUCCESS"
    }

    manifest_dir = os.path.join("metadata", "manifests")
    os.makedirs(manifest_dir, exist_ok=True)
    manifest_path = os.path.join(manifest_dir, f"manifest_{run_id}.json")

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=4)

    print("\n" + "="*50)
    print(f"[SUCCESS] Da nap thanh cong {record_count:,} ban ghi!")
    print(f"[RAW LOCATION] {raw_output_file}")
    print(f"[MANIFEST CREATED] {manifest_path}")
    print("="*50 + "\n")


if __name__ == "__main__":
    # Đường dẫn nguồn tới file CSV tải từ Kaggle
    SOURCE_PATH = "data/source_input/creditcard.csv"
    run_ingestion(SOURCE_PATH)