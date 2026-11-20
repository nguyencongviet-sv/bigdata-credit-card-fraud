import csv
import json
import os
import statistics
import subprocess
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


def run_benchmark(exp_name: str, runs: int = 3):
    csv_path = "benchmark.csv"
    metrics_json_path = "output/quality_metrics.json"
    file_exists = os.path.isfile(csv_path)

    total_times = []
    join_times = []
    agg_times = []

    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"

    print(f"\n{'='*68}")
    print(f"CHẠY BENCHMARK: {exp_name} ({runs} LẦN)")
    print(f"{'='*68}")

    for i in range(1, runs + 1):
        print(f"\n>>> [LẦN CHẠY {i}/{runs}] Đang thực thi src/pipeline.py...")

        res = subprocess.run(
            [sys.executable, "src/pipeline.py"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )

        if res.returncode != 0:
            print(f"❌ Lỗi ở Run {i}:\n{res.stderr}")
            return

        # Lọc và in ra đúng phần Correctness Check & Bảng tổng kết của lần chạy này
        lines = res.stdout.splitlines()
        start_idx = next(
            (idx for idx, line in enumerate(lines) if "CORRECTNESS CHECK" in line),
            max(0, len(lines) - 15),
        )
        print("\n".join(lines[start_idx:]))

        with open(metrics_json_path, "r", encoding="utf-8") as f:
            m = json.load(f)

        r_sec = m["step1_input_quality"]["elapsed_seconds"]
        t_sec = m["step2_transform_stats"]["elapsed_seconds"]
        j_sec = m["step3_join_stats"]["elapsed_seconds"]
        a_sec = m["step4_aggregate_stats"]["elapsed_seconds"]
        tot_sec = m["baseline_runtime_sec"]
        parts = m["partitions"]

        total_times.append(tot_sec)
        join_times.append(j_sec)
        agg_times.append(a_sec)

        with open(csv_path, "a", newline="", encoding="utf-8") as f_csv:
            writer = csv.writer(f_csv)
            if not file_exists and i == 1:
                writer.writerow([
                    "experiment", "run", "partitions",
                    "read_sec", "clean_sec", "join_sec", "agg_sec", "total_sec"
                ])
            writer.writerow([exp_name, i, parts, r_sec, t_sec, j_sec, a_sec, tot_sec])

    print(f"\n{'*'*68}")
    print(f"TỔNG HỢP MEDIAN SAU {runs} LẦN CHẠY ({exp_name}):")
    print(f"  • Các lần chạy (Total) : {total_times}")
    print(f"  • Median Step 3 (Join) : {statistics.median(join_times):.2f}s")
    print(f"  • Median Step 4 (Agg)  : {statistics.median(agg_times):.2f}s")
    print(f"  • MEDIAN TỔNG PIPELINE : {statistics.median(total_times):.2f}s")
    print(f"{'*'*68}\n")


if __name__ == "__main__":
    run_benchmark("baseline_no_cache", runs=3)