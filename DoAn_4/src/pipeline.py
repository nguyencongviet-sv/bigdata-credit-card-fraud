import json
import os
import sys
import time
from pyspark.sql import functions as F

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


# --> File #1. Read Data: Đọc dữ liệu Raw bằng Spark, kiểm tra schema, kiểu dữ liệu, số dòng và null.
from quality_hduc import (
    load_config,
    init_spark_session,
    get_credit_card_schema,
    read_raw_data,
    profile_data,
)
# --> File #Transform: Làm sạch dữ liệu, chuẩn hóa, lọc dữ liệu lỗi và tạo cột mới ("day_number", "hour_of_day", "risk_tier")
from transform_thuan import clean_and_transform
# --> File #Join & Quality: Kết nối các bảng, kiểm tra khóa trùng, null, số dòng trước/sau join và unmatched records.
from join_quality_tloc import join_and_check_quality
# --> File #Aggregate & Output: Thống kê bằng groupBy/agg, xuất kết quả CSV/Parquet, đo thời gian chạy và lưu execution plan
from agg_vd import aggregate_and_save


def run_full_pipeline(config_path: str = "config/config.json"):
    t_start = time.perf_counter()

    cfg = load_config(config_path)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    spark = init_spark_session("Checkpoint3_FullSparkPipeline")

    try:
        # 1. Read Data: Đọc dữ liệu Raw bằng Spark, kiểm tra schema, kiểu dữ liệu, số dòng và null.
        t_read = time.perf_counter()
        raw_path = os.path.join(project_root, cfg["raw_input_path"])
        schema = get_credit_card_schema()
        print("-" * 70)
        df_raw = read_raw_data(spark, raw_path, schema)
        scan_partitions = df_raw.rdd.getNumPartitions()
        shuffle_partitions = int(spark.conf.get("spark.sql.shuffle.partitions"))
        read_profile_stats = profile_data(df_raw)
        read_step = round(time.perf_counter() - t_read, 2)
        read_profile_stats["elapsed_seconds"] = read_step

        print(f"[1. READ DATA & QUALITY] Hoàn tất trong: {read_step}s\n")
        print("-" * 70)

        # 2. TRANSFORM & DATA CLEANING
        t_transform = time.perf_counter()
        df_clean, transform_stats = clean_and_transform(df_raw)
        transform_elapsed = round(time.perf_counter() - t_transform, 2)
        transform_stats["elapsed_seconds"] = transform_elapsed

        print(f"[2. TRANSFORM & CLEANING] Hoàn tất trong: {transform_elapsed}s\n")
        print("-" * 70)

        # 3. JOIN & QUALITY CHECK
        t_join = time.perf_counter()
        dim_path = os.path.join(project_root, cfg.get("dim_input_path", "data/dim/risk_tier_dim/"))
        df_joined, join_stats = join_and_check_quality(spark, df_clean, dim_path)
        join_elapsed = round(time.perf_counter() - t_join, 2)
        join_stats["elapsed_seconds"] = join_elapsed

        print(f"[3. JOIN & QUALITY] Hoàn tất trong: {join_elapsed}s\n")
        print("-" * 70)

        # 4. AGGREGATE & OUTPUT
        t_step4 = time.perf_counter()
        parquet_out = os.path.join(project_root, cfg.get("output_parquet_path", "output/summary_parquet/"))
        csv_out = os.path.join(project_root, cfg.get("output_csv_path", "output/summary_csv/"))
        plan_out = os.path.join(project_root, cfg.get("physical_plan_path", "output/physical_plan.txt"))

        _, agg_stats = aggregate_and_save(df_joined, parquet_out, csv_out, plan_out)
        step4_elapsed = round(time.perf_counter() - t_step4, 2)
        agg_stats["elapsed_seconds"] = step4_elapsed

        print(f"[4. AGGREGATE & OUTPUT] Hoàn tất trong: {step4_elapsed}s\n")
        print("-" * 70)

        # BƯỚC 5: TỔNG KẾT & TIME CHẠY TOÀN BỘ PIPELINE
        sum_steps = round(read_step + transform_elapsed + join_elapsed + step4_elapsed, 2)
        total_runtime = round(time.perf_counter() - t_start, 2)
        spark_init_overhead = round(max(0.0, total_runtime - sum_steps), 2)

        quality_metrics = {
            "step1_input_quality": read_profile_stats,
            "step2_transform_stats": transform_stats,
            "step3_join_stats": join_stats,
            "step4_aggregate_stats": agg_stats,
            "partitions": f"{scan_partitions} scan / {shuffle_partitions} shuffle",
            "steps_sum_runtime_sec": sum_steps,
            "spark_init_overhead_sec": spark_init_overhead,
            "baseline_runtime_sec": total_runtime,
        }

        rel_metrics_path = cfg.get("quality_metrics_path") or "output/quality_metrics.json"
        metrics_out = os.path.join(project_root, rel_metrics_path)
        metrics_dir = os.path.dirname(metrics_out)
        if metrics_dir:
            os.makedirs(metrics_dir, exist_ok=True)

        with open(metrics_out, "w", encoding="utf-8") as f_metrics:
            json.dump(quality_metrics, f_metrics, indent=2, ensure_ascii=False)

        print("=" * 68)
        print("TỔNG KẾT THỜI GIAN CHẠY PIPELINE ĐỒ ÁN 4 (BASELINE RUNTIME)")
        assert transform_stats["clean_rows"] == 283728, f"Sai clean_rows: {transform_stats['clean_rows']}"
        assert join_stats["rows_after_join"] == 283728, f"Sai rows_after_join: {join_stats['rows_after_join']}"
        assert join_stats["unmatched_records_count"] == 0, "Có bản ghi không khớp Dimension!"
        assert agg_stats["summary_group_rows"] == 96, f"Sai số nhóm tổng hợp: {agg_stats['summary_group_rows']}"
        print("✅ [CORRECTNESS CHECK] Tất cả 4 assertions đều PASS (0 regression)!")

        df_clean.unpersist() # Giải phóng bộ nhớ
        print("=" * 68)
        print(f"Partitions                                   : {scan_partitions} scan / {shuffle_partitions} shuffle")
        print(f"1. Read Data & Quality (quality_hduc.py)     : {read_step:>6.2f}s")
        print(f"2. Transform & Cleaning (transform_thuan.py) : {transform_elapsed:>6.2f}s")
        print(f"3. Join & Quality Check (join_quality_tloc.py): {join_elapsed:>6.2f}s")
        print(f"4. Aggregate & Output (agg_vd.py)            : {step4_elapsed:>6.2f}s")
        print("-" * 68)
        print(f"Tổng thời gian thực thi 4 bước xử lý         : {sum_steps:>6.2f}s")
        print(f"Thời gian khởi tạo Spark & điều phối         : {spark_init_overhead:>6.2f}s")
        print(f"TỔNG THỜI GIAN TOÀN PIPELINE                 : {total_runtime:>6.2f}s")
        print("=" * 68)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_full_pipeline()