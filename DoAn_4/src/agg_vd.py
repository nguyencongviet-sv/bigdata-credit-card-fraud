
#-------------------5 Action dc thực thi

import io
import os
from contextlib import redirect_stdout
from pyspark.sql import DataFrame, functions as F

def aggregate_and_save(
    df_joined: DataFrame,
    parquet_out: str,
    csv_out: str,
    plan_out: str,
) -> tuple[DataFrame, dict]:
    # 1. Tổng hợp đa chiều (Multi-dimensional Aggregation)
    df_summary = (
        df_joined.groupBy("hour_of_day", "risk_tier", "tier_description", "alert_priority")
        .agg(
            F.count("*").alias("total_txns"),
            F.sum(F.when(F.col("Class") == 1, 1).otherwise(0)).alias("fraud_txns"),
            F.round(
                F.sum(F.when(F.col("Class") == 1, 1).otherwise(0)) / F.count("*") * 100, 4
            ).alias("fraud_rate_pct"),
            F.round(F.sum("Amount"), 2).alias("total_amount_usd"),
            F.round(F.avg("Amount"), 2).alias("avg_amount_usd"),
            F.round(F.max("Amount"), 2).alias("max_amount_usd"),
        )
        .orderBy("hour_of_day", "alert_priority")
    )

    summary_rows = df_summary.count() #------------(1)

    # 2. Thống kê tổng quan theo từng phân tầng rủi ro
    df_tier_overview = (
        df_joined.groupBy("alert_priority", "risk_tier", "tier_description")
        .agg(
            F.count("*").alias("total_txns"),
            F.sum(F.when(F.col("Class") == 1, 1).otherwise(0)).alias("fraud_txns"),
            F.round(
                F.sum(F.when(F.col("Class") == 1, 1).otherwise(0)) / F.count("*") * 100, 4
            ).alias("fraud_rate_pct"),
            F.round(F.sum("Amount"), 2).alias("total_amount_usd"),
        )
        .orderBy("alert_priority")
    )

    # 3. In kết quả tổng hợp ra Console
    
    print("4. AGGREGATE")
    
    print(f"• Tổng số nhóm tổng hợp (hour x risk_tier): {summary_rows} dòng")

    print("\n--- Tổng hợp giao dịch & gian lận theo Risk Tier ---")
    df_tier_overview.show(truncate=False) #--------------(2)

    print("--- Top 10 khung giờ & phân nhóm có tỷ lệ gian lận cao nhất ---")
    (
        df_summary.filter(F.col("fraud_txns") > 0)        #-------------(3)
        .orderBy(F.desc("fraud_rate_pct"), F.desc("fraud_txns"))
        .select("hour_of_day", "risk_tier", "total_txns", "fraud_txns", "fraud_rate_pct", "total_amount_usd")
        .show(10, truncate=False)
    )

    # 4. Xuất dữ liệu ra Parquet & CSV
    os.makedirs(os.path.dirname(parquet_out), exist_ok=True)
    os.makedirs(os.path.dirname(csv_out), exist_ok=True)
    os.makedirs(os.path.dirname(plan_out), exist_ok=True)

    df_summary.write.mode("overwrite").parquet(parquet_out) #---------------(4)
    df_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_out)#------------(5)
    # Dọn các file metadata (_SUCCESS và *.crc) cho gọn thư mục
    for folder in [parquet_out, csv_out]:
        if os.path.exists(folder):
            for fname in os.listdir(folder):
                if fname == "_SUCCESS" or fname.endswith(".crc"):
                    os.remove(os.path.join(folder, fname))
    # 5. Lưu Physical Plan của truy vấn tổng hợp
    f_buf = io.StringIO()
    with redirect_stdout(f_buf):
        df_summary.explain("formatted")
    physical_plan_str = f_buf.getvalue()

    with open(plan_out, "w", encoding="utf-8") as f_plan:
        f_plan.write(physical_plan_str)

    print(f"[OUTPUT] Đã lưu Parquet       : {parquet_out}")
    print(f"[OUTPUT] Đã lưu CSV           : {csv_out}")
    print(f"[OUTPUT] Đã lưu Physical Plan : {plan_out}")

    agg_stats = {
        "summary_group_rows": summary_rows,
        "parquet_output_path": parquet_out,
        "csv_output_path": csv_out,
        "physical_plan_path": plan_out,
    }

    return df_summary, agg_stats