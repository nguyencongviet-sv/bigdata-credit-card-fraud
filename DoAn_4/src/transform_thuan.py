

#-------------------------5 Action dc thực thi

from pyspark.sql import DataFrame, functions as F


def clean_and_transform(df_raw: DataFrame) -> tuple[DataFrame, dict]:

    input_rows = df_raw.count()                         #---------(1) df_raw.count()

    # 1. Đánh dấu các điều kiện lỗi trên PySpark DataFrame
    cond_null = (
        F.col("Time").isNull() | F.isnan("Time")
        | F.col("Amount").isNull() | F.isnan("Amount")
        | F.col("Class").isNull()
    )
    cond_invalid_val = (
        (F.col("Time") < 0)
        | (F.col("Amount") < 0)
        | (~F.col("Class").isin(0, 1))
    )

    # Tách dữ liệu lỗi giá trị / Null (kèm cột LY_DO_LOI)
    df_val_errors = (
        df_raw.filter(cond_null | cond_invalid_val)        
        .withColumn(
            "LY_DO_LOI",
            F.concat_ws(
                "; ",
                F.when(cond_null, F.lit("Bị khuyết thiếu (Null/NaN)")),
                F.when(F.col("Amount") < 0, F.concat(F.lit("Amount < 0 ("), F.col("Amount"), F.lit(")"))),
                F.when(F.col("Time") < 0, F.lit("Time < 0")),
                F.when(~F.col("Class").isin(0, 1), F.lit("Class không hợp lệ")),
            ),
        )
    )
    invalid_removed = df_val_errors.count()                 #---------(2) df_val_errors.count()

    # Giữ lại các dòng hợp lệ để khử trùng lặp
    df_valid = df_raw.filter(~(cond_null | cond_invalid_val))

    # 2. Khử trùng lặp theo khóa nghiệp vụ (Time, V1, V2, Amount)
    df_dedup = df_valid.dropDuplicates(["Time", "V1", "V2", "Amount"])
    clean_rows = df_dedup.count()                           #---------(3)df_dedup.count()
    duplicate_removed = (input_rows - invalid_removed) - clean_rows
    total_removed = input_rows - clean_rows

    # 3. Chuẩn hóa & Tạo đặc trưng mới (Feature Engineering) cho STEP 3 & STEP 4
    df_clean = (
        df_dedup
        .withColumn("Class", F.col("Class").cast("int"))
        .withColumn("Amount", F.round(F.col("Amount"), 2))
        .withColumn("hour_of_day", (F.floor(F.col("Time") / 3600) % 24).cast("int"))
        .withColumn("day_number", (F.floor(F.col("Time") / 86400) + 1).cast("int"))
        .withColumn(
            "risk_tier",
            F.when(F.col("Amount") < 10, "MICRO")
            .when(F.col("Amount") < 100, "LOW")
            .when(F.col("Amount") < 500, "MEDIUM")
            .otherwise("HIGH"),
        ).cache() #01 Cache dữ liệu sạch
    )

    pct_err = round((total_removed / input_rows) * 100, 4) if input_rows > 0 else 0.0
    pct_clean = round((clean_rows / input_rows) * 100, 4) if input_rows > 0 else 0.0

    # 4. In báo cáo Data Quality Gate & Transform (không in thời gian tại đây)
    print("2. DATA QUALITY GATE & TRANSFORM (PYSPARK)")
    print(f"• Dữ liệu ban đầu (Raw)         : {input_rows:>10,} dòng (100.00%)")
    print(f"• Dữ liệu lỗi Null/Giá trị sai  : {invalid_removed:>10,} dòng")
    print(f"• Dữ liệu trùng lặp (Duplicate) : {duplicate_removed:>10,} dòng")
    print(f"• Tổng bị loại bỏ (Rejected)    : {total_removed:>10,} dòng ({pct_err}%)")
    print(f"• Dữ liệu sạch (Accepted)       : {clean_rows:>10,} dòng ({pct_clean}%)")

    if invalid_removed > 0:
        print("\n--- Preview tối đa 10 dòng dữ liệu lỗi kèm lý do ---")
        df_val_errors.select("Time", "Amount", "Class", "LY_DO_LOI").show(10, truncate=False)

    print("\n--- Phân bố phân tầng rủi ro (Risk Tier Distribution) ---")
    (
        df_clean.groupBy("risk_tier")                  #----------(4) df_clean.groupBy("risk_tier")...show()
        .agg(
            F.count("*").alias("count"),
            F.round(F.count("*") / F.lit(clean_rows) * 100, 2).alias("percentage"),
        )
        .orderBy(F.desc("count"))
        .show()
    )

    print("--- Preview 5 dòng dữ liệu sau khi làm sạch & chuẩn hóa ---")
    df_clean.select("Time", "day_number", "hour_of_day", "Amount", "risk_tier", "Class").show(5, truncate=False)
                                                        #----------(5) df_clean.select()
    transform_stats = {
        "input_rows": input_rows,
        "invalid_rows_removed": invalid_removed,
        "duplicate_rows_removed": duplicate_removed,
        "total_removed_rows": total_removed,
        "clean_rows": clean_rows,
        "retention_rate_pct": pct_clean,
    }

    return df_clean, transform_stats