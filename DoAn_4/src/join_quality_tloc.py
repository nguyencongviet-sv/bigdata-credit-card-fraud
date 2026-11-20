

#-----------------5 Action-----

import os
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType



# 
#--> Kiểm tra và tạo Dimension nếu chưa tồn tại
def get_or_create_dimension_table(spark: SparkSession, dim_path: str) -> DataFrame:

    dim_schema = StructType([
        StructField("risk_tier", StringType(), False),
        StructField("tier_description", StringType(), True),
        StructField("alert_priority", IntegerType(), True),
    ])

    # 1. Kiểm tra Dimension đã tồn tại hay chưa
    if os.path.exists(dim_path) and os.listdir(dim_path):

        print(f"--> Dimension đã tồn tại: {dim_path}")
        print("--> Đọc Dimension từ CSV...")

        df_dim = (
            spark.read
            .option("header", True)
            .schema(dim_schema)
            .csv(dim_path)
        )

    else:
        print("--> Dimension chưa tồn tại. Tiến hành tạo mới...")

        # 2. Khởi tạo dữ liệu Dimension
        dim_data = [
            ("MICRO", "Giao dịch siêu nhỏ (< $10)", 1),
            ("LOW", "Giao dịch thông thường ($10 - $100)", 2),
            ("MEDIUM", "Giao dịch trung bình ($100 - $500)", 3),
            ("HIGH", "Giao dịch giá trị cao (>= $500)", 4),
        ]

        df_dim = spark.createDataFrame(
            dim_data,
            schema=dim_schema
        )

        # 3. Tạo thư mục cha nếu chưa tồn tại
        os.makedirs(os.path.dirname(dim_path), exist_ok=True)

        # 4. Ghi Dimension xuống CSV
        (
            df_dim.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(dim_path)
        )

        print("--> Đã tạo và lưu Dimension thành công!")

    return df_dim

#--> Kết nối các bảng với nhau
def join_and_check_quality(spark: SparkSession, df_clean: DataFrame, dim_path: str) -> tuple[DataFrame, dict]:
    df_dim = get_or_create_dimension_table(spark, dim_path)

    # 1. Kiểm tra tính duy nhất của khóa chính trên bảng vừa tạo
    dim_total_rows = df_dim.count()
    dim_distinct_keys = df_dim.select("risk_tier").distinct().count()
    dim_duplicate_keys = dim_total_rows - dim_distinct_keys

    # 2. Đếm số dòng trước Join
    rows_before_join = df_clean.count()         #---------(1) df_clean.count()

    # 3. Thực hiện Broadcast Left Join
    df_joined = df_clean.join(F.broadcast(df_dim), on="risk_tier", how="left")

    # 4. Đếm số dòng sau Join & kiểm tra phát sinh nhân bản dòng
    rows_after_join = df_joined.count()         #---------(2) df_joined.count()
    row_diff = rows_after_join - rows_before_join

    # 5. Kiểm tra Null trên khóa Join và các cột lấy từ bảng Dimension
    null_join_key = df_joined.filter(F.col("risk_tier").isNull()).count() #---------(3)
    null_dim_cols = df_joined.filter(                                     #---------(4)
        F.col("tier_description").isNull() | F.col("alert_priority").isNull()
    ).count()

    # 6. Kiểm tra Unmatched Records (Left Anti Join)
    df_unmatched = df_clean.join(F.broadcast(df_dim), on="risk_tier", how="left_anti")
    unmatched_count = df_unmatched.count()                                  #---------(5)
    match_rate_pct = (
        round(((rows_before_join - unmatched_count) / rows_before_join) * 100, 4)
        if rows_before_join > 0
        else 0.0
    )

    # 7. In báo cáo kiểm tra chất lượng Join ra Console
    print("3.  JOIN & DATA QUALITY CHECK")
    print(f"• Bảng Dimension (risk_tier)    : {dim_total_rows} dòng | Khóa duy nhất: {dim_distinct_keys} | Khóa trùng: {dim_duplicate_keys}")
    print(f"• Số dòng trước Join (Fact)     : {rows_before_join:>10,} dòng")
    print(f"• Số dòng sau Join (Joined)     : {rows_after_join:>10,} dòng (Chênh lệch: {row_diff:+d})")
    print(f"• Null trên khóa 'risk_tier'    : {null_join_key:>10,} dòng")
    print(f"• Null cột Dimension sau Join   : {null_dim_cols:>10,} dòng")
    print(f"• Unmatched Records (không khớp): {unmatched_count:>10,} dòng (Tỷ lệ khớp: {match_rate_pct}%)")

    if unmatched_count > 0:
        print("\n--- Preview tối đa 5 dòng Unmatched Records ---")
        df_unmatched.select("Time", "Amount", "risk_tier", "Class").show(5, truncate=False)

    print("\n--- Preview 5 dòng dữ liệu sau khi Join ---")
    df_joined.select(
        "Time", "hour_of_day", "Amount", "risk_tier", "tier_description", "alert_priority", "Class"
    ).show(5, truncate=False)
    #df_unmatched.show(5, truncate=False)  #--> Hiễn thị toàn bộ cột
    join_stats = {
        "dim_total_rows": dim_total_rows,
        "dim_duplicate_keys": dim_duplicate_keys,
        "rows_before_join": rows_before_join,
        "rows_after_join": rows_after_join,
        "row_diff": row_diff,
        "null_join_key_count": null_join_key,
        "null_dim_cols_count": null_dim_cols,
        "unmatched_records_count": unmatched_count,
        "join_match_rate_pct": match_rate_pct,
    }

    return df_joined, join_stats