
#--------------------------------------------4 Action dc thực thi


import json
import os
import sys
import time
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


def load_config(config_path: str = "config/config.json") -> dict:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    full_path = os.path.join(project_root, config_path)
    with open(full_path, "r", encoding="utf-8") as f:
        return json.load(f)


def init_spark_session(app_name: str = "ProfileData") -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_credit_card_schema() -> T.StructType:
    return T.StructType(
        [T.StructField("Time", T.DoubleType(), False)]
        + [T.StructField(f"V{i}", T.DoubleType(), True) for i in range(1, 29)]
        + [
            T.StructField("Amount", T.DoubleType(), False),
            T.StructField("Class", T.IntegerType(), False),
        ]
    )


def read_raw_data(spark: SparkSession, input_path: str, schema: T.StructType) -> DataFrame:
    df = (
        spark.read
        .schema(schema)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .csv(input_path)
    )
    print("--- [SCHEMA & DATA TYPES] ---")
    df.printSchema()
    return df

#--> Bao gồm 4 action trog hàm gồm #(vitri) 
def profile_data(df: DataFrame) -> dict:
    total_rows = df.count()                             #(1)
    print(f"Total rows: {total_rows:,}")

    # Kiểm tra Null và NaN
    null_counts = (
        df.agg(*[                                       #(2) df.agg(...).first()
            F.sum(F.when(F.col(c).isNull() | F.isnan(c), 1).otherwise(0)).alias(c)
            for c in df.columns
        ])
        .first()
        .asDict()
    )
    total_nulls = int(sum(v or 0 for v in null_counts.values()))
    print(f"Total nulls/NaNs: {total_nulls}")

    # Thống kê phân phối nhãn Class
    print("\n--- [CLASS DISTRIBUTION] ---")
    (
        df.groupBy("Class")                             #(3) df.groupBy("Class")...show()
        .agg(
            F.count("*").alias("count"),
            F.round(F.count("*") / total_rows * 100, 4).alias("percentage"),
        )
        .orderBy("Class")
        .show()
    )

    # Hiển thị 5 dòng đầu tiên
    print("--- [SAMPLE 5 ROWS] ---")
    df.select("Time", "V1", "V2", "Amount", "Class").show(5)    #(4) df.select(...)...show(5)

    return {
        "total_rows": total_rows,
        "total_nulls": total_nulls,
        "null_by_column": null_counts,
    }

#--> Chạy các hàm
def run_read_and_profile(config_path: str = "config/config.json"):
    t0 = time.perf_counter()
    cfg = load_config(config_path)
    input_path = cfg["raw_input_path"]

    spark = init_spark_session("Checkpoint3_ReadAndProfile")
    try:
        schema = get_credit_card_schema()
        df_raw = read_raw_data(spark, input_path, schema)
        stats = profile_data(df_raw)
        print(f"Elapsed: {time.perf_counter() - t0:.2f}s")
        return stats
    finally:
        spark.stop()


if __name__ == "__main__":
    run_read_and_profile()