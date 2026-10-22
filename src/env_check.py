import sys
import os
import io
from pyspark.sql import SparkSession

if sys.platform.startswith("win"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

def check_environment():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    print("Starting Spark Environment Smoke Test...")
    try:
        spark = SparkSession.builder \
            .appName("FraudDetection_EnvCheck") \
            .master("local[2]") \
            .config("spark.python.worker.faulthandler.enabled", "true") \
            .getOrCreate()
        
        test_data = [(1, "Card_Test_Pass"), (2, "Spark_Running")]
        df = spark.createDataFrame(test_data, ["id", "status"])
        count = df.count()
        
        print("\n" + "=" * 40)
        print("ENVIRONMENT SMOKE TEST: PASS")
        print(f"Python Version: {sys.version.split()[0]}")
        print(f"Spark Version : {spark.version}")
        print(f"Test Execution: Processed {count} records successfully")
        print("=" * 40 + "\n")
        
        spark.stop()
    except Exception as e:
        print("\n" + "=" * 40)
        print(f"ENVIRONMENT SMOKE TEST: FAILED")
        print(f"Error Details: {str(e)}")
        print("=" * 40 + "\n")
        sys.exit(1)

if __name__ == "__main__":
    check_environment()