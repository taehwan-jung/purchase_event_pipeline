from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from config.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
import os

def create_view_session():
    spark = SparkSession.builder \
        .appName("check_s3_data") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4," 
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .getOrCreate()
    
# S3 authentication settings
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    return spark

def main():
    spark = create_view_session()

    s3_path = "s3a://purchase-pipeline/purchase_data_mart"

    print(f"🔍 Loading S3 data... Path: {s3_path}")

    try:
        # Read Parquet
        df = spark.read.parquet(s3_path)

        # Check schema (verify column types are preserved)
        df.printSchema()

        # Check feature summary statistics
        # To check if NULLs are properly filled with 0, verify min value is 0
        print("📊 Purchase cycle feature summary statistics:")
        target_cols = ["avg_purchase_interval", "std_purchase_interval", "last_purchase_interval"]
        df.describe(target_cols).show()

        # ✅ Modified null check logic: using F.sum and isNull().cast("int")
        print("🔎 Null check (should be 0):")
        df.select([
            F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
            for c in target_cols
        ]).show()

        print(f"✅ Total customers: {df.count()}")

    except Exception as e:
        print(f"❌ Error occurred: {e}")
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
    