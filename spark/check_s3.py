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
    
# S3 인증 설정
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    return spark     

def main():
    spark = create_view_session()

    s3_path = "s3a://purchase-pipeline/purchase_data_mart"
    
    print(f"🔍 S3 데이터를 불러오는 중... 경로: {s3_path}")

    try:
        # Parquet 읽기
        df = spark.read.parquet(s3_path)
        
        # 스키마 확인 (컬럼 타입이 잘 유지되었는지)
        df.printSchema()
        
        # 피처 요약 통계 확인 
        # NULL이 0으로 잘 채워졌는지 보려면 min값이 0인지 확인하면 됩니다.
        print("📊 신규 구매 주기 피처 요약 통계:")
        target_cols = ["avg_purchase_interval", "std_purchase_interval", "last_purchase_interval"]
        df.describe(target_cols).show()
        
        # ✅ 결측치 체크 로직 수정: F.sum과 isNull().cast("int") 사용
        print("🔎 결측치 체크 (0이어야 정상):")
        df.select([
            F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") 
            for c in target_cols
        ]).show()
        
        print(f"✅ 총 고객 수: {df.count()}명")
        
    except Exception as e:
        print(f"❌ 에러 발생: {e}")
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
    