import sys
import os


# 라이브러리 경로 탐색 추가
sys.path.append('/home/airflow/.local/lib/python3.8/site-packages')
sys.path.append('/opt')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, count, abs
from pyspark.sql.functions import max, countDistinct, sum, datediff, lit
from pyspark.sql.functions import hour, dayofweek, avg
from config.config import POSTGRES_URL, POSTGRES_PASSWORD, POSTGRES_TABLE, POSTGRES_USER, POSTGRES_JDBC_DRIVER, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# 1. Sparksession 생성
def create_spark_session():
    spark = (
        SparkSession.builder
            .appName("Spark_process")
            # .master("spark://spark-master:7077")
            .master("local[*]")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                    "org.postgresql:postgresql:42.7.3,"
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
            )
            .config("spark.sql.shuffle.partitions", 3)
            .getOrCreate()
    )

    # AWS 인증 설정
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") # 서울
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")   

    spark.sparkContext.setLogLevel("WARN")
    return spark

# 2. 스트리밍 결과 postgres 에서 읽기
def load_raw_transaction(spark):
    try:
        df_raw = spark.read \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", POSTGRES_JDBC_DRIVER)\
            .load()
        
        print(f"✅ PostgreSQL 테이블 '{POSTGRES_TABLE}'에서 데이터를 성공적으로 로드했습니다.")
        df_raw.printSchema()
        return df_raw
    except Exception as e:
        print(f"데이터 로드 중 오류 발생: {e}")
        return None

# 3. 클렌징(취소, 이상값, 타입, 파생컬럼, 중복제거)

def cleanse_data(df):
    if df is None:
        return None
    
    print("🚀데이터 전처리를 시작합니다...")

    # 중복 제거
    df_cleaned = df.dropDuplicates()

    # 취소 주문 처리
    # InvoiceNo 이 'c'인것은 반품/취소 데이터
    df_cleaned = df_cleaned.filter(~col('invoice_no').startswith("C"))

    # 이상치 처리 (수량이 0이하인것은 이상치임)
    df_cleaned = df_cleaned.filter((col("quantity")> 0) & (col("unit_price") > 0)) 

    # CustomerID 결측치 처리(customerID가 없는 값은 삭제함)
    df_cleaned = df_cleaned.dropna(subset=["customer_id"])

    # 파생컬럼 생성 (총 판매액)
    df_cleaned = df_cleaned.withColumn("total_amount", col("quantity")* col("unit_price"))

    print(f"✅ 데이터 전처리 완료 {df_cleaned.count()} 개의 행이 유지되었습니다.")

    return df_cleaned

# 4.RFM 피처 생성 (recency(최근성), monetary(금액), freqency(빈도))
def create_rfm_features(df_cleaned):
    if df_cleaned is None:
        return None
    print("📈 RFM 피처 생성을 시작합니다.")

    # max_date를 설정하는 이유는 데이터가 과거의 데이터이기 때문에, 마지막 날짜가 가장 최근의 데이터라는 점을 지정해줘야함 / 스파크 사용하기에 collect() 사용하고, lit는 상수컬럼으로 변환
    max_date = df_cleaned.select(max("invoice_date")).collect()[0][0]
    reference_date = lit(max_date)

    # recency = 기준일 - 마지막 구매일, frequency = 고유한 주문 수, monetary = 총 구매 합계 (고객별 관련 피처)
    rfm_df = df_cleaned.groupBy("customer_id").agg(datediff(reference_date, max("invoice_date")).alias("recency"),
                                                    countDistinct("invoice_no").alias("frequency"),
                                                      sum("total_amount").alias("monetary"))

    # 고객당 평균 주문 금액
    rfm_df = rfm_df.withColumn("avg_order_value", col("monetary") / col("frequency"))
    print(f"RFM 피처 생성 완료: {rfm_df.count()} 명의 고객 데이터")
    return rfm_df

def create_purchase_interval_features(df_cleaned):
    # 1. 고객별로 시간을 한 줄로 세우는 '기준(Window)' 만들기
    # partitionBy: 고객별로 그룹을 묶음
    # orderBy: 그 안에서 날짜순으로 정렬
    window_spec = Window.partitionBy("customer_id").orderBy("invoice_date")
    
    # 2. 주문일자만 뽑아서 중복 제거 (같은 날 여러 개 사도 하나의 '주문 시점'으로 취급)
    df_intervals = df_cleaned.select("customer_id", "invoice_no", "invoice_date").distinct()
    
    # 3. [핵심] F.lag를 사용해 '직전 구매일' 컬럼 생성
    # 현재 행의 invoice_date 바로 위에 있는 값을 가져와서 'prev_invoice_date'에 넣음
    df_intervals = df_intervals.withColumn(
        "prev_invoice_date", 
        F.lag("invoice_date").over(window_spec)
    )
    
    # 4. 두 날짜의 차이(Interval) 계산
    # datediff(나중 날짜, 이전 날짜) -> 결과는 정수(일수)
    df_intervals = df_intervals.withColumn(
        "days_since_last_purchase", 
        F.datediff(F.col("invoice_date"), F.col("prev_invoice_date"))
    )
    
    # 5. 고객별로 구매 간격들의 통계량 계산
    # 한 고객이 10번 샀다면 간격은 9개가 생김. 이 9개의 평균과 표준편차를 구함
    interval_stats = df_intervals.groupBy("customer_id").agg(
        F.avg("days_since_last_purchase").alias("avg_purchase_interval"),    # 평균 주기
        F.stddev("days_since_last_purchase").alias("std_purchase_interval"), # 주기 일관성
        F.last("days_since_last_purchase").alias("last_purchase_interval")   # 가장 최근 주기
    )
    
    return interval_stats

# XGboost용 피처 생성
def create_final_features(df_cleaned, rfm_df):
    print("🧪 XGboost용 최종 피처 생성을 시작합니다....")

    # 시간 관련 피쳐 생성, 1(일) ~ 7(토). 보통 1,7일 주말
    df_with_time = df_cleaned.withColumn("order_hour", hour(col("invoice_date"))) \
                             .withColumn("is_weekend", when(dayofweek(col("invoice_date")).isin(1,7), 1).otherwise(0))

    # 고객별 시간 패턴 집계
    # 평균 구매 시간대
    # 주말 구매 비율
    # 구매한 유니크한 상품 수
    customer_time_features = df_with_time.groupBy("customer_id").agg(avg("order_hour").alias("avg_shopping_hour"), \
                                                                avg("is_weekend").alias("weekend_purchase_ratio"), \
                                                                countDistinct("stock_code").alias("distinct_item_count"))

    # RFM 데이터와 시간 패턴 데이터 결합
    final_mart = rfm_df.join(customer_time_features, on="customer_id", how="inner")

    # 국가 정보 추가
    # customer_country = df_cleaned.select("customer_id" , "country").dropDuplicates(["customer_id"])
    # final_mart = final_mart.join(customer_country, on="customer_id", how="inner")

    print(f"✅ 최종 피처 마트 생성 완료!")
    return final_mart

# 결과 저장 s3 save_to s3
def save_to_s3(df, bucket_name, folder_path):
    full_path = f"s3a://{bucket_name}/{folder_path}"
    print(f"📦 s3 저장 중... 경로{full_path}")

    try:
        df.write.mode("overwrite").parquet(full_path)
        print("✅ S3 저장 완료!")

    except Exception as e:
        print(f"❌ S3 저장 실패: {e}")

#  결과 저장 save_to_postgre 
def save_to_postgres(final_mart):
    """
    전처리된 마트 데이터를 PostgreSQL에 저장합니다.
    """
    POSTGRES_MART_TABLE = "purchase_data_mart"  # 원본과 다른 이름 필수!
    
    print(f"🚀 [Mart Save] 데이터 마트 저장 시작 (Rows: {final_mart.count()})")
    
    (
        final_mart.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", POSTGRES_MART_TABLE)  
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_JDBC_DRIVER)
        .mode("overwrite")
        .save()
    )
    print(f"✅ [Mart Save] {POSTGRES_MART_TABLE} 테이블 저장 완료!")

# main
def main():
    # 스파크 세션 생성
    spark = create_spark_session()

    # 데이터 로드
    df_raw = load_raw_transaction(spark)

    if df_raw is not None:
        df_cleaned = cleanse_data(df_raw) # 데이터 전처리
                    
        # customer_id  가 없는 경우 삭제
        df_cleaned = df_cleaned.filter(col("customer_id").isNotNull())

        if df_cleaned is not None:
            # recency, frequency, monetary 생성 
            rfm_df = create_rfm_features(df_cleaned)
            
            # 구매 간격 피처 생성
            interval_df = create_purchase_interval_features(df_cleaned)
            
            # customer_id 기준 inner join
            combined_rfm = rfm_df.join(interval_df, on="customer_id", how="inner")
            
            # xgboost용 시간패턴 생성 
            final_mart = create_final_features(df_cleaned, combined_rfm)

            # 추후에 xgboost regression 사용시 한 번만 구매한 고객의 오류를 방지하기 위함 (결측치 채우기)
            final_mart = final_mart.na.fill({
            "avg_purchase_interval": 0, 
            "std_purchase_interval": 0,
            "last_purchase_interval": 0
            })

            # 최종 버전에서 join이후 na값이 있는 경우 제거 
            final_mart = final_mart.dropna(how='any')

            # 결측치를 제거해서 float -> int로 변경
            final_mart = final_mart.withColumn("customer_id", col("customer_id").cast("long"))

            final_mart.show(5)

            # Save to S3
            save_to_s3(final_mart, "purchase-pipeline" , "purchase_data_mart")      

            # Save tp postgres
            save_to_postgres(final_mart)      

    else:
        print("🛑 데이터를 로드하지 못해 프로세스를 종료합니다!")

    spark.stop()

if __name__ == "__main__":
    main()