from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import time
from config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_TABLE
from utils.log_utils import setup_logger

# 로깅 설정
logger = setup_logger("spark_stream", "logs/spark_stream.log")

# Create spark
def create_spark_session():
    spark = (
        SparkSession.builder
            .appName("PurchaseEventConsumer")
            .master("spark://spark-master:7077")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                    "org.postgresql:postgresql:42.7.3"
            )
            .config("spark.sql.shuffle.partitions", 3)
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

# Schema 설정
schema = StructType([
    StructField("invoice_no", StringType(), True),
    StructField("stock_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("invoice_date", StringType(), True), # 나중에 timestamp로 
    StructField("unit_price", DoubleType(), True),
    StructField("customer_id", StringType(), True),
    StructField("country", StringType(), True)

])


# Read from kafka
def read_from_kafka(spark):
    kafka_df = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 5000)
            .load()
    )

    raw_df = kafka_df.selectExpr("CAST(value as STRING) as json_str")
    return raw_df

# Parsed_df
def parse_json(raw_df):
    parsed_df = (
        raw_df
            .select(from_json(col("json_str"), schema).alias("data"))
            .select("data.*")
    )
    return parsed_df


# invoice_date timestamp로 변환(윈도우/워터마크)
def add_timestamp_column(parsed_df):
    df_with_ts = (
        parsed_df
        .withColumn(
            "invoice_ts",
            to_timestamp(col("invoice_date"), "yyyy-MM-dd H:mm")
        )
        # invoice_date: DB 컬럼과 타입 맞추기 위해 timestamp로 변환
        .withColumn(
            "invoice_date",
            to_timestamp(col("invoice_date"), "yyyy-MM-dd H:mm")
        )
    )
    return df_with_ts

# postgreSQL에 배치 단위 저장
def write_to_postgres(batch_df, batch_id):
    print("🔥 [foreachBatch] batch_id =", batch_id)    
    # 1) 이 배치에 실제로 데이터가 있는지 확인
    count = batch_df.count()
    print("🔥 [foreachBatch] row count =", count)
    (
        batch_df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", POSTGRES_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    print(f"Batch{batch_id} saved to postgreSQL")
    print("🔥 BATCH SIZE =", batch_df.count())
    batch_df.printSchema()



# Console query
def start_console_query(df):
    query = (
        df.writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", False)
            .option("numRows", 20)
            .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/console")
            .start()
    )
    return query 

# PoistgreSQL query
def start_postgres_query(df):
    return(
        df.writeStream
            .outputMode("append")
            .foreachBatch(write_to_postgres)
            .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/postgres")
            .start()
    )


# def main
def main():
    # 스파크 세션 생성
    spark = create_spark_session()

    # kafka로 json 스트링 읽기
    raw_df = read_from_kafka(spark)

    # 스키마 적용 json 파싱
    parsed_df = parse_json(raw_df)

    # timestamp 컬럼 추가
    df_with_ts = add_timestamp_column(parsed_df)

    # console + postgresql 실행
    console_query = start_console_query(df_with_ts)
    postgres_query = start_postgres_query(df_with_ts)

    # query.awaitTermination()
    time.sleep(60)  # 1분
    console_query.stop()
    postgres_query.stop()
    spark.stop()

    print("===== Final Postgres Query Progress =====")
    print(postgres_query.lastProgress)

if __name__ == "__main__":
    main()

