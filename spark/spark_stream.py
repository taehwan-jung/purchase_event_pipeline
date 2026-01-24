from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import time
from config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_TABLE
from utils.log_utils import setup_logger

# Logging setup
logger = setup_logger("spark_stream", "logs/spark_stream.log")

# Create spark session
def create_spark_session():
    spark = (
        SparkSession.builder
            .appName("PurchaseEventConsumer")
            .master("spark://spark-master:7077")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                    "org.postgresql:postgresql:42.7.3"
            )
            .config("spark.sql.shuffle.partitions", 3)
            .config("spark.ui.prometheus.enabled", "true")
            
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# Schema setup
schema = StructType([
    StructField("invoice_no", StringType(), True),
    StructField("stock_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("invoice_date", StringType(), True), # Convert to timestamp later
    StructField("unit_price", DoubleType(), True),
    StructField("customer_id", StringType(), True),
    StructField("country", StringType(), True)

])


# Read from Kafka
def read_from_kafka(spark):
    kafka_df = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 1000000)
            .option("kafka.group.id", "spark-streaming-consumer")
            .option("kafka.group.commit.offsets", "true")
            .option("failOnDataLoss", "false")
            .load()
    )

    raw_df = kafka_df.selectExpr("CAST(value as STRING) as json_str")
    return raw_df

# Parse JSON data
def parse_json(raw_df):
    parsed_df = (
        raw_df
            .select(from_json(col("json_str"), schema).alias("data"))
            .select("data.*")
    )
    return parsed_df

# Convert invoice_date to timestamp (window/watermark)
def add_timestamp_column(parsed_df):
    df_with_ts = (
        parsed_df
        .withColumn(
            "invoice_ts",
            to_timestamp(col("invoice_date"), "yyyy-MM-dd H:mm")
        )
        # invoice_date: Convert to timestamp to match DB column type
        .withColumn(
            "invoice_date",
            to_timestamp(col("invoice_date"), "yyyy-MM-dd H:mm")
        )
    )
    return df_with_ts

# Save to PostgreSQL in batch units
def write_to_postgres(batch_df, batch_id):
    # Start_time
    start_time = time.time()
    print("🔥 [foreachBatch] batch_id =", batch_id)
    # 1) Check if this batch actually has data
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
        .option("batchsize", "10000") # Add JDBC batch size
        .option("rewriteBatchedInserts", "true") # Performance optimization option
        .mode("append")
        .save()
    )
    print(f"Batch{batch_id} saved to postgreSQL")
    print("🔥 BATCH SIZE =", count)
    batch_df.printSchema()

    # 2. End_time
    end_time = time.time()
    duration = end_time - start_time
    
    # 3. End_time print
    print(f"⏱️ Batch Duration: {duration:.2f} seconds")


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

# PostgreSQL query
def start_postgres_query(df):
    return(
        df.writeStream
            .outputMode("append")
            .foreachBatch(write_to_postgres)
            .trigger(processingTime= '5 seconds') # Trigger every 5 seconds
            .option("checkpointLocation", "file:///opt/spark/work-dir/checkpoints/postgres")
            .start()
    )


# Main function
def main():
    # Create Spark session
    spark = create_spark_session()

    # Read JSON string from Kafka
    raw_df = read_from_kafka(spark)

    # Parse JSON with schema applied
    parsed_df = parse_json(raw_df)

    # Add timestamp column
    df_with_ts = add_timestamp_column(parsed_df)

    # Start console + PostgreSQL queries
    # console_query = start_console_query(df_with_ts)
    postgres_query = start_postgres_query(df_with_ts)

    try:
        postgres_query.awaitTermination(timeout=3600)
    except KeyboardInterrupt:
        print("Stopping query by user...")
        postgres_query.stop()
    finally:
        spark.stop()
        print("===== Final Postgres Query Progress =====")
        print(postgres_query.lastProgress)

    print("===== Final Postgres Query Progress =====")
    print(postgres_query.lastProgress)

if __name__ == "__main__":
    main()

# Test Sync