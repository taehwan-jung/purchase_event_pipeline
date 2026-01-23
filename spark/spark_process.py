import sys
import os


# Add library path search
sys.path.append('/home/airflow/.local/lib/python3.8/site-packages')
sys.path.append('/opt')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, count, abs
from pyspark.sql.functions import max, countDistinct, sum, datediff, lit
from pyspark.sql.functions import hour, dayofweek, avg
from config.config import POSTGRES_URL, POSTGRES_PASSWORD, POSTGRES_TABLE, POSTGRES_USER, POSTGRES_JDBC_DRIVER, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# 1. Create Spark session
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

    # AWS authentication setup
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") # Seoul
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    spark.sparkContext.setLogLevel("WARN")
    return spark

# 2. Read streaming results from PostgreSQL
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

        print(f"✅ Data successfully loaded from PostgreSQL table '{POSTGRES_TABLE}'.")
        df_raw.printSchema()
        return df_raw
    except Exception as e:
        print(f"Error occurred while loading data: {e}")
        return None

# 3. Cleansing (cancellations, outliers, types, derived columns, deduplication)

def cleanse_data(df):
    if df is None:
        return None

    print("🚀 Starting data preprocessing...")

    # Remove duplicates
    df_cleaned = df.dropDuplicates()

    # Handle cancelled orders
    # InvoiceNo starting with 'C' indicates return/cancellation data
    df_cleaned = df_cleaned.filter(~col('invoice_no').startswith("C"))

    # Handle outliers (quantity 0 or less is an outlier)
    df_cleaned = df_cleaned.filter((col("quantity")> 0) & (col("unit_price") > 0))

    # Handle CustomerID missing values (delete records without customerID)
    df_cleaned = df_cleaned.dropna(subset=["customer_id"])

    # Create derived column (total sales amount)
    df_cleaned = df_cleaned.withColumn("total_amount", col("quantity")* col("unit_price"))

    print(f"✅ Data preprocessing completed. {df_cleaned.count()} rows retained.")

    return df_cleaned

# 4. Create RFM features (recency, monetary, frequency)
def create_rfm_features(df_cleaned):
    if df_cleaned is None:
        return None
    print("📈 Starting RFM feature creation.")

    # Set max_date because the data is historical, need to specify the last date as the most recent / Use collect() for Spark, lit converts to constant column
    max_date = df_cleaned.select(max("invoice_date")).collect()[0][0]
    reference_date = lit(max_date)

    # recency = reference date - last purchase date, frequency = unique order count, monetary = total purchase sum (customer-related features)
    rfm_df = df_cleaned.groupBy("customer_id").agg(datediff(reference_date, max("invoice_date")).alias("recency"),
                                                    countDistinct("invoice_no").alias("frequency"),
                                                      sum("total_amount").alias("monetary"))

    # Average order value per customer
    rfm_df = rfm_df.withColumn("avg_order_value", col("monetary") / col("frequency"))
    print(f"RFM feature creation completed: {rfm_df.count()} customer records")
    return rfm_df

def create_purchase_interval_features(df_cleaned):
    # 1. Create a 'Window' to line up time chronologically per customer
    # partitionBy: Group by customer
    # orderBy: Sort by date within each group
    window_spec = Window.partitionBy("customer_id").orderBy("invoice_date")

    # 2. Extract order dates and remove duplicates (treat multiple purchases on same day as one 'purchase time')
    df_intervals = df_cleaned.select("customer_id", "invoice_no", "invoice_date").distinct()

    # 3. [KEY] Use F.lag to create 'previous purchase date' column
    # Get the value right above the current row's invoice_date and put it in 'prev_invoice_date'
    df_intervals = df_intervals.withColumn(
        "prev_invoice_date",
        F.lag("invoice_date").over(window_spec)
    )

    # 4. Calculate difference (Interval) between two dates
    # datediff(later date, earlier date) -> result is integer (days)
    df_intervals = df_intervals.withColumn(
        "days_since_last_purchase",
        F.datediff(F.col("invoice_date"), F.col("prev_invoice_date"))
    )

    # 5. Calculate statistics of purchase intervals per customer
    # If a customer purchased 10 times, there are 9 intervals. Calculate mean and std of these 9
    interval_stats = df_intervals.groupBy("customer_id").agg(
        F.avg("days_since_last_purchase").alias("avg_purchase_interval"),    # Average cycle
        F.stddev("days_since_last_purchase").alias("std_purchase_interval"), # Cycle consistency
        F.last("days_since_last_purchase").alias("last_purchase_interval")   # Most recent cycle
    )

    return interval_stats

# Create features for XGBoost
def create_final_features(df_cleaned, rfm_df):
    print("🧪 Starting final feature creation for XGBoost....")

    # Create time-related features, 1(Sun) ~ 7(Sat). Usually 1,7 are weekends
    df_with_time = df_cleaned.withColumn("order_hour", hour(col("invoice_date"))) \
                             .withColumn("is_weekend", when(dayofweek(col("invoice_date")).isin(1,7), 1).otherwise(0))

    # Aggregate time patterns per customer
    # Average shopping hour
    # Weekend purchase ratio
    # Distinct item count purchased
    customer_time_features = df_with_time.groupBy("customer_id").agg(avg("order_hour").alias("avg_shopping_hour"), \
                                                                avg("is_weekend").alias("weekend_purchase_ratio"), \
                                                                countDistinct("stock_code").alias("distinct_item_count"))

    # Combine RFM data with time pattern data
    final_mart = rfm_df.join(customer_time_features, on="customer_id", how="inner")

    # Add country information
    # customer_country = df_cleaned.select("customer_id" , "country").dropDuplicates(["customer_id"])
    # final_mart = final_mart.join(customer_country, on="customer_id", how="inner")

    print(f"✅ Final feature mart creation completed!")
    return final_mart

# Save results to S3
def save_to_s3(df, bucket_name, folder_path):
    full_path = f"s3a://{bucket_name}/{folder_path}"
    print(f"📦 Saving to S3... Path: {full_path}")

    try:
        df.write.mode("overwrite").parquet(full_path)
        print("✅ S3 save completed!")

    except Exception as e:
        print(f"❌ S3 save failed: {e}")

# Save results to PostgreSQL
def save_to_postgres(final_mart):
    """
    Save preprocessed mart data to PostgreSQL.
    """
    POSTGRES_MART_TABLE = "purchase_data_mart"  # Must use different name from source!

    print(f"🚀 [Mart Save] Starting data mart save (Rows: {final_mart.count()})")

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
    print(f"✅ [Mart Save] {POSTGRES_MART_TABLE} table save completed!")

# main
def main():
    # Create Spark session
    spark = create_spark_session()

    # Load data
    df_raw = load_raw_transaction(spark)

    if df_raw is not None:
        df_cleaned = cleanse_data(df_raw) # Data preprocessing

        # Delete records without customer_id
        df_cleaned = df_cleaned.filter(col("customer_id").isNotNull())

        if df_cleaned is not None:
            # Create recency, frequency, monetary
            rfm_df = create_rfm_features(df_cleaned)

            # Create purchase interval features
            interval_df = create_purchase_interval_features(df_cleaned)

            # Inner join based on customer_id
            combined_rfm = rfm_df.join(interval_df, on="customer_id", how="inner")

            # Create time patterns for XGBoost
            final_mart = create_final_features(df_cleaned, combined_rfm)

            # Fill missing values to prevent errors from customers with only one purchase when using XGBoost regression
            final_mart = final_mart.na.fill({
            "avg_purchase_interval": 0,
            "std_purchase_interval": 0,
            "last_purchase_interval": 0
            })

            # Remove NA values after join in final version
            final_mart = final_mart.dropna(how='any')

            # Change float -> int after removing missing values
            final_mart = final_mart.withColumn("customer_id", col("customer_id").cast("long"))

            final_mart.show(5)

            # Save to S3
            save_to_s3(final_mart, "purchase-pipeline" , "purchase_data_mart")

            # Save to PostgreSQL
            save_to_postgres(final_mart)

    else:
        print("🛑 Failed to load data, terminating process!")

    spark.stop()

if __name__ == "__main__":
    main()