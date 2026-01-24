from airflow import DAG
import pandas as pd
from pyspark.sql import SparkSession
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import xgboost as xgb
from sqlalchemy import create_engine
import os
import sys
import pendulum

# Path setup
sys.path.append('/opt')
from config.config import AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID

# Korea timezone setting
local_tz = pendulum.timezone("Asia/Seoul")

def train_purchase_interval_model():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Airflow_ML_Training") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .getOrCreate()

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")

    try:
        # Load S3 data mart
        df_spark = spark.read.parquet("s3a://purchase-pipeline/purchase_data_mart")
        data = df_spark.toPandas()
        spark.stop()

        # Keep only data where purchase cycle is greater than 0 (data with 0 interferes with model training)
        data = data[data['avg_purchase_interval'] > 0]

        print(f"✅ S3 data loaded successfully: {len(data)} rows")
    except Exception as e:
        print(f"❌ Data load failed: {e}")
        return

    if data.empty or len(data) < 10:
        print("⚠ Insufficient data, skipping training.")
        return

    # Feature and target setup
    target = 'avg_purchase_interval'
    features = [
        'recency', 'frequency', 'avg_order_value',
        'avg_shopping_hour', 'weekend_purchase_ratio', 'distinct_item_count',
        'std_purchase_interval', 'last_purchase_interval'
    ]

    X = data[features]
    y = data[target]

    # Model training (Regressor)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Regressor
    model = xgb.XGBRegressor(
        n_estimators=100,
        learning_rate=0.1,
        max_depth=5,
        objective='reg:squarederror',
        random_state=42
    )

    print("🚀 Starting regression model training...")
    model.fit(X_train, y_train)

    # Evaluation log output
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    print(f"✅ Training completed! Mean Absolute Error (MAE): {mae:.2f} days")

    # Save model (shared path with FastAPI)
    model_dir = "/opt/airflow/models"
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)

    save_path = os.path.join(model_dir, "customer_model.json")
    model.save_model(save_path)
    print(f"✅ Model saved successfully: {save_path}")

# DAG configuration
default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'customer_vp_full_pipeline_v1',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    preprocess_data = BashOperator(
        task_id='spark_preprocessing',
        bash_command='export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && export PATH=$JAVA_HOME/bin:$PATH && python3 /opt/spark/spark_process.py',
        env={'MSYS_NO_PATHCONV': '1'}
    )

    train_model = PythonOperator(
        task_id='xgboost_training',
        python_callable=train_purchase_interval_model
    )

    preprocess_data >> train_model