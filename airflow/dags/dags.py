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

# 경로 설정
sys.path.append('/opt')
from config.config import AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID

# 한국 시간 설정
local_tz = pendulum.timezone("Asia/Seoul")

def train_purchase_interval_model():
    # Spark 세션 생성
    spark = SparkSession.builder \
        .appName("Airflow_ML_Training") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .getOrCreate()

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")

    try:
        # S3 데이터 마트 로드
        df_spark = spark.read.parquet("s3a://purchase-pipeline/purchase_data_mart")
        data = df_spark.toPandas()
        spark.stop()

        # 구매 주기가 0보다 큰 데이터만 남김 (0인 데이터는 모델학습을 방해함)
        data = data[data['avg_purchase_interval'] > 0]

        print(f"✅ S3 데이터 로드 완료: {len(data)} 행")
    except Exception as e:
        print(f"❌ 데이터 로드 실패: {e}")
        return

    if data.empty or len(data) < 10:
        print("⚠ 데이터가 부족하여 학습을 건너뜁니다.")
        return

    # 피처 및 타겟 설정 
    target = 'avg_purchase_interval'
    features = [
        'recency', 'frequency', 'avg_order_value', 
        'avg_shopping_hour', 'weekend_purchase_ratio', 'distinct_item_count',
        'std_purchase_interval', 'last_purchase_interval' 
    ]
    
    X = data[features]
    y = data[target]

    # 모델 학습 (Regressor)    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Regressor
    model = xgb.XGBRegressor(
        n_estimators=100, 
        learning_rate=0.1, 
        max_depth=5, 
        objective='reg:squarederror', 
        random_state=42
    )
    
    print("🚀 회귀 모델 학습 시작...")
    model.fit(X_train, y_train)

    # 평가 로그 출력
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    print(f"✅ 학습 완료! 평균 오차(MAE): {mae:.2f}일")

    # 모델 저장 (FastAPI와 공유되는 경로)
    model_dir = "/opt/airflow/models"
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)

    save_path = os.path.join(model_dir, "customer_model.json")
    model.save_model(save_path)
    print(f"✅ 모델 자동 저장 성공: {save_path}")

# DAG 설정
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