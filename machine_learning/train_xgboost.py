import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
from sklearn.metrics import mean_absolute_error, r2_score # 평가 지표 변경
from pyspark.sql import SparkSession
from config.config import AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID
import os



# load from s3
def load_data_from_s3():
    spark = SparkSession.builder \
        .appName("LoadForML") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .getOrCreate()
    
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
    
    # s3에서 읽기
    df_spark = spark.read.parquet("s3a://purchase-pipeline/purchase_data_mart")

    # pandas로 변환
    pdf = df_spark.toPandas()
    spark.stop()
    return pdf 

# main
def main():

    # 데이터로드
    data = load_data_from_s3()
    print(f"✅ 데이터 로드 완료: {len(data)} 행")
    
    target_column = 'avg_purchase_interval' 
    
    # 3. 피처 선택 (새로 만든 주기 피처들 추가) ⭐️
    features = [
        'recency', 'frequency', 'avg_order_value', 
        'avg_shopping_hour', 'weekend_purchase_ratio', 'distinct_item_count',
        'std_purchase_interval', 'last_purchase_interval'
    ]
    
    X = data[features]
    y = data[target_column] # 일수(Days) 예측

    # 4. 데이터분할
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state=42)
    
    # 5. 모델 정의 및 학습 (Regressor로 변경) ⭐️
    model = xgb.XGBRegressor(
        n_estimators=100,
        learning_rate=0.1,
        max_depth=5,
        objective='reg:squarederror', # 회귀용 목적 함수
        random_state=42
    )
    
    print("🚀 구매 주기 예측 모델(Regression) 학습 시작...")
    model.fit(X_train, y_train)

    # 6. 예측 및 평가 (평가 지표 변경) ⭐️
    y_pred = model.predict(X_test)
    print("\n [모델 평가 결과]")
    # 분류 리포트 대신 오차(Error)를 확인합니다.
    mae = mean_absolute_error(y_test, y_pred)
    print(f"Mean Absolute Error: {mae:.2f} 일") # 평균적으로 며칠이나 틀리는지
    print(f"R2 Score: {r2_score(y_test, y_pred):.4f}")

    # 7. 피처 중요도 확인 (어떤 데이터가 주기를 맞추는데 중요했나?)
    importances = pd.Series(model.feature_importances_, index=features).sort_values(ascending=False)
    print("\n [피처 중요도]")
    print(importances)

    # 8. 학습 모델 저장
    model_dir = "/opt/spark-apps/models"
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)

    save_path = os.path.join(model_dir, "customer_model.json")
    model.save_model(save_path)
    print(f" \n 모델이 성공적으로 저장되었습니다: {save_path}")

if __name__ == "__main__":
    main()

