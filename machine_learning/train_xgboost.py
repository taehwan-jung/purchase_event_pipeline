import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
from sklearn.metrics import mean_absolute_error, r2_score # Change evaluation metrics
from pyspark.sql import SparkSession
from config.config import AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID
import os



# Load from S3
def load_data_from_s3():
    spark = SparkSession.builder \
        .appName("LoadForML") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .getOrCreate()

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")

    # Read from S3
    df_spark = spark.read.parquet("s3a://purchase-pipeline/purchase_data_mart")

    # Convert to pandas
    pdf = df_spark.toPandas()
    spark.stop()
    return pdf

# Main
def main():

    # Load data
    data = load_data_from_s3()
    print(f"✅ Data loaded successfully: {len(data)} rows")

    target_column = 'avg_purchase_interval'

    # 3. Feature selection (add newly created cycle features)
    features = [
        'recency', 'frequency', 'avg_order_value',
        'avg_shopping_hour', 'weekend_purchase_ratio', 'distinct_item_count',
        'std_purchase_interval', 'last_purchase_interval'
    ]

    X = data[features]
    y = data[target_column] # Predict days

    # 4. Data split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state=42)

    # 5. Define and train model (changed to Regressor)
    model = xgb.XGBRegressor(
        n_estimators=100,
        learning_rate=0.1,
        max_depth=5,
        objective='reg:squarederror', # Regression objective function
        random_state=42
    )

    print("🚀 Starting purchase cycle prediction model (Regression) training...")
    model.fit(X_train, y_train)

    # 6. Predict and evaluate (changed evaluation metrics)
    y_pred = model.predict(X_test)
    print("\n [Model Evaluation Results]")
    # Check error instead of classification report
    mae = mean_absolute_error(y_test, y_pred)
    print(f"Mean Absolute Error: {mae:.2f} days") # How many days off on average
    print(f"R2 Score: {r2_score(y_test, y_pred):.4f}")

    # 7. Check feature importance (which data was important for predicting the cycle?)
    importances = pd.Series(model.feature_importances_, index=features).sort_values(ascending=False)
    print("\n [Feature Importance]")
    print(importances)

    # 8. Save trained model
    model_dir = "/opt/spark-apps/models"
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)

    save_path = os.path.join(model_dir, "customer_model.json")
    model.save_model(save_path)
    print(f" \n Model saved successfully: {save_path}")

if __name__ == "__main__":
    main()

