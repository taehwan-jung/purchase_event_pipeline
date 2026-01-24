import pandas as pd
import numpy as np
import xgboost as xgb

def predict_customer_grade():

    # Load saved model
    model_path = "/opt/spark-apps/models/customer_model.json"
    model = xgb.XGBRegressor()
    model.load_model(model_path)
    print(f"✅ Model loaded successfully: {model_path}")

    # Generate sample customer data
    new_customers = pd.DataFrame([
        [5, 20, 500.0, 14, 0.2, 10],  # Expected premium customer (good recency, high frequency)
        [150, 1, 10.0, 23, 0.0, 1]     # Expected regular customer (old visit, low frequency)
    ], columns=['recency', 'frequency', 'avg_order_value', 'avg_shopping_hour', 'weekend_purchase_ratio', 'distinct_item_count'])

    # Execute prediction
    predictions = model.predict(new_customers)
    probabilities = model.predict_proba(new_customers)

    # Output results
    for i, pred in enumerate(predictions):
        grade = "VIP" if pred == 1 else "Regular"
        prob = probabilities[i][pred]*100
        print(f"Customer {i+1} result: {grade} (Probability: {prob:.2f}%)")


if __name__ == "__main__":
    predict_customer_grade()