import pandas as pd
import numpy as np
import xgboost as xgb

def predict_customer_grade():

    # 저장 된 모델 불러오기
    model_path = "/opt/spark-apps/models/customer_model.json"
    model = xgb.XGBRegressor()
    model.load_model(model_path)
    print(f"✅ 모델 로드 완료: {model_path}")

    # 가상의 고객 데이터 생성 
    new_customers = pd.DataFrame([
        [5, 20, 500.0, 14, 0.2, 10],  # 우수 고객 예상 (최근성 좋고, 빈도 높음)
        [150, 1, 10.0, 23, 0.0, 1]     # 일반 고객 예상 (오래전 방문, 빈도 낮음)
    ], columns=['recency', 'frequency', 'avg_order_value', 'avg_shopping_hour', 'weekend_purchase_ratio', 'distinct_item_count'])

    # 예측 실행
    predictions = model.predict(new_customers)
    probabilities = model.predict_proba(new_customers)

    # 결과 출력 
    for i, pred in enumerate(predictions):
        grade = "VIP" if pred == 1 else "일반"
        prob = probabilities[i][pred]*100
        print(f"고객 {i+1} 결과: {grade} (확률: {prob:.2f}%)")

if __name__ == "__main__":
    predict_customer_grade()