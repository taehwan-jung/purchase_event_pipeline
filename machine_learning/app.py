from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import xgboost as xgb
import pandas as pd
import uvicorn
from sqlalchemy import create_engine


# api앱 설정
app = FastAPI(title="Customer_purchase_interval_prediction_service")

# 1. DB 연결 설정
# 형식: postgresql://ID:PW@HOST:PORT/DB_NAME
db_url = "postgresql://admin:admin@postgres:5432/purchasedb"

DB_URL = db_url
engine = create_engine(DB_URL)

# 모델 로드
Model_path = "/opt/spark-apps/models/customer_model.json"
model = xgb.XGBRegressor()
model.load_model(Model_path)


@app.get("/")
def read_root():
    return {"message": "Customer purchase interval prediction api is running!"}

# 2. 고객 ID로 예측하는 GET 엔드포인트 추가
@app.get("/predict/{customer_id}")
def predict_by_customer_id(customer_id: int):
    try:
        # DB에서 해당 고객의 최신 전처리 데이터(Feature) 조회
        query = f"SELECT * FROM purchase_data_mart WHERE customer_id = '{customer_id}' LIMIT 1"
        input_df = pd.read_sql(query, engine)

        # 고객 데이터가 없는 경우 처리
        if input_df.empty:
            raise HTTPException(status_code=404, detail=f"고객 번호 {customer_id}를 찾을 수 없습니다.")

        # 피처 리스트
        features = [
        'recency', 'frequency', 'avg_order_value', 
        'avg_shopping_hour', 'weekend_purchase_ratio', 'distinct_item_count',
        'std_purchase_interval', 'last_purchase_interval'
        ]
        
        # 예측 수행 
        predicted_cycle = float(model.predict(input_df[features])[0])

        # [추가] 비즈니스 로직: 남은 일수 계산
        # recency: 마지막 구매 후 지난 일수
        current_recency = float(input_df['recency'].iloc[0])
        remaining_days_raw = predicted_cycle - current_recency
        remaining_days = round(remaining_days_raw, 1)


        # 상태 판단
        if remaining_days < 0:
            status = "Immediate Action Needed (Overdue)"
            message = f"이 고객은 재구매 예상 시점이 {abs(remaining_days)}일 지났습니다!"
        elif remaining_days <= 3:
            status = "High Probability to Buy Soon"
            message = f"이 고객은 약 {remaining_days}일 내에 재구매할 것으로 예상됩니다."
        else:
            status = "Stable"
            message = f"다음 재구매까지 약 {remaining_days}일 정도 남았습니다."

        return {
            "customer_id": customer_id,
            "prediction_results": {
                "average_cycle_predicted": round(predicted_cycle, 1),
                "days_since_last_purchase": current_recency,
                "expected_remaining_days": round(remaining_days, 1)
            },
            "marketing_status": status,
            "message": message
        }
    except HTTPException as he:
        raise he  # 404 에러 등은 그대로 전달
    except Exception as e:
        print(f"❌ 내부 서버 에러 발생: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
