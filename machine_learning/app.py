from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import xgboost as xgb
import pandas as pd
import uvicorn
from sqlalchemy import create_engine


# API app setup
app = FastAPI(title="Customer_purchase_interval_prediction_service")

# 1. Database connection setup
# Format: postgresql://ID:PW@HOST:PORT/DB_NAME
db_url = "postgresql://admin:admin@postgres:5432/purchasedb"

DB_URL = db_url
engine = create_engine(DB_URL)

# Load model
Model_path = "/opt/spark-apps/models/customer_model.json"
model = xgb.XGBRegressor()
model.load_model(Model_path)


@app.get("/")
def read_root():
    return {"message": "Customer purchase interval prediction api is running!"}

# 2. GET endpoint to predict by customer ID
@app.get("/predict/{customer_id}")
def predict_by_customer_id(customer_id: int):
    try:
        # Query latest preprocessed data (Features) for the customer from DB
        query = f"SELECT * FROM purchase_data_mart WHERE customer_id = '{customer_id}' LIMIT 1"
        input_df = pd.read_sql(query, engine)

        # Handle case when customer data is not found
        if input_df.empty:
            raise HTTPException(status_code=404, detail=f"Customer ID {customer_id} not found.")

        # Feature list
        features = [
        'recency', 'frequency', 'avg_order_value',
        'avg_shopping_hour', 'weekend_purchase_ratio', 'distinct_item_count',
        'std_purchase_interval', 'last_purchase_interval'
        ]

        # Perform prediction
        predicted_cycle = float(model.predict(input_df[features])[0])

        # [Additional] Business logic: Calculate remaining days
        # recency: days since last purchase
        current_recency = float(input_df['recency'].iloc[0])
        remaining_days_raw = predicted_cycle - current_recency
        remaining_days = round(remaining_days_raw, 1)


        # Determine status
        if remaining_days < 0:
            status = "Immediate Action Needed (Overdue)"
            message = f"This customer is {abs(remaining_days)} days overdue for repurchase!"
        elif remaining_days <= 3:
            status = "High Probability to Buy Soon"
            message = f"This customer is expected to repurchase within {remaining_days} days."
        else:
            status = "Stable"
            message = f"Approximately {remaining_days} days remaining until next purchase."

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
        raise he  # Pass through 404 errors etc.
    except Exception as e:
        print(f"❌ Internal server error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
