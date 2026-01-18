"""
데이터 수집기
구매 이력 데이터를 사용하여 데이터를 수집하고 kafka로 전송
"""
import pandas as pd
import json
import os
import sys
import time
from datetime import datetime
from kafka import KafkaProducer
from config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, CSV_FILE_PATH
from utils.log_utils import setup_logger

# 다른 모듈 import를 위한 상위디렉토리 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 로깅 설정
logger = setup_logger("producer", "logs/producer.log")

# producer 생성
def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logger.info(f"kafka producer 연결 성공: {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error("Kafka producer 연결 실패: {e}")
        return None
    
def collect_data(producer):
    if not producer:
        logger.info("producer가 없습니다.")
        return
    logger.info("데이터 수집을 시작합니다.")
    
    # CSV 파일 읽기

    df = pd.read_csv(CSV_FILE_PATH, encoding="utf-8")

    # Kafka 메시지 전송
    for _, row in df.iterrows():
        message = {
            "invoice_no": str(row["InvoiceNo"]),
            "stock_code": str(row["StockCode"]),
            "description": str(row["Description"]),
            "quantity": int(row["Quantity"]),
            "invoice_date": str(row["InvoiceDate"]),
            "unit_price": float(row["UnitPrice"]),
            "customer_id": str(row["CustomerID"]) if "CustomerID" in row else None,
            "country": str(row["Country"])
        }

        try:
            producer.send(KAFKA_TOPIC, value=message)
            logger.info(f"메시지 전송 성공: {message['invoice_no']}")
        except Exception as e:
            logger.error(f"메시지 전송 실패: {e}")

        time.sleep(0.01) # 0.01초 간격

    producer.flush()
    logger.info("모든 메시지 전송 완료")



def main():
    logger.info("데이터 수집 시작")
    producer = create_producer()
    
    try:
        collect_data(producer)
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 종료되었습니다.")
    except Exception as e:
        logger.error(f"오류 발생: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("kafka producer 연결 종료")

if __name__ == "__main__":
    main()
