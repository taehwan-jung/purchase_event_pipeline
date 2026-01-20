"""
데이터 수집기
구매 이력 데이터를 사용하여 데이터를 수집하고 kafka로 전송
"""
import pandas as pd
import json
import time
import os
import sys
from datetime import datetime
from kafka import KafkaProducer
from config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, CSV_FILE_PATH
from utils.log_utils import setup_logger

# 다른 모듈 import를 위한 상위디렉토리 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 로깅 설정
logger = setup_logger("producer", "logs/producer.log")

def on_send_success(record_metadata):
    """메시지 전송 성공 시 호출"""
    pass 

def on_send_error(excp):
    """메시지 전송 실패 시 호출"""
    logger.error(f'메시지 전송 실패 세부 내용: {excp}')

# producer 생성
def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            acks=1,
             # 성능 최적화 설정
            batch_size=32768,           # 32KB 배치 크기
            linger_ms=10,                # 10ms 대기 후 전송
            compression_type='lz4',     # lz4 압축
            buffer_memory=67108864,      # 64MB 버퍼 
            max_in_flight_requests_per_connection=5  # 동시 요청 수
        )
        logger.info(f"kafka producer 연결 성공: {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Kafka producer 연결 실패: {e}")
        return None

def collect_data(producer):
    if not producer:
        logger.info("producer가 없습니다.")
        return
    logger.info("데이터 수집을 시작합니다.")
    
     # 성능 메트릭
    start_time = time.time()
    message_count = 0
    error_count = 0

    # CSV 파일 읽기
    df = pd.read_csv(CSV_FILE_PATH, encoding="utf-8")
    total_records = len(df)

    logger.info(f"총 {total_records}개의 레코드를 전송합니다.")
    
    
    column_mapping = {
    "InvoiceNo": "invoice_no",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "UnitPrice": "unit_price",
    "CustomerID": "customer_id",
    "Country": "country"
    }
    df = df.rename(columns=column_mapping)

    # 3. 데이터 전처리 (결측치 및 형변환)
    df['customer_id'] = df['customer_id'].fillna(0).astype(int).astype(str).replace('0', None)
    df['quantity'] = df['quantity'].astype(int)
    df['unit_price'] = df['unit_price'].astype(float)
    # 나머지 컬럼들도 문자열로 미리 변환
    for col in ['invoice_no', 'stock_code', 'description', 'invoice_date', 'country']:
        df[col] = df[col].astype(str)

    # 4. 최종 변환 
    messages = df.to_dict('records')

    

    # Kafka 메시지 전송 (비동기)
    for idx, msg in enumerate(messages):
        try:
            # 비동기 전송 (콜백 사용)
            producer.send(KAFKA_TOPIC, value=msg).add_callback(
                on_send_success
            ).add_errback(on_send_error)
            message_count += 1

            # 진행 상황 로깅 (10%마다)
            if (idx + 1) % (total_records // 10) == 0:
                progress = ((idx + 1) / total_records) * 100
                logger.info(f"진행률: {progress:.1f}% ({idx + 1}/{total_records})")

        except Exception as e:
            logger.error(f"메시지 전송 실패: {e}")
            error_count += 1

    # 모든 메시지 전송 대기
    producer.flush()

    # 성능 메트릭 계산
    end_time = time.time()
    elapsed_time = end_time - start_time
    throughput = message_count / elapsed_time if elapsed_time > 0 else 0

    logger.info("=" * 50)
    logger.info("모든 메시지 전송 완료")
    logger.info(f"총 메시지 수: {message_count}")
    logger.info(f"실패 메시지 수: {error_count}")
    logger.info(f"소요 시간: {elapsed_time:.2f}초")
    logger.info(f"처리량(Throughput): {throughput:.2f} msg/sec")
    logger.info("=" * 50)

    return {
        "total_messages": message_count,
        "failed_messages": error_count,
        "elapsed_time": elapsed_time,
        "throughput": throughput
    }

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
