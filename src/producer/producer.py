"""
Data Collector
<<<<<<< HEAD
Collect purchase history data and send to Kafka

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

# Add parent directory for importing other modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Logging setup
logger = setup_logger("producer", "logs/producer.log")


def on_send_success(record_metadata):
    """Called when message send succeeds"""
    pass

def on_send_error(excp):
    """Called when message send fails"""
    logger.error(f'Message send failed details: {excp}')

# Create producer
def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            acks=1,
             # Performance optimization settings
            batch_size=32768,           # 32KB batch size
            linger_ms=10,                # Send after 10ms wait
            compression_type='lz4',     # lz4 compression
            buffer_memory=67108864,      # 64MB buffer
            max_in_flight_requests_per_connection=5  # Concurrent request count
        )
        logger.info(f"Kafka producer connected successfully: {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Kafka producer connection failed: {e}")
        return None

def collect_data(producer):
    if not producer:

        logger.info("No producer available.")
        return
    logger.info("Starting data collection.")

     # Performance metrics

    start_time = time.time()
    message_count = 0
    error_count = 0

    # Read CSV file
    df = pd.read_csv(CSV_FILE_PATH, encoding="utf-8")
    total_records = len(df)

    logger.info(f"Sending a total of {total_records} records.")
    
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

    # 3. Data preprocessing (missing values and type conversion)
    df['customer_id'] = df['customer_id'].fillna(0).astype(int).astype(str).replace('0', None)
    df['quantity'] = df['quantity'].astype(int)
    df['unit_price'] = df['unit_price'].astype(float)
    # Convert remaining columns to strings in advance
    for col in ['invoice_no', 'stock_code', 'description', 'invoice_date', 'country']:
        df[col] = df[col].astype(str)

    # 4. Final conversion
    messages = df.to_dict('records')



    # Send Kafka messages (asynchronous)
    for idx, msg in enumerate(messages):
        try:
            # Asynchronous send (using callbacks)
            producer.send(KAFKA_TOPIC, value=msg).add_callback(
                on_send_success
            ).add_errback(on_send_error)

            message_count += 1

            # Log progress (every 10%)
            if (idx + 1) % (total_records // 10) == 0:
                progress = ((idx + 1) / total_records) * 100
                logger.info(f"Progress: {progress:.1f}% ({idx + 1}/{total_records})")

        except Exception as e:
            logger.error(f"Message send failed: {e}")
            error_count += 1

    # Wait for all messages to be sent
    producer.flush()

    # Calculate performance metrics
    end_time = time.time()
    elapsed_time = end_time - start_time
    throughput = message_count / elapsed_time if elapsed_time > 0 else 0

    logger.info("=" * 50)
    logger.info("All messages transmitted successfully")
    logger.info(f"Total messages: {message_count}")
    logger.info(f"Failed messages: {error_count}")
    logger.info(f"Elapsed time: {elapsed_time:.2f} sec")
    logger.info(f"Throughput: {throughput:.2f} msg/sec")
    logger.info("=" * 50)

    return {
        "total_messages": message_count,
        "failed_messages": error_count,
        "elapsed_time": elapsed_time,
        "throughput": throughput
    }

def main():
    logger.info("Starting data collection")
    producer = create_producer()

    try:
        metrics = collect_data(producer)
        return metrics
    except KeyboardInterrupt:
        logger.info("Program terminated by user.")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer connection closed")

if __name__ == "__main__":
    main()
