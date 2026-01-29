"""
Data Collector
Collects purchase history data and transmits it to Kafka
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

# Add parent directory for importing other modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Logging setup
logger = setup_logger("producer", "logs/producer.log")

# Create producer
def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            enable_idempotence=True,
            acks='all',
            retries=5
        )
        logger.info(f"Kafka producer connected successfully: {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Kafka producer connection failed: {e}")
        return None

def collect_data(producer):
    if not producer:
        logger.info("Producer not available.")
        return
    logger.info("Starting data collection.")

    # Performance metrics
    start_time = time.time()
    message_count = 0
    error_count = 0

    # Read CSV file
    df = pd.read_csv(CSV_FILE_PATH, encoding="utf-8")
    total_records = len(df)

    logger.info(f"Transmitting {total_records} records.")

    # Send Kafka messages
    for idx, row in enumerate(df.iterrows()):
        _, row = row
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
            message_count += 1

            # Log progress (every 10%)
            if (idx + 1) % (total_records // 10) == 0:
                progress = ((idx + 1) / total_records) * 100
                logger.info(f"Progress: {progress:.1f}% ({idx + 1}/{total_records})")

        except Exception as e:
            logger.error(f"Message send failed: {e}")
            error_count += 1

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
