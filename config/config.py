import os
from dotenv import load_dotenv

load_dotenv()

# Load .env file
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')

# Environment variables
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', "localhost:9092")
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'purchase-events')

# CSV file path
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", os.path.join(BASE_DIR, "data", "Online Retail.csv"))

# PostgreSQL
POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")
POSTGRES_JDBC_DRIVER = os.getenv("POSTGRES_JDBC_DRIVER")

# S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")