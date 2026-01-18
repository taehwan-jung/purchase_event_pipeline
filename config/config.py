import os
from dotenv import load_dotenv

load_dotenv()

# .env 파일 로드
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')

# 환경 변수 
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Kafka 
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', "localhost:9092")  
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'purchase-events')

# CSV 파일 경로
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", os.path.join(BASE_DIR, "data", "Online Retail.csv"))

# Postgre 
POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")
POSTGRES_JDBC_DRIVER = os.getenv("POSTGRES_JDBC_DRIVER")

# S3 
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")