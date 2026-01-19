# Purchase Event Pipeline

Kafka 기반 구매 이벤트 데이터 파이프라인 프로젝트입니다.

## 프로젝트 구조

```
purchase_event_pipeline/
├── src/
│   ├── producer/
│   │   └── producer.py          # Kafka Producer
│   └── consumer/
│       └── consumer.py          # Kafka Consumer
├── config/
│   └── config.py                # 설정 파일
├── utils/
│   └── log_utils.py             # 로깅 유틸리티
├── data/
│   └── Online Retail.csv        # 구매 이벤트 데이터
└── logs/                        # 로그 디렉토리
```

## 브랜치 구조

### main 브랜치
최적화 전 기본 구현 버전입니다.

### perf-opt 브랜치
성능 최적화가 적용된 버전입니다.

## 성능 최적화 비교

두 브랜치 간의 차이를 확인하여 최적화 효과를 파악할 수 있습니다.

### 브랜치 비교 방법

#### 1. Git Diff로 코드 변경사항 확인
```bash
# main 브랜치와 perf-opt 브랜치의 차이 확인
git diff main perf-opt

# producer.py 파일만 비교
git diff main perf-opt -- src/producer/producer.py
```

#### 2. GitHub/GitLab PR 비교
웹 인터페이스에서 Pull Request를 생성하여 시각적으로 비교할 수 있습니다.
```bash
# perf-opt 브랜치를 main과 비교하는 PR 생성
```

#### 3. 각 브랜치에서 직접 실행하여 성능 비교

**main 브랜치 (최적화 전) 실행:**
```bash
git checkout main
python src/producer/producer.py
```

**perf-opt 브랜치 (최적화 후) 실행:**
```bash
git checkout perf-opt
python src/producer/producer.py
```

### 적용된 최적화 기법

perf-opt 브랜치에 적용된 주요 최적화:

1. **데이터 전처리 (Vectorization)**
   - Pandas 벡터화 연산으로 타입 변환
   - 루프 전 결측치 처리
   - `to_dict('records')`로 일괄 변환

2. **배치 처리 (Batching)**
   - `batch_size`: 32KB
   - `linger_ms`: 10ms

3. **압축 (Compression)**
   - `compression_type`: lz4
   - 네트워크 대역폭 절약

4. **비동기 전송**
   - Sleep 제거
   - 비동기 메시지 전송

5. **버퍼 최적화**
   - `buffer_memory`: 64MB
   - `max_in_flight_requests_per_connection`: 5

## 사용 방법

### 1. 환경 설정

```bash
pip install kafka-python pandas python-dotenv
```

### 2. Kafka 실행

```bash
# Docker Compose로 Kafka 실행 (옵션)
docker-compose up -d
```

### 3. Producer 실행

```bash
python src/producer/producer.py
```

### 4. Consumer 실행

```bash
python src/consumer/consumer.py
```

## 요구사항

- Python 3.8+
- Kafka 2.0+
- pandas
- kafka-python

## 설정

`config/config.py` 파일에서 Kafka 연결 정보를 설정합니다:

```python
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'purchase-events'
CSV_FILE_PATH = 'data/Online Retail.csv'
```

## 라이선스

MIT License
