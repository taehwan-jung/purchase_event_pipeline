# Purchase Event Pipeline

## Project 개요

Kafka, Spark, Airflow를 활용하여 온라인 소매 데이터를 실시간으로 수집하고, 분석 및 머신러닝 기반의 예측 서비스를 제공하는 데이터 파이프라인 프로젝트입니다.

## Project Workflow 

1. **Data Ingestion**: 온라인 소매 거래 데이터를 Kafka Producer를 통해 실시간 전송합니다.
2. **Data Processing**: Spark Streaming을 활용한 실시간 수집, 전처리 및 학습용 데이터셋 구축합니다.
3. **Orchestration**: Airflow를 통한 데이터 가공 및 모델 업데이트 과정을 자동 트리거합니다.
4. **Model Serving**: FastAPI를 이용해 분석된 데이터를 바탕으로 다음 구매 시점 예측 결과를 제공합니다.

## 요구사항

- **Language**: Python 3.8+
- **Infrastructure**: Kafka 2.0+, Spark 3.0+
- **Database/Storage**: PostgreSQL, S3

## ⚡성능 최적화(Kafka, Spark)

### Kafka producer 성능 최적화

- **Pandas Vectorization**: 반복문('iterrows') 대신 **pandas 벡터연산** 과 'to_dict' 변환
- **Batching & Linger**: **32KB 배치 크기** & **10ms** 지연 설정
- **Compression**: **LZ4 압축** (낮은 CPU 부하와 고성능 전송 효율 고려)
- **Asynchronous I/O**: **비동기 콜백** 시스템의 메시지 전송

### Kafka 성능 비교 결과 (데이터 54만건 테스트 기준)

| 지표 | 기존 (Baseline) | 최적화 (Optimized) | 개선율 |
|------|----------------|-------------------|--------|
| **처리량** | 10,582.92 msg/sec | 32,526.56 msg/sec | **+207.3% (약 3배)** |
| **소요 시간** | 51.21초 | 16.66초 | **67.5% 단축** |
| **평균 레이턴시** | 약 0.094 ms | 약 0.030 ms | **약 3배 향상** |

