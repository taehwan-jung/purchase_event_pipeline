# Purchase Event Pipeline

## 📋 Project Overview

A real-time data pipeline project that collects online retail data, performs analytics, and provides machine learning-based prediction services using Kafka, Spark, and Airflow.

## 🗺️ System Architecture
<p align="center">
  <img src="https://github.com/user-attachments/assets/d6d5800a-94a8-4e85-a9bf-c1669ccdd3ff" >
</p>

## 🔄 Project Workflow

1. **Data Ingestion**: Real-time transmission of online retail transaction data through Kafka Producer.
2. **Data Processing**: Real-time collection, preprocessing, and building training datasets using Spark Streaming.
3. **Orchestration**: Automated triggering of data processing and model update workflows through Airflow.
4. **Model Serving**: Provides next purchase prediction results based on analyzed data using FastAPI.
5. **Monitoring**: Real-time metrics collection using Prometheus and system health visualization through Grafana dashboards.

## 🛠️ Requirements

- **Language**: Python 3.8+
- **Infrastructure**: Kafka 2.0+, Spark 3.0+
- **Database/Storage**: PostgreSQL, S3

## ⚡ Performance Optimization (Kafka, Spark)

### Kafka Producer Performance Optimization

- **Pandas Vectorization**: Replaced loop-based `iterrows()` with **pandas vectorization** and `to_dict()` conversion
- **Batching & Linger**: **32KB batch size** & **10ms linger** configuration
- **Compression**: **LZ4 compression** (optimized for low CPU overhead and high transmission efficiency)
- **Asynchronous I/O**: Message transmission via **asynchronous callback** system

### Kafka Performance Comparison Results (Based on 540K records test)

| Metric | Baseline | Optimized | Improvement |
|--------|----------|-----------|-------------|
| **Throughput** | 10,582.92 msg/sec | 32,526.56 msg/sec | **+207.3% (~3x)** |
| **Processing Time** | 51.21 sec | 16.66 sec | **67.5% reduction** |
| **Average Latency** | ~0.094 ms | ~0.030 ms | **~3x improvement** |

### Spark Streaming Performance Optimization

- **SQL Statement Rewriting**: Enabled **rewriteBatchedInserts** to reduce DB parsing overhead
- **Batch Size Tuning**: Set **maxOffsetsPerTrigger** from 100,000 to 1,000,000 (increased batch throughput)
- **JDBC Batch Writing**: Set **batchsize=10,000** to minimize SQL and network round-trips

### Spark Streaming Performance Comparison Results (Based on 1-minute streaming test)

| Batch Size (Rows) | Baseline Duration | Optimized Duration | Baseline TPS | Optimized TPS | Performance Change (Duration) |
|---|---|---|---|---|---|
| 100,000 | 1.70s | 1.82s | ~ 58,800 | ~ 54,945 | 🔻 -0.12s |
| 200,000 | 3.43s | 3.36s | ~ 58,500 | ~ 59,523 | 🟢 0.07s |
| 400,000 | 5.65s | 5.47s | ~ 70,800 | ~ 73,125 | 🟢 0.18s |
| 500,000 | 6.51s | 6.54s | ~ 76,800 | ~ 76,452 | ➖ same level |
| 700,000 | 9.15s | 8.65s | ~ 76,500 | ~ 80,924 | 🚀 0.50s (best efficiency) |
| 1,000,000 | 12.68s | 12.61s | ~ 78,800 | ~ 79,302 | 🟢 0.07s |
