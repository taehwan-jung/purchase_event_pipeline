# Purchase Event Pipeline

## 📋 Project Overview

A real-time data pipeline project that collects online retail data, performs analytics, and provides machine learning-based prediction services using Kafka, Spark, and Airflow.

## 🗺️ System Architecture
<p align="center">
  <img src="https://github.com/user-attachments/assets/a97d0a0f-24f4-43e1-8b20-d28889ead545" width="80%" alt="Data Pipeline Architecture">
</p>

## 🔄 Project Workflow

1. **Data Ingestion**: Real-time transmission of online retail transaction data through Kafka Producer.
2. **Data Processing**: Real-time collection, preprocessing, and building training datasets using Spark Streaming.
3. **Orchestration**: Automated triggering of data processing and model update workflows through Airflow.
4. **Model Serving**: Provides next purchase prediction results based on analyzed data using FastAPI.

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
- **Batch Size Tuning**: Set **maxOffsetsPerTrigger=30000** (increased batch throughput)
- **JDBC Batch Writing**: Set **batchsize=10,000** to minimize SQL and network round-trips

### Spark Streaming Performance Comparison Results (Based on 1-minute streaming test)

| Metric | Baseline | Optimized | Improvement |
|--------|----------|-----------|-------------|
| **Throughput** | 26,780 msg/sec | 77,380 msg/sec | **+188.9% (~3x)** |
| **Batch Input Volume** | 14,997 rows | 89,994 rows | **600% increase** |
| **DB Write Latency (addBatch)** | (14,997 rows / 290ms) | (89,994 rows / 755ms) | **Minimized latency despite 6x data increase** |
