📊 Media Stream Analytics – Real-Time Intelligence for Media Decisions

## 🧠 Project Overview
This project enables real-time and batch data analytics for the media industry by integrating AWS cloud-native services with Apache Spark. It helps media companies:

- Analyze viewership behavior  
- Optimize ad revenue strategies  
- Understand demographic trends  
- Automate data pipelines using Airflow and Lambda  

---

## 🏗️ Architecture & Technologies Used

### 🔧 AWS Services
- **Amazon S3** – Data lake and intermediate storage  
- **AWS Glue** – ETL (Transform, Load) and Crawlers  
- **AWS Lambda** – Triggers for real-time orchestration  
- **Amazon EMR** – Spark-based batch & stream processing  
- **Amazon Kinesis** – Real-time data ingestion  
- **Amazon Athena** – Serverless interactive SQL analytics  
- **Amazon MWAA (Airflow)** – DAG-based orchestration  

### ⚙️ Processing & Query Engines
- Apache Spark (batch & structured streaming)  
- Iceberg Table Format  
- Snowflake (optional for warehouse queries)  
- Athena SQL (for querying Iceberg/Parquet)  

### 📦 Data Formats
- CSV – Raw source files  
- Parquet – Optimized storage  
- Iceberg – ACID-compliant modern table format  
- JSON – For real-time streaming  

---

## 🚀 Project Workflow

### 📌 Phase 1: Data Generation (Local)
Generate synthetic media datasets.

```bash
vi generate_media_data.py
python3 generate_media_data.py
```

Files generated:
- `channel_metadata.csv`
- `demographics.csv`
- `ad_revenue.csv`
- `viewership_logs.csv`

---

### 📌 Phase 2: Upload Raw Files to S3
```bash
aws s3 cp *.csv s3://aahash-project2/raw/
```

---

### 📌 Phase 3: Catalog with AWS Glue Crawlers
Create and run crawlers for:
- `ad_revenue`
- `channel_metadata`
- `demographics`

Verify table creation in Glue database: `media_raw_aahash`

---

### 📌 Phase 4: Transform and Store as Iceberg
Create Spark ETL jobs using AWS Glue Studio or submit jobs to EMR.

Example target paths:
- `s3://aahash-project2/media/iceberg/ad_revenue/`
- `s3://aahash-project2/media/iceberg/channel_metadata/`
- `s3://aahash-project2/media/iceberg/demographics/`

---

### 📌 Phase 5: Lambda & MWAA (Airflow) Integration
- **Lambda Function**: Triggers MWAA DAG when new Iceberg files are added.  
- **Airflow DAG**: Executes Glue job for incremental merge.

```python
# Lambda Function
TriggerMWAADAGOnIcebergUpdate

# Airflow DAG ID
trigger_glue_merge_dag
```

---

### 📌 Phase 6: Real-Time Log Streaming with Kinesis

Send Logs to Kinesis:
```bash
python3 send_to_kinesis.py
```

Process Streaming Data on EMR:
```bash
spark-submit spark_to_s3_snowflake.py
```

Ensure Spark script writes to:
- S3 → `s3://aahash-project2/stream1/`
- (Optionally) Snowflake

---

### 📌 Phase 7: Query Streamed Data Using Athena
Use Glue Crawler to crawl the S3 path: `s3://aahash-project2/stream1/`

Query via Athena using prebuilt SQL queries.

Example query:
```sql
SELECT region, COUNT(*) AS views 
FROM viewership_stream 
GROUP BY region;
```

---

## 📁 Folder Structure
```bash
.
├── generate_media_data.py
├── send_to_kinesis.py
├── spark_to_s3_snowflake.py
├── scripts/
│   └── [supporting transformation scripts]
├── historical_data/
├── s3://aahash-project2/
│   ├── raw/
│   ├── media/iceberg/
│   ├── stream1/
```

---

## ✅ Prerequisites
- AWS CLI configured  
- AWS EMR cluster or AWS Glue jobs  
- IAM roles with access to:
  - S3  
  - Kinesis  
  - Glue  
  - Lambda  
  - Athena  
- Python 3.x  
- Apache Spark 3.x compatible setup  
```
