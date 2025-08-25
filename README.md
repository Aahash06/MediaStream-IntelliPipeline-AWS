
# MediaStream-IntelliPipeline-AWS

## 📊 Project Overview
MediaStream-IntelliPipeline is a cloud-native big data analytics solution designed to deliver real-time and batch insights from large-scale media consumption data. Built by Aahash Kamble, this project leverages AWS services and modern data lake architecture to help media companies optimize programming, ad placements, and user engagement.

## 🚀 Features
- Real-time ingestion of viewership logs using Amazon Kinesis
- Batch processing of demographics, ad revenue, and channel metadata
- ETL transformation using AWS Glue and Spark
- Data lake storage using Apache Iceberg with schema evolution and ACID support
- Automated updates via AWS Lambda and Airflow (MWAA)
- Analytics and visualization using Athena, Snowflake, and QuickSight

## 🧱 Architecture Summary
1. **Data Generation**: Synthetic CSV files for demographics, ad revenue, channel metadata, and viewership logs
2. **Data Ingestion**: Upload to S3 and stream to Kinesis
3. **ETL & Transformation**: Glue jobs convert raw data to Iceberg format
4. **Automation**: Lambda triggers Airflow DAGs for updates
5. **Real-Time Streaming**: Spark Structured Streaming on EMR processes Kinesis data
6. **Analytics**: Data queried via Athena and Snowflake
7. **Visualization**: Dashboards built using QuickSight

## 🛠️ Technologies Used
- **AWS Services**: S3, Glue, Lambda, EMR, Kinesis, Athena, MWAA
- **Data Formats**: CSV, Parquet, Iceberg
- **Processing Engines**: Apache Spark (Batch & Streaming)
- **Analytics Tools**: Athena SQL, Snowflake
- **Automation**: Airflow DAGs, Lambda triggers

## ⚙️ Setup Instructions
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/MediaStream-IntelliPipeline.git
   ```
2. Generate synthetic data using `generate_media_data.py`
3. Upload CSV files to your S3 bucket
4. Configure Glue Crawlers and ETL jobs
5. Deploy Lambda function and Airflow DAG
6. Launch Spark Streaming job on EMR
7. Query data using Athena or Snowflake

## 💼 Business Use Cases
- Identify top-performing channels and shows
- Analyze ad revenue by demographic segments
- Discover peak viewing hours and device preferences
- Track engagement and completion rates
- Optimize content strategy and monetization

## 📁 Folder Structure & Scripts
The following folders and scripts are part of the MediaStream-IntelliPipeline project:

```
/Project2-MediaAnalytics/
├── generate_media_data.py

/Scripts/
├── dag_glue_triggrer.py
├── ec2tosns
├── Glue_job_script.py
├── lambda.py
├── readme.md
├── Script_to_iceberg.py
├── send_to_kinesis.py
├── spark_to_s3_snowflake.py

requirements.txt
Project 2- Media Stream Analytics.pdf
Aahash_Kamble_Project2_Submissionreport.pdf
```

Each script plays a specific role in the pipeline:
- `generate_media_data.py`: Generates synthetic media datasets
- `dag_glue_triggrer.py`: Airflow DAG to trigger Glue jobs
- `ec2tosns`: Script to send EC2 alerts to SNS
- `Glue_job_script.py`: Glue ETL job for Iceberg transformation
- `lambda.py`: Lambda function to trigger Airflow DAG
- `Script_to_iceberg.py`: Converts raw data to Iceberg format
- `send_to_kinesis.py`: Sends viewership logs to Kinesis stream
- `spark_to_s3_snowflake.py`: Spark job to write streaming data to S3 and Snowflake

---
© 2025 Aahash Kamble. All rights reserved.
