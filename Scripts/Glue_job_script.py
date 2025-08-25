from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Step 1: Initialize SparkSession with Iceberg configs
spark = SparkSession.builder \
    .appName("Glue Iceberg Table CDC") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://aahash-project2/media/iceberg/") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

# Step 2: Create Iceberg table (only first time)
spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.media_iceberg.ad_revenuedata (
    channel_id STRING,
    channel_name STRING,
    date DATE,
    ad_revenue DOUBLE
)
USING ICEBERG
PARTITIONED BY (date)
""")

# Step 3: Read source CSV
df = spark.read.option("header", True).option("inferSchema", True).csv("s3://aahash-project2/raw/ad_revenue/ad_revenue.csv")

# Step 4: Transform
df = df.withColumn("date", to_date("date", "dd-MM-yyyy")) \
       .withColumn("ad_revenue", col("ad_revenue").cast("double"))

# Step 5: Register temporary view
df.createOrReplaceTempView("updates")

# Step 6: Perform MERGE into Iceberg table
spark.sql("""
MERGE INTO glue_catalog.media_iceberg.ad_revenuedata AS target
USING updates AS source
ON target.channel_id = source.channel_id AND target.date = source.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
