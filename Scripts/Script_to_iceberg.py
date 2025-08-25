import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql import SparkSession

# Accept JOB_NAME as argument
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize contexts ONCE
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Set Iceberg-specific configurations
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://aahash-project2/media/iceberg/")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Load raw tables from Glue Data Catalog
try:
    # Load and transform ad_revenue data
    ad_revenue_raw = spark.table("`media_raw-aahash`.ad_revenue")
    ad_revenue_df = ad_revenue_raw.withColumn("date", ad_revenue_raw["date"].cast("date"))
    print(f"Ad revenue columns: {ad_revenue_df.columns}")
    
    # Load channel metadata
    channel_metadata_df = spark.table("`media_raw-aahash`.channel_metadata")
    print(f"Channel metadata columns: {channel_metadata_df.columns}")
    
    # Load demographics and fix column names
    demographics_raw = spark.table("`media_raw-aahash`.demographics")
    print(f"Demographics raw columns: {demographics_raw.columns}")
    
    # Map generic column names to proper names
    # Assuming order: user_id, gender, age_group, region, subscription_type
    demographics_df = demographics_raw \
        .withColumnRenamed("col0", "user_id") \
        .withColumnRenamed("col1", "gender") \
        .withColumnRenamed("col2", "age_group") \
        .withColumnRenamed("col3", "region") \
        .withColumnRenamed("col4", "subscription_type")
    
    print(f"Demographics after renaming: {demographics_df.columns}")
    
    # Show sample to verify the mapping is correct
    print("Demographics sample data:")
    demographics_df.show(5)
    
    print("Successfully loaded and transformed source tables")
except Exception as e:
    print(f"Error loading source tables: {e}")
    raise
except Exception as e:
    print(f"Error loading source tables: {e}")
    raise

# Create Iceberg Database and Tables
try:
    # Create the database first
    print("Creating media_iceberg database...")
    create_db_sql = "CREATE DATABASE IF NOT EXISTS `glue_catalog`.`media_iceberg` LOCATION 's3://aahash-project2/media/iceberg/'"
    spark.sql(create_db_sql)
    print("Database media_iceberg created/verified")

    # Create ad_revenue table
    print("Creating ad_revenue table...")
    ad_revenue_sql = """CREATE TABLE IF NOT EXISTS `glue_catalog`.`media_iceberg`.`ad_revenue` (
        channel_id STRING,
        channel_name STRING,
        date DATE,
        ad_revenue FLOAT
    ) USING iceberg LOCATION 's3://aahash-project2/media/iceberg/ad_revenue/'"""
    spark.sql(ad_revenue_sql)
    print("Ad revenue table created/verified")

    # Create channel_metadata table
    print("Creating channel_metadata table...")
    channel_sql = """CREATE TABLE IF NOT EXISTS `glue_catalog`.`media_iceberg`.`channel_metadata` (
        channel_id STRING,
        channel_name STRING,
        genre STRING,
        language STRING,
        launch_year INT
    ) USING iceberg LOCATION 's3://aahash-project2/media/iceberg/channel_metadata/'"""
    spark.sql(channel_sql)
    print("Channel metadata table created/verified")

    # Create demographics table
    print("Creating demographics table...")
    demographics_sql = """CREATE TABLE IF NOT EXISTS `glue_catalog`.`media_iceberg`.`demographics` (
        user_id STRING,
        gender STRING,
        age_group STRING,
        region STRING,
        subscription_type STRING
    ) USING iceberg LOCATION 's3://aahash-project2/media/iceberg/demographics/'"""
    spark.sql(demographics_sql)
    print("Demographics table created/verified")
    print("All Iceberg tables created/verified successfully")

except Exception as e:
    print(f"Error creating Iceberg tables: {e}")
    raise

# Write DataFrames to Iceberg Tables
try:
    # Write ad_revenue data
    ad_revenue_df.writeTo("glue_catalog.media_iceberg.ad_revenue").using("iceberg").option("overwrite-mode", "dynamic").tableProperty("format-version", "2").append()
    print("Ad revenue data written successfully")

    # Write channel_metadata data
    channel_metadata_df.writeTo("glue_catalog.media_iceberg.channel_metadata").using("iceberg").option("overwrite-mode", "dynamic").tableProperty("format-version", "2").append()
    print("Channel metadata written successfully")

    # Write demographics data
    demographics_df.writeTo("glue_catalog.media_iceberg.demographics").using("iceberg").option("overwrite-mode", "dynamic").tableProperty("format-version", "2").append()
    print("Demographics data written successfully")

except Exception as e:
    print(f"Error writing to Iceberg tables: {e}")
    raise

# Job Commit
print("Job completed successfully")
job.commit()

