import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month, dayofmonth, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Kafka Configuration
KAFKA_BROKER_LOCAL = 'localhost:9092'
KAFKA_TOPIC = 'iot_data'

# Load environment variables
access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
output_bucket = os.getenv('OUTPUT_BUCKET_NAME')
jdbc_url = os.getenv('PG_JDBC_URL')
db_user = os.getenv('PG_USER')
db_password = os.getenv('PG_PASSWORD')

def validate_env_vars():
    required_vars = {
        'AWS_ACCESS_KEY_ID': access_key,
        'AWS_SECRET_ACCESS_KEY': secret_key,
        'OUTPUT_BUCKET_NAME': output_bucket,
        'PG_JDBC_URL': jdbc_url,
        'PG_USER': db_user,
        'PG_PASSWORD': db_password
    }
    for var, value in required_vars.items():
        if not value:
            raise ValueError(f"Missing required environment variable: {var}")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def spark_connection():
    try:
        spark = (SparkSession.builder
                 .appName('spark_processing_s3')
                 .master('local[*]')
                 .config('spark.executor.memory', '8g')
                 .config('spark.driver.memory', '8g')
                 .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.780,org.postgresql:postgresql:42.7.4")
                 .config("spark.hadoop.fs.s3a.access.key", access_key)
                 .config("spark.hadoop.fs.s3a.secret.key", secret_key)
                 .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        logging.info("Spark connection established.")
        return spark
    except Exception as e:
        logging.error(f"Error establishing Spark connection: {e}")
        raise

def connect_to_kafka(spark):
    try:
        df = (spark.readStream
              .format('kafka')
              .option('kafka.bootstrap.servers', KAFKA_BROKER_LOCAL)
              .option('subscribe', KAFKA_TOPIC)
              .option('startingOffsets', 'latest')
              .option("failOnDataLoss", "true")
              .load())
        logging.info("Connected to Kafka successfully.")
        return df
    except Exception as e:
        logging.error(f"Kafka connection failed: {e}")
        raise

def transform_kafka_data(df):
    schema = StructType([
        StructField('device_id', StringType(), True),
        StructField('timestamp', DoubleType(), True),
        StructField('temperature', DoubleType(), True),
        StructField('humidity', DoubleType(), True)
    ])
    try:
        transformed_df = (df.selectExpr("CAST(value AS STRING)")
                           .select(from_json(col('value'), schema).alias('data'))
                           .select("data.*"))

        transformed_df = (transformed_df
                          .withColumn("timestamp", from_unixtime(col("timestamp")).cast(TimestampType()))
                          .withColumn("year", year(col("timestamp")))
                          .withColumn("month", month(col("timestamp")))
                          .withColumn("day", dayofmonth(col("timestamp"))))
        logging.info("Data transformed successfully.")
        return transformed_df
    except Exception as e:
        logging.error(f"Data transformation failed: {e}")
        raise

def write_to_timescaledb(batch_df, batch_id):
    if batch_df.isEmpty():
        logging.info("Skipping empty batch.")
        return
    try:
        batch_df.write.jdbc(
            url=jdbc_url,
            table="sensor_data",
            mode="append",
            properties={"user": db_user, "password": db_password, "driver": "org.postgresql.Driver"}
        )
        logging.info(f"Batch {batch_id} written to TimescaleDB.")
    except Exception as e:
        logging.error(f"Failed to write batch {batch_id} to TimescaleDB: {e}")
        raise

def main():
    validate_env_vars()
    spark = spark_connection()
    kafka_df = connect_to_kafka(spark)
    transformed_df = transform_kafka_data(kafka_df)
    
    checkpoint_location = f"s3a://{output_bucket}/checkpoints"
    output_location = f"s3a://{output_bucket}/output"
    
    try:
        s3_query = (transformed_df.writeStream
                    .format("parquet")
                    .option("path", output_location)
                    .option("checkpointLocation", checkpoint_location)
                    .partitionBy("year", "month", "day")
                    .outputMode("append")
                    .start())
        
        db_query = (transformed_df.writeStream
                    .foreachBatch(write_to_timescaledb)
                    .outputMode("append")
                    .start())
        
        logging.info("Streaming processes started.")
        spark.streams.awaitAnyTermination()
    except Exception as e:
        logging.error(f"Streaming process failed: {e}")
        raise

if __name__ == "__main__":
    main()
