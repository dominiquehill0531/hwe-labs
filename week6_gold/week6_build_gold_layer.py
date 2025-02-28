import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")


#AmazonID: 153601099083
#SupersetURL: 3.86.203.46:8088/login
##user: admin
##password: 1904labs

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week6Lab") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375,io.delta:delta-core_2.12:1.0.1') \
    .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .master('local[*]') \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

#Define a Schema which describes the Parquet files under the silver reviews directory on S3
silver_schema = StructType([
    StructField("marketplace", StringType(), nullable=True),
    StructField("customer_id", StringType(), nullable=True),
    StructField("review_id", StringType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("product_parent", StringType(), nullable=True),
    StructField("product_title", StringType(), nullable=True),
    StructField("product_category", StringType(), nullable=True),
    StructField("star_rating", IntegerType(), nullable=True),
    StructField("helpful_votes", IntegerType(), nullable=True),
    StructField("total_votes", IntegerType(), nullable=True),
    StructField("vine", StringType(), nullable=True),
    StructField("review_headline", StringType(), nullable=True),
    StructField("review_body", StringType(), nullable=True),
    StructField("purchase_date", StringType(), nullable=True),
    StructField("review_timestamp", TimestampType(), nullable=True),
    StructField("customer_name", StringType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("date_of_birth", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True)
])

#Define a streaming dataframe using readStream on top of the silver reviews directory on S3
silver_data = spark.readStream \
    .schema(silver_schema) \
    .parquet("s3a://hwe-fall-2023/dhill/silver/reviews")

#Define a watermarked_data dataframe by defining a watermark on the `review_timestamp` column with an interval of 10 seconds
watermarked_data = silver_data.withWatermark("review_timestamp", "10 seconds")

#Define an aggregated dataframe using `groupBy` functionality to summarize that data over any dimensions you may find interesting
aggregated_data = watermarked_data \
    .select(
        "state",
        "gender",
        "helpful_votes",
        "total_votes",
        "review_timestamp"
    ) \
    .groupBy(
        "review_timestamp",
        "state",
        "gender"
    ) \
    .agg({
        "gender": "count", \
        "helpful_votes": "sum", \
        "total_votes": "sum" \
    }) \
    .withColumnRenamed("COUNT(gender)", "ct_by_gender") \
    .withColumnRenamed("SUM(helpful_votes)", "helpful_votes") \
    .withColumnRenamed("SUM(total_votes)", "total_votes")
    
aggregated_data.printSchema()

#Write that aggregate data to S3 under s3a://hwe-$CLASS/$HANDLE/gold/fact_review using append mode and a checkpoint location of `/tmp/gold-checkpoint`
write_gold_query = aggregated_data.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("path", "s3a://hwe-fall-2023/dhill/gold/fact_review") \
    .option("checkpointLocation", "/tmp/gold-checkpoint") \
    .option("truncate", False) \
    .option("headers", True)
    
""" write_gold_query = aggregated_data.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .option("headers", True) """
    
    

write_gold_query.start().awaitTermination()

## Stop the SparkSession
spark.stop()