import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, BinaryType, LongType

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week5Lab") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .master('local[*]') \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

bronze_schema = StructType([
    StructField("key", BinaryType(), nullable=True),
    StructField("topic", StringType(), nullable=True),
    StructField("partition", IntegerType(), nullable=True),
    StructField("offset", LongType(), nullable=True),
    StructField("timestamp", TimestampType(), nullable=True),
    StructField("timestampType", IntegerType(), nullable=True),
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
    StructField("verified_purchase", StringType(), nullable=True),
    StructField("review_headline", StringType(), nullable=True),
    StructField("review_body", StringType(), nullable=True),
    StructField("purchase_date", StringType(), nullable=True),
    StructField("review_timestamp", TimestampType(), nullable=False)
])

bronze_reviews = spark.readStream \
    .schema(bronze_schema) \
    .parquet("s3a://hwe-fall-2023/dhill/bronze/reviews") \

bronze_reviews.createOrReplaceTempView("reviews")
    
bronze_customers = spark.read \
    .parquet("s3a://hwe-fall-2023/dhill/bronze/customers")
    
bronze_customers.printSchema()
bronze_reviews.printSchema()
bronze_customers.createOrReplaceTempView("customers")

silver_data = spark.sql(" \
    SELECT \
        r.marketplace, \
        r.customer_id, \
        r.review_id, \
        r.product_id, \
        r.product_parent, \
        r.product_title, \
        r.product_category, \
        r.star_rating, \
        r.helpful_votes, \
        r.total_votes, \
        r.vine, \
        r.review_headline, \
        r.review_body, \
        r.purchase_date, \
        r.review_timestamp, \
        c.customer_name, \
        c.gender, \
        c.date_of_birth, \
        c.city, \
        c.state \
    FROM \
        reviews r \
    INNER JOIN customers c\
        ON r.customer_id = c.customer_id \
    WHERE r.verified_purchase = 'Y' \
")

silver_data.printSchema()

streaming_query = silver_data.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "s3a://hwe-fall-2023/dhill/silver/reviews") \
    .option("checkpointLocation", "/tmp/silver-checkpoint") \
    .option("truncate", False) \
    .option("headers", True)

""" streaming_query = silver_data.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .option("headers", True) """

streaming_query.start().awaitTermination()

## Stop the SparkSession
spark.stop()
