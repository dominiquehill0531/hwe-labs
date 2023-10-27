import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, split


def getScramAuthString(username, password):
  return f"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username="{username}"
   password="{password}";
  """

# Define the Kafka broker and topic to read from
kafka_bootstrap_servers = os.environ.get("HWE_BOOTSTRAP")
username = os.environ.get("HWE_USERNAME")
password = os.environ.get("HWE_PASSWORD")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
kafka_topic = "reviews"

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week4Lab") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

# Read data from Kafka using the DataFrame API
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "1000") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", getScramAuthString(username, password)) \
    .load()

df.printSchema()

# Process the received data
value = split(col("value").cast("string"), '\t')

query = df \
  .withColumn("marketplace", value.getItem(0)) \
  .withColumn("customer_id", value.getItem(1)) \
  .withColumn("review_id", value.getItem(2)) \
  .withColumn("product_id", value.getItem(3)) \
  .withColumn("product_parent", value.getItem(4)) \
  .withColumn("product_title", value.getItem(5)) \
  .withColumn("product_category", value.getItem(6)) \
  .withColumn("star_rating", value.getItem(7).cast("integer").alias("star_rating")) \
  .withColumn("helpful_votes", value.getItem(8).cast("integer").alias("helpful_votes")) \
  .withColumn("total_votes", value.getItem(9).cast("integer").alias("total_votes")) \
  .withColumn("vine", value.getItem(10)) \
  .withColumn("verified_purchase", value.getItem(11)) \
  .withColumn("review_headline", value.getItem(12)) \
  .withColumn("review_body", value.getItem(13)) \
  .withColumn("purchase_date", value.getItem(14)) \
  .withColumn("review_timestamp", current_timestamp()) \
  .drop("value")  

query.printSchema()

write_query = query.writeStream \
  .format("parquet") \
  .outputMode("append") \
  .option("path", "s3a://hwe-fall-2023/dhill/bronze/reviews") \
  .option("checkpointLocation", "/tmp/kafka-checkpoint") \
  .option("truncate", False) \
  .option("headers", True)

""" write_query = query.writeStream \
  .format("console") \
  .outputMode("append") \
  .option("truncate", False) \
  .option("headers", True) """

# Wait for the streaming query to finish
write_query.start().awaitTermination()


# Stop the SparkSession
spark.stop()