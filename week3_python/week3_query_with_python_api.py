import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col
from pyspark.sql.functions import current_timestamp


aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week3Lab") \
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

### Questions

# Remember, this week we are using the Spark DataFrame API (and last week was the Spark SQL API).

#Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe.
#You will use the "reviews" dataframe defined here to answer all the questions below...
reviews = spark.read.csv("resources/reviews.tsv.gz", sep="\t", header=True)
#Question 2: Display the schema of the dataframe.
reviews.printSchema()
#Question 3: How many records are in the dataframe? 
#Store this number in a variable named "reviews_count".
reviews_count = reviews.count()
print(reviews_count)
#Question 4: Print the first 5 rows of the dataframe. 
#Some of the columns are long - print the entire record, regardless of length.
reviews.show(5, False)
#Question 5: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
productCategories = reviews.select("product_category")
#Look at the first 50 rows of that dataframe.
productCategories.show(50)
#Which value appears to be the most common?
    # ANSWER: Digital_Video_Games
#Question 6: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
reviews.sort("helpful_votes", ascending=False) \
    .select("product_title", "helpful_votes") \
    .show(5, False)
#What is the product title for that review? How many helpful votes did it have?
    # ANSWER: Xbox Live Subscription
#Question 7: How many reviews have a 5 star rating? ANSWER: 80,677
#reviews.selectExpr("CAST(star_rating AS INT) star_rating")
reviews.select(col("star_rating").cast("integer")).sort("star_rating", ascending=False).groupBy("star_rating").count().show
#Question 8: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
#Create a new dataframe with just those 3 columns, except cast them as "int"s.
castInt = reviews.selectExpr("CAST(star_rating AS INT) star_rating",
                             "CAST(helpful_votes AS INT) helpful_votes",
                             "CAST(total_votes AS INT) total_votes")
#Look at 10 rows from this dataframe.
castInt.show(10)
#Question 8: Find the date with the most purchases.
countByDate = reviews.select("purchase_date").groupBy("purchase_date").count().sort("count", ascending=False)
#Print the date and total count of the date with the most purchases
countByDate.limit(1).show()
#Question 9: Add a column to the dataframe named "review_timestamp", representing the current time on your computer. 
#Hint: Check the documentation for a function that can help: https://spark.apache.org/docs/3.1.3/api/python/reference/pyspark.sql.html#functions
timestamped = reviews.withColumn("review_timestamp", current_timestamp())
#Print the schema and inspect a few rows of data to make sure the data is correctly populated.
timestamped.printSchema()
timestamped.show(5)
#Question 10: Write the dataframe with load timestamp to s3a://hwe-$CLASS/$HANDLE/bronze/reviews_static in Parquet format.
#Make sure to write it using overwrite mode: append will keep appending duplicates, which will cause problems in later labs...
timestamped.write.parquet("s3a://hwe-fall-2023/dhill/bronze/reviews_static", "overwrite")
#Question 11: Read the tab separated file named "resources/customers.tsv.gz" into a dataframe
customers = spark.read.csv("resources/customers.tsv.gz", sep="\t")
#Write to S3 under s3a://hwe-$CLASS/$HANDLE/bronze/customers
#Make sure to write it using overwrite mode: append will keep appending duplicates, which will cause problems in later labs...
customers.write.parquet("s3a://hwe-fall-2023/dhill/bronze/customers", "overwrite")
#There are no questions to answer about this data set right now, but you will use it in a later lab...

# Stop the SparkSession
spark.stop()
