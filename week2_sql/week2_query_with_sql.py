from pyspark.sql import SparkSession

### Setup: Create a SparkSession
spark = SparkSession.builder \
    .appName("My First Dataframe") \
    .master("local[2]") \
    .getOrCreate()

# For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
# For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

### Questions

# Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe. Call it "reviews".
reviews = spark.read.csv ("resources/reviews.tsv.gz", sep="\t", header=True)
# Question 2: Create a virtual view on top of the reviews dataframe, so that we can query it with Spark SQL.
reviews.createOrReplaceTempView("reviews")
# Question 3: Add a column to the dataframe named "review_timestamp", representing the current time on your computer. 
timestamped = spark.sql("SELECT * , current_date() AS review_timestamp FROM reviews")
# Question 4: How many records are in the reviews dataframe? // 145,431
count = spark.sql("SELECT COUNT(*) FROM reviews")
# Question 5: Print the first 5 rows of the dataframe. 
# Some of the columns are long - print the entire record, regardless of length.
reviews.show(5, False)
# Question 6: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
# Look at the first 50 rows of that dataframe. 
# Which value appears to be the most common? // Digital Video Games
prodCatgs = spark.sql("SELECT product_category FROM reviews")
prodCatgs.show(50, False)
# Question 7: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
# What is the product title for that review? How many helpful votes did it have? // Xbox Live Subscription; 993
maxHelp = spark.sql("SELECT product_title, helpful_votes FROM reviews ORDER BY helpful_votes DESC")
maxHelp.show(2, False)
# Question 8: How many reviews exist in the dataframe with a 5 star rating? // 80,677
spark.sql("SELECT COUNT(*) FROM reviews WHERE star_rating = 5").show()
# Question 9: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
# Create a new dataframe with just those 3 columns, except cast them as "int"s.
castNumbers = spark.sql("SELECT CAST(star_rating AS INT), CAST(helpful_votes AS INT), CAST(total_votes AS INT) FROM reviews")
# Look at 10 rows from this dataframe.
castNumbers.show(10)
# Question 10: Find the date with the most purchases.
maxDate = spark.sql("SELECT purchase_date, COUNT(*) AS count FROM reviews GROUP BY purchase_date ORDER BY count DESC")
# Print the date and total count of the date which had the most purchases.
maxDate.show(1)
##Question 11: Write the dataframe from Question 3 to your drive in JSON format.
##Feel free to pick any directory on your computer.
##Use overwrite mode.
timestamped.printSchema()
timestamped.write.json("week2_sql/reviews_json", "overwrite")
### Teardown
# Stop the SparkSession
spark.stop()