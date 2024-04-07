import sys
from pyspark.sql import SparkSession
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

hdfs_path = "hdfs://%s:9000"

# read the .csv
hdfs_nn = "localhost"
df = spark.read.option('header', True)\
        .csv(f"{hdfs_path}/assignment2/part1/input/" % (hdfs_nn))

# convert the "Number of Reviews" annd "Rating" columns to int and float respectively
num_of_reviews = "Number of Reviews"
rating = "Rating"
df = df.withColumn(num_of_reviews, df[num_of_reviews].astype("int"))
df = df.withColumn(rating,  df[rating].astype("float"))

# clean the .csv
cleaned_df = df.filter((df[num_of_reviews] > 0) & (df[rating] >= 1.0))

cleaned_df.show()

# write the .csv
save_path = f"{hdfs_path}/assignment2/output/question1/" % (hdfs_nn)
cleaned_df.write.format("csv").save(save_path)

spark.stop()
