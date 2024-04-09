import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, lit
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
hdfs_path = "hdfs://%s:9000"

# read the .csv
hdfs_nn = "localhost"
df = spark.read.option('header', True)\
        .csv(f"{hdfs_path}/assignment2/part1/input/" % (hdfs_nn))
        

rating = "Rating"
city = "City"
avg_rating = "avg(rating)"
average_rating = "AverageRating"
rating_group = "RatingGroup"
top_rating = "Top"
bottom_rating = "Bottom"
# convert rating to float and remove null ratings
df = df.withColumn(rating,  df[rating].astype("float"))
df = df.filter(df[rating].isNotNull())

# calculate the average rating for each city and rename column
average_rating_by_city = df.groupBy(city).agg(avg(rating)).withColumnRenamed(avg_rating, average_rating)

# get top 3 and bottom 3 cities by average rating and add rating group
top_3_df = average_rating_by_city.orderBy(average_rating_by_city[average_rating].desc()).limit(3).withColumn(rating_group, lit(top_rating))
bottom_3_df = average_rating_by_city.orderBy(average_rating_by_city[average_rating].asc()).limit(3).withColumn(rating_group, lit(bottom_rating))

# combine top 3 and bottom 3 cities
top_bottom_3_df = top_3_df.union(bottom_3_df)
top_bottom_3_df.show()

# write the .csv
save_path = f"{hdfs_path}/assignment2/output/question3/" % (hdfs_nn)
top_bottom_3_df.write.format("csv").save(save_path)

spark.stop()