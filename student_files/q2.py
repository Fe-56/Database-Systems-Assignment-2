import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
from pyspark.sql.functions import max, min, col

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

hdfs_path = "hdfs://%s:9000"

# read the .csv
hdfs_nn = "localhost"
df = spark.read.option('header', True)\
        .csv(f"{hdfs_path}/assignment2/part1/input/" % (hdfs_nn))

# convert the "Rating" column to float
rating = "Rating"
city = "City"
price_range = "Price Range"
df = df.withColumn(rating,  df[rating].astype("float"))

# ignore the rows where "Price Range" is null
df = df.filter(df[price_range].isNotNull())

# find the best and worst restaurants for each city for each price range
best_restaurants = df.groupBy(city, price_range) \
                     .agg(max(rating).alias(rating))
worst_restaurants = df.groupBy(city, price_range) \
                     .agg(min(rating)).alias(rating)

# so that the worst restaurants are below the best restaurants
resulting_df = best_restaurants.union(worst_restaurants)

# join with the original df to get back the original schema
resulting_df = resulting_df.join(df, [price_range, city, rating], how="inner")

# remove any duplicate rows for "Price Range", "City", and "Rating", so that there is only row for a unique value of those columns
resulting_df = resulting_df.dropDuplicates([price_range, city, rating])

# rearrange the order of the columns
resulting_df = resulting_df.select(
  "_c0",
  "Name",
  city,
  "Ranking",
  rating,
  price_range,
  "Number of Reviews",
  "Reviews",
  "URL_TA",
  "ID_TA"
)

# sort the columns
resulting_df = resulting_df.sort(
  col(city).asc(),
  col(price_range).asc(),
  col(rating).desc()
)

resulting_df.show()

# write the .csv
save_path = f"{hdfs_path}/assignment2/output/question2/" % (hdfs_nn)
resulting_df.write.format("csv").save(save_path)

spark.stop()