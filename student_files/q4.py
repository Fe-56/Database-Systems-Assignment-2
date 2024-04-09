import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, regexp_replace, count
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
hdfs_path = "hdfs://%s:9000"

# read the .csv
hdfs_nn = "localhost"
df = spark.read.option('header', True)\
        .csv(f"{hdfs_path}/assignment2/part1/input/" % (hdfs_nn))
        
city = "City"
cuisine_style = "Cuisine Style"
cuisine = "Cuisine"
cuisine_count = "count(Cuisine)"
counts = "count"
# Filter out rows where "Cuisine Style" is null
df = df.filter(df[cuisine_style].isNotNull())
# Remove brackets and single quotes from "Cuisine Style" column
df_replace_brackets = df.withColumn(cuisine_style, regexp_replace(col(cuisine_style), r"[\[\]\'\']", ""))

# Split the "Cuisine Style" column by comma and rename to cuisine   
df_split = df_replace_brackets.withColumn(cuisine_style, split(df_replace_brackets[cuisine_style], ",").alias(cuisine))
# Explode the "Cuisine Style" column so that there is only one cuisine per row
df_exploded = df_split.withColumn(cuisine_style, explode(cuisine_style)).withColumnRenamed(cuisine_style, cuisine)

# Group by "City" and "Cuisine" and count the number of restaurants
restaurants_by_city_cuisine = df_exploded.groupBy(city, cuisine).agg(count(cuisine))
# Rename the count column to "count"
df_renamed = restaurants_by_city_cuisine.withColumnRenamed(cuisine_count, counts)
df_renamed.show()

# write the .csv
save_path = f"{hdfs_path}/assignment2/output/question4/" % (hdfs_nn)
df_renamed.write.format("csv").save(save_path)

spark.stop()