import sys 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, collect_list, size, array_distinct
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, IntegerType
import ast
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

hdfs_path = "hdfs://%s:9000"

# Read the Parquet file
df = spark.read.option("header",True)\
    .parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))


df.printSchema()

# Define UDF to extract all unique pairs of actors from the cast list
@udf(returnType=ArrayType(ArrayType(StringType())))
def extract_actor_pairs(cast_str):
    cast_list = ast.literal_eval(cast_str)
    pairs = set()
    for i in range(len(cast_list)):
        for j in range(i + 1, len(cast_list)):
            pairs.add(tuple(sorted([cast_list[i]['name'], cast_list[j]['name']])))
    return list(pairs)

# Apply UDF to get actor pairs for each movie
df_with_pairs = df.withColumn("actor_pairs", extract_actor_pairs(col("cast")))

print("df_with_pairs: ")
df_with_pairs.select("movie_id", "title", "actor_pairs").show(1, truncate=False)

# CHECK FOR DUPLICATES IN THE ACTOR PAIRS for each movie:

# Collect all actor pairs for each movie into a list
df_with_pairs_check = df_with_pairs.groupBy("movie_id").agg(collect_list("actor_pairs").alias("all_pairs"))

# Add a new column with the size of the list of all pairs
df_with_pairs_check = df_with_pairs_check.withColumn("all_pairs_size", size(df_with_pairs_check.all_pairs[0]))

# Add a new column with the size of the list of distinct pairs
df_with_pairs_check = df_with_pairs_check.withColumn("distinct_pairs_size", size(array_distinct(df_with_pairs_check.all_pairs[0])))

# Check if the sizes match
df_with_pairs_check = df_with_pairs_check.withColumn("has_duplicates", df_with_pairs_check.all_pairs_size != df_with_pairs_check.distinct_pairs_size)

# Filter the DataFrame
df_no_duplicates = df_with_pairs_check.filter(df_with_pairs_check.has_duplicates == True)

# Check if the resulting DataFrame is empty
if df_no_duplicates.count() == 0:
    print("No 'True' values in the 'has_duplicates' column")
else:
    print("'True' values found in the 'has_duplicates' column")

# Flatten the DataFrame into rows of individual actor pairs per movie, using RDD for transformation
rdd_exploded_pairs = df_with_pairs.rdd.flatMap(lambda row: [(tuple(pair), row['movie_id'], row['title']) for pair in row['actor_pairs']])
print(rdd_pair_counts.take(5))

# Map each pair to a tuple ((actor1, actor2), (movie_id, title)), then reduce by key to aggregate movie_ids and titles
rdd_pair_counts = rdd_exploded_pairs.map(lambda x: (x[0], [(x[1], x[2])])).reduceByKey(lambda a, b: a + b)
print(rdd_pair_counts.take(5))

# Filter out pairs that do not have at least 2 movies together and sort
rdd_filtered_sorted_pairs = rdd_pair_counts.filter(lambda x: len(x[1]) >= 2).sortByKey()
print(rdd_filtered_sorted_pairs.take(5))

# Convert back to DataFrame
schema = StructType([
    StructField("actor_pair", ArrayType(StringType()), False),
    StructField("movies", ArrayType(StructType([
        StructField("movie_id", StringType(), False),
        StructField("title", StringType(), False)
    ])), False),
])

df_final_pairs = spark.createDataFrame(rdd_filtered_sorted_pairs.map(lambda x: (list(x[0]), x[1])), schema)
df_final_pairs.show(truncate=False)

# Explode the movies array to separate rows and select the columns
df_exploded = df_final_pairs.withColumn("movie", explode(col("movies")))

# Select the necessary columns and perform the transformation to split actor pairs
df_result = df_exploded.select(
    col("movie.movie_id").alias("movie_id"),
    col("movie.title").alias("title"),
    col("actor_pair").getItem(0).alias("actor1"),
    col("actor_pair").getItem(1).alias("actor2")
)

# Show result (for debug purpose)
df_result.show(truncate=False)

# Filtering specific actor pairs
filtered_df_result = df_result.filter(
    ((col("actor1") == "James Gammon")        & (col("actor2") == "Tom Berenger")) |
    ((col("actor1") == "David Bailie")        & (col("actor2") == "Ho-Kwan Tse")) |
    ((col("actor1") == "Conrad Bergschneider")     & (col("actor2") == "Michael Stevens")) |
    ((col("actor1") == "Dimitry Elyashkevich") & (col("actor2") == "Manny Puig"))
)

# Show result (for debug purpose)
filtered_df_result.show()

# Write the result to Parquet
# write the .csv
save_path = f"{hdfs_path}/assignment2/output/question5/" % (hdfs_nn)
df_result.write.parquet(save_path)

spark.stop()