# PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, count, when, desc, split, explode

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Netflix EDA") \
    .getOrCreate()

# Load Dataset with PySpark
file_path = "/data/netflix_titles_2021.csv"
netflix_spark = spark.read.csv(file_path, header=True, inferSchema=True)

# Show schema and first few rows
netflix_spark.printSchema()
netflix_spark.show(5)

# Count total rows and columns
rows = netflix_spark.count()
columns = len(netflix_spark.columns)
print(f"Rows: {rows}, Columns: {columns}")

# Summary statistics for numeric columns
numeric_cols = [field.name for field in netflix_spark.schema.fields if field.dataType.typeName() in ('int', 'double', 'float')]
netflix_spark.select(*[
    netflix_spark.describe(column) for column in numeric_cols
]).show()

# Checking for null values
missing_values = netflix_spark.select(
    [count(when(isnull(c), c)).alias(c) for c in netflix_spark.columns]
)
missing_values.show()

# Drop rows with missing 'director' or 'cast' columns
netflix_cleaned = netflix_spark.dropna(subset=["director", "cast"])

# Verify nulls are dropped
missing_values_cleaned = netflix_cleaned.select(
    [count(when(isnull(c), c)).alias(c) for c in netflix_cleaned.columns]
)
missing_values_cleaned.show()

# Count distribution of Movies and TV Shows
type_distribution = netflix_cleaned.groupBy("type").count().orderBy(desc("count"))
type_distribution.show()

# Extract and count top genres
# Split genres by commas, explode into individual rows, and count occurrences
genres_split = netflix_cleaned.select(explode(split(col("listed_in"), ", ")).alias("genre"))
top_genres = genres_split.groupBy("genre").count().orderBy(desc("count")).limit(10)
top_genres.show()

# Export `type_distribution` and `top_genres` for visualization (if needed)
output_dir = "/output/netflix_analysis/"
type_distribution.write.csv(output_dir + "type_distribution", header=True)
top_genres.write.csv(output_dir + "top_genres", header=True)
