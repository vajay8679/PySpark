from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read Parquet") \
    .master("local[*]") \
    .getOrCreate()

# Read the Parquet file into a DataFrame
df = spark.read.parquet('part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet')

# Show the contents of the DataFrame
df.show()