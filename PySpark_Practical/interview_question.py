1. fil e without header at hdfs/adls: (emp_name,emp_id, department_salary)
2. There are some null values present in emp_id, those rows, we want to remove from data.
3. Find the second highest salary for each department using spark API.
4. remove the emp_name column from df and add "emp_location" with value ="HYDERABAD"
5. broadcast join this dataframe with another dataframe "manager_df" on "emp_id" as left outer join .
write this dataframe at another location with header and 5 output files



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, broadcast
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Employee Data Processing") \
    .getOrCreate()

# Define column names for the DataFrame
columns = ["emp_name", "emp_id", "department_salary"]

# Step 1: Read the file without header
df = spark.read.csv("hdfs://path/to/file.csv", header=False, inferSchema=True).toDF(*columns)

# Step 2: Remove rows where emp_id is null
df_filtered = df.filter(df.emp_id.isNotNull())

# Step 3: Find the second highest salary for each department
window_spec = Window.partitionBy("department_salary").orderBy(col("emp_id").desc())
df_ranked = df_filtered.withColumn("rank", rank().over(window_spec))
df_second_highest_salary = df_ranked.filter(df_ranked.rank == 2).select("emp_name", "emp_id", "department_salary")

# Step 4: Remove emp_name column and add emp_location
df_modified = df_second_highest_salary.drop("emp_name").withColumn("emp_location", lit("HYDERABAD"))

# Step 5: Broadcast join with manager_df on emp_id (assumed to be already defined)
df_final = df_modified.join(broadcast(manager_df), on="emp_id", how="left")

# Step 6: Write the final DataFrame to another location with header and 5 output files
df_final.write.option("header", "true").csv("hdfs://path/to/output/folder", numPartitions=5)

# Stop the Spark session
spark.stop()




