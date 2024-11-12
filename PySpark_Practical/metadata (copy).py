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







find the second highest salary in sql from employee table


1. Using LIMIT with OFFSET (Works in MySQL, PostgreSQL, SQLite, etc.)



SELECT salary
FROM employee
ORDER BY salary DESC
LIMIT 1 OFFSET 1;


2. Using MAX and Subquery (Works in all SQL databases)

SELECT MAX(salary) AS second_highest_salary
FROM employee
WHERE salary < (SELECT MAX(salary) FROM employee);


Using ROW_NUMBER() (Works in SQL Server, PostgreSQL, Oracle, MySQL 8.0+, etc.)


WITH RankedSalaries AS (
    SELECT salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rank
    FROM employee
)
SELECT salary
FROM RankedSalaries
WHERE rank = 2;



4. Using DENSE_RANK() (Works in SQL Server, PostgreSQL, Oracle, MySQL 8.0+, etc.)


WITH RankedSalaries AS (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rank
    FROM employee
)
SELECT salary
FROM RankedSalaries
WHERE rank = 2;




#  find duplicate records from same table

1. Find Duplicate Records Based on All Columns


SELECT *
FROM employee
GROUP BY emp_name, emp_id, department_salary
HAVING COUNT(*) > 1;


2. Find Duplicate Records Based on Specific Columns

SELECT emp_id, COUNT(*)
FROM employee
GROUP BY emp_id
HAVING COUNT(*) > 1;


4. Using Window Functions (SQL Server, PostgreSQL, MySQL 8.0+, etc.)

WITH Duplicates AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY emp_id) AS row_num
    FROM employee
)
SELECT *
FROM Duplicates
WHERE row_num > 1;




# drop duplicate records from employee table


1. Using DELETE with a Subquery (SQL)

DELETE FROM employee
WHERE employee_id NOT IN (
    SELECT MIN(employee_id)
    FROM employee
    GROUP BY emp_name, department, salary
);


2. Using ROW_NUMBER() with a Common Table Expression (CTE) (SQL Server, PostgreSQL, Oracle)

WITH CTE AS (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY emp_name, department, salary ORDER BY employee_id) AS row_num
    FROM employee
)
DELETE FROM employee
WHERE employee_id IN (
    SELECT employee_id
    FROM CTE
    WHERE row_num > 1
);


3. Using DISTINCT to Select Only Unique Records (Retrieving without Duplication)

SELECT DISTINCT emp_name, department, salary
FROM employee;



