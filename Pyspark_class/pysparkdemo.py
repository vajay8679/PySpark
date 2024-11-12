from pyspark.sql import SparkSession

from pyspark.sql.functions import *

spark = SparkSession.builder.appName("SimpleApp5").config("spark.ui.port", "4046").master("local[*]").getOrCreate()

empDf = spark.read.option("header",True).option("inferSchema",True).csv("employees.csv")

print(empDf.rdd.getNumPartitions())

empDf = empDf.repartition(2)

print(empDf.rdd.getNumPartitions())

# empDf = empDf.filter(col("SALARY") > 5000).select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").groupby("DEPARTMENT_ID").count()

empDf.count()

empDf.collect()

input("Press enter to terminate")