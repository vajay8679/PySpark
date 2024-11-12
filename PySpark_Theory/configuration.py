import findspark

findspark.init()


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# configuration for driver
spark = SparkSession.builder.master("loca1[5]").appName("Testing")\
        .config("spark.driver.memory", "12G") # Set driver memory
        .config("spark.driver.cores", "4")      # Set driver cores (only in cluster mode)
        .getOrCreate()



spark = SparkSession.builder \
        .appName("Testing") \
        .master("local[5]") \
        .config("spark.executor.memory", "4G")    # Set executor memory
        .config("spark.executor.cores", "2")     # Set number of cores per executor
        .config("spark.executor.instances", "10") # Set the number of executors
        .getOrCreate()

print(spark)

# sc = spark.sparkContext


spark2 = spark.newSession()

sc = spark.sparkContext()

print(sc)

empDf = spark.read.csv("employee.csv")