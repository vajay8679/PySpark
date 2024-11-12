from pyspark.sql import SparkSession

from pyspark.sql.functions import *

spark = SparkSession.builder.appName("SimpleApp2").config("spark.ui.port", "4046").master("local[*]").getOrCreate()


empDf = empDf = spark.read.option("header",True).option("inferSchema",True).csv("employees.csv")

empDf.count()

input("Press enter to terminate")
