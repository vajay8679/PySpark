from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("repartition_into1")\
    .master("local[*]")\
    .config("spark.ui.port", "4041")\
    .config("spark.driver.memory","1g")\
    .config("spark.executor.memory","1g")\
    .config("spark.executor.core","2")\
    .config("spark.executor.instance","1")\
    .getOrCreate()

sc = spark.sparkContext

df1 = spark.read.format("csv")\
        .load("employee_dataset.csv")


print("df1 no. of records", df1.count())

print("By default parallelism in spark ",sc.defaultParallelism)

print("No. of partition in df1 ", df1.rdd.getNumPartitions())

df2 = df1.repartition(1)


print("Number of partition in df2 ", df2.rdd.getNumPartitions())


df2.write.mode("overwrite").parquet("json_data_repartiton/output.parquet")


input("press enter to close")