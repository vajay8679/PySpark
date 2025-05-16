from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder\
        .appName('Testing-ajay')\
        .config("spark.ui.port", "4050").master("local[*]")\
        .config('spark.sql.shuffle.partitions',3)\
        .config('spark.sql.adaptive.enabled','false')\
        .getOrCreate()

id_values = [1] * 10000000 + [2] * 5 + [3] * 5
table1 = spark.createDataFrame(id_values,"int").toDF("id")

id_values = [1] * 100 + [2] * 5 + [3] * 2
table2 = spark.createDataFrame(id_values,"int").toDF("id")

data = table1.join(table2,'id','inner')
print(data.count())

df_with_random = table1.withColumn("random_num",(rand() * 10 + 1).cast("int"))

table1 = df_with_random.withColumn("salted_key",concat(expr("id"),lit('_'),expr("random_num")))

table2_replicated = table2.withColumn("sequence", array([lit(i) for i in range(1,11)]))
# table2_replicated.show(20,truncate=False)

table2 = table2_replicated.withColumn("exploded_col",explode(col("sequence")))\
            .withColumn("salted_key",concat(expr("id"),lit("_"),(expr("exploded_col"))))\
            .drop("exploded_col","sequence")

data = table1.join(table2,'salted_key','inner')
print(data.count())



input("Enter somethign here")