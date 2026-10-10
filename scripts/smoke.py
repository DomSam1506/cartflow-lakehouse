from pyspark.sql import SparkSession

s = SparkSession.builder.master("local[*]").getOrCreate()
s.range(5).show()
