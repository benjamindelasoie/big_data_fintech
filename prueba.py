from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

print(spark.version)  # Should show 3.5.1

# Scala version can be confirmed by inspecting the JAR:
print(spark.sparkContext._jvm.scala.util.Properties.versionString())

