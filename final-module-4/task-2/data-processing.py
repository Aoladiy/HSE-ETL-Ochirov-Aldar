from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round

spark = SparkSession.builder \
    .appName("temperature-processing") \
    .enableHiveSupport() \
    .getOrCreate()

# Читаем CSV из Object Storage
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://default-bucket-1234/city_temperature_gt60mb.csv")

# Агрегация — средняя температура по стране и году
result = df.groupBy("Country", "Year") \
    .agg(round(avg("AvgTemperature"), 2).alias("avg_temp")) \
    .orderBy("Country", "Year")

# Пишем результат обратно в Object Storage
result.write \
    .mode("overwrite") \
    .option("path", "s3a://default-bucket-1234/temperature-result") \
    .saveAsTable("temperature_by_country_year")