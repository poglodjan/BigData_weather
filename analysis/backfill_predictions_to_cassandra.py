import pyspark
import pyspark.sql.functions as sf

spark = (
    pyspark.sql.SparkSession.builder
    .appName("backfill-predictions-to-cassandra")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(
    "hdfs://namenode:9000/user/hdfs/predictions"
)

df = df.withColumn(
    "day",
    sf.to_date("datetime")
)

(
    df.select("day", "datetime", "solar_pred", "wind_pred")
      .write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "weather")
      .option("table", "predictions")
      .mode("append")
      .save()
)

print("Backfill finished successfully")
