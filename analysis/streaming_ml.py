import pyspark
import pyspark.sql.functions as sf
from pyspark.sql.types import *
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler

spark = (
    pyspark.sql.SparkSession.builder
    .appName("weather-ml-streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "openmeteo_current")
    .option("startingOffsets", "latest")
    .load()
)

json_stream = raw_stream.select(
    sf.from_json(
        sf.col("value").cast("string"),
        StructType([
            StructField("current", StructType([
                StructField("time", StringType()),
                StructField("temperature_2m", DoubleType()),
                StructField("relative_humidity_2m", DoubleType()),
                StructField("wind_speed_10m", DoubleType()),
                StructField("cloud_cover", DoubleType()),
                StructField("shortwave_radiation", DoubleType()),
            ]))
        ])
    ).alias("data")
)

weather = json_stream.select(
    sf.to_timestamp("data.current.time").alias("datetime"),
    sf.col("data.current.temperature_2m"),
    sf.col("data.current.relative_humidity_2m"),
    sf.col("data.current.wind_speed_10m"),
    sf.col("data.current.cloud_cover"),
    sf.col("data.current.shortwave_radiation"),
)

weather_vars = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "cloud_cover",
    "shortwave_radiation",
]


MODEL_BASE = "hdfs://namenode:9000/user/hdfs/models"

assembler = VectorAssembler.load(
    f"{MODEL_BASE}/weather_assembler"
)

features_df = assembler.transform(weather)

MODEL_BASE = "hdfs://namenode:9000/user/hdfs/models"

solar_model = LinearRegressionModel.load(
    f"{MODEL_BASE}/solar_model"
)
wind_model = LinearRegressionModel.load(
    f"{MODEL_BASE}/wind_model"
)

solar_pred = solar_model.transform(features_df) \
    .select("datetime", sf.col("prediction").alias("solar_pred"))

wind_pred = wind_model.transform(features_df) \
    .select("datetime", sf.col("prediction").alias("wind_pred"))

predictions = solar_pred.join(wind_pred, "datetime")

query = (
    predictions.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()

