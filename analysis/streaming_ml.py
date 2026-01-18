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

def cur_weather_stream():
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "openmeteo_current_out")
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

    cur_weather = json_stream.select(
        sf.to_timestamp("data.current.time").alias("datetime"),
        sf.col("data.current.temperature_2m"),
        sf.col("data.current.relative_humidity_2m"),
        sf.col("data.current.wind_speed_10m"),
        sf.col("data.current.cloud_cover"),
        sf.col("data.current.shortwave_radiation"),
    )
    # Apply watermark of 1 hour
    return cur_weather.withWatermark("datetime", "20 minutes")

def electricity_mix_stream():
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "electricity_mix_out")
        .option("startingOffsets", "latest")
        .load()
    )

    json_stream = raw_stream.select(
        sf.from_json(
            sf.col("value").cast("string"),
            StructType([
                StructField("data", ArrayType(StructType([
                    StructField("datetime", StringType()),
                    StructField("updatedAt", StringType()),
                    StructField("mix", StructType([
                        StructField("wind", DoubleType()),
                        StructField("solar", DoubleType()),
                    ]))
                ])))
            ])
        )["data"].alias("data")
    )

    data_stream = json_stream.select(
        sf.explode(sf.col("data")).alias("row")
    )

    stream = data_stream.select(
        sf.col("row.datetime").cast(TimestampType()).alias("datetime"),
        #sf.col("row.updatedAt").cast(TimestampType()).alias("updatedAt"),
        sf.col("row.mix")["solar"].cast(DoubleType()).alias("solar"),
        sf.col("row.mix")["wind"].cast(DoubleType()).alias("wind"),
    )
    return stream.withWatermark("datetime", "20 minutes")


cur_weather = cur_weather_stream()
electricity_mix = electricity_mix_stream()

#weather_windows = cur_weather.groupBy(sf.window("datetime", "15 minutes")).avg()
#electricity_windows = electricity_mix.groupBy(sf.window("datetime", "15 minutes")).avg()
joined = electricity_mix.join(cur_weather.hint("broadcast"), "datetime", "inner")

equery = (
    joined.writeStream
    .outputMode("append")
    .format("console")
    .trigger(processingTime="1 second")
    .option("truncate", "false")
    .start()
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

features_df = assembler.transform(cur_weather)

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

#query = (
#    predictions.writeStream
#    .outputMode("append")
#    .format("console")
#    .trigger(processingTime="1 second")
#    .option("truncate", "false")
#    .start()
#)

query = (
    predictions.writeStream
    .outputMode("append")
    .format("parquet")
    .option(
        "path",
        "hdfs://namenode:9000/user/hdfs/predictions"
    )
    .option(
        "checkpointLocation",
        "hdfs://namenode:9000/user/hdfs/checkpoints/predictions"
    )
    .trigger(processingTime="1 second")
    .start()
)

query.awaitTermination()

