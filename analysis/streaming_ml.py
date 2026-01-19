import pyspark
import pyspark.sql.functions as sf
from pyspark.sql.types import *
from pyspark.ml.regression import GBTRegressionModel
from pyspark.ml.feature import VectorAssembler

spark = (
    pyspark.sql.SparkSession.builder
    .appName("weather-ml-streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

weather_vars = [
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "precipitation",
    "shortwave_radiation",
]

def weather_forecast_stream():
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "openmeteo_minutely_15_out")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    json_stream = raw_stream.select(
        sf.from_json(
            sf.col("value").cast("string"),
            StructType([
                StructField("minutely_15", StructType([
                    StructField("time", ArrayType(StringType())),
                    StructField("temperature_2m", ArrayType(DoubleType())),
                    StructField("relative_humidity_2m", ArrayType(DoubleType())),
                    StructField("wind_speed_10m", ArrayType(DoubleType())),
                    StructField("precipitation", ArrayType(DoubleType())),
                    StructField("shortwave_radiation", ArrayType(DoubleType())),
                ]))
            ])
        )["minutely_15"].alias("data")
    )

    array_of_structs = json_stream.select(
        sf.arrays_zip(*[sf.col("data." + c) for c in weather_vars]).alias("rows")
    ).withColumn("forecast_dt", sf.get(sf.col("rows"), 0)["time"])
    exploded = array_of_structs.select(
        sf.col("forecast_dt"),
        sf.explode("rows").alias("data"),
    )

    return (
        exploded.select(
            sf.to_timestamp("forecast_dt").alias("forecast_dt"),
            sf.to_timestamp("data.time").alias("datetime"),
            sf.col("data.temperature_2m"),
            sf.col("data.relative_humidity_2m"),
            sf.col("data.wind_speed_10m"),
            sf.col("data.precipitation"),
            sf.col("data.shortwave_radiation"),
        )
        .withWatermark("datetime", "20 minutes")
    )

def cur_weather_stream():
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "openmeteo_current_out")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
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
                    StructField("precipitation", DoubleType()),
                    StructField("shortwave_radiation", DoubleType()),
                ]))
            ])
        ).alias("data")
    )

    return (
        json_stream.select(
            sf.to_timestamp("data.current.time").alias("datetime"),
            sf.col("data.current.temperature_2m"),
            sf.col("data.current.relative_humidity_2m"),
            sf.col("data.current.wind_speed_10m"),
            sf.col("data.current.precipitation"),
            sf.col("data.current.shortwave_radiation"),
        )
        .withWatermark("datetime", "20 minutes")
    )


def electricity_mix_stream():
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "electricity_mix_out")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
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

    return (
        json_stream
        .select(sf.explode("data").alias("row"))
        .select(
            sf.col("row.datetime").cast(TimestampType()).alias("datetime"),
            sf.col("row.mix.solar").alias("solar_actual"),
            sf.col("row.mix.wind").alias("wind_actual"),
        )
        .withWatermark("datetime", "20 minutes")
    )


cur_weather = cur_weather_stream()
weather_forecast = weather_forecast_stream()
electricity_mix = electricity_mix_stream()

#joined = (
#    electricity_mix
#    .join(cur_weather, "datetime", "inner")
#)

MODEL_BASE = "hdfs://namenode:9000/user/hdfs/models"

assembler = VectorAssembler.load(
    f"{MODEL_BASE}/weather_assembler"
)

features_df = assembler.transform(weather_forecast)

solar_model = GBTRegressionModel.load(
    f"{MODEL_BASE}/solar_model"
)

wind_model = GBTRegressionModel.load(
    f"{MODEL_BASE}/wind_model"
)


predictions = (
    features_df
    .transform(
        lambda df: solar_model.transform(df)
        .withColumnRenamed("prediction", "solar_pred")
    )
    .transform(
        lambda df: wind_model.transform(df)
        .withColumnRenamed("prediction", "wind_pred")
    )
    .select(
        "forecast_dt",
        sf.to_date("datetime").alias("day"),
        "datetime",
        sf.greatest(sf.col("solar_pred"), sf.lit(0)).alias("solar_pred"),
        sf.greatest(sf.col("wind_pred"), sf.lit(0)).alias("wind_pred"),
    )
)

def debug_and_write(batch_df, batch_id, table):
    count = batch_df.count()
    print(f"\n===== {table.upper()} BATCH {batch_id} | rows={count} =====")

    if count == 0:
        print("Empty batch – skipping write")
        return

    batch_df.show(5, truncate=False)

    (
        batch_df
        .write
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "weather")
        .option("table", table)
        .mode("append")
        .save()
    )
    print("Batch written to Cassandra")

wquery = (
    cur_weather
    .select("datetime", "shortwave_radiation", "temperature_2m", "wind_speed_10m")
    .writeStream
    .queryName("cur_weather")
    .foreachBatch(lambda df, bid: debug_and_write(df, bid, "cur_weather"))
    .option(
        "checkpointLocation",
        "/opt/spark/work-dir/checkpoints/cur_weather"
    )
    .start()
)
equery = (
    electricity_mix
    .writeStream
    .queryName("cur_electricity")
    .foreachBatch(lambda df, bid: debug_and_write(df, bid, "cur_electricity"))
    .option(
        "checkpointLocation",
        "/opt/spark/work-dir/checkpoints/cur_electricity"
    )
    .start()
)

def save_forecast(batch_df, batch_id):
    # There might be multiple forecasts in a batch,
    # Only pick the latest forecast
    latest = batch_df.select(sf.max("forecast_dt")).head()[0]
    last_forecast = batch_df.filter(sf.col("forecast_dt") == latest)
    debug_and_write(last_forecast.drop("forecast_dt"), batch_id, "predictions")

query = (
    predictions
    .writeStream
    .queryName("predictions")
    .foreachBatch(save_forecast)
    .option(
        "checkpointLocation",
        "/opt/spark/work-dir/checkpoints/predictions"
    )
    .start()
)

wquery.awaitTermination()
equery.awaitTermination()
query.awaitTermination()
