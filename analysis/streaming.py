import datetime
import json
import os

import pandas
import pyspark
import pyspark.sql.functions as sf
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType,
    DoubleType,
    TimestampType,
    BooleanType,
    LongType,
)


spark = pyspark.sql.SparkSession.builder.getOrCreate()

input = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "weather_current")
    .load()
)

weather_data = input.select(sf.from_json(input.value)["current"].name("val"))

keys = list(weather_data.first()["val"].keys())

cols = [sf.col("val").getItem(f).name(f) for f in keys]
unpacked = weather_data.select(cols)

weather_vars = keys
weather_vars.remove("time")
weather_vars.remove("interval")

data = unpacked.select(
    unpacked.time.name("datetime"),
    "interval",
    pyspark.ml.functions.array_to_vector(sf.array(weather_vars)).name("features"),
)

solar_model = LinearRegressionModel.load("solar_model")
wind_model = LinearRegressionModel.load("wind_model")

solar_prediction = solar_model.transform(data)
wind_prediction = wind_model.transform(data)

common_pred = solar_prediction.join(wind_prediction, "datetime")

debug = common_pred.writeStream.outputMode("append").format("console").start()
debug.awaitTermination()
