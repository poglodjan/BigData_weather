# Using Spark, reads the data files, processes and analyses data,
# then trains a regression model for predicting power output.

import datetime
import json
import os

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator


import pandas
import pyspark
import pyspark.sql.functions as sf
from pyspark.ml.regression import GBTRegressor
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


# Only use last 28 days of data
THRESHOLD = datetime.datetime.now() - datetime.timedelta(days=28)

def electricity_data():
    path = "hdfs://namenode:9000/user/hdfs/electricitymaps/zone_PL/electricity-mix/*"

    raw = spark.read.json(path)

    # data -> array of hourly records
    df = raw.select(sf.explode("data").alias("row"))

    df = df.select(
        sf.col("row.datetime").cast(TimestampType()).alias("datetime"),
        sf.col("row.updatedAt").cast(TimestampType()).alias("updatedAt"),
        sf.col("row.mix")["solar"].cast(DoubleType()).alias("solar"),
        sf.col("row.mix")["wind"].cast(DoubleType()).alias("wind"),
    )
    df = df.filter(df.datetime >= THRESHOLD)

    return df

def weather_data():
    path = "hdfs://namenode:9000/user/hdfs/openmeteo/warsaw/hourly/*"

    raw = spark.read.json(path)

    # hourly is a struct with arrays -> convert to rows
    df = raw.select(sf.explode(sf.arrays_zip(*[
        sf.col("hourly." + c) for c in raw.select("hourly.*").columns
    ])).alias("row"))

    df = df.select("row.*")

    # Rename and cast time
    df = df.withColumnRenamed("time", "datetime") \
           .withColumn("datetime", sf.to_timestamp("datetime"))

    # Cutoff data range
    df = df.filter(df.datetime >= THRESHOLD)

    return df


spark = pyspark.sql.SparkSession.builder.appName("weatherBatch").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

ed = electricity_data()
wd = weather_data()

# Reduce electricity data to hourly to match weather data
ed = (
    ed.groupBy(sf.date_trunc("hour", ed.datetime).name("datetime"))
    .avg()
    .withColumnsRenamed({"avg(solar)": "solar", "avg(wind)": "wind"})
)

# Join both datasets and format for model training
# weather_vars = wd.columns
# weather_vars.remove("datetime")

weather_vars = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "precipitation",
    "shortwave_radiation",
]

common_raw = ed.join(wd, "datetime").select(
    "datetime",
    "solar",
    "wind",
    *weather_vars
)

common_raw = common_raw.na.drop(subset=weather_vars + ["solar", "wind"])

assembler = VectorAssembler(
    inputCols=weather_vars,
    outputCol="features"
)

common = assembler.transform(common_raw).persist()

MODEL_BASE = "hdfs://namenode:9000/user/hdfs/models"

assembler.write().overwrite().save(
    f"{MODEL_BASE}/weather_assembler"
)

(train, test) = common.randomSplit([0.8, 0.2])
train = common # Final model: train with full dataset

solar_reg = GBTRegressor(labelCol="solar")
solar_model = solar_reg.fit(train)
print("Saving solar model...")
MODEL_BASE = "hdfs://namenode:9000/user/hdfs/models"

solar_model.write().overwrite().save(
    f"{MODEL_BASE}/solar_model"
)
print("Solar model saved")

solar_prediction = solar_model.transform(test)\
    .withColumn("prediction", sf.greatest(sf.col("prediction"), sf.lit(0)))

solar_rmse = RegressionEvaluator(
    labelCol="solar",
    predictionCol="prediction",
    metricName="rmse"
).evaluate(solar_prediction)

solar_r2 = RegressionEvaluator(
    labelCol="solar",
    predictionCol="prediction",
    metricName="r2"
).evaluate(solar_prediction)

print("SOLAR RMSE:", solar_rmse)
print("SOLAR R2:", solar_r2)



wind_reg = GBTRegressor(labelCol="wind")
wind_model = wind_reg.fit(train)
wind_model.write().overwrite().save(
    f"{MODEL_BASE}/wind_model"
)



wind_prediction = wind_model.transform(test)\
    .withColumn("prediction", sf.greatest(sf.col("prediction"), sf.lit(0)))

wind_rmse = RegressionEvaluator(
    labelCol="wind",
    predictionCol="prediction",
    metricName="rmse"
).evaluate(wind_prediction)

wind_r2 = RegressionEvaluator(
    labelCol="wind",
    predictionCol="prediction",
    metricName="r2"
).evaluate(wind_prediction)

print("WIND RMSE:", wind_rmse)
print("WIND R2:", wind_r2)


# Generating plots:
#sample = solar_prediction.where(solar_prediction.datetime > datetime.datetime(2024, 3, 8)).sort("datetime")
#sample.plot(x="datetime", y=["solar", "prediction"], labels={"value": "MW"},
#            title="Solar power test data prediction")

#sample = wind_prediction.where(wind_prediction.datetime > datetime.datetime(2024, 3, 8)).sort("datetime")
#sample.plot(x="datetime", y=["wind", "prediction"], labels={"value": "MW"},
#            title="Wind power test data prediction")


#saving metrics

metrics_df = spark.createDataFrame(
    [
        ("solar", solar_rmse, solar_r2),
        ("wind", wind_rmse, wind_r2),
    ],
    ["model", "rmse", "r2"]
).withColumn("datetime", sf.current_timestamp())

metrics_df.show()

(
    metrics_df
    .write
    .format("org.apache.spark.sql.cassandra")
    .option("keyspace", "weather")
    .option("table", "ml_metrics")
    .mode("append")
    .save()
)

# saving data for plots
solar_plot_df = solar_prediction.select(
    sf.lit("0").name("id"), # For Grafana plot
    "datetime",
    sf.col("solar").alias("actual"),
    sf.col("prediction").alias("predicted")
)

wind_plot_df = wind_prediction.select(
    sf.lit("0").name("id"),
    "datetime",
    sf.col("wind").alias("actual"),
    sf.col("prediction").alias("predicted")
)

(
    solar_plot_df
    .write
    .format("org.apache.spark.sql.cassandra")
    .option("keyspace", "weather")
    .option("table", "solar_training")
    .option("confirm.truncate", "true")
    .mode("overwrite")
    .save()
)
print("Saved solar_plot_df to Cassandra")

(
    wind_plot_df
    .write
    .format("org.apache.spark.sql.cassandra")
    .option("keyspace", "weather")
    .option("table", "wind_training")
    .option("confirm.truncate", "true")
    .mode("overwrite")
    .save()
)
print("Saved wind_plot_df to Cassandra")
