# Using Spark, reads the data files, processes and analyses data,
# then trains a regression model for predicting power output.

import datetime
import json
import os

from pyspark.ml.feature import VectorAssembler


import pandas
import pyspark
import pyspark.sql.functions as sf
from pyspark.ml.regression import LinearRegression
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

    return df



# def electricity_data():
#     schema = StructType(
#         [
#             StructField("datetime", StringType(), False),
#             StructField("updatedAt", StringType()),
#             StructField("mix", MapType(StringType(), LongType())),
#             # StructField("isEstimated", BooleanType()),
#         ]
#     )
#     dir = "historical-electricity/zone_PL/"
#     measurement = "electricity-mix"

#     full_df = None
#     files = os.listdir(dir + "/" + measurement)
#     files.sort()

#     for file in files[:100]:
#         with open(dir + "/" + measurement + "/" + file) as fp:
#             contents = json.load(fp)
#         df = spark.createDataFrame(contents["data"], schema)
#         transformed = df.select(
#             df.datetime.cast(TimestampType()),
#             df.updatedAt.cast(TimestampType()),
#             df.mix["solar"].name("solar"),
#             df.mix["wind"].name("wind"),
#             df.mix,
#         )
#         if full_df is not None:
#             full_df = full_df.union(transformed)
#         else:
#             full_df = transformed
#     return full_df


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

    return df


# def weather_data():
#     dir = "historical-weather/52.2298,21.0118/"

#     full_df = None
#     files = os.listdir(dir)
#     files.sort()
#     for file in iter(e for e in os.listdir(dir) if int(e[:4]) >= 2024):
#         with open(dir + "/" + file) as fp:
#             contents = json.load(fp)

#         # Use pandas for data structuring first because it can handle this
#         # structure better than spark
#         pandas_df = pandas.DataFrame(contents["hourly"])
#         pandas_df.time = pandas.to_datetime(pandas_df.time)

#         # Remove columns with only null values
#         to_drop = []
#         for col in pandas_df.columns:
#             if pandas_df[col].isna().all():
#                 to_drop.append(col)
#         pandas_df.drop(columns=to_drop, inplace=True)

#         df = spark.createDataFrame(pandas_df).withColumnRenamed("time", "datetime")
#         if full_df is not None:
#             full_df = full_df.union(df)
#         else:
#             full_df = df

#     return full_df


spark = pyspark.sql.SparkSession.builder.getOrCreate()

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
    "cloud_cover",
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

common = assembler.transform(common_raw)

MODEL_BASE = "hdfs://namenode:9000/user/hdfs/models"

assembler.write().overwrite().save(
    f"{MODEL_BASE}/weather_assembler"
)

(train, test) = common.randomSplit([0.5, 0.5])

solar_reg = LinearRegression(labelCol="solar")
solar_model = solar_reg.fit(train)
print("Saving solar model...")
MODEL_BASE = "hdfs://namenode:9000/user/hdfs/models"

solar_model.write().overwrite().save(
    f"{MODEL_BASE}/solar_model"
)
print("Solar model saved")

solar_prediction = solar_model.transform(test)

wind_reg = LinearRegression(labelCol="wind")
wind_model = wind_reg.fit(train)
wind_model.write().overwrite().save(
    f"{MODEL_BASE}/wind_model"
)



wind_prediction = wind_model.transform(test)

# Generating plots:
#sample = solar_prediction.where(solar_prediction.datetime > datetime.datetime(2024, 3, 8)).sort("datetime")
#sample.plot(x="datetime", y=["solar", "prediction"], labels={"value": "MW"},
#            title="Solar power test data prediction")

#sample = wind_prediction.where(wind_prediction.datetime > datetime.datetime(2024, 3, 8)).sort("datetime")
#sample.plot(x="datetime", y=["wind", "prediction"], labels={"value": "MW"},
#            title="Wind power test data prediction")
