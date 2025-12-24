# Using Spark, reads the data files, processes and analyses data,
# then trains a regression model for predicting power output.

import datetime
import json
import os

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
    schema = StructType(
        [
            StructField("datetime", StringType(), False),
            StructField("updatedAt", StringType()),
            StructField("mix", MapType(StringType(), LongType())),
            # StructField("isEstimated", BooleanType()),
        ]
    )
    dir = "historical-electricity/zone_PL/"
    measurement = "electricity-mix"

    full_df = None
    files = os.listdir(dir + "/" + measurement)
    files.sort()

    for file in files[:100]:
        with open(dir + "/" + measurement + "/" + file) as fp:
            contents = json.load(fp)
        df = spark.createDataFrame(contents["data"], schema)
        transformed = df.select(
            df.datetime.cast(TimestampType()),
            df.updatedAt.cast(TimestampType()),
            df.mix["solar"].name("solar"),
            df.mix["wind"].name("wind"),
            df.mix,
        )
        if full_df is not None:
            full_df = full_df.union(transformed)
        else:
            full_df = transformed
    return full_df


def weather_data():
    dir = "historical-weather/52.2298,21.0118/"

    full_df = None
    files = os.listdir(dir)
    files.sort()
    for file in iter(e for e in os.listdir(dir) if int(e[:4]) >= 2024):
        with open(dir + "/" + file) as fp:
            contents = json.load(fp)

        # Use pandas for data structuring first because it can handle this
        # structure better than spark
        pandas_df = pandas.DataFrame(contents["hourly"])
        pandas_df.time = pandas.to_datetime(pandas_df.time)

        # Remove columns with only null values
        to_drop = []
        for col in pandas_df.columns:
            if pandas_df[col].isna().all():
                to_drop.append(col)
        pandas_df.drop(columns=to_drop, inplace=True)

        df = spark.createDataFrame(pandas_df).withColumnRenamed("time", "datetime")
        if full_df is not None:
            full_df = full_df.union(df)
        else:
            full_df = df

    return full_df


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
weather_vars = wd.columns
weather_vars.remove("datetime")
common = ed.join(wd, "datetime")
common = common.select(
    "datetime",
    "solar",
    "wind",
    # For the regression implementations, all input features must be collected in a Vector
    pyspark.ml.functions.array_to_vector(sf.array(weather_vars)).name("features"),
)

(train, test) = common.randomSplit([0.5, 0.5])

solar_reg = LinearRegression(labelCol="solar")
solar_model = solar_reg.fit(train)
solar_model.save("solar_model")
solar_prediction = solar_model.transform(test)

wind_reg = LinearRegression(labelCol="wind")
wind_model = wind_reg.fit(train)
wind_model.save("wind_model")
wind_prediction = wind_model.transform(test)

# Generating plots:
#sample = solar_prediction.where(solar_prediction.datetime > datetime.datetime(2024, 3, 8)).sort("datetime")
#sample.plot(x="datetime", y=["solar", "prediction"], labels={"value": "MW"},
#            title="Solar power test data prediction")

#sample = wind_prediction.where(wind_prediction.datetime > datetime.datetime(2024, 3, 8)).sort("datetime")
#sample.plot(x="datetime", y=["wind", "prediction"], labels={"value": "MW"},
#            title="Wind power test data prediction")
