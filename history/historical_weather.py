#!/usr/bin/env python3

import datetime
import json
import os
import sys
import time

import pyhdfs
import requests

"""
NOTE:
The data is downloaded in chunks of 1 month. With the number of selected
parameters one such request costs 6 API calls towards the rate limits
of 600 calls / min.
"""

START_YEAR = 2017

URL = "https://archive-api.open-meteo.com/v1/archive"
LATITUDE = "52.2298"
LONGITUDE = "21.0118"

PARAMETERS = "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation_probability,precipitation,rain,showers,snowfall,snow_depth,pressure_msl,surface_pressure,cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,visibility,wind_speed_10m,wind_speed_180m,wind_speed_80m,wind_speed_120m,wind_direction_10m,wind_direction_80m,wind_direction_120m,wind_direction_180m,wind_gusts_10m,shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance,terrestrial_radiation,sunshine_duration"

NAMENODE_HOST = os.environ.get('NAMENODE_HOST', 'localhost')
NAMENODE_PORT = os.environ.get('NAMENODE_PORT', '9870')
HDFS_USER     = os.environ.get('HDFS_USER', 'hdfs')
HDFS_PATH = f"/user/{HDFS_USER}/openmeteo/history/warsaw"


hdfs = pyhdfs.HdfsClient(f"{NAMENODE_HOST}:{NAMENODE_PORT}",
                         user_name=HDFS_USER)

hdfs.mkdirs(HDFS_PATH)
params = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    "hourly": PARAMETERS,
}
today = datetime.date.today()
cur_year = today.year
cur_month = today.month

for year in range(START_YEAR, cur_year+1):
    max_month = cur_month if year == cur_year else 13
    for month in range(1, max_month):
        # Write data to PATH/LOCATION/DATE.json, for example
        # /user/hdfs/openmeteo/warsaw/2025-01.json
        fname = HDFS_PATH + f"/{year}-{month:02}.json"

        # Calculate end of month
        start_date = datetime.date(year=year, month=month, day=1)
        n_month = (month % 12) + 1
        n_year = year + 1 if n_month < month else year
        next_month = datetime.date(year=n_year, month=n_month, day=1)
        end_date = next_month - datetime.timedelta(days=1)
        # Number of hours in this month
        expected_values = (next_month - start_date).days * 24

        # Check if the file is already present
        if hdfs.exists(fname):
            try:
                with hdfs.open(fname) as file:
                    data = json.load(file)
                n = len(data["hourly"]["time"])
                if n == expected_values:
                    print(f"{year}-{month:02} already present, skipping")
                    continue
            except:
                pass

        params["start_date"] = start_date
        params["end_date"] = end_date
        r = requests.get(URL, params=params)
        #print(r.request.url)
        print(f"Downloading start_date={start_date}, end_date={end_date}")

        if r.ok:
            data = r.json()
            n = len(data["hourly"]["time"])
            if n != expected_values:
                print(f"Expected {expected_values} data points this month, got {n} at {year}-{month:02}. Continuing anyway.")

            hdfs.create(fname, overwrite=True, data=r.text)
        else:
            print(f"Request failed with status {r.status_code}: {r.reason}")
            print(r.text)
            if r.status_code == 429:
                print("\n\nWe exceeded the rate limit! Waiting one minute...")
                time.sleep(61)
