#!/usr/bin/env python3

import datetime
import json
import os
import sys

import pyhdfs
import requests


START = datetime.date(2024, 1, 1)
# Which timerange to pull in one request
# 24 hours is the most that is allowed with 5-minute granularity
STEP = datetime.timedelta(days=1)

BASE_URL = "https://api.electricitymaps.com/v3/"
VALUES = ["carbon-intensity", "renewable-energy", "electricity-mix", "total-load"]
ZONE = "PL"

TOKEN = os.environ.get("ELECTRICITYMAPS_TOKEN")

NAMENODE_HOST = os.environ.get('NAMENODE_HOST', 'localhost')
NAMENODE_PORT = os.environ.get('NAMENODE_PORT', '9870')
HDFS_USER     = os.environ.get('HDFS_USER', 'hdfs')
HDFS_PATH = f"/user/{HDFS_USER}/electricitymaps/history/zone_PL"


hdfs = pyhdfs.HdfsClient(f"{NAMENODE_HOST}:{NAMENODE_PORT}",
                         user_name=HDFS_USER)
if TOKEN is None:
    print("Missing ELECTRICITYMAPS_TOKEN environment variable")
    exit(1)


def fetch_value(value):
    dir = HDFS_PATH + "/" + value
    hdfs.mkdirs(dir)
    cur = START
    today = datetime.date.today()
    url = BASE_URL + value + "/past-range"
    params = {
        "zone": ZONE,
        "temporalGranularity": "5_minutes",
        # Nor estimations in historical data
        "disableEstimations": "true",
        # Consider only local production, not imported energy
        # (only affects electricity-mix)
        "flowTraced": "false",
    }
    while cur <= today:
        # Write data to PATH/ZONE/VALUE/DATE.json, for example
        # ./historical-electricity/zone_PL/electricity-mix/2025-01-01.json
        fname = dir + "/" + str(cur) + ".json"

        # Check if the file is already present
        if hdfs.exists(fname):
            try:
                with hdfs.open(fname) as file:
                    data = json.load(file)
                if len(data["data"]) == 288:
                    print(f"{value}@{cur} already present, skipping")
                    cur += STEP
                    continue
            except:
                pass

        params["start"] = cur
        params["end"] = cur + STEP
        r = requests.get(url, params=params, headers={"auth-token": TOKEN})
        print(r.request.url)

        if r.ok:
            data = r.json()
            n = len(data["data"])
            if n != 288:
                print(f"Expected 288 data points per day, got {n} at day {cur}. Continuing anyway.")

            hdfs.create(fname, overwrite=True, data=r.text)
        else:
            print(f"Request failed with status {r.status_code}: {r.reason}")
            print(r.text)

        cur += STEP

for val in VALUES:
    fetch_value(val)
