#!/usr/bin/env python3

import datetime
import json
import os
import sys

if len(sys.argv) > 1:
    PATH = sys.argv[1]
else:
    PATH = "../historical-weather"

any_errors = False

for location in os.listdir(PATH):
    dir = PATH + "/" + location
    files = os.listdir(dir)
    files.sort()
    start = files[0].removesuffix(".json")
    start_date = datetime.date.fromisoformat(start + "-01")
    end = files[-1].removesuffix(".json")
    end_date = datetime.date.fromisoformat(end + "-01")

    cur = start_date
    while cur <= end_date:
        fname = dir + f"/{cur.year}-{cur.month:02}.json"

        # Calculate end of month
        start_month = datetime.date(year=cur.year, month=cur.month, day=1)
        n_month = (cur.month % 12) + 1
        n_year = cur.year + 1 if n_month < cur.month else cur.year
        next_month = datetime.date(year=n_year, month=n_month, day=1)
        end_month = next_month - datetime.timedelta(days=1)
        # Number of hours in this month
        expected_values = (next_month - start_month).days * 24

        with open(fname) as file:
            data = json.load(file)
        n = len(data["hourly"]["time"])
        if n != expected_values:
            print(f"{cur.year}-{cur.month:02} has wrong amount of entries!")
            print(f"Has {n}, should be {expected_values}")
            any_errors = True

        # Verify first timestamp
        start_datetime = datetime.datetime.combine(start_month, datetime.time())
        first_datetime = datetime.datetime.fromisoformat(data["hourly"]["time"][0])
        if first_datetime != start_datetime:
            print(f"Unexpected timestamp: Expected {start_datetime}, got {first_datetime}")
            any_errors = True

        cur = next_month

if not any_errors:
    print(f"Complete data between {start_date} and {end_month} found")
else:
    print("Some data was found missing")
    exit(1)
