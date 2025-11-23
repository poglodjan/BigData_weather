#!/usr/bin/env python3

import datetime
import json
import os
import sys

if len(sys.argv) > 1:
    PATH = sys.argv[1]
else:
    PATH = "../historical-electricity"

# Expected number of 5-minute observations per day
EXPECTED_NUMBER = 24 * 60 // 5

any_errors = False

for zone in os.listdir(PATH):
    zone_path = PATH + "/" + zone
    for value in os.listdir(zone_path):
        dir = zone_path + "/" + value
        files = os.listdir(dir)
        files.sort()
        start = files[0].removesuffix(".json")
        start_date = datetime.date.fromisoformat(start)
        end = files[-1].removesuffix(".json")
        end_date = datetime.date.fromisoformat(end)

        cur = start_date
        while cur <= end_date:
            fname = dir + "/" + str(cur) + ".json"
            with open(fname) as file:
                data = json.load(file)
            n = len(data["data"])
            # Allow less data on last day
            if n != EXPECTED_NUMBER and cur != end_date:
                print(f"{value}@{cur} has wrong amount of entries!")
                print(f"Has {n}, should be {EXPECTED_NUMBER}")
                any_errors = True

            # Verify initial timestamp
            start_datetime = datetime.datetime.combine(cur, datetime.time())
            first_datetime = datetime.datetime.fromisoformat(data["data"][0]["datetime"]).replace(tzinfo=None)
            if start_datetime != first_datetime:
                print(f"Unexpected timestamp: Expected {start_datetime}, got {first_datetime}")
                any_errors = True

            # Parameter-specific tests
            if value == "carbon-intensity":
                for d in data["data"]:
                    if not isinstance(d["carbonIntensity"], int):
                        print(f"No integer value at {value}@{cur}, but value: {d["carbonIntensity"]}")
                        any_errors = True
            elif value == "renewable-energy":
                for d in data["data"]:
                    val = d["value"]
                    if not isinstance(val, (int, float)):
                        print(f"No integer value at {value}@{cur}, but value: {val}")
                        any_errors = True
                    if val < 0 or val > 100:
                        print(f"{value}@{cur}: Expected a percentage between 0-100, but got {val}")
                        any_errors = True
            elif value == "electricity-mix":
                for d in data["data"]:
                    mix = d["mix"]
                    for part in ("nuclear", "geothermal", "biomass", "coal",
                                 "wind", "solar", "hydro", "gas", "oil",
                                 "hydro discharge", "battery discharge"):
                        if not isinstance(mix[part], (int, float, type(None))):
                            print(f"No integer value at {value}@{cur}, but value: {mix[part]}")
                            any_errors = True
            elif value == "total-load":
                for d in data["data"]:
                    val = d["value"]
                    if not isinstance(val, (int, float)):
                        print(f"No numerical value at {value}@{cur}, but value: {val}")
                        any_errors = True

            cur += datetime.timedelta(days=1)

if not any_errors:
    print(f"Complete data between {start_date} and {end_date} found")
else:
    print("Some data was found missing")
    exit(1)
