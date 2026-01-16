#!/usr/bin/bash

PYTHON_ENV=~/env/bin/python3
REPO=~/BigData_weather

export ELECTRICITYMAPS_TOKEN
ELECTRICITYMAPS_TOKEN=$(cat ~/token)

# Fetch any missing history
${PYTHON_ENV} "${REPO}/history/historical_weather.py"
${PYTHON_ENV} "${REPO}/history/historical_electricity.py"

# Run spark batch processing job
docker exec --workdir="/app" spark spark-submit "./analysis/analysis.py"
exit $?
