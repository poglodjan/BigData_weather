#!/usr/bin/bash

PYTHON_ENV=~/env/bin/python3
REPO=~/BigData_weather

export ELECTRICITYMAPS_TOKEN
ELECTRICITYMAPS_TOKEN=$(cat ~/token)

# Fetch any missing history
${PYTHON_ENV} "${REPO}/history/historical_weather.py"
${PYTHON_ENV} "${REPO}/history/historical_electricity.py"

# Run spark batch processing job
docker exec --workdir="/app" spark \
	spark-submit \
	--conf spark.cassandra.connection.host=cassandra \
	--conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
	--packages "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1" \
	"./analysis/analysis.py"
exit $?
