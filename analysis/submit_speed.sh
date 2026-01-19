#!/usr/bin/bash

# There are problems getting the kafka library into spark. Solution from
# https://stackoverflow.com/a/69559038
exec spark-submit \
  --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
  --conf spark.cassandra.connection.host=cassandra \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,com.datastax.spark:spark-cassandra-connector_2.13:3.5.1" \
  /app/analysis/streaming_ml.py
