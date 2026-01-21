# Big Data Pipeline — Docker Compose Overview

This project runs a data pipeline for real-time weather data analysis and renewable energy production prediction using HDFS, Apache NiFi, Apache Kafka, Apache Spark, Cassandra, and Grafana.
It consists of 11 containers defined in docker-compose.yml, implementing a Lambda Architecture for both batch and stream processing.

## How to Run

```bash
docker compose up -d --build
```
 This will start 11 containers. Initial startup takes 2-3 minutes.

### Verify Services

```bash
docker compose up -d --build
```
All containers should show "Up" status (except hdfs-init which exits after completion).

### If directiories are not created automatically:
```bash
docker exec -it namenode hdfs dfs -mkdir -p /user/hdfs/electricitymaps/zone_PL
docker exec -it namenode hdfs dfs -mkdir -p /user/hdfs/openmeteo/warsaw
docker exec -it namenode hdfs dfs -chmod -R 777 /user
docker exec -it namenode hdfs dfs -ls -R /user
```

### Initialize Cassandra Schema
```bash
docker exec -i cassandra cqlsh < setup/cassandra_schema.cql
```

### Configure NiFi Flow

Open NiFi UI: **http://localhost:8080/nifi**
Upload template: **nifi/nifi-final.xml**
Add your Electricity Maps API token in the Electricity Maps processor
Start all processors

### Clean 

```bash
docker-compose down
```

---

## System Architecture

The system implements **Lambda Architecture** with three layers:

- **Speed Layer**: Real-time processing with Kafka + Spark Streaming
- **Batch Layer**: Historical analysis with HDFS + Spark
- **Serve Layer**: Results storage in Cassandra + visualization in Grafana

### Data Sources
- **Open-Meteo API**: Weather forecasts (temperature, wind speed, solar radiation, humidity)
- **Electricity Maps API**: Real-time electricity mix data for Poland (solar and wind power production)

---

## Containers

### 1) NameNode
Main HDFS service storing **filesystem metadata** (namespace, block locations).
- Image: `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`
- Ports: `9000` (HDFS RPC), `9870` (Web UI / WebHDFS)
- Volume: `hdfs_namenode`

### 2) DataNode
Stores the actual **HDFS data blocks** and registers with the NameNode.
- Image: `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8`
- Ports: `9864`, `9866`
- Volume: `hdfs_datanode`
- Depends on: NameNode

### 3) hdfs-init
Initialization container that creates HDFS directory structure on startup.
- Image: `bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8`
- Creates: `/user/hdfs/electricitymaps/zone_PL`, `/user/hdfs/openmeteo/warsaw`
- Exits after: Successfully creating directories

### 4) hdfs-ingester
Flask microservice that receives JSON (`POST /ingest`) from NiFi and writes it to HDFS using WebHDFS.
- Built from: `./hdfs_ingester`
- Port: `5050` (mapped from internal 5000)
- Depends on: NameNode, DataNode, hdfs-init

### 5) NiFi
Dataflow engine that orchestrates data ingestion from external APIs and routes to HDFS and Kafka.
- Image: `apache/nifi:1.25.0`
- Port: `8080` (NiFi UI)
- Volumes: `./nifi` (templates), `nifi_state`, `nifi_conf`
- Depends on: hdfs-ingester, Kafka

### 6) Kafka
Message broker for stream processing.
- Image: `apache/kafka-native:4.1.1`
- Port: `9092`
- Mode: KRaft (no ZooKeeper)
- Topics: `openmeteo_current`, `openmeteo_minutely_15`, `openmeteo_hourly`, `electricity_mix`, etc.

### 7) Spark
Stream processing and ML engine for real-time predictions.
- Built from: `./spark`
- Port: `4040` (Spark UI)
- Volume: `./:/app`
- Depends on: Kafka
- Runs: `./analysis/streaming_ml.py` for real-time ML predictions

### 8) Cassandra
Time-series database storing prediction results.
- Image: `cassandra:4.1`
- Port: `9042`
- Volume: `cassandra_data`
- Keyspace: `weather`
- Tables: `cur_weather`, `cur_electricity`, `predictions`, `ml_metrics`, etc.

### 9) Grafana
Visualization platform for dashboards.
- Image: `grafana/grafana`
- Port: `2000` (mapped from internal 3000)
- Volume: `grafana-storage`
- Dashboard: `setup/grafana-dashboard.json`

### 10) Data-Dam
Kafka consumer for backup of streaming data to files.
- Built from: `./data-dam`
- Volume: `data-dam-store:/data`
- Backs up all Kafka messages to: `/data/{topic}/{timestamp}`

### 11) Tester
Runs end-to-end tests validating the entire pipeline (NiFi → Ingester → HDFS → Kafka → Cassandra).
- Built from: `./tests`
- Depends on: NiFi, Ingester, NameNode

--- 
## Persisted Data (Volumes)

| Volume            | Purpose                              |
|-------------------|--------------------------------------|
| `hdfs_namenode`   | HDFS metadata (NameNode)             |
| `hdfs_datanode`   | HDFS data blocks (DataNode)          |
| `nifi_state`      | NiFi flow state & configuration      |
| `nifi_conf`       | NiFi configuration files             |
| `cassandra_data`  | Cassandra database                   |
| `grafana-storage` | Grafana dashboards                   |
| `data-dam-store`  | Kafka message backups                |

---

## Key URLs

| Service       | URL                              | Description                    |
|---------------|----------------------------------|--------------------------------|
| Grafana       | http://localhost:2000            | Dashboards and visualizations  |
| NameNode UI   | http://localhost:9870            | HDFS browser and status        |
| NiFi UI       | http://localhost:8080/nifi       | Data flow designer             |
| Spark UI      | http://localhost:4040            | Spark jobs and stages          |
| Ingester API  | http://localhost:5050/ingest     | REST endpoint for HDFS writes  |

---


## Machine Learning Models

The system trains **Gradient Boosted Tree (GBT) regression models** for predicting renewable energy production:

- **Solar power prediction model** 
- **Wind power prediction model** 

**Features used:**
- Temperature (2m)
- Relative humidity
- Wind speed (10m)
- Cloud cover
- Shortwave radiation

**Models stored in HDFS:**
- `hdfs://namenode:9000/user/hdfs/models/solar_model`
- `hdfs://namenode:9000/user/hdfs/models/wind_model`
- `hdfs://namenode:9000/user/hdfs/models/weather_assembler`

---

## Contributors

- Magdalena Jeczeń
- Aleksandra Kulczycka
- Jan Pogłód
- Zofia Sasimowska
- Gabriel Vogel

---

**Project**: Real-Time Weather Data Analysis for Predicting Renewable Energy Production  
**Last Updated**: January 2025
