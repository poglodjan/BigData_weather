# Files for scheduling batch processing

## Files in this directory

* `batch_process.timer` — Systemd timer (runs daily at midnight)
* `batch_process.service` — Systemd service for batch processing
* `bigdata.service` — Main systemd service for Docker Compose
* `run_batch_processing.sh` — Batch processing script
* `cassandra_schema.cql` — Cassandra database schema (6 tables)
* `grafana-dashboard.json` — Grafana dashboard configuration

## Installation

The systemd service files are:
* `batch_process.timer`
* `batch_process.service`
* `bigdata.service`

They should be copied (or symlinked) into `~/.config/systemd/user/`
to be available for the systemd user daemon.

```bash
mkdir -p ~/.config/systemd/user/
cp setup/batch_process.timer ~/.config/systemd/user/
cp setup/batch_process.service ~/.config/systemd/user/
cp setup/bigdata.service ~/.config/systemd/user/
```

Further, a file `~/token` containing the electricitymaps API token
and a python venv at `~/env/` need to be set up.

```bash
# Create API token file
echo "your_electricitymaps_api_token" > ~/token
chmod 600 ~/token

# Create Python virtual environment
python3 -m venv ~/env
source ~/env/bin/activate
pip install requests pyhdfs pandas python-dateutil
deactivate
```

**Update the `REPO` path** in `run_batch_processing.sh` to point to your project directory.

The batch processing timer is started using

```bash
systemctl --user enable --now batch_process.timer
```

which runs the `run_batch_processing.sh` script every day at midnight.

Logs are found in

```bash
journalctl --user -u batch_process.service
```

## What batch processing does

The `run_batch_processing.sh` script performs three tasks:

1. **Fetch historical weather data** — Downloads data from 2017 to present (Open-Meteo API)
2. **Fetch historical electricity data** — Downloads Poland electricity data (Electricity Maps API)
3. **Train ML models** — Trains Gradient Boosted Tree models for solar (R²=0.78) and wind (R²=0.40) prediction, saves to HDFS

## Cassandra setup

Initialize the database schema:

```bash
docker exec -i cassandra cqlsh < setup/cassandra_schema.cql
```

This creates keyspace `weather` with 6 tables:
* `cur_weather` — Current weather measurements
* `cur_electricity` — Current electricity production
* `predictions` — ML predictions
* `solar_training` — Solar model training data
* `wind_training` — Wind model training data
* `ml_metrics` — Model performance metrics

## Grafana setup

1. Open http://localhost:2000
2. Add Cassandra data source: `cassandra:9042`, keyspace: `weather`
3. Import dashboard: `setup/grafana-dashboard.json`
