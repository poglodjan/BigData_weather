# Files for scheduling batch processing

## Installation

The systemd service files are:

* `batch_process.timer`
* `batch_process.service`
* `bigdata.service`

They should be copied (or symlinked) into `~/.config/systemd/user/`
to be available for the systemd user daemon.

Further, a file `~/token` containing the electricitymaps API token
and a python venv at `~/env/` need to be set up.

The batch processing timer is started using

`systemctl --user enable --now batch_process.timer`

which runs the `run_batch_processing.sh` script every day at midnight.

Logs are found in

`journalctl -eu batch_process.service`
