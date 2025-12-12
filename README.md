## Usage
To explore all recorded experiments, its only necessary to start the monitoring containers:
```bash
docker compose up grafana prometheus
```
To quickly inspect the data and automatically open the Grafana dashboard with the correct time range, use the `open_grafana_range.py` script:
```bash
python scripts/open_grafana_range.py dhr
```
The script also supports more fine-grained filtering. For example: 
- filtering by both workload AND TPS, e.g. to get all 'dhr' runs with 10k TPS: `python scripts/open_grafana_range.py dhr 10000tps` 
- filtering for experiements ran with a certain number of partitions: `python scripts/open_grafana_range.py 8part` 

After running the script, just select the index of the experiment and a browser window will open with the Grafana dashboard preloaded to the experiment’s time range. All the time-series data is stored in the `prometheus-data` directory. 
Any results that has the **old_** prefix are still valid runs, but they happened before the implementation of the extra metrics, so some of the newer dashboards will have missing data (mainly the operator level metrics and the downscaling metrics, these still have cpu, memory, backpressure, latency breakdown, networking, etc...). 

*Sidenote*: in some experiments with a lot of backpressure, the script sometimes records the end timestamp slightly too early. This can cause the Grafana view to stop before backpressure fully returns to zero. If the dashboard seems truncated, simply extend the “to” timestamp in Grafana by 10–20 seconds to show the full experiment duration.

Alternatively, in each experiment directory in `results/` there is a `metadata.json` file that contains the information partaining to that experiment run (it contains some extra info compared to the overview that is displayed by `open_grafana_range.py`), example: 
```json
{
  "workload": "dhr",
  "messages_per_second": 10000,
  "n_partitions": 4,
  "n_keys": 2000,
  "start": "2025-11-26T23:41:21.895897",
  "end": "2025-11-26T23:43:41.336478",
  "duration (s)": 60,
  "zipf_const": 0
}
```
From here, it is also possible to just copy the `start` and `end` timestamps directly into Grafana to display the correct time window.

## Running extra experiments
Running command experiments was not changed:
`./scripts/run_experiment.sh [WORKLOAD_NAME] [INPUT_RATE] [N_KEYS] [N_PART] [ZIPF_CONST] [CLIENT_THREADS] [TOTAL_TIME] [SAVING_DIR] [WARMUP_SECONDS] [EPOCH_SIZE]`
example: (`./scripts/run_experiment.sh ycsbt 5000 100000 4 0.0 1 180 results 10 4000`)
The only difference is after the workload finished, only the workers and coordinators containers are stopped, *the prometheus and grafana container will keep running in order to easily observe the recently captured metrics*. Thus, to fully shut doen the cluster it is needed to manually run: `./scripts/stop_styx_cluster.sh`.