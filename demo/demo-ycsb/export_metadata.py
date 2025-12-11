import requests, json
import os 
from datetime import datetime

PROM = "http://localhost:9090"


def save_data(data, save_dir, filename):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    with open(os.path.join(save_dir, filename), "w") as f:
        json.dump(data, f, indent=2)

def save_metadata(workload, start, end, out_path, n_partitions, messages_per_second, n_keys, seconds, interval_seconds, delta_tps, zipf_const, epoch_size):
    metadata = {
        "workload": workload,
        "messages_per_second": messages_per_second,
        "n_partitions": n_partitions,
        "n_keys": n_keys,
        "start": datetime.fromtimestamp(start).isoformat(),
        "end": datetime.fromtimestamp(end).isoformat(),
        "duration (s)": seconds,
        "zipf_const": zipf_const,
        "epoch_size": epoch_size,
    }
    if interval_seconds is not None:
        metadata["increase_interval"] = interval_seconds
    if delta_tps is not None:
        metadata["increase_amount"] = delta_tps

    metadata["n_threads"] = 1
    
    save_data(metadata, out_path, "metadata.json")


def export_all_metrics(workload, start, end, step, out_path, n_partitions, messages_per_second):
    metric_set = {
        "latency": "avg by(instance) (worker_cpu_usage_percent)",
        "memory": "avg by(instance) (worker_memory_usage_mb) * 1000000",
        "throughput": f"sum(rate(worker_epoch_throughput_tps[{step}]))" ,
        "latency_breakdown": "avg(latency_breakdown) by (component)",
        "transaction_latency": "avg(worker_epoch_latency_ms)",
        "snapshotting_time": "avg(worker_total_snapshotting_time_ms)",
        "backpressure": "sum(worker_backpressure)", 
        "queue_backlog": "sum(queue_backlog)"
    }

    timestamp = datetime.now().strftime("%m%d_%H%M")  # e.g., "1205_1430" for Dec 5th, 2:30 PM
    save_dir = os.path.join(out_path, f"{workload}_{messages_per_second}tps_{n_partitions}partitions_{timestamp}")
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    
    print(f"Exporting metrics to {save_dir}")
    for metric_name, metric_query in metric_set.items(): 
        data = get_metric_data(metric_query, start, end, step)
        with open(os.path.join(save_dir, f"{metric_name}.json"), "w") as f:
            json.dump(data, f, indent=2)


def get_metric_data(query, start, end, step):
    resp = requests.get(f"{PROM}/api/v1/query_range", params={
        "query": query,
        "start": start,
        "end": end,
        "step": step,
    })
    resp.raise_for_status()
    return resp.json()
