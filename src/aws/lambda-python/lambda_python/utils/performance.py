
metrics = {}


def log_metric(name: str, time_ms: float):
    metrics[name] = metrics.get(name, 0) + time_ms


def metric_value(name: str) -> float:
    return metrics[name]

