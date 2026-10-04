import argparse
import json
import os
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("FLINK_LOG_DIR", str(Path("output/flink-log").resolve()))
os.environ.setdefault("PYFLINK_PYTHON", r"C:\Progra~1\Python311\python.exe")
os.environ.setdefault("PYFLINK_CLIENT_EXECUTABLE", r"C:\Progra~1\Python311\python.exe")

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat


STATE = {
    "seen": 0,
    "failures": 0,
    "anomalies": 0,
    "recent": deque(maxlen=250),
}


def as_float(row, key):
    return float(row[key])


def classify(row):
    row = {key.lstrip("\ufeff"): value for key, value in row.items()}
    rpm = as_float(row, "Rotational speed [rpm]")
    torque = as_float(row, "Torque [Nm]")
    wear = as_float(row, "Tool wear [min]")
    air = as_float(row, "Air temperature [K]")
    process = as_float(row, "Process temperature [K]")
    power = rpm * torque / 9550.0

    reasons = []
    if int(row["Machine failure"]) == 1:
        reasons.append("dataset_failure_label")
    if wear >= 200:
        reasons.append("high_tool_wear")
    if power < 3.5 or power > 9.5:
        reasons.append("abnormal_power")
    if process - air < 7.5 or process - air > 12.5:
        reasons.append("thermal_drift")
    if torque > 60 or rpm < 1200:
        reasons.append("mechanical_load")

    return {
        "udi": int(row["UDI"]),
        "product_id": row["Product ID"],
        "type": row["Type"],
        "air_temp_k": air,
        "process_temp_k": process,
        "rpm": rpm,
        "torque_nm": torque,
        "tool_wear_min": wear,
        "power_kw": round(power, 3),
        "failure": int(row["Machine failure"]),
        "anomaly": bool(reasons),
        "reasons": reasons,
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


class JsonMetricsWriter:
    def __init__(self, output_dir):
        self.output_dir = Path(output_dir)
        self.events_path = self.output_dir / "flink_events.jsonl"
        self.latest_path = self.output_dir / "latest_metrics.json"

    def __call__(self, line):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        try:
            event = classify(json.loads(line))
        except Exception as exc:
            event = {"event_time": datetime.now(timezone.utc).isoformat(), "parse_error": str(exc), "raw": line}

        STATE["seen"] += 1
        if event.get("failure"):
            STATE["failures"] += 1
        if event.get("anomaly"):
            STATE["anomalies"] += 1
        STATE["recent"].append(event)

        with self.events_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")

        snapshot = {
            "processed": STATE["seen"],
            "failures": STATE["failures"],
            "anomalies": STATE["anomalies"],
            "failure_rate": round(STATE["failures"] / STATE["seen"], 4),
            "anomaly_rate": round(STATE["anomalies"] / STATE["seen"], 4),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "recent": list(STATE["recent"]),
        }
        self.latest_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        return json.dumps(event)


def main():
    parser = argparse.ArgumentParser(description="PyFlink socket job for machine anomaly analysis.")
    parser.add_argument("--stream-dir", default="input_stream")
    parser.add_argument("--output", default="output")
    args = parser.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_python_executable(os.environ["PYFLINK_PYTHON"])
    Path(args.output, "flink-log").mkdir(parents=True, exist_ok=True)
    source = (
        FileSource.for_record_stream_format(StreamFormat.text_line_format(), args.stream_dir)
        .monitor_continuously(Duration.of_seconds(1))
        .build()
    )
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "machine-file-stream")
    stream.map(JsonMetricsWriter(args.output), output_type=Types.STRING()).print()
    env.execute("industrial-machinery-anomaly-detection")


if __name__ == "__main__":
    main()
