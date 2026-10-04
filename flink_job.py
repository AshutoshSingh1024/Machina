import argparse
import ctypes
import json
import os
import sys
from ctypes import wintypes
from datetime import datetime, timezone
from pathlib import Path


def python_executable():
    """PyFlink's Beam runner splits the interpreter path on spaces, so prefer the 8.3 short path."""
    candidate = sys.executable
    if os.name != "nt":
        return candidate
    try:
        get_short_path_name = ctypes.windll.kernel32.GetShortPathNameW
        get_short_path_name.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
        get_short_path_name.restype = wintypes.DWORD
        size = get_short_path_name(candidate, None, 0)
        if size == 0:
            return candidate
        buffer = ctypes.create_unicode_buffer(size)
        if get_short_path_name(candidate, buffer, size) == 0:
            return candidate
        return buffer.value or candidate
    except (AttributeError, OSError):
        return candidate


PYTHON_EXECUTABLE = python_executable()

os.environ.setdefault("FLINK_LOG_DIR", str(Path("output/flink-log").resolve()))
os.environ.setdefault("PYFLINK_PYTHON", PYTHON_EXECUTABLE)
os.environ.setdefault("PYFLINK_CLIENT_EXECUTABLE", PYTHON_EXECUTABLE)

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def classify(record):
    rpm = float(record["rpm"])
    torque = float(record["torque_nm"])
    wear = float(record["tool_wear_min"])
    air = float(record["air_temp_k"])
    process = float(record["process_temp_k"])
    power = float(record["power_kw"])

    reasons = []
    if record["failure"]:
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
        "udi": record["udi"],
        "product_id": record["product_id"],
        "type": record["type"],
        "air_temp_k": air,
        "process_temp_k": process,
        "rpm": rpm,
        "torque_nm": torque,
        "tool_wear_min": wear,
        "power_kw": power,
        "failure": bool(record["failure"]),
        "anomaly": bool(reasons),
        "reasons": reasons,
        "event_time": utc_now(),
    }


class EventStreamWriter:
    """Classifies each telemetry line, appends it to the event stream, and returns it for stdout.

    The stream file handle is opened lazily on the first call because the operator is
    serialised into the Flink task, which cannot pickle an open file object.
    """

    def __init__(self, event_log):
        self.event_log = Path(event_log)
        self.handle = None

    def __call__(self, line):
        if self.handle is None:
            self.event_log.parent.mkdir(parents=True, exist_ok=True)
            self.handle = self.event_log.open("a", encoding="utf-8")

        try:
            event = classify(json.loads(line))
        except Exception as exc:
            event = {
                "event_time": utc_now(),
                "parse_error": str(exc),
                "raw": line if isinstance(line, str) else line.decode("utf-8", "replace"),
            }

        payload = json.dumps(event)
        self.handle.write(payload + "\n")
        self.handle.flush()
        return payload


def main():
    parser = argparse.ArgumentParser(
        description="PyFlink streaming job for industrial machine anomaly detection."
    )
    parser.add_argument("--stream-dir", default="stream")
    parser.add_argument("--event-log", default="output/flink_events.jsonl")
    parser.add_argument(
        "--append", action="store_true", help="keep an existing event log instead of restarting it"
    )
    args = parser.parse_args()

    stream_dir = Path(args.stream_dir)
    stream_dir.mkdir(parents=True, exist_ok=True)
    Path("output/flink-log").mkdir(parents=True, exist_ok=True)

    if not args.append:
        event_log = Path(args.event_log)
        event_log.parent.mkdir(parents=True, exist_ok=True)
        event_log.write_text("", encoding="utf-8")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_python_executable(os.environ["PYFLINK_PYTHON"])

    source = (
        FileSource.for_record_stream_format(StreamFormat.text_line_format(), str(stream_dir))
        .monitor_continuously(Duration.of_seconds(1))
        .build()
    )
    telemetry = env.from_source(source, WatermarkStrategy.no_watermarks(), "telemetry-stream", Types.STRING())
    telemetry.map(EventStreamWriter(args.event_log), output_type=Types.STRING()).print()

    env.execute("industrial-machinery-anomaly-detection")


if __name__ == "__main__":
    main()
