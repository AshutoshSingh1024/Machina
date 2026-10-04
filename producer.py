import argparse
import csv
import json
import sys
import time
from pathlib import Path


POWER_DIVISOR = 9550.0


def read_rows(path):
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def to_telemetry(row):
    rpm = float(row["Rotational speed [rpm]"])
    torque = float(row["Torque [Nm]"])
    return {
        "udi": int(row["UDI"]),
        "product_id": row["Product ID"],
        "type": row["Type"],
        "air_temp_k": float(row["Air temperature [K]"]),
        "process_temp_k": float(row["Process temperature [K]"]),
        "rpm": rpm,
        "torque_nm": torque,
        "tool_wear_min": float(row["Tool wear [min]"]),
        "power_kw": round(rpm * torque / POWER_DIVISOR, 3),
        "failure": int(row["Machine failure"]) == 1,
    }


def write_stream(telemetry, path, rate, loops, append):
    delay = 1.0 / rate if rate > 0 else 0.0
    sent = 0
    cycle = 0
    next_report = 1000

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a" if append else "w", encoding="utf-8") as stream:
        while loops < 0 or cycle < loops:
            for record in telemetry:
                stream.write(json.dumps(record) + "\n")
                stream.flush()
                sent += 1
                if sent == next_report:
                    print(f"Sent {sent} events", flush=True)
                    next_report += 1000
                if delay:
                    time.sleep(delay)
            cycle += 1
            if loops >= 0 and cycle >= loops:
                break
            print(f"Replay {cycle} complete ({sent} events); starting the next pass", flush=True)

    return sent


def main():
    parser = argparse.ArgumentParser(
        description="Replay AI4I machine telemetry into a newline-delimited JSON stream for Flink."
    )
    parser.add_argument("--dataset", default="ai4i2020.csv")
    parser.add_argument("--stream-dir", default="stream")
    parser.add_argument("--rate", type=float, default=20.0, help="events per second, 0 for as fast as possible")
    parser.add_argument("--loops", type=int, default=-1, help="dataset passes to send, -1 to replay forever")
    parser.add_argument("--append", action="store_true", help="append to an existing stream instead of restarting it")
    args = parser.parse_args()

    rows = read_rows(Path(args.dataset))
    if not rows:
        raise SystemExit(f"No rows found in {args.dataset}")
    telemetry = [to_telemetry(row) for row in rows]

    path = Path(args.stream_dir) / "telemetry.jsonl"
    print(
        f"Loaded {len(telemetry)} rows from {args.dataset}; "
        f"streaming at {args.rate:g}/sec to {path.resolve()}",
        flush=True,
    )

    sent = write_stream(telemetry, path, args.rate, args.loops, args.append)
    print(f"Done. Sent {sent} events.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
