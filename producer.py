import argparse
import csv
import json
import os
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


class SegmentWriter:
    """Writes the stream as complete segment files, publishing each one only once it is finished.

    Flink's monitor_continuously discovers new files but never returns to a file that grew
    after it was first read, so a segment has to be closed before it is published or the
    events written after the first read would be lost. Each segment is built inside the
    .staging subdirectory, which the monitored directory does not recurse into, and is then
    moved into place with an atomic rename.
    """

    def __init__(self, directory, segment_events, segment_seconds):
        self.directory = directory
        self.staging = directory / ".staging"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.staging.mkdir(parents=True, exist_ok=True)
        self.segment_events = segment_events
        self.segment_seconds = segment_seconds
        self.index = 0
        self.handle = None
        self.pending = None
        self.written = 0
        self.opened = 0.0

    def start(self, index):
        self.index = index
        self._open()

    def _open(self):
        self.pending = self.staging / f"segment-{self.index:012d}.jsonl"
        self.handle = self.pending.open("w", encoding="utf-8")
        self.index += 1
        self.written = 0
        self.opened = time.monotonic()

    def write(self, line):
        self.handle.write(line)
        self.handle.flush()
        self.written += 1

    def due(self):
        if self.written >= self.segment_events:
            return True
        return self.segment_seconds > 0 and (time.monotonic() - self.opened) >= self.segment_seconds

    def roll(self):
        self._publish()
        self._open()

    def close(self):
        if self.handle is None:
            return
        if self.written:
            self._publish()
        else:
            self.handle.close()
            if self.pending.exists():
                self.pending.unlink()
        self.handle = None

    def _publish(self):
        self.handle.close()
        target = self.directory / self.pending.name
        os.replace(self.pending, target)
        print(f"Published {target.name} ({self.written} events)", flush=True)


def next_index(directory):
    existing = sorted(directory.glob("segment-*.jsonl"))
    return int(existing[-1].name[len("segment-") : -len(".jsonl")]) + 1 if existing else 0


def write_stream(telemetry, directory, rate, loops, append, segment_events, segment_seconds):
    delay = 1.0 / rate if rate > 0 else 0.0

    if append:
        index = next_index(directory)
    else:
        for stale in directory.glob("segment-*.jsonl"):
            stale.unlink()
        index = 0

    writer = SegmentWriter(directory, segment_events, segment_seconds)
    writer.start(index)

    sent = 0
    cycle = 0
    next_report = 1000

    try:
        while loops < 0 or cycle < loops:
            for record in telemetry:
                writer.write(json.dumps(record) + "\n")
                sent += 1
                if sent == next_report:
                    print(f"Sent {sent} events", flush=True)
                    next_report += 1000
                if writer.due():
                    writer.roll()
                if delay:
                    time.sleep(delay)
            cycle += 1
            if loops >= 0 and cycle >= loops:
                break
            print(f"Replay {cycle} complete ({sent} events); starting the next pass", flush=True)
    finally:
        writer.close()

    return sent


def main():
    parser = argparse.ArgumentParser(
        description="Replay AI4I machine telemetry as newline-delimited JSON segments for Flink."
    )
    parser.add_argument("--dataset", default="ai4i2020.csv")
    parser.add_argument("--stream-dir", default="stream")
    parser.add_argument("--rate", type=float, default=20.0, help="events per second, 0 for as fast as possible")
    parser.add_argument("--loops", type=int, default=-1, help="dataset passes to send, -1 to replay forever")
    parser.add_argument("--segment-seconds", type=float, default=10.0, help="roll a segment after this long")
    parser.add_argument("--segment-events", type=int, default=2000, help="roll a segment after this many events")
    parser.add_argument("--append", action="store_true", help="keep existing segments instead of restarting")
    args = parser.parse_args()

    rows = read_rows(Path(args.dataset))
    if not rows:
        raise SystemExit(f"No rows found in {args.dataset}")
    telemetry = [to_telemetry(row) for row in rows]

    directory = Path(args.stream_dir)
    print(
        f"Loaded {len(telemetry)} rows from {args.dataset}; "
        f"streaming at {args.rate:g}/sec into {directory.resolve()}",
        flush=True,
    )

    sent = write_stream(
        telemetry,
        directory,
        args.rate,
        args.loops,
        args.append,
        args.segment_events,
        args.segment_seconds,
    )
    print(f"Done. Sent {sent} events.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
