import argparse
import csv
import json
import socket
import time
from pathlib import Path


def rows(path: Path):
    with path.open(newline="", encoding="utf-8-sig") as f:
        cached_rows = list(csv.DictReader(f))
        while True:
            for row in cached_rows:
                yield {key.lstrip("\ufeff"): value for key, value in row.items()}


def main():
    parser = argparse.ArgumentParser(description="Replay AI4I machine rows to Flink at a fixed rate.")
    parser.add_argument("--dataset", default="ai4i2020.csv")
    parser.add_argument("--mode", choices=["files", "socket"], default="files")
    parser.add_argument("--stream-dir", default="input_stream")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--rate", type=float, default=20.0)
    args = parser.parse_args()

    delay = 1.0 / args.rate
    dataset = Path(args.dataset)

    if args.mode == "files":
        stream_dir = Path(args.stream_dir)
        stream_dir.mkdir(parents=True, exist_ok=True)
        print(f"Writing {args.rate:g} JSON events/sec to {stream_dir.resolve()}")
        for index, row in enumerate(rows(dataset), start=1):
            event_path = stream_dir / f"event_{index:012d}.json"
            event_path.write_text(json.dumps(row), encoding="utf-8")
            time.sleep(delay)
        return

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((args.host, args.port))
        server.listen(1)
        print(f"Producer ready on {args.host}:{args.port}; waiting for Flink...")
        conn, addr = server.accept()
        print(f"Flink connected from {addr}; sending {args.rate:g} rows/sec")
        with conn:
            for row in rows(dataset):
                conn.sendall((json.dumps(row) + "\n").encode("utf-8"))
                time.sleep(delay)


if __name__ == "__main__":
    main()
