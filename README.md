# Industrial Machinery Anomaly Detection with Apache Flink

## Architecture

```
ai4i2020.csv
    |  producer.py            typed telemetry, rate limited, rolled into segments
    v
stream/segment-*.jsonl        complete segments, each published with an atomic rename
    |  flink_job.py           FileSource + monitor_continuously (discovers each segment)
    v
classified events  ->  stdout  +  output/flink_events.jsonl
    |  app.py                 tails the event stream from its last byte offset
    v
Streamlit dashboard
```

There are no per-event files anywhere: the producer batches events into one segment at a time and Flink picks up each finished segment as it appears. Both generated locations are gitignored and can be deleted at any time.

Flink's `monitor_continuously` discovers *new* files but never returns to a file that grew after it was first read, so the producer builds each segment inside `stream/.staging` and moves it into `stream/` only once complete. That keeps delivery lossless while the stream keeps growing.

## Methodology

1. **Dataset replay**: `producer.py` reads `ai4i2020.csv` once, converts each row into a typed telemetry record, and streams it in segments of 20 events at 20 events/sec by default.
2. **Flink processing**: `flink_job.py` discovers each segment with a continuous `FileSource`, parses every event, applies the anomaly rules, prints the classified event, and appends it to `output/flink_events.jsonl`.
3. **Analysis logic**: failures come from the dataset label, while extra anomalies are flagged from tool wear, power range, thermal drift, and mechanical load.
4. **Frontend**: `app.py` tails `output/flink_events.jsonl` starting from the byte offset it last read, so every refresh only parses the events written since the previous one. It keeps every event it has seen, so the KPI totals and the trend chart both cover the whole run: the x-axis always starts at the first event and grows 0-20, 0-40, 0-60 rather than sliding a fixed window. The page refreshes every second, the y-axis is snapped to a coarse 1/2/5 step so it does not rescale on every refresh, points are dropped once the series is dense, and a checkbox switches the event axis to a log scale. Only the most recent 20 events are listed in the table and counted for anomaly causes.

## Files

- `ai4i2020.csv`: source industrial machinery dataset (10,000 rows).
- `producer.py`: telemetry simulator, 20 events/sec by default.
- `flink_job.py`: Apache Flink streaming anomaly job.
- `app.py`: Streamlit dashboard.
- `requirements.txt`: Python dependencies.
- `install_windows.ps1`: installs Python, the dependencies, and Flink 1.19.1 under `C:\flink`.

Created at runtime and gitignored: `stream/` (incoming segments) and `output/` (classified event stream).

## Setup

Install Python, Apache Flink, and project dependencies:

```powershell
.\install_windows.ps1
```

Or manually:

```powershell
py -m pip install -r requirements.txt
```

## Run

Open three terminals in this folder. Start the job first so it is already following the stream, then start the producer:

```powershell
py -3.11 flink_job.py
```

```powershell
py -3.11 producer.py
```

```powershell
py -3.11 -m streamlit run app.py
```

Open the Streamlit URL shown in the terminal.

## Storage

Everything is disposable and nothing is tracked by git. While the job runs, two locations grow:

| Location | Growth at the defaults (20 events/sec, 20 events per segment) |
| --- | --- |
| `stream/` | ~20 segment files per second, ~4 KB each: ~275 MB and ~12,000 files per hour |
| `output/flink_events.jsonl` | ~300 KB per minute |
| `output/flink-log/` | a few MB per hour |

Each run resets both locations at startup, so there is nothing to do between runs. To remove everything after the last one:

```powershell
Remove-Item -Recurse -Force stream, output
```

Short segments mean many files. Raise `--segment-events` (or lower `--rate`) to trade granularity for a lot less file churn.

The dashboard holds every event it has seen for the length of the browser session, so a very long run grows its memory use. The chart itself is capped at 3000 points (older points are sampled out), but the totals and the event table still walk the full history on each refresh. Restarting the page clears it.

The producer and the job each restart their own output at startup so a run always starts from zero. Pass `--append` to either one to keep going from the existing files.

Segment size is controlled with `--segment-seconds` and `--segment-events`; shorter segments mean lower end-to-end latency and more files in `stream/`.
