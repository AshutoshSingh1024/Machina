import json
from collections import deque
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st


EVENT_LOG = Path("output/flink_events.jsonl")
WINDOW = 250
REFRESH_SECONDS = 1.0
SENSORS = ["rpm", "torque_nm", "tool_wear_min", "power_kw", "air_temp_k", "process_temp_k"]


def new_state():
    return {
        "offset": 0,
        "pending": "",
        "index": 0,
        "processed": 0,
        "failures": 0,
        "anomalies": 0,
        "parse_errors": 0,
        "last_event_at": None,
        "recent": deque(maxlen=WINDOW),
    }


def consume(path):
    """Append whatever the Flink job wrote to the event stream since the previous rerun."""
    if "stream" not in st.session_state:
        st.session_state.stream = new_state()

    state = st.session_state.stream
    if not path.exists():
        return state

    size = path.stat().st_size
    if size < state["offset"]:
        # The log was truncated or replaced, so start reading it from the beginning.
        st.session_state.stream = new_state()
        state = st.session_state.stream
        size = path.stat().st_size

    if size <= state["offset"]:
        return state

    with path.open("rb") as f:
        f.seek(state["offset"])
        chunk = f.read()
    state["offset"] += len(chunk)

    lines = (state["pending"] + chunk.decode("utf-8", "replace")).split("\n")
    state["pending"] = lines.pop()

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue

        state["index"] += 1
        state["processed"] += 1
        state["parse_errors"] += int("parse_error" in event)
        state["failures"] += int(bool(event.get("failure")))
        state["anomalies"] += int(bool(event.get("anomaly")))
        if event.get("event_time"):
            state["last_event_at"] = event["event_time"]
        event["event_index"] = state["index"]
        state["recent"].append(event)

    return state


def percent(part, whole):
    return f"{(part / whole * 100):.2f}%" if whole else "0.00%"


@st.fragment(run_every=REFRESH_SECONDS)
def live_dashboard():
    state = consume(EVENT_LOG)

    cols = st.columns(4)
    cols[0].metric("Processed", state["processed"])
    cols[1].metric("Failures", state["failures"], percent(state["failures"], state["processed"]))
    cols[2].metric("Anomalies", state["anomalies"], percent(state["anomalies"], state["processed"]))
    cols[3].metric(
        "Last update",
        (state["last_event_at"] or "n/a").split(".")[0].replace("T", " "),
    )

    if state["parse_errors"]:
        st.warning(f"{state['parse_errors']} events could not be parsed and are listed below.")

    if not state["recent"]:
        st.info("Waiting for the first events. Start flink_job.py and producer.py, then refresh.")
        return

    recent = pd.DataFrame(state["recent"])
    if "reasons" not in recent.columns:
        st.dataframe(recent, use_container_width=True, hide_index=True)
        return

    recent["reason_text"] = recent["reasons"].apply(
        lambda values: ", ".join(values) if isinstance(values, list) and values else "normal"
    )

    left, right = st.columns([2, 1])

    with left:
        st.subheader("Live Sensor Trend")
        metric = st.selectbox("Signal", SENSORS)
        if metric in recent.columns:
            chart = (
                alt.Chart(recent)
                .mark_line(point=True)
                .encode(
                    x="event_index:Q",
                    y=f"{metric}:Q",
                    color="anomaly:N",
                    tooltip=["udi", metric, "reason_text"],
                )
                .properties(height=360)
            )
            st.altair_chart(chart, use_container_width=True)

    with right:
        st.subheader("Anomaly Causes")
        reasons = recent.explode("reasons")
        reasons = reasons[reasons["reasons"].notna() & (reasons["reasons"] != "")]
        if reasons.empty:
            st.success("No anomalies in the rolling window.")
        else:
            st.bar_chart(reasons["reasons"].value_counts())

    st.subheader("Recent Events")
    columns = [c for c in ["event_index", "udi", "product_id", "type", *SENSORS, "failure", "anomaly", "reason_text"] if c in recent.columns]
    st.dataframe(
        recent[columns].sort_values("event_index", ascending=False),
        use_container_width=True,
        hide_index=True,
    )


st.set_page_config(page_title="Industrial Machine Anomaly Monitor", layout="wide")
st.title("Industrial Machine Anomaly Monitor")
st.caption(
    "Methodology: producer.py appends telemetry to a stream, flink_job.py tails it and applies "
    "the anomaly rules, and this page consumes the classified event stream as it is written."
)
live_dashboard()
