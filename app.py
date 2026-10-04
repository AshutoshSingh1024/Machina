import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st


OUTPUT = Path("output/latest_metrics.json")


st.set_page_config(page_title="Industrial Machine Anomaly Monitor", layout="wide")
st.title("Industrial Machine Anomaly Monitor")

st.caption(
    "Methodology: Streamlit watches the PyFlink output snapshot, then renders rolling counts, anomaly causes, and recent sensor values."
)

if not OUTPUT.exists():
    st.warning("No Flink output yet. Start producer.py and flink_job.py, then refresh this page.")
    st.stop()

data = json.loads(OUTPUT.read_text(encoding="utf-8"))
recent = pd.DataFrame(data.get("recent", []))

cols = st.columns(4)
cols[0].metric("Processed", data.get("processed", 0))
cols[1].metric("Failures", data.get("failures", 0), f"{data.get('failure_rate', 0) * 100:.2f}%")
cols[2].metric("Anomalies", data.get("anomalies", 0), f"{data.get('anomaly_rate', 0) * 100:.2f}%")
cols[3].metric("Last update", data.get("updated_at", "n/a").split(".")[0].replace("T", " "))

if recent.empty:
    st.info("Waiting for streamed events.")
    st.stop()

recent["event_index"] = range(max(0, data["processed"] - len(recent) + 1), data["processed"] + 1)
recent["reason_text"] = recent["reasons"].apply(lambda values: ", ".join(values) if values else "normal")

left, right = st.columns([2, 1])

with left:
    st.subheader("Live Sensor Trend")
    metric = st.selectbox("Signal", ["rpm", "torque_nm", "tool_wear_min", "power_kw", "air_temp_k", "process_temp_k"])
    chart = (
        alt.Chart(recent)
        .mark_line(point=True)
        .encode(x="event_index:Q", y=f"{metric}:Q", color="anomaly:N", tooltip=["udi", metric, "reason_text"])
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
st.dataframe(
    recent[
        [
            "event_index",
            "udi",
            "product_id",
            "type",
            "rpm",
            "torque_nm",
            "tool_wear_min",
            "power_kw",
            "failure",
            "anomaly",
            "reason_text",
        ]
    ].sort_values("event_index", ascending=False),
    use_container_width=True,
    hide_index=True,
)
