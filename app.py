import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st


EVENT_LOG = Path("output/flink_events.jsonl")
REFRESH_SECONDS = 1.0
RECENT = 20
MAX_POINTS = 3000
DENSE_ABOVE = 200
IST = timezone(timedelta(hours=5, minutes=30))
SENSORS = ["rpm", "torque_nm", "tool_wear_min", "power_kw", "air_temp_k", "process_temp_k"]
DISPLAY = {
    "event_index": "Event",
    "udi": "UDI",
    "product_id": "Product ID",
    "type": "Type",
    "rpm": "RPM",
    "torque_nm": "Torque (Nm)",
    "tool_wear_min": "Tool Wear (min)",
    "power_kw": "Power (kW)",
    "air_temp_k": "Air Temp (K)",
    "process_temp_k": "Process Temp (K)",
    "failure": "Failure",
    "anomaly": "Anomaly",
    "reason_text": "Reason",
    "event_time": "Time",
    "status": "Status",
}
STATUS_EVENT = "Event"
STATUS_ANOMALY = "Anomaly"
SHOW_FIELDS = ["event_index", "udi", "product_id", "type", *SENSORS, "failure", "anomaly", "reason_text"]
REASON_LABELS = {
    "dataset_failure_label": "Machine Failure",
    "high_tool_wear": "High Tool Wear",
    "abnormal_power": "Abnormal Power",
    "thermal_drift": "Thermal Drift",
    "mechanical_load": "Mechanical Load",
}
SEARCH_FIELDS = ["Event", "UDI", "Product ID", "Type", "Reason"]
FAILURE_LIMITS = [10, 20, 50, 100]


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
        "events": [],
    }


def consume(path):
    """Append whatever the Flink job wrote to the event stream since the previous rerun.

    Every event is kept for the lifetime of the session so the trend chart can cover the
    whole run instead of sliding a fixed window.
    """
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

        reasons = event.get("reasons")
        event["reason_text"] = ", ".join(reasons) if isinstance(reasons, list) and reasons else "normal"
        event["event_index"] = state["index"]
        state["events"].append(event)

    return state


def percent(part, whole):
    return f"{(part / whole * 100):.2f}%" if whole else "0.00%"


def nice_domain(series):
    """Round the y range outward to a coarse step so it only rescale occasionally.

    Streamlit redraws the whole chart every second, so a domain recomputed from the raw
    min and max would make the line jump on almost every refresh. Snapping to a 1/2/5 step
    keeps it still for long stretches and makes the movement read as smooth.
    """
    try:
        lo = float(series.min())
        hi = float(series.max())
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(lo) and math.isfinite(hi)) or hi <= lo:
        return None

    raw = (hi - lo) / 6
    magnitude = 10 ** math.floor(math.log10(raw))
    step = magnitude
    for factor in (1, 2, 2.5, 5, 10):
        step = factor * magnitude
        if step >= raw:
            break
    return [math.floor(lo / step) * step, math.ceil(hi / step) * step]


def ist_time(value):
    """The event log is stamped in UTC, so shift it to IST for display."""
    if not value:
        return "n/a"
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")


def reason_counts(view):
    if "reasons" not in view.columns:
        return pd.Series(dtype="int64")
    exploded = view[["reasons"]].explode("reasons")
    exploded = exploded[exploded["reasons"].notna() & (exploded["reasons"] != "")]
    return exploded["reasons"].value_counts()


def sensor_stats(failed, healthy):
    """Compare the failed events against the baseline the healthy events set."""
    rows = []
    for name in SENSORS:
        label = DISPLAY[name]
        if label not in failed.columns:
            continue
        measured = pd.to_numeric(failed[label], errors="coerce")
        baseline = pd.to_numeric(healthy[label], errors="coerce") if not healthy.empty else pd.Series(dtype="float64")
        expected_mean = baseline.mean() if len(baseline) else float("nan")
        measured_mean = measured.mean()
        delta = measured_mean - expected_mean
        rows.append(
            {
                "Parameter": label,
                "Expected": expected_mean,
                "Measured": measured_mean,
                "Delta": delta,
                "Delta %": (delta / expected_mean * 100) if expected_mean else float("nan"),
                "Min": measured.min(),
                "Max": measured.max(),
            }
        )
    return pd.DataFrame(rows)


def reason_label(reason):
    """Readability for the rule codes the job emits."""
    return REASON_LABELS.get(reason, str(reason).replace("_", " ").title())


def fault_parameters(view):
    """The distinct fault parameters this session has produced, as labels."""
    if "reasons" not in view.columns:
        return []
    labels = set()
    for reasons in view["reasons"]:
        if isinstance(reasons, list):
            labels.update(reason_label(reason) for reason in reasons)
    return sorted(labels)


def failure_results(view, parameters, search):
    """Failed events matching the chosen parameters and search text, oldest first."""
    failed = view[view["Failure"] == True]  # noqa: E712
    if parameters:
        wanted = set(parameters)
        failed = failed[
            failed["reasons"].map(
                lambda reasons: any(reason_label(r) in wanted for r in reasons)
                if isinstance(reasons, list)
                else False
            )
        ]
    if search:
        needle = search.strip().lower()
        if needle:
            fields = [field for field in SEARCH_FIELDS if field in failed.columns]
            haystack = (
                failed[fields].astype(str).agg(" ".join, axis=1).str.lower()
            )
            failed = failed[haystack.str.contains(needle, regex=False, na=False)]
    return failed


@st.fragment(run_every=REFRESH_SECONDS)
def live_dashboard():
    state = consume(EVENT_LOG)

    cols = st.columns(4)
    cols[0].metric("Processed", state["processed"])
    cols[1].metric("Failures", state["failures"], percent(state["failures"], state["processed"]))
    cols[2].metric("Anomalies", state["anomalies"], percent(state["anomalies"], state["processed"]))
    cols[3].metric("Last update", ist_time(state["last_event_at"]))

    if state["parse_errors"]:
        st.warning(f"{state['parse_errors']} events could not be parsed and are listed below.")

    if not state["events"]:
        st.info("Waiting for the first events. Start flink_job.py and producer.py, then refresh.")
        return

    frame = pd.DataFrame(state["events"])
    if "reasons" not in frame.columns:
        st.dataframe(frame, use_container_width=True, hide_index=True)
        return

    view = frame.rename(columns=DISPLAY)
    # A categorical Status drives both the legend and the KPI counters.
    view["Status"] = pd.Series(frame["anomaly"], index=frame.index).map(
        {True: STATUS_ANOMALY, False: STATUS_EVENT}
    ).fillna(STATUS_EVENT)

    recent = view.tail(RECENT)
    anomalies = view[view["Status"] == STATUS_ANOMALY].tail(RECENT)
    failed = view[view["Failure"] == True]  # noqa: E712
    healthy = view[view["Failure"] != True]  # noqa: E712
    signal_labels = [DISPLAY[name] for name in SENSORS]

    left, right = st.columns([2, 1])

    with left:
        st.subheader("Live Sensor Trend")
        pick_col, log_col = st.columns([3, 1])
        with pick_col:
            chosen = st.selectbox("Signal", signal_labels)
        with log_col:
            log_scale = st.checkbox("Log event axis", value=False)

        if chosen in view.columns:
            plotted = view
            if len(view) > MAX_POINTS:
                plotted = view.iloc[:: len(view) // MAX_POINTS + 1]

            domain = nice_domain(plotted[chosen])
            chart = (
                alt.Chart(plotted)
                .mark_line(point=len(plotted) <= DENSE_ABOVE, strokeWidth=1.5)
                .encode(
                    x=alt.X(
                        "Event:Q",
                        title="Event",
                        scale=alt.Scale(type="log" if log_scale else "linear", nice=False),
                    ),
                    y=alt.Y(
                        f"{chosen}:Q",
                        title=chosen,
                        scale=alt.Scale(domain=domain, nice=False) if domain else alt.Scale(nice=False),
                    ),
                    color=alt.Color(
                        "Status:N",
                        title="Status",
                        scale=alt.Scale(domain=[STATUS_EVENT, STATUS_ANOMALY], range=["#9ecae1", "#08519c"]),
                        legend=alt.Legend(title="Status", orient="top"),
                    ),
                    tooltip=["UDI", chosen, "Reason", "Status"],
                )
                .properties(height=380)
            )
            st.altair_chart(chart, use_container_width=True)
            st.caption(
                f"Events 1 to {state['index']}"
                + (f", every {len(view) // MAX_POINTS + 1}th shown" if len(view) > MAX_POINTS else "")
            )

    with right:
        st.subheader(f"Last {RECENT} Anomalies")
        if anomalies.empty:
            st.success("No anomalies yet this session.")
        else:
            anomaly_columns = [
                DISPLAY[name] for name in SHOW_FIELDS if DISPLAY[name] in anomalies.columns
            ]
            panel = anomalies[anomaly_columns]
            if "Event" in panel.columns:
                panel = panel.sort_values("Event", ascending=False)
            st.dataframe(panel, use_container_width=True, hide_index=True)
            st.caption(f"{len(anomalies)} most recent of {len(view[view['Status'] == STATUS_ANOMALY])} total.")

    st.divider()
    st.subheader("Session Analytics")
    st.caption("Cumulative for every event received since this page started.")

    summary = st.columns(5)
    rate = percent(state["anomalies"], state["processed"])
    summary[0].metric("Events", state["processed"])
    summary[1].metric("Anomalies", state["anomalies"], rate)
    summary[2].metric("Failures", state["failures"], percent(state["failures"], state["processed"]))
    summary[3].metric("Anomaly rate", rate)
    last_anomaly = anomalies["Event"].max() if not anomalies.empty else None
    summary[4].metric("Last anomaly event", "n/a" if last_anomaly is None else int(last_anomaly))

    cause_col, stats_col = st.columns([1, 2])

    with cause_col:
        st.markdown("**Anomalies by parameter**")
        causes = reason_counts(view)
        if causes.empty:
            st.info("No anomaly causes recorded yet.")
        else:
            st.bar_chart(causes.rename_axis("Reason").rename("Anomalies"))

    with stats_col:
        st.markdown("**Expected vs measured**")
        st.caption("Expected is the mean of events that did not fail; measured is the mean of failed events.")
        if failed.empty:
            st.info("No failed events yet.")
        else:
            stats = sensor_stats(failed, healthy)
            if stats.empty:
                st.info("No sensor readings yet.")
            else:
                st.dataframe(
                    stats.style.format(
                        {
                            "Expected": "{:.3f}",
                            "Measured": "{:.3f}",
                            "Delta": "{:+.3f}",
                            "Delta %": "{:+.1f}%",
                            "Min": "{:.3f}",
                            "Max": "{:.3f}",
                        },
                        na_rep="n/a",
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

    st.divider()
    st.subheader("Failure Results")
    st.caption(
        "Failed events from this session, narrowed by the parameter they "
        "faulted on and by a free-text search."
    )

    search_col, param_col, limit_col = st.columns([2, 3, 1])
    with search_col:
        search = st.text_input(
            "Search",
            value="",
            placeholder="UDI, product, type, event or reason",
            key="failure_search",
        )
    with param_col:
        parameters = st.multiselect(
            "Fault parameters",
            options=fault_parameters(view),
            default=[],
            key="failure_parameters",
        )
    with limit_col:
        limit = st.selectbox(
            "Show",
            options=FAILURE_LIMITS,
            index=1,
            key="failure_limit",
        )

    matched = failure_results(view, parameters, search)
    if matched.empty:
        st.info("No failed events match the current filters.")
    else:
        panel = matched.tail(limit)
        failure_columns = [
            DISPLAY[name] for name in SHOW_FIELDS if DISPLAY[name] in panel.columns
        ]
        table = panel[failure_columns]
        if "Event" in table.columns:
            table = table.sort_values("Event", ascending=False)
        st.dataframe(table, use_container_width=True, hide_index=True)
        st.caption(
            f"{len(table)} shown of {len(matched)} matching "
            f"out of {len(view[view['Failure'] == True])} failed events."  # noqa: E712
        )

    st.divider()
    st.subheader(f"Recent {RECENT} Events")
    # view is already renamed to display labels, so filter on the labels, not the raw fields.
    columns = [DISPLAY[name] for name in SHOW_FIELDS if DISPLAY[name] in view.columns]
    table = recent[columns]
    if "Event" in columns:
        table = table.sort_values("Event", ascending=False)
    st.dataframe(table, use_container_width=True, hide_index=True)


st.set_page_config(page_title="Industrial Machine Anomaly Monitor", layout="wide")
st.title("Industrial Machine Anomaly Monitor")
st.caption(
    "Methodology: producer.py streams telemetry in segments, flink_job.py picks up each "
    "segment and applies the anomaly rules, and this page consumes the classified event "
    "stream as it is written, refreshing every second."
)
live_dashboard()
