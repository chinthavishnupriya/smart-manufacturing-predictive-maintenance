"""
Smart Manufacturing Predictive Maintenance Dashboard

Architecture:
Kafka -> Spark Structured Streaming -> HBase -> FastAPI -> Streamlit
"""

from pathlib import Path
import json
import re

import pandas as pd
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh


# ============================================================
# PROJECT PATHS
# ============================================================

ROOT = Path(__file__).resolve().parents[1]

DATA = ROOT / "data" / "processed" / "cleaned_data.csv"

METRICS = ROOT / "outputs" / "metrics.json"

PLOTS = ROOT / "outputs" / "plots"

LIVE_FILE = ROOT / "outputs" / "live_prediction.json"

HISTORY_FILE = ROOT / "outputs" / "live_sensor_history.csv"


# ============================================================
# FASTAPI CONFIGURATION
# ============================================================

API_URL = "http://127.0.0.1:8002"


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Smart Manufacturing | Predictive Maintenance",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# AUTOMATIC LIVE REFRESH
# ============================================================

st_autorefresh(
    interval=5000,
    limit=None,
    key="live_dashboard_refresh",
)


# ============================================================
# CUSTOM CSS
# ============================================================

st.markdown(
    """
    <style>

    .main-title {
        font-size: 2.2rem;
        font-weight: 700;
        margin-bottom: 0;
    }

    .subtitle {
        color: #6b7280;
        margin-top: 0.15rem;
        margin-bottom: 1.2rem;
    }

    .small-note {
        color: #6b7280;
        font-size: 0.85rem;
    }

    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# TITLE
# ============================================================

st.markdown(
    '<div class="main-title">'
    '🏭 Smart Manufacturing — Predictive Maintenance'
    '</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="subtitle">'
    'Sensor analytics, machine health monitoring '
    'and ML-based failure prediction'
    '</div>',
    unsafe_allow_html=True,
)


# ============================================================
# CHECK DATASET
# ============================================================

if not DATA.exists():

    st.error(
        "Cleaned dataset not found. "
        "Run: python data_processing/clean_data.py"
    )

    st.stop()


# ============================================================
# LOAD HISTORICAL DATA
# ============================================================

try:

    df = pd.read_csv(DATA)

except Exception as e:

    st.error(
        f"Unable to load cleaned dataset: {e}"
    )

    st.stop()


# ============================================================
# LOAD MODEL METRICS
# ============================================================

if METRICS.exists():

    try:

        metrics = json.loads(
            METRICS.read_text(
                encoding="utf-8"
            )
        )

    except Exception:

        metrics = None

else:

    metrics = None


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.header(
    "System Information"
)

st.sidebar.success(
    "Data pipeline loaded"
)

st.sidebar.write(
    f"**Dataset:** {len(df):,} sensor records"
)

if "machine_id" in df.columns:

    st.sidebar.write(
        f"**Machines:** "
        f"{df['machine_id'].nunique():,}"
    )

if "failure" in df.columns:

    failure_count = int(
        df["failure"].sum()
    )

    st.sidebar.write(
        f"**Failure records:** "
        f"{failure_count:,}"
    )

    if len(df) > 0:

        st.sidebar.write(
            f"**Failure rate:** "
            f"{failure_count / len(df):.2%}"
        )

st.sidebar.divider()

st.sidebar.caption(
    "Model: Random Forest"
)

st.sidebar.caption(
    "Prediction threshold: 50%"
)

st.sidebar.divider()

st.sidebar.info(
    "Live architecture:\n\n"
    "Kafka → Spark → HBase → FastAPI → Streamlit"
)

st.sidebar.divider()

st.sidebar.caption(
    "Live dashboard refresh: 5 seconds"
)


# ============================================================
# MODEL & DATASET OVERVIEW
# ============================================================

st.subheader(
    "📊 Model & Dataset Overview"
)

if metrics:

    k1, k2, k3, k4, k5, k6 = st.columns(6)

    k1.metric(
        "Sensor Records",
        f"{len(df):,}"
    )

    k2.metric(
        "Accuracy",
        f"{metrics['accuracy']:.2%}"
    )

    k3.metric(
        "Precision",
        f"{metrics['precision']:.2%}"
    )

    k4.metric(
        "Recall",
        f"{metrics['recall']:.2%}"
    )

    k5.metric(
        "F1 Score",
        f"{metrics['f1']:.2%}"
    )

    k6.metric(
        "ROC-AUC",
        f"{metrics['roc_auc']:.4f}"
    )

else:

    st.warning(
        "Model metrics not found. "
        "Run: python models/train_model.py"
    )


# ============================================================
# FAILURE DISTRIBUTION
# ============================================================

st.subheader(
    "📈 Failure Distribution"
)

if "failure" in df.columns:

    normal_count = int(
        (df["failure"] == 0).sum()
    )

    failure_count = int(
        (df["failure"] == 1).sum()
    )

    d1, d2, d3 = st.columns(3)

    d1.metric(
        "Normal",
        f"{normal_count:,}"
    )

    d2.metric(
        "Failure",
        f"{failure_count:,}"
    )

    d3.metric(
        "Failure Rate",
        f"{failure_count / len(df):.2%}"
    )

    distribution_df = pd.DataFrame(
        {
            "Type": [
                "Normal",
                "Failure",
            ],
            "Count": [
                normal_count,
                failure_count,
            ],
        }
    )

    st.bar_chart(
        distribution_df.set_index("Type"),
        use_container_width=True,
    )


# ============================================================
# HISTORICAL SENSOR OVERVIEW
# ============================================================

st.subheader(
    "📉 Historical Sensor Overview"
)

chart_df = df.head(500).copy()


if "temperature" in chart_df.columns:

    temperature_chart = chart_df[
        ["temperature"]
    ].reset_index(drop=True)

    temperature_chart.index += 1

    st.markdown(
        "#### Temperature"
    )

    st.line_chart(
        temperature_chart,
        use_container_width=True,
    )


if "vibration" in chart_df.columns:

    vibration_chart = chart_df[
        ["vibration"]
    ].reset_index(drop=True)

    vibration_chart.index += 1

    st.markdown(
        "#### Vibration"
    )

    st.line_chart(
        vibration_chart,
        use_container_width=True,
    )


# ============================================================
# FASTAPI HEALTH CHECK
# ============================================================

st.divider()

st.subheader(
    "🔌 Live System Connection"
)


def get_health():

    try:

        response = requests.get(
            f"{API_URL}/health",
            timeout=5,
        )

        response.raise_for_status()

        return response.json()

    except requests.exceptions.RequestException:

        return None


health = get_health()


if health is None:

    st.error(
        "FastAPI is not reachable."
    )

    st.code(
        "uvicorn api.main:app --host 0.0.0.0 --port 8000"
    )

else:

    c1, c2, c3 = st.columns(3)


    if health.get("status") == "healthy":

        c1.success(
            "API: ONLINE"
        )

    else:

        c1.error(
            "API: OFFLINE"
        )


    if health.get("model_available"):

        c2.success(
            "ML Model: AVAILABLE"
        )

    else:

        c2.error(
            "ML Model: NOT FOUND"
        )


    if health.get("hbase_available"):

        c3.success(
            "HBase: CONNECTED"
        )

    else:

        c3.error(
            "HBase: DISCONNECTED"
        )


# ============================================================
# LIVE MACHINE DATA
# ============================================================

st.divider()

st.subheader(
    "🔴 Live Machine Monitoring"
)


def get_live_machines():

    try:

        response = requests.get(
            f"{API_URL}/machines",
            timeout=5,
        )

        response.raise_for_status()

        return response.json()

    except requests.exceptions.RequestException as e:

        st.error(
            f"Unable to retrieve live machine data: {e}"
        )

        return None


live_data = get_live_machines()


if live_data is not None:

    machines = live_data.get(
        "machines",
        []
    )


    if not machines:

        st.warning(
            "No live machine data is available in HBase."
        )


    else:

        st.success(
            f"Live monitoring active — "
            f"{len(machines)} machine(s) detected"
        )


        # ====================================================
        # BUILD MACHINE RECORDS AND MACHINE ID LIST
        # ====================================================

        # Row keys use MACHINE_ID_YYYYMMDD_HHMM_HHMM. Keep the
        # complete machine ID, including any underscores.
        machine_records = {}

        for machine in machines:
            row_key = str(machine.get("row_key", ""))
            match = re.match(r"^(.*)_\d{8}_\d{4}_\d{4}$", row_key)
            if not match:
                continue

            machine_id = match.group(1)
            current = machine_records.get(machine_id)

            # The date/time suffix sorts chronologically in string order.
            if current is None or row_key > str(current.get("row_key", "")):
                machine_records[machine_id] = machine

        # Show production IDs (M001, M002, ...) and the latest records
        # whose status is ANOMALY (including DEMO_ANOMALY_01).
        machine_ids = sorted(
            machine_id
            for machine_id, record in machine_records.items()
            if re.fullmatch(r"M\d+", machine_id)
            or str(record.get("status", "")).upper() == "ANOMALY"
        )

        if machine_ids:

            # ================================================
            # MACHINE SELECTOR
            # ================================================

            selected_machine = st.selectbox(
                "Select Machine",
                machine_ids,
                key="live_machine_selector",
            )

            # ================================================
            # FIND THE EXACT SELECTED MACHINE
            # ================================================

            selected = machine_records.get(selected_machine)

            # ================================================
            # SELECTED MACHINE
            # ================================================

            if selected:

                status = str(
                    selected.get(
                        "status",
                        "UNKNOWN"
                    )
                ).upper()


                # ============================================
                # MACHINE STATUS
                # ============================================

                if status == "ANOMALY":

                    st.error(
                        f"🚨 {selected_machine} — "
                        f"ANOMALY DETECTED"
                    )

                else:

                    st.success(
                        f"✅ {selected_machine} — NORMAL"
                    )


                # ============================================
                # SENSOR MEASUREMENTS
                # ============================================

                st.markdown(
                    "### Sensor Measurements"
                )


                c1, c2, c3, c4 = st.columns(4)


                temperature = selected.get(
                    "avg_temperature",
                    selected.get(
                        "temperature",
                        "N/A"
                    )
                )


                vibration = selected.get(
                    "avg_vibration",
                    selected.get(
                        "vibration",
                        "N/A"
                    )
                )


                rpm = selected.get(
                    "avg_rpm",
                    selected.get(
                        "rpm",
                        "N/A"
                    )
                )


                pressure = selected.get(
                    "avg_pressure",
                    selected.get(
                        "pressure",
                        "N/A"
                    )
                )


                # --------------------------------------------
                # SAFE CONVERSION
                # --------------------------------------------

                try:

                    temperature_value = (
                        f"{float(temperature):.2f} °C"
                    )

                except (
                    TypeError,
                    ValueError
                ):

                    temperature_value = "N/A"


                try:

                    vibration_value = (
                        f"{float(vibration):.2f}"
                    )

                except (
                    TypeError,
                    ValueError
                ):

                    vibration_value = "N/A"


                try:

                    rpm_value = (
                        f"{float(rpm):.0f}"
                    )

                except (
                    TypeError,
                    ValueError
                ):

                    rpm_value = "N/A"


                try:

                    pressure_value = (
                        f"{float(pressure):.2f}"
                    )

                except (
                    TypeError,
                    ValueError
                ):

                    pressure_value = "N/A"


                c1.metric(
                    "🌡️ Temperature",
                    temperature_value,
                )


                c2.metric(
                    "📳 Vibration",
                    vibration_value,
                )


                c3.metric(
                    "⚙️ RPM",
                    rpm_value,
                )


                c4.metric(
                    "💨 Pressure",
                    pressure_value,
                )


                # ============================================
                # MACHINE HEALTH
                # ============================================

                st.markdown(
                    "### Machine Health"
                )


                h1, h2, h3, h4 = st.columns(4)


                anomaly_rate = selected.get(
                    "anomaly_rate",
                    "0"
                )


                total_anomalies = selected.get(
                    "total_anomaly_count",
                    "0"
                )


                rul_hours = selected.get(
                    "predicted_rul_hours",
                    "N/A"
                )


                rul_minutes = selected.get(
                    "predicted_rul_minutes",
                    "N/A"
                )


                # --------------------------------------------
                # SAFE CONVERSION
                # --------------------------------------------

                try:

                    anomaly_rate_value = (
                        f"{float(anomaly_rate):.1f}%"
                    )

                except (
                    TypeError,
                    ValueError
                ):

                    anomaly_rate_value = "N/A"


                try:

                    rul_hours_value = (
                        f"{float(rul_hours):.2f} h"
                    )

                except (
                    TypeError,
                    ValueError
                ):

                    rul_hours_value = "N/A"


                try:

                    rul_minutes_value = (
                        f"{float(rul_minutes):.1f} min"
                    )

                except (
                    TypeError,
                    ValueError
                ):

                    rul_minutes_value = "N/A"


                h1.metric(
                    "Anomaly Rate",
                    anomaly_rate_value,
                )


                h2.metric(
                    "Total Anomalies",
                    str(total_anomalies),
                )


                h3.metric(
                    "Predicted RUL",
                    rul_hours_value,
                )


                h4.metric(
                    "RUL",
                    rul_minutes_value,
                )


                # ============================================
                # STREAMING INFORMATION
                # ============================================

                st.markdown(
                    "### Streaming Information"
                )


                s1, s2, s3, s4 = st.columns(4)


                operating_hours = selected.get(
                    "avg_operating_hours",
                    "N/A"
                )


                event_count = selected.get(
                    "event_count",
                    "N/A"
                )


                window_start = selected.get(
                    "window_start",
                    "N/A"
                )

                window_end = selected.get(
                    "window_end",
                    "N/A"
                )

                # Derive window boundaries from the HBase row key
                # format: MACHINE_ID_YYYYMMDD_HHMM_HHMM.
                if (
                    window_start == "N/A"
                    or window_end == "N/A"
                ):
                    import re
                    from datetime import datetime, timedelta

                    row_key = str(selected.get("row_key", ""))
                    match = re.match(
                        r"^(.+)_([0-9]{8})_([0-9]{4})_([0-9]{4})$",
                        row_key,
                    )

                    if match:
                        _, date_part, start_part, end_part = match.groups()

                        try:
                            start_dt = datetime.strptime(
                                date_part + start_part, "%Y%m%d%H%M"
                            )
                            end_dt = datetime.strptime(
                                date_part + end_part, "%Y%m%d%H%M"
                            )

                            # Handle a window that crosses midnight.
                            if end_dt < start_dt:
                                end_dt += timedelta(days=1)

                            if window_start == "N/A":
                                window_start = start_dt.strftime(
                                    "%Y-%m-%d %H:%M"
                                )

                            if window_end == "N/A":
                                window_end = end_dt.strftime(
                                    "%Y-%m-%d %H:%M"
                                )

                        except ValueError:
                            pass


                try:

                    operating_hours_value = (
                        f"{float(operating_hours):.1f}"
                    )

                except (
                    TypeError,
                    ValueError
                ):

                    operating_hours_value = "N/A"


                s1.metric(
                    "Operating Hours",
                    operating_hours_value,
                )


                s2.metric(
                    "Events",
                    str(event_count),
                )


                s3.metric(
                    "Window Start",
                    str(window_start),
                )


                s4.metric(
                    "Window End",
                    str(window_end),
                )


                # ============================================
                # ANOMALY BREAKDOWN
                # ============================================

                st.markdown(
                    "### Anomaly Breakdown"
                )


                a1, a2, a3, a4 = st.columns(4)


                a1.metric(
                    "Temperature Anomalies",
                    str(
                        selected.get(
                            "temperature_count",
                            0
                        )
                    ),
                )


                a2.metric(
                    "Vibration Anomalies",
                    str(
                        selected.get(
                            "vibration_count",
                            0
                        )
                    ),
                )


                a3.metric(
                    "RPM Anomalies",
                    str(
                        selected.get(
                            "rpm_count",
                            0
                        )
                    ),
                )


                a4.metric(
                    "Pressure Anomalies",
                    str(
                        selected.get(
                            "pressure_count",
                            0
                        )
                    ),
                )


                # ============================================
                # RAW HBASE RECORD
                # ============================================

                with st.expander(
                    "View raw HBase record"
                ):

                    st.json(
                        selected
                    )


# ============================================================
# CURRENT MACHINE ANOMALIES
# ============================================================

st.divider()

st.subheader(
    "🚨 Current Machine Anomalies"
)


def get_latest_anomalies():

    try:

        response = requests.get(
            f"{API_URL}/anomalies/latest",
            timeout=5,
        )

        response.raise_for_status()

        return response.json()

    except requests.exceptions.RequestException:

        return None


anomaly_data = get_latest_anomalies()


if anomaly_data is not None:

    anomalies = anomaly_data.get(
        "anomalies",
        []
    )


    if anomalies:

        anomaly_table = []


        for machine in anomalies:

            row_key = str(
                machine.get(
                    "row_key",
                    ""
                )
            )


            anomaly_table.append(
                {
                    "Machine":
                        (
                            row_key.rsplit("_", 3)[0]
                            if len(row_key.rsplit("_", 3)) == 4
                            else row_key
                        ),

                    "Status":
                        machine.get(
                            "status",
                            "UNKNOWN"
                        ),

                    "Temperature":
                        machine.get(
                            "avg_temperature",
                            "N/A"
                        ),

                    "Vibration":
                        machine.get(
                            "avg_vibration",
                            "N/A"
                        ),

                    "RPM":
                        machine.get(
                            "avg_rpm",
                            "N/A"
                        ),

                    "Pressure":
                        machine.get(
                            "avg_pressure",
                            "N/A"
                        ),

                    "Anomaly Rate":
                        machine.get(
                            "anomaly_rate",
                            "N/A"
                        ),

                    "RUL Hours":
                        machine.get(
                            "predicted_rul_hours",
                            "N/A"
                        ),
                }
            )


        anomaly_df = pd.DataFrame(
            anomaly_table
        )


        st.dataframe(
            anomaly_df,
            use_container_width=True,
            hide_index=True,
        )


    else:

        st.success(
            "No active machine anomalies."
        )


else:

    st.warning(
        "Unable to retrieve current anomaly information."
    )


# ============================================================
# DASHBOARD CONTROLS
# ============================================================

st.divider()

st.subheader(
    "🔄 Dashboard Controls"
)


col1, col2 = st.columns(2)


with col1:

    if st.button(
        "🔄 Refresh Now",
        use_container_width=True,
    ):

        st.rerun()


with col2:

    st.info(
        "Automatic refresh: every 5 seconds"
    )


# ============================================================
# LIVE ARCHITECTURE
# ============================================================

st.divider()

st.subheader(
    "🏗️ Live System Architecture"
)


st.code(
    """
IoT Sensor / CSV
       ↓
     Kafka
       ↓
Spark Structured Streaming
       ↓
 Anomaly Detection
       ↓
    RUL Model
       ↓
     HBase
       ↓
    FastAPI
       ↓
   Streamlit
       ↓
Machine Health Dashboard
""",
    language="text",
)


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "Smart Manufacturing Predictive Maintenance | "
    "Kafka + Spark + HBase + FastAPI + Streamlit + ML"
 )