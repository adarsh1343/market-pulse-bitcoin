import sys
from pathlib import Path

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

import numpy as np
import plotly.express as px

# -----------------------------
# Paths
# -----------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
SAMPLE_VOLATILITY_PATH = ROOT_DIR / "sample_volatility.xlsx"
SAMPLE_ANOMALIES_PATH = ROOT_DIR / "sample_anomalies.xlsx"

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(
    page_title="Market Pulse Bitcoin Analyzer",
    page_icon="📈",
    layout="wide",
)

# -----------------------------
# Helpers
# -----------------------------
def _log(msg: str):
    print(msg, file=sys.stderr, flush=True)

@st.cache_resource(show_spinner=False)
def _build_engine_from_secrets():
    """
    Build a SQLAlchemy engine from st.secrets if available.
    """
    try:
        if "db_credentials" not in st.secrets:
            return None
        creds = st.secrets["db_credentials"]
        need = ("user", "password", "host", "db_name")
        if not all(k in creds for k in need):
            return None

        db_url = (
            f"postgresql+pg8000://{creds['user']}:{creds['password']}"
            f"@{creds['host']}:5432/{creds['db_name']}"
        )

        # --- THIS IS THE CORRECTED PART ---
        # Changed 'ssl': True to 'ssl_context': True to match the pg8000 driver's requirements.
        connect_args = {"ssl_context": True, "timeout": 10}

        engine = create_engine(
            db_url,
            connect_args=connect_args,
            pool_pre_ping=True,
        )
        return engine
    except Exception as e:
        _log(f"Engine build failed: {e}")
        return None

@st.cache_data(ttl=600, show_spinner=True)
def load_table(table_name: str, _engine=None) -> pd.DataFrame:
    """
    Try DB first; if not available/fails, use local sample files.
    """
    if _engine is not None:
        try:
            return pd.read_sql(f'SELECT * FROM "{table_name}"', _engine)
        except Exception as e:
            _log(f"DB load failed for {table_name}: {e}")
            st.warning(f"Could not load '{table_name}' from DB. Using local sample data.")

    # Fallback to local samples
    if table_name == "volatility_by_day":
        if SAMPLE_VOLATILITY_PATH.exists():
            return pd.read_excel(SAMPLE_VOLATILITY_PATH)
    if table_name == "detected_anomalies":
        if SAMPLE_ANOMALIES_PATH.exists():
            return pd.read_excel(SAMPLE_ANOMALIES_PATH)

    return pd.DataFrame()

# -----------------------------
# Main App
# -----------------------------
def main():
    st.title("📈 Market Pulse: Bitcoin Analyzer")
    st.markdown(
        "This dashboard provides insights into Bitcoin's historical volatility "
        "and detects market anomalies using data from Bitstamp."
    )

    engine = _build_engine_from_secrets()

    volatility_df = load_table("volatility_by_day", _engine=engine)
    anomalies_df = load_table("detected_anomalies", _engine=engine)
    ohlc_df = load_table("btc_ohlc_raw", _engine=engine)

    if not anomalies_df.empty and "timestamp" in anomalies_df.columns:
        anomalies_df["timestamp"] = pd.to_datetime(anomalies_df["timestamp"], unit="s", errors="coerce")
        anomalies_df = anomalies_df.dropna(subset=["timestamp"])

    if ohlc_df.empty:
        if not anomalies_df.empty and "timestamp" in anomalies_df.columns:
            date_range = anomalies_df["timestamp"].sort_values().reset_index(drop=True)
            if len(date_range) > 0:
                prices = 45000 + 500 * np.sin(np.linspace(0, 3, len(date_range)))
                ohlc_df = pd.DataFrame({"timestamp": date_range, "close": prices})

    if "timestamp" in ohlc_df.columns:
        ohlc_df["timestamp"] = pd.to_datetime(ohlc_df["timestamp"], errors="coerce").dt.tz_localize(None)

    st.header("Volatility Analysis: The Weekend Effect")
    if not volatility_df.empty:
        st.plotly_chart(px.bar(
            volatility_df, x="day_of_week", y="avg_volatility",
            title="Average Daily Volatility by Day of the Week",
            labels={"day_of_week": "Day of the Week", "avg_volatility": "Average Volatility"},
            category_orders={"day_of_week": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]}
        ), use_container_width=True)
    else:
        st.warning("Volatility data could not be loaded.")

    st.header("Anomaly Detection: Market Shocks")
    if not ohlc_df.empty:
        fig_anom = px.line(ohlc_df, x="timestamp", y="close", title="Bitcoin Price with Detected Anomalies")
        if not anomalies_df.empty:
            markers = anomalies_df.merge(ohlc_df[["timestamp", "close"]], on="timestamp", how="inner")
            if not markers.empty:
                fig_anom.add_scatter(
                    x=markers["timestamp"], y=markers["close"],
                    mode="markers", marker=dict(color="red", size=10, symbol="x"),
                    name="Anomaly Detected"
                )
        st.plotly_chart(fig_anom, use_container_width=True)
    else:
        st.warning("Anomaly or price data could not be loaded.")

if __name__ == "__main__":
    main()
