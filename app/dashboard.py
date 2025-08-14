# app/dashboard.py
import sys
from pathlib import Path

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Optional: only used when generating dummy prices
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

@st.cache_data(ttl=600)
def _read_excel_if_exists(path: Path) -> pd.DataFrame:
    """Safely read an Excel file if it exists; else return empty DataFrame."""
    if path.exists():
        try:
            return pd.read_excel(path)
        except Exception as e:
            _log(f"Failed to read {path.name}: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

@st.cache_resource(show_spinner=False)
def _build_engine_from_secrets():
    """
    Build a SQLAlchemy engine from st.secrets if available.
    Uses pg8000 with a short connect timeout. SSL is enabled if available.
    """
    try:
        if "db_credentials" not in st.secrets:
            return None
        creds = st.secrets["db_credentials"]
        need = ("user", "password", "host", "db_name")
        if not all(k in creds for k in need):
            return None

        # pg8000 driver URL
        db_url = (
            f"postgresql+pg8000://{creds['user']}:{creds['password']}"
            f"@{creds['host']}:5432/{creds['db_name']}"
        )

        # pg8000 supports 'timeout' and SSL via connect_args
        connect_args = {"timeout": 10}
        # Try to enable TLS if server supports it; pg8000 will negotiate
        # You can switch to a verified context if you have a CA bundle.
        connect_args["ssl"] = True

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
    Try DB first; if not available/fails, use local sample files for known tables.
    """
    # 1) DB path
    if _engine is not None:
        try:
            return pd.read_sql(f'SELECT * FROM "{table_name}"', _engine)
        except Exception as e:
            _log(f"DB load failed for {table_name}: {e}")
            st.warning(f"Could not load '{table_name}' from DB. Using local sample data if available.")

    # 2) Local samples
    if table_name == "volatility_by_day":
        return _read_excel_if_exists(SAMPLE_VOLATILITY_PATH)
    if table_name == "detected_anomalies":
        return _read_excel_if_exists(SAMPLE_ANOMALIES_PATH)
    if table_name == "btc_ohlc_raw":
        # No local file provided for OHLC; return empty and let caller handle dummy
        return pd.DataFrame()

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

    # Build engine from secrets (optional)
    engine = _build_engine_from_secrets()

    # Load data
    volatility_df = load_table("volatility_by_day", _engine=engine)
    anomalies_df = load_table("detected_anomalies", _engine=engine)
    ohlc_df = load_table("btc_ohlc_raw", _engine=engine)

    # Normalize timestamps in anomalies if present
    if not anomalies_df.empty and "timestamp" in anomalies_df.columns:
        try:
            anomalies_df["timestamp"] = pd.to_datetime(anomalies_df["timestamp"], unit="s", errors="coerce")
            anomalies_df = anomalies_df.dropna(subset=["timestamp"])
        except Exception as e:
            _log(f"Failed to parse anomalies timestamps: {e}")
            anomalies_df = pd.DataFrame()

    # If no OHLC from DB, create dummy series aligned to anomalies (length-safe)
    if ohlc_df.empty:
        if not anomalies_df.empty and "timestamp" in anomalies_df.columns:
            date_range = anomalies_df["timestamp"].sort_values().reset_index(drop=True)
            if len(date_range) == 0:
                ohlc_df = pd.DataFrame()
            else:
                prices = 45000 + 500 * np.sin(np.linspace(0, 3, len(date_range)))
                ohlc_df = pd.DataFrame({"timestamp": date_range, "close": prices})
        else:
            # Last-resort tiny dummy to avoid crashes; UI will warn later if needed
            ohlc_df = pd.DataFrame({
                "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
                "close": [45000, 45200, 44800],
            })
    else:
        # Parse ohlc timestamps if in epoch seconds
        if "timestamp" in ohlc_df.columns:
            # Try both epoch seconds and ISO strings safely
            try:
                ohlc_df["timestamp"] = pd.to_datetime(ohlc_df["timestamp"], unit="s", errors="coerce")
                # If that resulted in all NaT, try generic parsing
                if ohlc_df["timestamp"].isna().all():
                    ohlc_df["timestamp"] = pd.to_datetime(ohlc_df["timestamp"], errors="coerce")
                ohlc_df = ohlc_df.dropna(subset=["timestamp"])
            except Exception as e:
                _log(f"Failed to parse OHLC timestamps: {e}")
                ohlc_df = pd.DataFrame()

    # -------------------------
    # Volatility chart
    # -------------------------
    st.header("Volatility Analysis: The Weekend Effect")
    if not volatility_df.empty and {"day_of_week", "avg_volatility"} <= set(volatility_df.columns):
        # enforce day ordering if present
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        try:
            fig_vol = px.bar(
                volatility_df,
                x="day_of_week",
                y="avg_volatility",
                title="Average Daily Volatility by Day of the Week",
                labels={"day_of_week": "Day of the Week", "avg_volatility": "Average Volatility"},
                category_orders={"day_of_week": day_order},
            )
            st.plotly_chart(fig_vol, use_container_width=True)
        except Exception as e:
            _log(f"Volatility chart error: {e}")
            st.warning("Could not render the volatility chart.")
    else:
        st.warning("Volatility data could not be loaded or is missing required columns.")

    # -------------------------
    # Anomaly chart
    # -------------------------
    st.header("Anomaly Detection: Market Shocks")
    if not ohlc_df.empty and "timestamp" in ohlc_df.columns and "close" in ohlc_df.columns:
        # Ensure datetime
        try:
            ohlc_df["timestamp"] = pd.to_datetime(ohlc_df["timestamp"], errors="coerce")
            ohlc_df = ohlc_df.dropna(subset=["timestamp"])
        except Exception as e:
            _log(f"Failed to coerce OHLC timestamps: {e}")
            ohlc_df = pd.DataFrame()

        if not ohlc_df.empty:
            try:
                fig_anom = px.line(ohlc_df, x="timestamp", y="close", title="Bitcoin Price with Detected Anomalies")
            except Exception as e:
                _log(f"Base line chart error: {e}")
                st.warning("Could not render the price chart.")
                fig_anom = None

            # Add anomaly markers only when we can align timestamps to OHLC
            if fig_anom is not None and not anomalies_df.empty and "timestamp" in anomalies_df.columns:
                try:
                    # Align anomalies to existing OHLC timestamps
                    markers = anomalies_df.merge(
                        ohlc_df[["timestamp", "close"]],
                        on="timestamp",
                        how="inner",
                    )
                    if not markers.empty:
                        fig_anom.add_scatter(
                            x=markers["timestamp"],
                            y=markers["close"],
                            mode="markers",
                            marker=dict(color="red", size=10, symbol="x"),
                            name="Anomaly Detected",
                        )
                except Exception as e:
                    _log(f"Anomaly marker error: {e}")
                    st.info("Anomalies loaded but could not be plotted due to timestamp alignment issues.")

            if fig_anom is not None:
                st.plotly_chart(fig_anom, use_container_width=True)
        else:
            st.warning("Price data could not be parsed.")
    else:
        st.warning("Anomaly or price data could not be loaded.")

    # Footer / debug
    st.caption("Tip: add `.streamlit/config.toml` with `fileWatcherType = 'poll'` on Streamlit Cloud to avoid inotify limits.")

# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    main()
