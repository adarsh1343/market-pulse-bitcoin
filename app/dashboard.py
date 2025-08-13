import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
from pathlib import Path

# --- Robust File Paths (Updated for .xlsx) ---
SCRIPT_DIR = Path(__file__).parent
ROOT_DIR = SCRIPT_DIR.parent
SAMPLE_VOLATILITY_PATH = ROOT_DIR / "sample_volatility.xlsx"
SAMPLE_ANOMALIES_PATH = ROOT_DIR / "sample_anomalies.xlsx"

# --- Page Configuration ---
st.set_page_config(
    page_title="Market Pulse Bitcoin Analyzer",
    page_icon="📈",
    layout="wide"
)

# --- Data Loading (Updated for .xlsx) ---
@st.cache_data(ttl=600)
def load_data(table_name, engine=None):
    """
    Tries to load data from the database. If it fails, it loads from a local XLSX file.
    """
    if engine:
        try:
            return pd.read_sql(f'SELECT * FROM "{table_name}"', engine)
        except Exception:
            st.warning("Could not connect to the database. Using local sample data.")
            if table_name == 'volatility_by_day':
                return pd.read_excel(SAMPLE_VOLATILITY_PATH) # Use read_excel
            if table_name == 'detected_anomalies':
                return pd.read_excel(SAMPLE_ANOMALIES_PATH) # Use read_excel
    
    # If no engine, run in local mode
    if table_name == 'volatility_by_day':
        return pd.read_excel(SAMPLE_VOLATILITY_PATH) # Use read_excel
    if table_name == 'detected_anomalies':
        return pd.read_excel(SAMPLE_ANOMALIES_PATH) # Use read_excel
    
    return pd.DataFrame()

# --- Main Dashboard ---
st.title("📈 Market Pulse: Bitcoin Analyzer")
st.markdown("This dashboard provides insights into Bitcoin's historical volatility and detects market anomalies using data from Bitstamp.")

engine = None
try:
    if all(k in st.secrets.db_credentials for k in ['user', 'password', 'host', 'db_name']):
        engine = create_engine(
            f"postgresql+pg8000://{st.secrets.db_credentials.user}:{st.secrets.db_credentials.password}@{st.secrets.db_credentials.host}:5432/{st.secrets.db_credentials.db_name}"
        )
except Exception:
    pass

volatility_df = load_data('volatility_by_day', engine)
anomalies_df = load_data('detected_anomalies', engine)

if engine:
    ohlc_df = load_data('btc_ohlc_raw', engine)
    if not ohlc_df.empty:
        ohlc_df['timestamp'] = pd.to_datetime(ohlc_df['timestamp'], unit='s')
else:
    date_range = pd.to_datetime(anomalies_df['timestamp'], unit='s')
    dummy_prices = pd.DataFrame({
        'timestamp': date_range,
        'close': [45000, 43000, 46000]
    })
    ohlc_df = dummy_prices

st.header("Volatility Analysis: The Weekend Effect")
if not volatility_df.empty:
    fig_volatility = px.bar(
        volatility_df, x='day_of_week', y='avg_volatility',
        title='Average Daily Volatility by Day of the Week',
        labels={'day_of_week': 'Day of the Week', 'avg_volatility': 'Average Volatility'},
        category_orders={"day_of_week": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]}
    )
    st.plotly_chart(fig_volatility, use_container_width=True)
else:
    st.warning("Volatility data could not be loaded.")

st.header("Anomaly Detection: Market Shocks")
if not ohlc_df.empty and not anomalies_df.empty:
    anomalies_df['timestamp'] = pd.to_datetime(anomalies_df['timestamp'], unit='s')
    
    fig_anomalies = px.line(ohlc_df, x='timestamp', y='close', title='Bitcoin Price with Detected Anomalies')
    fig_anomalies.add_scatter(
        x=anomalies_df['timestamp'],
        y=ohlc_df.loc[ohlc_df['timestamp'].isin(anomalies_df['timestamp']), 'close'],
        mode='markers', marker=dict(color='red', size=10, symbol='x'), name='Anomaly Detected'
    )
    st.plotly_chart(fig_anomalies, use_container_width=True)
else:
    st.warning("Anomaly or price data could not be loaded.")