import sys
import pandas as pd
from sqlalchemy import create_engine
from sklearn.ensemble import IsolationForest
from awsglue.utils import getResolvedOptions # Import Glue utility

def run_analysis():
    """
    Main function to be executed by the AWS Glue job.
    Connects to DB using job parameters, runs analyses, and stores results.
    """
    # --- Get Job Parameters ---
    args = getResolvedOptions(sys.argv, [
        'DB_USER',
        'DB_PASSWORD',
        'DB_HOST',
        'DB_NAME'
    ])

    # --- Database Connection ---
    try:
        # --- THIS IS THE ONLY LINE THAT CHANGES ---
        # We've changed the connection string from "postgresql://" to "postgresql+pg8000://"
        engine = create_engine(
            f"postgresql+pg8000://{args['DB_USER']}:{args['DB_PASSWORD']}@{args['DB_HOST']}:5432/{args['DB_NAME']}"
        )
        print("Database connection successful using pg8000 driver.")
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise e

    # --- Volatility Analysis ---
    print("Starting volatility analysis...")
    volatility_query = "SELECT timestamp, close FROM btc_ohlc_raw;"
    volatility_df = pd.read_sql(volatility_query, engine)
    volatility_df['timestamp'] = pd.to_datetime(volatility_df['timestamp'], unit='s')
    volatility_df.set_index('timestamp', inplace=True)
    returns = volatility_df['close'].pct_change().dropna()
    daily_volatility = returns.resample('D').std()
    volatility_by_day = daily_volatility.groupby(daily_volatility.index.day_name()).mean().reset_index()
    volatility_by_day.columns = ['day_of_week', 'avg_volatility']
    volatility_by_day.to_sql('volatility_by_day', engine, if_exists='replace', index=False)
    print("Volatility analysis complete and results saved.")

    # --- Anomaly Detection ---
    print("Starting anomaly detection...")
    anomaly_query = "SELECT timestamp, close, volume_btc FROM btc_ohlc_raw;"
    anomaly_df = pd.read_sql(anomaly_query, engine, index_col='timestamp')
    anomaly_df['price_change'] = anomaly_df['close'].diff().abs()
    anomaly_df['volume_change'] = anomaly_df['volume_btc'].diff().abs()
    anomaly_df.dropna(inplace=True)
    features = anomaly_df[['price_change', 'volume_change']]
    model = IsolationForest(n_estimators=100, contamination=0.001)
    anomaly_df['anomaly_score'] = model.fit_predict(features)
    anomalies = anomaly_df[anomaly_df['anomaly_score'] == -1]
    anomalies_to_save = anomalies.reset_index()[['timestamp']]
    anomalies_to_save.to_sql('detected_anomalies', engine, if_exists='replace', index=False)
    print(f"Anomaly detection complete. Found {len(anomalies_to_save)} anomalies and results saved.")

if __name__ == "__main__":
    run_analysis()