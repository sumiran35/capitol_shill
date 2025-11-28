import pandas as pd
from pathlib import Path
from src.ingestion.capitol_client import CapitolTradesClient
import logging

logger = logging.getLogger(__name__)

# Where we keep the loot
DATA_PATH = Path("data/processed/senate_trades_history.csv")


def load_local_data() -> pd.DataFrame:
    """Reads the CSV. No cap."""
    if not DATA_PATH.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(DATA_PATH)
        # Fix date types cuz CSVs turn them into strings
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        df['disclosure_date'] = pd.to_datetime(df['disclosure_date'])
        return df
    except Exception as e:
        print(f"Error loading local data: {e}")
        return pd.DataFrame()


def sync_data():
    """
    The Master Function.
    1. Checks local DB for last date.
    2. Scrapes only what's new.
    3. Merges and Saves.
    """
    df_local = load_local_data()

    start_date = None
    if not df_local.empty:
        # Find the latest trade we already have
        last_date = df_local['transaction_date'].max()
        # Format it for the scraper URL
        start_date = last_date.strftime('%Y-%m-%d')
        print(f" Local data found up to {start_date}. Checking for fresh tea...")
    else:
        print("🆕 No local data. Finna do a full 90-day scrape...")

    # Run the scraper
    client = CapitolTradesClient()
    df_new = client.fetch_trades(start_date=start_date)

    if df_new.empty:
        print(" No new trades found. We good.")
        return df_local

    # Merge logic (Avoid dupes)
    if not df_local.empty:
        # Combine old and new
        df_combined = pd.concat([df_local, df_new])
        # Dedupe based on key fields (cuz we don't have a unique ID)
        df_combined = df_combined.drop_duplicates(
            subset=['transaction_date', 'senator', 'ticker', 'amount_est', 'type'],
            keep='last'
        )
    else:
        df_combined = df_new

    # Save to disk (Persistence)
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_combined.to_csv(DATA_PATH, index=False)
    print(f"Database updated. Total records: {len(df_combined)}")

    return df_combined