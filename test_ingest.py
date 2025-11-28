from src.ingestion.capitol_client import SenateStockWatcherClient
from src.enrichment.asset_metadata import AssetEnricher
from src.analysis.metrics import EventStudy
from src.analysis.portfolio import PortfolioManager
import pandas as pd

# Display settings
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


def test_pipeline():
    # 1. Ingest
    print("--- 1. Ingestion ---")
    client = SenateStockWatcherClient()
    df = client.fetch_all_transactions()

    # 2. Filter Data (The Cleanup Step)
    print("--- 2. Filtering & Cleaning ---")
    # Only Purchases of Stocks
    df_clean = df[
        (df['asset_type'] == 'Stock') &
        (df['type'] == 'Purchase')
        ].copy()

    # Remove garbage tickers immediately
    df_clean = df_clean[~df_clean['ticker'].isin(['--', '---', 'NaN'])]
    df_clean = df_clean[df_clean['ticker'].str.len() > 1]

    # Take the top 50 to ensure we have enough valid data
    df_batch = df_clean.head(50).copy()
    print(f"Processing batch of {len(df_batch)} valid trades...")

    # 3. Enrich
    print("--- 3. Enrichment ---")
    enricher = AssetEnricher()
    df_enriched = enricher.enrich_dataframe(df_batch)

    # 4. Analyze (Alpha)
    print("--- 4. Event Study (Alpha) ---")
    analyzer = EventStudy()
    df_analyzed = analyzer.analyze_batch(df_enriched)

    # Show top 5 Alpha results
    print("\n[Top 5 Alpha Generating Trades]")
    cols = ['transaction_date', 'senator', 'ticker', 'car_30d']
    # Filter out failed calculations (NaN) and sort by highest Alpha
    valid_alpha = df_analyzed.dropna(subset=['car_30d'])
    print(valid_alpha[cols].sort_values(by='car_30d', ascending=False).head(5))

    # 5. Optimize Portfolio
    print("\n--- 5. Portfolio Optimization ---")
    unique_tickers = df_analyzed['ticker'].unique().tolist()

    pm = PortfolioManager(unique_tickers)
    results = pm.optimize_portfolio()

    if results:
        print(f"Expected Annual Return: {results['expected_return']:.1%}")
        print(f"Annual Volatility:      {results['volatility']:.1%}")
        print(f"Sharpe Ratio:           {results['sharpe_ratio']:.2f}")
        print("\n[Recommended Allocation]")
        # Only show allocations > 1%
        for ticker, weight in results['weights'].items():
            if weight > 0.01:
                print(f"  {ticker}: {weight:.1%}")
    else:
        print("Optimization could not be performed.")


if __name__ == "__main__":
    test_pipeline()