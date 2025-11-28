import pandas as pd
import yfinance as yf
from pypfopt import EfficientFrontier, risk_models, expected_returns
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class PortfolioManager:
    def __init__(self, tickers: list):
        # Improved Filter: Must be a string, not '--' or '---'
        self.tickers = [
            t.upper() for t in tickers
            if isinstance(t, str)
               and len(t) > 1
               and t not in ['--', '---', 'NaN']
               and t.replace('.', '').isalnum()
        ]
        self.tickers = list(set(self.tickers))

    def optimize_portfolio(self):
        if not self.tickers:
            logger.warning("No valid tickers provided for optimization.")
            return None

        logger.info(f"Optimizing portfolio for {len(self.tickers)} assets...")

        try:
            # 1. Download Data
            df = yf.download(self.tickers, period="2y", progress=False, auto_adjust=False)['Adj Close']

            if isinstance(df, pd.Series):
                df = df.to_frame()

            # Drop empty columns
            df = df.dropna(axis=1, how='all')
            # Drop columns with >20% missing data
            df = df.dropna(axis=1, thresh=int(len(df) * 0.8))

            if df.empty or df.shape[1] < 2:
                logger.warning("Insufficient data (needs at least 2 stocks with 2y history).")
                return None

            # 2. Optimization
            mu = expected_returns.mean_historical_return(df)
            S = risk_models.sample_cov(df)

            ef = EfficientFrontier(mu, S)
            weights = ef.max_sharpe()
            cleaned_weights = ef.clean_weights()
            perf = ef.portfolio_performance(verbose=False)

            # RETURN DICTIONARY (Must match test_ingest.py keys exactly)
            return {
                "weights": cleaned_weights,
                "expected_return": perf[0],
                "volatility": perf[1],
                "sharpe_ratio": perf[2]
            }

        except Exception as e:
            logger.error(f"Portfolio Optimization failed: {e}")
            return None