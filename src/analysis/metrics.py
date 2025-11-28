import pandas as pd
import numpy as np
import yfinance as yf
from sklearn.linear_model import LinearRegression
import logging
from datetime import timedelta
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class EventStudy:
    def __init__(self, benchmark_ticker='^GSPC'):
        #gspc for snp 500
        self.benchmark = benchmark_ticker

    def calculate_car(self, ticker: str, trade_date: pd.Timestamp, window_days=30) -> float:
        """
        calcualate car(cum. abnorm. return) for a trade, return decimal percent or None
        :param ticker:
        :param trade_date:
        :param window_days:
        :return:
        """
        if pd.isna(trade_date):
            return None

        # Skip invalid tickers to avoid 404 errors
        if not isinstance(ticker, str) or ticker in ['--', 'NaN', '']:
            return None

        #1. Define time windows
        #estimation window: t-00 to T-10 (used to learn the stocks normal behaiour)
        est_start = trade_date - timedelta(days=200)
        est_end = trade_date - timedelta(days=10)

        #event window t-0 to t+30 (used to measure the "congressional effect")
        evt_end = trade_date + timedelta(days=window_days)

        try:
            #fetch stock + market benchmark data
    #add buffer to begin and end to get more trading days
            data = yf.download(
                [ticker, self.benchmark],
                start = est_start,
                end= evt_end + timedelta(days=5),
                progress=False,
                auto_adjust=False,
            )['Adj Close']

            #fix for yfinance returning multiindex columns
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.droplevel(1)

            #calculate daily returns (percent change)
            returns = data.pct_change().dropna()

            #if less data then cant be considered
            if len(returns) < 50:
                return None

            #time to train model (calc alpha and beta)
            #we only look at est window here
            est_data = returns.loc[est_start:est_end]

            # check if both assets exist in the data
            if est_data.empty or self.benchmark not in est_data or ticker not in est_data:
                return None

            X = est_data[self.benchmark].values.reshape(-1, 1)#market return
            y = est_data[ticker].values #stock return

            model = LinearRegression()
            model.fit(X, y)

            alpha = model.intercept_
            beta = model.coef_[0]

            #calculate abnormal returns in event window
            evt_data = returns.loc[trade_date:evt_end]
            if evt_data.empty:
                return None

            actual_returns = evt_data[ticker]
            market_returns = evt_data[self.benchmark]

            # expected return = alpha + (beta * market return)
            expected_returns = alpha + (beta * market_returns)

            #abnormal return = actual - expected
            abnormal_returns = actual_returns - expected_returns

            car = abnormal_returns.sum()

            return car
        except Exception as e:
            logger.debug(f"error calculating CAR for {ticker}: {e}")
            return None

    def analyze_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        apploes car calcution to a whole dataframe of trades.
        :param df:
        :return:
        """

        if df.empty:
            return df

        logger.debug(f"Calculating fiancial metrics (Alpha/Beta) for {len(df)} trades...")

        #apply the calculation row-by-row

        df['car_30d'] = df.apply(
            lambda row: self.calculate_car(row['ticker'], row['transaction_date']),
            axis=1
        )
        return df





