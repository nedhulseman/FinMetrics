from yahoo_historical import Fetcher
import pandas as pd

def Price(tickers, start, end):
    #- 2020-12-31 2020-09-30 2020-06-30 2020-03-31
    tracker = []
    for ticker in tickers:
        quarters = []
        fet = Fetcher(ticker, start, end)
        prices = fet.get_historical()
        if not prices.empty:
            prices['Date'] = pd.to_datetime(prices['Date'])
            prices['year'] = prices['Date'].dt.year
            prices['month'] = prices['Date'].dt.month
            prices['day'] = prices['Date'].dt.day
            prices['last_day_of_month'] = prices.groupby(['year', 'month'])['day'].transform(max)
            prices['last_day_of_month_ind'] = prices['day']==prices['last_day_of_month']
            prices['quarter_end_ind'] = False
            #prices.loc[(prices['last_day_of_month_ind']==True) & (prices['month'].isin([3,6,9,12])), 'quarter_end_ind'] = True
            #prices = prices.loc[prices['quarter_end_ind']==True]

            prices['true_day'] = 30
            #prices.loc[prices['month'].isin([3, 12]), 'true_day'] = 31
            prices['date'] = prices['year'].astype(str)+'-'+prices['month'].astype(str).str.pad(2, 'left', '0')+'-'+prices['day'].astype(str).str.pad(2, 'left', '0')
            prices['date'] = prices['date'].str.strip()
            prices['ticker'] = ticker
            keep_cols = ['date', 'Close', 'ticker']
            tracker.append(prices[keep_cols])

    return pd.concat(tracker, axis=0, ignore_index=True)
