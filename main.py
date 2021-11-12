

#-- Pypi
import pandas as pd

#-- Local
from FinMetrics import Metrics
from Prices import Price


if __name__ == '__main__':
    d = Metrics()
    ticks = d.stock_df.loc[0:25, 'ticker'].tolist()
    ticks = ['AAPL', 'MSFT', 'NVDA']
    metrics = d.meta_metrics['metric'].tolist()
    df = d.fetch_append(ticks, metrics)

    prices = Price(ticks, [2005,1,1], [2021,11,1])
    df = pd.merge(df, prices, how='left', on=['ticker', 'date'])
    df.to_csv('./test.csv', index=False)
