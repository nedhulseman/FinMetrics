

#-- base
import time

#-- Pypi
import pandas as pd

#-- Local
from FinMetrics import Metrics
from Prices import Price



if __name__ == '__main__':
    start = time.time()
    d = Metrics()

    ticks = d.stock_df.loc[100:1000, 'ticker'].unique().tolist()
    print(len(ticks))
    #ticks = ['AAPL', 'MSFT', 'NVDA', 'ITCB', 'SG']
    metrics = d.meta_metrics['metric'].tolist()
    df = d.fetch_append(ticks, metrics)
    df['date_asdate'] = pd.to_datetime(df['date'])
    df=df.sort_values(by=['date_asdate'])

    prices = Price(ticks, [2005,1,1], [2021,11,1])
    prices = prices.rename(columns={'date':'price_date'})
    prices['price_date_asdate'] = pd.to_datetime(prices['price_date'])
    prices=prices.sort_values(by=['price_date_asdate'])

    df = pd.merge_asof(df, prices, left_on="date_asdate",right_on='price_date_asdate', by="ticker", direction='backward')
    df['days_between_financials_price'] = df['date_asdate'] - df['price_date_asdate']

    t = round((time.time() - start), 2)
    t_per_tick = round(t/ len(ticks), 2)
    print('Data query took: {} minutes or {} seconds per ticker.'.format(round(t/60,2), t_per_tick))
    #df = pd.merge(df, prices, how='left', on=['ticker', 'date'])
    df.to_csv('./stocks100-1000.csv', index=False)
