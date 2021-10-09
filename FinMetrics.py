import pandas as pd
import requests
import re
import os
from bs4 import BeautifulSoup







class Metrics:
    def __init__(self):
        self.stock_df = pd.read_csv('./Data/tickers.csv')
        self.meta_metrics = pd.read_csv('./Data/meta_metrics.csv')
        self.meta_metrics = self.meta_metrics.fillna('')
        self.ticker    = None
        self.metric    = None
        self.tick_info = None
        self.tick_meta = None
        self.tick_url  = None
        self.html      = None
        self.text_html = None
        self.soup      = None
        self.str_jsvar = None
        self.jsvar     = None

    def fetch(self, ticker, metric):
        ticker = ticker.upper()
        self.ticker = ticker
        metric = metric.lower()
        self.metric = metric

        self.tick_info = self.stock_df.loc[self.stock_df['ticker']==ticker]#.iloc[0]
        if self.tick_info.shape[0] == 0:
            raise ValueError('Ticker provided does not exist...')
        self.tick_info = self.tick_info.iloc[0]
        self.tick_meta = self.meta_metrics[self.meta_metrics['metric']==metric]#.iloc[0]
        if self.tick_meta.shape[0] == 0:
            raise ValueError('Metric provided does not exist...')
        self.tick_meta = self.tick_meta.iloc[0]
        self.create_url()
        self.request_html()
        if self.tick_meta['format']=='json':
            kw_start = self.tick_meta['kw_start']
            kw_end = self.tick_meta['kw_end']
            self.find_jsvar(kw_start, kw_end)
        elif self.tick_meta['format']=='html':
            kw_start = self.tick_meta['kw_start']
            self.find_tables(kw_start)
        return self.ticker_metric_df

    def create_url(self):
        base = self.tick_info['full_url']
        ext = self.tick_meta['ext']
        freq = self.tick_meta['freq']
        self.tick_url = base + ext + freq
        print(type(self.tick_url))
        print(self.tick_url)
    def request_html(self):
        self.html = requests.get(self.tick_url)
        self.text_html = self.html.text
        self.soup = BeautifulSoup(self.html.text, 'html.parser')
    def find_tables(self, kw):
        dfs = pd.read_html(self.text_html)
        matched_dfs = []
        for d in dfs:
            print(d.columns)
            for c in list(d.columns):
                if kw in c:
                    matched_dfs.append(d)
                    break
        if len(matched_dfs) > 1:
            raise Exception('kw found in multiple html tables...')
        elif len(matched_dfs) == 0:
            raise Exception('no kw found in any html tables...')
        self.ticker_metric_df = matched_dfs[0]

    def find_jsvar(self, kw_start, kw_end):
        match = re.search('{}(.*?){}'.format(kw_start, kw_end), self.text_html)
        if match == None:
            raise ValueError('search terms did not work for... ticker: {}; metric: {};  start: {}; end: {}'.format(self.ticker, self.metric, kw_start, kw_end))
        self.str_jsvar = match.group(1) + kw_end
        df = pd.read_json(self.str_jsvar, orient='records')
        df.columns = ['field_name'] + list(df.columns)[1:]
        df['field_name'] = df['field_name'].apply(lambda x: re.search('>(.*?)</', x).group(1))
        df = df.set_index('field_name')
        df = df.transpose()
        df['date'] = df.index
        df = df.reset_index()
        self.ticker_metric_df = df
    def parse_date(self):
        pass
