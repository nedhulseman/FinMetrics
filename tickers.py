import pandas as pd
import os
from bs4 import BeautifulSoup
import requests
import re



class Tickers:
    def __init__(self, table='industry'):
        self.url = 'https://www.macrotrends.net/stocks/research'
        self.base_url = 'https://www.macrotrends.net/'
        self.kws = {'industry':'Industry'}
        self.table = table
        self.html = requests.get(self.url)
        self.soup = BeautifulSoup(self.html.text, 'html.parser')
        self.tables = self.soup.findAll("table")
        #self.industry_table = self.find_table()
        #self.stock_table = self.create_df()
    def find_table(self):
        print('... collecting tables')
        for t in self.tables:
            _ = pd.read_html(str(t))[0]
            if self.kws[self.table] in _.columns[0][0]:
                self.industry_table = t
                return None
    def create_df(self):
        print('... identifying Industry Table')
        df = pd.read_html(str(self.industry_table))[0]
        links = self.industry_table.findAll('a')
        #print(df.head())
        #print(links)
        df.columns = [i[1] for i in df.columns]
        df['atag'] = [i['href'] for i in links]
        df['link'] = self.base_url + df['atag']
        self.industry_df = df
    def iter_industries(self):

        rename_cols = {
            'ticker': 'ticker',
            'zacks_x_ind_desc': 'industry',
            'comp_name': 'mt_name',
            'comp_name_2': 'comp_name',
            'country_code':'country',
            'link': 'link'
        }
        stocks = pd.DataFrame(columns=list(rename_cols.values()) + ['full_url', 'url'])
        for i, row in self.industry_df.iterrows():
            print('... fetching tickers in -{}- industry'.format(str(row['Name'])))
            html = requests.get(row['link'])
            match = re.search('var data = (.*?)}]', html.text)
            data = match.group(1) + '}]'
            df = pd.read_json(data, orient='records')
            df = df[rename_cols.keys()]
            df = df.rename(columns = rename_cols)
            df['full_url'] = df['link'].apply(lambda x:  re.search("href='(.*?)stock-price-history", x).group(1))
            df['url'] = df['full_url'].apply(lambda x: x.split('stock-price-history')[0])
            stocks = pd.concat([stocks, df], axis=0, ignore_index=True)
        print('... saving tickers to local csv')
        stocks.to_csv('./Data/tickers.csv', index=False)
        self.industry_df.to_csv('./Data/industries.csv', index=False)


if __name__ == '__main__':
    tick = Tickers()
    tick.find_table()
    tick.create_df()
    tick.iter_industries()
