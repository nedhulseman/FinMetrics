import pandas as pd
import os
from bs4 import BeautifulSoup
import requests
import re



class Tickers:
    def __init__(self):
        self.url = 'https://www.macrotrends.net/stocks/research'
        self.base_url = 'https://www.macrotrends.net/'
        self.kws = {'industry':'Industry',
                    'market_cap': 'Market Cap',
                    'volume': 'Volume'
                    }
        self.table = 'industry'
        self.html = requests.get(self.url)
        self.soup = BeautifulSoup(self.html.text, 'html.parser')
        self.tables = self.soup.findAll("table")
        self.parsed_tables= {
            'industry':None,
            'market_cap':None,
            '':None
        }
        #self.industry_table = self.find_table()
        #self.stock_table = self.create_df()
    def find_tables(self, kw):
        print('... finding tables from {} tables'.format(str(len(self.tables))))
        for t in self.tables:
            _ = pd.read_html(str(t))[0]
            if kw in _.columns[0][0]:
                #self.industry_table = t
                print(kw)
                print(_.columns)
                return t
        raise KeyError('KW: {} not found in tables'.format(kw))
    def create_df(self, table):
        print('... posturing html tables')
        df = pd.read_html(str(table))[0]
        rows = table.tbody.findAll('tr')
        links = [i.findAll('td')[0].findAll('a')[0] for i  in rows]

        #print(df.head())
        #print(links)

        df.columns = [i[1] for i in df.columns]
        df['atag'] = [i['href'] for i in links][0: df.shape[0]]
        df['link'] = self.base_url + df['atag']
        #self.industry_df = df
        return df
    def create_dfs(self):
        self.parsed_tables['industry'] = self.find_tables(self.kws['industry'])
        self.parsed_tables['market_cap'] = self.find_tables(self.kws['market_cap'])
        self.parsed_tables['volume'] = self.find_tables(self.kws['volume'])

        self.industry_table = self.create_df(self.parsed_tables['industry'])
        self.market_cap = self.create_df(self.parsed_tables['market_cap'])
        self.volume = self.create_df(self.parsed_tables['volume'])
        self.top_stocks = pd.concat([self.market_cap, self.volume], axis=0, ignore_index=True)

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
        for i, row in self.industry_table.iterrows():
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

        rename_topstocks = {
            'Ticker': 'ticker',
            'Name': 'comp_name',
            'link': 'full_url',
        }
        self.top_stocks = self.top_stocks.rename(columns = rename_topstocks)
        self.top_stocks['mt_name'] = self.top_stocks['full_url'].apply(lambda x:  re.search("/(.*?)/stock-price-history", x).group(1))
        self.top_stocks['full_url'] = self.top_stocks['full_url'].str.replace('stock-price-history', '').str.replace('t//', 't/')
        self.top_stocks = self.top_stocks[list(rename_topstocks.values()) + ['mt_name']]
        stocks = pd.concat([self.top_stocks, stocks], axis=0, ignore_index=True)


        print('... saving tickers to local csv')
        #self.top_stocks.to_csv('./Data/top_stocks.csv', index=False))
        stocks.to_csv('./Data/tickers.csv', index=False)
        self.industry_table.to_csv('./Data/industries.csv', index=False)



if __name__ == '__main__':
    tick = Tickers()
    #tick.find_table()
    tick.create_dfs()
    tick.iter_industries()
