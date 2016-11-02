from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import re
import time
# import logging
from .base import BaseScraper


class CMESP500Scraper(BaseScraper):

    fut_url = r'http://www.cmegroup.com/trading/equity-index/us-index/e-mini-sandp500.html'
    opt_url = r'http://www.cmegroup.com/trading/equity-index/us-index/' \
              r'e-mini-sandp500_quotes_globex_options.html?optionExpiration={}#strikeRange=ALL'

    mon = {'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06',
           'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'}

    def __init__(self):
        super().__init__()
        self.__futures_data = None
        self.__options_data = None

        self.__fut_page = None
        self.__opt_list = []


    @property
    def futures_data(self):
        assert type(self.__futures_data) == pd.DataFrame
        return self.__futures_data.copy()

    @property
    def options_data(self):
        assert type(self.__options_data) == pd.DataFrame
        return self.__options_data.copy()

    def load_all(self, n=3):
        self.__load_futures(n)
        self.__load_options()

    def __load_futures(self, n):
        """Load the first n futures contracts"""

        print(r"Loading Futures Data ..")
        self.__fut_page = self.read_url(self.fut_url)

        soup = BeautifulSoup(self.__fut_page, 'html.parser')
        tbl = soup.find(id='quotesFuturesProductTable1')
        assert tbl is not None, "Failed to find table quotesFuturesProductTable11"

        pattern = re.compile(r'quotesFuturesProductTable1\_(.*)\_last')
        tds = [td for td in tbl.findAll('td') if 'id' in td.attrs and pattern.match(td.attrs['id']) is not None]
        trs = [(td, td.parent) for td in tds if td.parent.name == 'tr']
        trs = trs[:n]

        cols = ['last', 'priorSettle', 'change', 'volume']
        df = pd.DataFrame(columns=cols)
        for td, tr in trs:
            contract = pattern.search(td.attrs['id']).group(1)

            expiry = tr.find('span', {'class': 'cmeNoWrap'})
            assert expiry is not None, 'Failed to parse expiry'
            exp_ym = self.parse_my(expiry.text)
            option_url = self.opt_url.format(contract[-2:])
            self.__opt_list.append((exp_ym, option_url))

            res = {'Expiry': exp_ym}
            for col in cols:
                z = tr.find('td', {'id': 'quotesFuturesProductTable1_{}_{}'.format(contract, col)})
                z = z.text.replace(r',', '').replace(r'-', '') if z is not None else ''
                res[col] = np.float(z) if z != '' else np.nan

            df = df.append(pd.Series(res), ignore_index=True)

        self.__futures_data = df

    def __load_options(self):

        print(r"Loading Options Data ..")

        holder = []
        pattern = re.compile(r'optionQuotesProductTable1\_(.*)\_C(\d+)\_updated')
        for exp_ym, url in self.__opt_list:
            print("Sleeping 10 seconds")
            time.sleep(10)

            print(r"  parsing page: {} ...".format(url))
            page = self.read_url(url)

            soup = BeautifulSoup(page, 'html.parser')
            tbl = soup.find(id='optionQuotesProductTable1')
            assert tbl is not None, "Failed to find table quotesFuturesProductTable11"

            tds = [td for td in tbl.findAll('td') if 'id' in td.attrs and pattern.match(td.attrs['id']) is not None]
            trs = [(td, td.parent) for td in tds if td.parent.name == 'tr']

            cols = ['volume', 'change', 'last', 'priorSettle']
            df = pd.DataFrame(columns=['Type', 'Strike'] + cols)
            for td, tr in trs:
                s = pattern.search(td.attrs['id'])
                contract, strike = s.group(1), s.group(2)

                # parse strike
                x = tr.find('th', {'scope': 'row'})
                x = np.float(x.text)

                for otype in ('C', 'P'):
                    res = {'Expiry': exp_ym, 'Type': otype, 'Strike': x}
                    for col in cols:
                        z = tr.find('td', {'id': 'optionQuotesProductTable1_{}_{}{}_{}'.format(
                            contract, otype, strike, col)})
                        z = z.text.replace(r',', '').replace(r'-', '') if z is not None else ''
                        res[col] = np.float(z) if z != '' else np.nan
                    df = df.append(pd.Series(res), ignore_index=True)

            holder.append(df)

        self.__options_data = pd.concat(holder, axis=0)

    @classmethod
    def parse_my(cls, x):
        m, y = x.split(' ')
        m = cls.mon[m.lower()]
        assert m is not None, 'Failed to parse MON YYYY'
        return y + m

