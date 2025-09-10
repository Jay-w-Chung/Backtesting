import os
import finterstellar as fs
import pandas as pd
import numpy as np
import datetime as dt

class LoadData:
    

    def read_investing_price(self, path, cd):
        file_name = path + cd + ' Historical Data.csv'
        df = pd.read_csv(file_name, index_col='Date', parse_dates=True)
        return (df)

    def _master_path(self, path, f_name): 
        return os.path.join(path, f'fs {f_name}.csv')

    def _master_exists(self, path, f_name):
        return os.path.exists(self._master_path(path, f_name))

    def create_portfolio_df(self, path, p_name, p_cd):
        new_df = self.make_historical_price_df(path, p_cd) 
        if not self._master_exists(path, p_name):
            prices_df = self.create_master_file(path, p_name, new_df)
        else:
            prices_df = self.update_master_file(path, p_name, new_df)
        return prices_df

    def make_historical_price_df(self, path, s_cd): # build a unified historical DataFrame (universe_df)
        cds = fs.str_list(s_cd)
        all_dates = pd.DatetimeIndex([])
        # collect all unique dates across symbols
        for c in cds:
            prices_df = self.read_investing_price(path, c)
            prices_df = self.date_formatting(prices_df)
            all_dates = all_dates.union(prices_df.index)
        universe_df = pd.DataFrame(index=all_dates.sort_values())
        universe_df.index.name = 'Date'
        # add each symbol's prices into the universe DataFrame
        for c in cds:
            prices_df = self.read_investing_price(path, c)
            prices_df = self.date_formatting(prices_df)
            prices_df = self.price_df_trimming(prices_df, c)
            universe_df[c] = prices_df[c]

        # sort and forward fill missing values
        universe_df = universe_df.sort_index().ffill()
        return universe_df

    def create_master_file(self, path, f_name, df): # save a new master portfolio CSV if it does not exist
        file_name = self._master_path(path, f_name)
        try:
            with open(file_name): 
                print('Updating master file')
        except IOError: 
            df = df.copy()
            df.index = pd.to_datetime(df.index)
            df.index.name = 'Date'
            df.to_csv(file_name)
        return df

    def read_master_file(self, path, n): # read an existing master portfolio CSV
        file_name = self._master_path(path, n)
        prices_df = pd.read_csv(file_name, index_col='Date', parse_dates=True)
        prices_df.sort_index(axis=0, inplace=True)
        return prices_df

    def get_codes(self, prices_df): # return to the list of colunn names (symbols) in the DataFrame
        return prices_df.columns.values

    def read_raw_csv(self, path, n):
        file_name = path + n + '.csv'
        df = pd.read_csv(file_name, index_col='Date', parse_dates=True)
        df.sort_index(axis=0, inplace=True)
        return df

    def read_raw_excel(self, path, n, sheet=None):
        file_name = path + n
        df = pd.read_excel(file_name, index_col=0, sheet_name=sheet)
        df.index = pd.to_datetime(df.index)
        df.sort_index(axis=0, inplace=True)
        return df

    def date_formatting(self, df):
        df = df.copy()
        df.index = pd.to_datetime(df.index)
        df.index.name = 'Date'
        return df

    def price_formatting(self, df, c='Price'): # convert price columns to numeric
        s = df[c].astype(str).str.replace(',', '', regex=False) # remove commas
        df[c] = pd.to_numeric(s, errors='coerce')
        return df

    def price_df_trimming(self, df, cd): # extract and clean a single column of prices
        df = self.price_formatting(df, 'Price')
        df_new = pd.DataFrame({cd: df['Price']})
        df_new.sort_index(inplace=True)
        return df_new

    def read_intraday_csv(self, path, n):
        file_name = path + n + '.csv'
        df = pd.read_csv(file_name, index_col=0)
        df.index = pd.to_datetime(df.index).time
        df.index.name = 'Time'
        df.sort_index(axis=0, inplace=True)
        return df

    def read_intraday_excel(self, path, n):
        file_name = path + n + '.xlsx'
        df = pd.read_excel(file_name, index_col=0)
        df.index = pd.to_datetime(df.index).time
        df.index.name = 'Time'
        df.sort_index(axis=0, inplace=True)
        return df
    
