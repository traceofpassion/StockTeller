import pandas as pd
from tqdm import tqdm
import json
import sqlalchemy
from sqlalchemy import create_engine
import FinanceDataReader as fdr
from datetime import datetime
from datetime import timedelta
from functools import reduce
import warnings
warnings.filterwarnings('ignore')
tqdm.pandas()

with open('../credential/config_local.json', encoding='UTF-8') as f:
    config = json.load(f)
engine = create_engine('postgresql://' +
                       config['user'] +
                       ':' + config['password'] +
                       '@' + config['host'] +
                       ':' + config['port'] +
                       '/' + config['dbname'])

stock_price = pd.read_sql_table(table_name='STOCK_PRICE_KOSPI', con=engine, schema='public', parse_dates='Date')

st_date = stock_price['Date'].max() + timedelta(days=1)
today = datetime.today().strftime('%Y-%m-%d')


def PriceUpdate(stock_code):
    temp = fdr.krx.data.KrxDelistingReader(symbol=stock_code, start=st_date, end=today).read()
    temp['Symbol'] = stock_code
    temp = temp.reset_index()
    output = temp[['Symbol', 'Date', 'Close', 'Open', 'High', 'Low', 'Volume']]
    return output


symbol_list = stock_price['Symbol'].drop_duplicates()
stacked = symbol_list.progress_apply(lambda x: PriceUpdate(x))
temp = reduce(lambda x, y: pd.concat([x, y], axis=0), stacked)

temp.to_sql(name='STOCK_PRICE_KOSPI',
            con=engine,
            schema='public',
            if_exists='append',
            index=False)