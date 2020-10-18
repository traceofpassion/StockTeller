import pandas as pd
from tqdm import tqdm
from datetime import datetime
today = datetime.today().strftime('%Y%m%d')
df = pd.read_csv('../database/stock_price/stock_price.csv')

import requests
from bs4 import BeautifulSoup

def stock_price_append(stock_code):
    url = 'http://asp1.krx.co.kr/servlet/krx.asp.XMLSiseEng?code=' + str(stock_code).zfill(6)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml-xml')
    stock = soup.find_all('DailyStock')
    
    if len(stock) == 0:
        return pd.DataFrame()
    else:
        output = pd.DataFrame()
        for i in stock:
            temp = pd.DataFrame(i.attrs, index=[0])
            output = pd.concat([output,temp], axis=0, ignore_index=True)
        output = output.rename(columns=dict(day_Date='Date', day_EndPrice='Price', day_Volume='Volume'))[['Date','Price','Volume']]
        output['Date'] = output['Date'].apply(lambda x: '20' + x).apply(lambda x: x.replace('/','-'))
        output['Price'] = output['Price'].apply(lambda x: int(x.replace(',','')))
        output['Volume'] = output['Volume'].apply(lambda x: x.replace(',','')).apply(lambda x: int(x))
        output['StockCode'] = stock_code
        return output   

def job_collect(data):
    print('-------- Crawling Start --------')
    stock_codes = data['StockCode'].unique()
    output = pd.DataFrame()
    for i in tqdm(stock_codes):
        temp = stock_price_append(i)
        output = pd.concat([output,temp], axis=0, ignore_index=True)
    data = data[~data['Date'].isin(output['Date'].unique())]
    output = pd.concat([output,data], axis=0, ignore_index=True)
    output = output.drop_duplicates()
    output = output.sort_values(['StockCode','Date'], ascending=False).reset_index(drop=True)
    output.to_csv(f'../database/stock_price/stock_price.csv', index=False)
    output.to_csv(f'../database/stock_price/stock_price_{today}.csv', index=False)
    print('-------- Crawling Done --------')

if __name__=="__main__":
    job_collect(df)

