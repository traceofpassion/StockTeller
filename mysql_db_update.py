import sys
import requests
import base64
import pymysql
import pandas as pd
import logging

# RDS info
host = "yhsdb.cdhihmlo4gqv.us-east-2.rds.amazonaws.com"  # aws endpoint
port = 3306  # port number
username = "amanofmyword"  # aws rds db user name
database = "stock_price"  # aws rds db name
password = "sunny0619"  # aws rds db user password

# Authorization Client Credentials Flow
def connect_RDS():
    try:
        conn = pymysql.connect(host=host,
                               port=port,
                               user=username,
                               passwd=password,
                               db=database,
                               charset='utf8',
                               use_unicode=True)
        cursor = conn.cursor()
        print("--- RDS Connected ---")
    except:
        logging.error("--- RDS NOT Connected ---")
        sys.exit(1)
    return conn, cursor

conn, cursor = connect_RDS()

query_1st = """
    CREATE TABLE KOSPI_INFO (
        StockCode INT NOT NULL AUTO_INCREMENT,
        StockName VARCHAR(20) NOT NULL,
        TypeBiz VARCHAR(20),
        MainProduct VARCHAR(20),
        OriginDate DATE, 
        PRIMARY KEY(StockCode)
    );
"""
#cursor.execute(query_1st)
#conn.commit()

query_2nd = """
    LOAD DATA LOCAL INFILE 'C:/Users/YHS/Github/StockTeller/database/stock_code/stock_code_kospi.csv'
    INTO TABLE KOSPI_INFO;   
"""
cursor.execute(query_2nd)
conn.commit()
cursor.close()




