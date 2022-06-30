#memasukkan file 4 csv ke dalam postgre
#!python3
import os #untukmanage semua file didalam satu project file
import json #untuk meletakkan file config

import pandas as pd #membuat dataframe lebih familiar untuk data processing
from sqlalchemy import create_engine #membuat engine koneksi

if __name__=="__main__":
    path = os.getcwd() + "/"+"dataset"+"/"
    #nama file input dan nama file di postgre
    file_name = [('TR_OrderDetails.csv','fact_orderdetails'),
            ('TR_Products.csv','dim_products'),
            ('TR_PropertyInfo.csv','dim_location'),
            ('TR_UserInfo.csv','dim_users'),
            ]
    #database credential
    db = 'postgresql://postgres:12345@localhost:5432/digitalskola'
    #looping baca dan insert to sql
    for i in file_name:
        df = pd.read_csv(path+i[0])
        engine = create_engine(db)
        df.to_sql(i[1], engine, if_exists='replace', index=False)
