#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  6 11:30:05 2023

@author: Robert_Hennings
"""

#EntsoE Energy Transparency Platform
from entsoe import EntsoePandasClient
from entsoe import EntsoeRawClient
import pandas as pd
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup as bs
import numpy as np 

client = EntsoeRawClient(api_key="cbeb3939-e7e1-4033-8d0d-c82c46e96c8b")
client = EntsoePandasClient(api_key="cbeb3939-e7e1-4033-8d0d-c82c46e96c8b")


dir(client)

start = pd.Timestamp('20201201', tz ='Europe/Berlin')
end = pd.Timestamp('20201202', tz ='Europe/Berlin')
country_code = "DE_LU"

time_res_dict = {"PT15M": "15Min",
                 "PT60M": "60Min"}


#day-ahead market prices (€/MWh)
DA_prices = client.query_day_ahead_prices("DE_LU", start=start,end=end)




# Parse the xml data with bs4
bs_content = bs(DA_prices, "xml")
bs_content


# extract the TimeSeries data , Day Ahead res is 15 Minute intervals 
for res in bs_content.find_all("resolution"):
     print(res.text.split("T")[1].replace("M", "Min"))
     print(type(res.text.split("T")[1].replace("M", "Min")))


res = bs_content.find_all("TimeSeries")
res[1]

dir(bs_content.find_all("mRID")[0])

for i in bs_content.find_all("mRID")[0].next_elements:
    print(i)
      

for i in res[0].find_all("position"):
    print(i.text)
# 1 - 96

for j in res[0].find_all("price.amount"):
    print(j.text)



for i in res:
    print(i)



def get_Day_Ahead_prices(country_code, start, end, return_hourly):
    try:
        xml_data = client.query_day_ahead_prices(country_code, start=start,end=end)
        bs_data = bs(xml_data, "xml")
        print("Data loaded successfully")
        [print("Available Time Frequencies: ", time_res_dict[t.text]) for t in bs_data.find_all("resolution")]
        
        ts_tags = bs_data.find_all("TimeSeries")
        res_freq = bs_data.find_all("resolution")
        list_dfs = []
        
        for res in res_freq:
            locals()["price_df_{}".format(res.text.split("T")[1].replace("M", "Min"))] = pd.DataFrame(index = pd.date_range(start, end, freq=res.text.split("T")[1].replace("M", "Min")))
            locals()["price_df_{}".format(res.text.split("T")[1].replace("M", "Min"))].drop(locals()["price_df_{}".format(res.text.split("T")[1].replace("M", "Min"))].index[-1], inplace=True)
            locals()["price_list_{}".format(res.text.split("T")[1].replace("M", "Min"))] = []
            list_dfs.append(locals()["price_df_{}".format(res.text.split("T")[1].replace("M", "Min"))])
        
        for res, ts in zip(res_freq, ts_tags):
            for i in ts.find_all("price.amount"): 
                locals()["price_list_{}".format(res.text.split("T")[1].replace("M", "Min"))].append(i.text)
        
        for d, res in zip(list_dfs, res_freq):
            d["Euro_MWh"] = np.float64(locals()["price_list_{}".format(res.text.split("T")[1].replace("M", "Min"))])
            
        if return_hourly:
            return list_dfs
        else:
            return list_dfs[0]
    except:
        print("No valid country code: ", country_code)

d1, d2 = get_Day_Ahead_prices(country_code =country_code,start=start, end=end, return_hourly=True) #But watch out for the country codes, two resolutions only for DE_LU
    

xml = client.query_crossborder_flows("DE", "LU", start, end)
bs = bs(xml, "xml")
bs.find_all("TimeSeries")

