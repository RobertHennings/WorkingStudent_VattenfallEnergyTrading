#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 11 11:02:34 2023

@author: Robert_Hennings
"""

# Broker Quote Parsing from E Mail Text
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup as bs
import re


file = "/Users/Robert_Hennings/Dokumente/Uni/MusterBewerbung/MeineArbeitgeber/Vattenfall/BrokerQuotesExample.txt"

text = pd.read_table(file, header=None,names=["Dat"], dtype={"Dat": "string"})
text = text.loc[2:]
text.reset_index(inplace=True, drop=True)



def parse_Email_text(text):
    # Now parse the information accordingly, separate first part that states the Hedges from the rest
    df_hedges = pd.DataFrame()
    df_quotes = pd.DataFrame()
    
    for line in range(text.shape[0]):
        if "@" in text.loc[line, :][0]:
            df_quotes = pd.concat([df_quotes, text.loc[line, :]], ignore_index=True)
        else:
            df_hedges = pd.concat([df_hedges, text.loc[line, :]], ignore_index=True)

    
    def find_whitespaces(text):
        ws_list = [m.start() for m in re.finditer(' ', text)]
        return ws_list
    
    
    df_hedges_cleaned = pd.DataFrame()
    df_quotes_cleaned = pd.DataFrame(index=list(range(df_quotes.shape[0])),
                                     data = np.nan,
                                     columns=["FirstLeg", "L1_Bid", "L1_Offer",
                                              "L2_Vol", "L2_Type", "L2_Price",
                                              "L3_Vol", "L3_Type", "L3_Price",
                                              "Ratio"]) # also include a column that flags the entries containing * and strategy no strategy
    for line in range(df_hedges.shape[0]):
        list_ws = find_whitespaces(df_hedges.loc[line, :][0])
    
        df_temp = df_hedges.loc[0].str[list_ws[1]:].str.split(" ", expand=True)
        
        df_hedges_cleaned = pd.concat([df_hedges_cleaned, df_temp], axis=0, ignore_index=True)
        df_hedges_cleaned.iloc[line, 0] = df_hedges.loc[0].str[:list_ws[1]][0]
    
    
    df_hedges_cleaned.columns = ["Product", "P1", "P2", "P3"]
    
    for line in range(df_quotes.shape[0]):
        list_ws = find_whitespaces(df_quotes.loc[line, :][0])
        
        if 3 not in list_ws: # if there isnt a white space in the third position of the string chain it has to be a TTF case
            df_temp = df_quotes.loc[0, :].str.split(" ", expand=True).add_prefix("TTF_").drop(["TTF_2", "TTF_4", "TTF_6"], axis=1)
            df_temp2 = df_temp.TTF_5.str.split("@", expand=True).add_prefix("TTF5_")
            df_temp3 = df_temp.TTF_7.str.split("@", expand=True).add_prefix("TTF7_")
        
        
            df_quotes_cleaned.iloc[line, 0] = df_temp.iloc[0,0] # FirstLeg
            df_quotes_cleaned.iloc[line, 1] = df_temp.iloc[0,1] # L1_Bid
            df_quotes_cleaned.iloc[line, 2] = df_temp.iloc[0,2] # L1_Offer
            df_quotes_cleaned.iloc[line, 3] = df_temp2.TTF5_0.str[:-1] # L2_Vol
            df_quotes_cleaned.iloc[line, 4] = df_temp2.TTF5_0.str[-1:] # L2_Type
            df_quotes_cleaned.iloc[line, 5] = df_temp2.TTF5_1 # L2_Price
            df_quotes_cleaned.iloc[line, 6] = df_temp3.TTF7_0.str[:-1] # L3_Vol
            df_quotes_cleaned.iloc[line, 7] = df_temp3.TTF7_0.str[-1:] # L3_Type
            df_quotes_cleaned.iloc[line, 8] = df_temp3.TTF7_1 # L3_Price
            df_quotes_cleaned.iloc[line, 9] = df_temp.iloc[0,5] # Ratio
        
        
        else: # normal variant that has to be splitted into pieces 
            df_temp = df_quotes.loc[1].str[list_ws[1]:].str.split(" ", expand=True).add_prefix("Norm_").drop(["Norm_0", "Norm_2", "Norm_4"], axis=1)
            df_temp2 = df_temp.Norm_5.str.split("@", expand=True).add_prefix("Norm5_")
        
            df_quotes_cleaned.iloc[line, 0] = df_quotes.loc[1].str[:list_ws[1]] # FirstLeg
            df_quotes_cleaned.iloc[line, 1] = df_temp.iloc[0,0] # L1_Bid
            df_quotes_cleaned.iloc[line, 2] = df_temp.iloc[0,1]
            df_quotes_cleaned.iloc[line, 3] = df_temp2.Norm5_0.str[:-1]
            df_quotes_cleaned.iloc[line, 4] = df_temp2.Norm5_0.str[-1:]
            df_quotes_cleaned.iloc[line, 5] = df_temp2.Norm5_1
            df_quotes_cleaned.iloc[line, 9] = df_temp.iloc[0,3]
    

    return df_hedges_cleaned, df_quotes_cleaned

df1, df2 = parse_Email_text(text = text)
df1
df2

