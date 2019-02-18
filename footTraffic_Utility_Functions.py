
# coding: utf-8

# In[ ]:


import pandas as pd 
import numpy as np 
import datetime as dt
from datetime import timedelta
import os
import sys
import pyodbc
import time
import matplotlib.pyplot as plt
from pandas.tseries.offsets import BDay


punchTimesDF = pd.read_csv(r'/Users/sanjaygopinath/Documents/Data/foot_traffic_project/RetailDailyPunches(09.30.18 to 01.01.19).csv', index_col=None)

punchTimesDF.rename(columns= {'In Punch Date':'In_Date', 'In Punch Time':'In_Time', 'Out Punch Date':'Out_Date',
                                  'Out Punch Time':'Out_Time','Store Number':'Store_Number', 'EE Number':'EE_Number'}, inplace=True)
title_dictionary = {'Key Holder full time':'Key Holder', 'Key Holder Part Time':'Key Holder', 'Key Holder Part time':'Key Holder',
            'Key Holder part time':'Key Holder', 'Key Holder Full Time':'Key Holder', 'Sr. Sales Associate':'Sales Associate'}

punchTimesDF.replace({"Title": title_dictionary}, inplace=True)


#Combining date and hours into a single colummn 
punchTimesDF['in'] = punchTimesDF['In_Date'] + ' ' + punchTimesDF['In_Time']

punchTimesDF['out'] = punchTimesDF['Out_Date'] + ' ' + punchTimesDF['Out_Time']

#Converting to datetime 

punchTimesDF['in'] = pd.to_datetime(punchTimesDF['in'])
punchTimesDF['out'] = pd.to_datetime(punchTimesDF['out'])


#calculating the hours worked from our raw times 
punchTimesDF['raw_hours'] = punchTimesDF['out'] - punchTimesDF['in']

#ROunding the in and out dates to the nearest half hour 




punchTimesDF['rounded_in'] = punchTimesDF['in'].dt.round('30min')

punchTimesDF['rounded_out'] = punchTimesDF['out'].dt.round('30min')





#calcuatling the hours worked from our rounded times

punchTimesDF['rounded_hours'] = punchTimesDF['rounded_out'] - punchTimesDF['rounded_in']



def Generic_QL_Query(query, UID, password ):

#Establish our connection to the server 
    conn_string = 'DRIVER=/usr/local/lib/libtdsodbc.so;SERVER=wnj-datasizesql;PORT=1433;DATABASE=Data Analytics;UID={};PWD={}'.format(UID,password)
    conn = pyodbc.connect(conn_string)
    
#Run our sql query

    
    start_time = time.time()

    trFrame = pd.read_sql_query(query, conn)

    conn.close()

    end_time = time.time()

    run_time = (end_time-start_time)/60

    print(run_time)
    return(trFrame)


query = """   select
       a.transaction_datetime, a.store_no,
       count(a.transaction_id) as transaction_count,
       SUM(a.total_amount) as total_amount,
       SUM(a.total_amount)/count(a.transaction_id) as AvgTransactionAmntEvryHalfHour
  FROM (
         SELECT transaction_id, store_no,
                CONVERT(smalldatetime, ROUND(CONVERT(float, CONVERT(datetime, entry_date_time)) * 24.0, 0, 1) /
                                       24.0)                 as transaction_datetime,
                SUM(gross_line_amount - pos_discount_amount) as total_amount
         FROM transaction_detail_entry_datetime
         where
           sku_id is not null
          and store_no in
(28,  229,  397,   18,   30,   40,   47,   67,   69,   71,  120,
        136,  144,  147,  167,  187,  192,  201,  210,  214,  242,  275,
        297,  301,  309,  312,  314,  315,  319,  323,  342,  350,  355,
        357,  359,  388,  442,  450,  452,  453,  456,  466,  488,  511,
        517,  528,  536,  554,  559,  571,  583,  597,  598,  607,  612,
        616,  617,  623,  633,  679,  689,  703,  710,  712,  763,  777,
        784,  823,  828,  833,  861, 1003, 1013,   60,  189,  444,  507,
        573,  868)
         GROUP BY transaction_id, store_no,
         CONVERT(smalldatetime, ROUND(CONVERT(float, CONVERT(datetime, entry_date_time)) * 24.0, 0, 1) / 24.0)
       )a
GROUP BY a.store_no, a.transaction_datetime
ORDER BY  a.store_no,a.transaction_datetime;"""



storeSales_frame = Generic_QL_Query(query,'Dhananjay.Kumar','password1!')



new_df = pd.merge(storeSales_frame, traffic_frame,  how='left', left_on=['transaction_datetime','store_no'], right_on = ['traffic_from','store_no'])

