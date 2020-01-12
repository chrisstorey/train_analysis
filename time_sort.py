#!/usr/bin/env python
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, date
from dateutil import parser
from datetime import timedelta
conn = sqlite3.connect("/home/chris/projects/trains/database/trains.sqlite")
df = pd.read_sql_query("select * from traindata;", conn)

df['date-as-date'] = dateutil.parse(df['runDate'])
print(df)

df['runDate'] = pd.to_datetime(df['runDate'], format='%Y-%m-%d')

df['runDate']

for ind in df.index:
    if df['gbttBookedDepartureNextDay'][ind] == "1":
        df['date-as-date'][ind] = df['runDate'][ind] + timedelta(days=1)
    else:
        df['date-as-date'][ind] = df['runDate'][ind]

df['booked-time'] = pd.to_datetime(df['gbttBookedDeparture'], format='%H%M')

for ind in df.index:
    if df['realtimeDeparture'][ind] == "None":
        df['realtime-departure-time'][ind] = None
    else:
        df['realtime-departure-time'][ind] = pd.to_datetime(df['realtimeDeparture'][ind], format='%H%M')

df

df['time-difference'] = datetime.combine(date.min,df['booked-time'])-datetime.combine(date.min,df['realtime-departure-time'])

df['time-difference']=''

for ind in df.index:
    if df['realtime-departure-time'][ind] == None:
        print("a None!")
    else:
        df['time-difference'][ind] = (df['realtime-departure-time'][ind]-df['booked-time'][ind]).totalseconds
        print(df['time-difference'][ind])

df

#df.to_sql('train_out',conn, if_exists='replace')

df.dtypes

hist()

print((df['time-difference']))
