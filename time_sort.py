#!/usr/bin/env python
import sqlite3
import datetime
import pandas as pd

# Constants
DB_PATH = "/home/chris/projects/trains/database/trains.sqlite"
SQL_QUERY = "SELECT * FROM traindata;"
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H%M"

# Load data
conn = sqlite3.connect(DB_PATH)
train_data = pd.read_sql_query(SQL_QUERY, conn)

# Process 'runDate' as datetime
train_data['runDate'] = pd.to_datetime(train_data['runDate'], format=DATE_FORMAT)


# Update 'date-as-date' column
def update_date_as_date(row):
    if row['gbttBookedDepartureNextDay'] == "1":
        return row['runDate'] + datetime.timedelta(days=1)
    return row['runDate']


train_data['date-as-date'] = train_data.apply(update_date_as_date, axis=1)

# Process booked and real-time departures
train_data['booked-time'] = pd.to_datetime(train_data['gbttBookedDeparture'], format=TIME_FORMAT)


def process_realtime_departure(departure):
    if departure == "None":
        return None
    return pd.to_datetime(departure, format=TIME_FORMAT)


train_data['realtime-departure-time'] = train_data['realtimeDeparture'].apply(process_realtime_departure)


# Calculate time differences
def calculate_time_difference(row):
    if row['realtime-departure-time'] is None:
        return None
    return (row['realtime-departure-time'] - row['booked-time']).total_seconds()


train_data['time-difference'] = train_data.apply(calculate_time_difference, axis=1)

# Display and final output
print(train_data.dtypes)
print(train_data['time-difference'])

# Uncomment to save to database
# train_data.to_sql('train_out', conn, if_exists='replace')
