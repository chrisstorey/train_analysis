#!/usr/bin/env python

import json
from pandas.io.json import json_normalize
import pandas as pd
import datetime
import dateutil
import argparse
import sqlite3


parser = argparse.ArgumentParser(description='RealTimeTrains data cleaner')
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str)
parser.add_argument("--database", "-d", type=str)
args = parser.parse_args()

print(args.file)

print_test=0


with open (args.file) as f:
    data = json.load(f)

#clear screen
print('\033[2J')

#give status message
status = "  Data Loaded  "
print(status.center(70, '#'))
print("\n")

numberOfServices = len(data['services'])

print("There are %3d services listed in this file for %s" %(numberOfServices,data["location"]["name"]))
print("\n")

runDate = ['']
trainIdentity =['']
serviceUid = ['']
atocCode = ['']
atocName = ['']
description = ['']
gbttBookedDeparture = ['']
gbttBookedDepartureNextDay = ['']
serviceType = ['']
origin = ['']
destination = ['']
realtimeArrival = ['']
realtimeDeparture = ['']
cancelReasonCode = ['']
cancelReasonShortText = ['']
cancelReasonLongText = ['']
displayAs =['']
origin_tiploc = ['']
origin_description = ['']
origin_workingTime = ['']
origin_publicTime = ['']
destination_tiploc = ['']
destination_description = ['']
destination_workingTime = ['']
destination_publicTime = ['']

for i in range(1,numberOfServices):
#for i in range(numberOfServices):
    runDate.append(data['services'][i]['runDate'])
    trainIdentity.append(data['services'][i]['trainIdentity'])
    serviceUid.append(data['services'][i]['serviceUid'])
    atocCode.append(data['services'][i]['atocCode'])
    atocName.append(data['services'][i]['atocName'])
    description.append(data['services'][i]['locationDetail']['description'])
    gbttBookedDeparture.append(data['services'][i]['locationDetail']['gbttBookedDeparture'])
    serviceType.append(data['services'][i]['serviceType'])

    #select details from origin string to gather sub details
    origin.append(data['services'][i]['locationDetail']['origin'])

    origin_str = str(origin[i])


    start = origin_str.find("tiploc")
    end = origin_str.find("'",start+10)
    origin_tiploc.append(origin_str[start+10:end])

    start = origin_str.find("description")
    end = origin_str.find("'",start+15)
    origin_description.append(origin_str[start+15:end])

    start = origin_str.find("workingTime")
    if start > 0:
        end = origin_str.find("'",start+15)
        origin_workingTime.append(origin_str[start+15:end])
    else:
        origin_workingTime.append("not-present")


    start = origin_str.find("publicTime")
    if start > 0:
        end = origin_str.find("'",start+14)
        origin_publicTime.append(origin_str[start+14:end])
    else:
        origin_publicTime.append("not-present")

    #select details from destination string to gather sub details
    destination.append(data['services'][i]['locationDetail']['destination'])
    destination_str = str(destination[i])

    start = destination_str.find("tiploc")
    end = destination_str.find("'",start+10)
    destination_tiploc.append(destination_str[start+10:end])


    start = destination_str.find("description")
    end = destination_str.find("'",start+15)
    destination_description.append(destination_str[start+15:end])

    start = destination_str.find("workingTime")
    if start > 0:
        end = destination_str.find("'",start+15)
        destination_workingTime.append(destination_str[start+15:end])
    else:
        destination_workingTime.append("not-present")

    start = destination_str.find("publicTime")
    if start > 0:
        end = destination_str.find("'",start+14)
        destination_publicTime.append(destination_str[start+14:end])
    else:
        destination_publicTime.append("not-present")

    try:
        realtimeArrival.append(data['services'][i]['locationDetail']['realtimeArrival'])
    except:
        realtimeArrival.append("None")

    try:
        gbttBookedDepartureNextDay.append(data['services'][i]['locationDetail']['gbttBookedDepartureNextDay'])
    except:
        gbttBookedDepartureNextDay.append("None")

    try:
        realtimeDeparture.append(data['services'][i]['locationDetail']['realtimeDeparture'])
    except:
        realtimeDeparture.append("None")

    try:
        cancelReasonCode.append(data['services'][i]['locationDetail']['cancelReasonCode'])
    except:
        cancelReasonCode.append("")
    try:
        cancelReasonShortText.append(data['services'][i]['locationDetail']['cancelReasonShortText'])
    except:
        cancelReasonShortText.append("")
    try:
        cancelReasonLongText.append(data['services'][i]['locationDetail']['cancelReasonLongText'])
    except:
        cancelReasonLongText.append("")
    try:
        displayAs.append(data['services'][i]['locationDetail']['displayAs'])
    except:
        displayAs.append("")



    if print_test==1:
        print("---------------------------------------------------------")
        print(trainIdentity[i])
        print(serviceUid[i])
        print(gbttBookedDeparture[i])
        print("\n")
        print(origin_tiploc[i])
        print(origin_description[i])
        print(origin_workingTime[i])
        print(origin_publicTime[i])
        print("\n")
        print(destination_tiploc[i])
        print(destination_description[i])
        print(destination_workingTime[i])
        print(destination_publicTime[i])
        print("\n")
        print("---------------------------------------------------------")
#give status message
status = "  Data Transformed  "
print(status.center(70, '#'))
print("\n")

#combine series to form one dataframe
df_combined = pd.DataFrame({'runDate': runDate,
                            'trainIdentity': trainIdentity,
                            'serviceUid': serviceUid,
                            'atocCode': atocCode,
                            'atocName': atocName,
                            'description': description,
                            'gbttBookedDeparture': gbttBookedDeparture,
                            'gbttBookedDepartureNextDay': gbttBookedDepartureNextDay,
                            'serviceType': serviceType,
                            'origin_tiploc': origin_tiploc,
                            'origin_description': origin_description,
                            'origin_workingTime': origin_workingTime,
                            'origin_publicTime': origin_publicTime,
                            'destination_tiploc': destination_tiploc,
                            'destination_description': destination_description,
                            'destination_workingTime': destination_workingTime,
                            'destination_publicTime': destination_publicTime,
                            'realtimeArrival': realtimeArrival,
                            'realtimeDeparture': realtimeDeparture,
                            'cancelReasonCode': cancelReasonCode,
                            'cancelReasonShortText': cancelReasonShortText,
                            'cancelReasonLongText': cancelReasonLongText,
                            'displayAs': displayAs})

status = "  Dataframe created "
print(status.center(70, '#'))
print("\n")

if args.out:
    #export dataframe to csv
    out_file= ""
    df_combined.to_csv(args.out)

    status = "  Output to csv file complete "
    print(status.center(70, '#'))
    print("\n")


if args.database:
    #export dataframe to sqlite
    conn = sqlite3.connect(args.database)

    df_combined.to_sql("traindata", conn, if_exists="append")
    status = "  Output to database complete "
    print(status.center(70, '#'))
    print("\n")

    df_check = pd.read_sql_query("select * from traindata;", conn)
    print(df_check)
