import argparse
import json
import sqlite3
import pandas as pd

# Constants
STATUS_DATA_LOADED = "  Data Loaded  "
STATUS_DATA_TRANSFORMED = "  Data Transformed  "
STATUS_DF_CREATED = "  Dataframe created "
STATUS_CSV_OUTPUT_COMPLETE = "  Output to csv file complete "
STATUS_DB_OUTPUT_COMPLETE = "  Output to database complete "
DEFAULT_MISSING_VALUE = "not-present"


# Function to display a status message
def print_status(message):
    print("\033[2J")  # Clear screen
    print(message.center(70, '#'))
    print("\n")


# Function to extract location details
def extract_location_details(location_str, key, default=DEFAULT_MISSING_VALUE):
    start = location_str.find(key)
    if start > 0:
        end = location_str.find("'", start + len(key) + 3)
        return location_str[start + len(key) + 3:end]
    return default


# Function to process origin or destination data
def process_location_data(location_detail, key):
    location_str = str(location_detail.get(key, {}))
    return {
        "tiploc": extract_location_details(location_str, "tiploc"),
        "description": extract_location_details(location_str, "description"),
        "workingTime": extract_location_details(location_str, "workingTime"),
        "publicTime": extract_location_details(location_str, "publicTime"),
    }


# Function to handle optional fields with defaults
def get_optional_field(data, key, default=""):
    return data.get(key, default)


# Main function to process train data
def process_train_data(data: dict) -> list[dict]:
    services = []
    for service in data["services"]:
        origin_data = process_location_data(service["locationDetail"], "origin")
        destination_data = process_location_data(service["locationDetail"], "destination")

        # Append processed service details to the list
        services.append({
            "runDate": get_optional_field(service, "runDate"),
            "trainIdentity": get_optional_field(service, "trainIdentity"),
            "serviceUid": get_optional_field(service, "serviceUid"),
            "atocCode": get_optional_field(service, "atocCode"),
            "atocName": get_optional_field(service, "atocName"),
            "description": get_optional_field(service["locationDetail"], "description"),
            "gbttBookedDeparture": get_optional_field(service["locationDetail"], "gbttBookedDeparture"),
            "gbttBookedDepartureNextDay": get_optional_field(service["locationDetail"], "gbttBookedDepartureNextDay",
                                                             "None"),
            "serviceType": get_optional_field(service, "serviceType"),
            "origin_tiploc": origin_data["tiploc"],
            "origin_description": origin_data["description"],
            "origin_workingTime": origin_data["workingTime"],
            "origin_publicTime": origin_data["publicTime"],
            "destination_tiploc": destination_data["tiploc"],
            "destination_description": destination_data["description"],
            "destination_workingTime": destination_data["workingTime"],
            "destination_publicTime": destination_data["publicTime"],
            "realtimeArrival": get_optional_field(service["locationDetail"], "realtimeArrival", "None"),
            "realtimeDeparture": get_optional_field(service["locationDetail"], "realtimeDeparture", "None"),
            "cancelReasonCode": get_optional_field(service["locationDetail"], "cancelReasonCode"),
            "cancelReasonShortText": get_optional_field(service["locationDetail"], "cancelReasonShortText"),
            "cancelReasonLongText": get_optional_field(service["locationDetail"], "cancelReasonLongText"),
            "displayAs": get_optional_field(service["locationDetail"], "displayAs"),
        })
    return services


# Argument setup
parser = argparse.ArgumentParser(description='RealTimeTrains data cleaner')
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str)
parser.add_argument("--database", "-d", type=str)
args = parser.parse_args()

# Load data
with open(args.file) as file:
    data = json.load(file)

print_status(STATUS_DATA_LOADED)

# Process services
processed_services = process_train_data(data)

print_status(STATUS_DATA_TRANSFORMED)

# Create dataframe
df_combined = pd.DataFrame(processed_services)

print_status(STATUS_DF_CREATED)

# Output to CSV if specified
if args.out:
    df_combined.to_csv(args.out, index=False)
    print_status(STATUS_CSV_OUTPUT_COMPLETE)

# Output to SQLite database if specified
if args.database:
    conn = sqlite3.connect(args.database)
    df_combined.to_sql("traindata", conn, if_exists="append", index=False)
    print_status(STATUS_DB_OUTPUT_COMPLETE)
    df_check = pd.read_sql_query("SELECT * FROM traindata;", conn)
    print(df_check)
