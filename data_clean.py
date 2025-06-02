import argparse
import json
import sqlite3
import pandas as pd
import sys
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# --- Constants for Dictionary Keys ---
KEY_SERVICES = "services"
KEY_LOCATION_DETAIL = "locationDetail"
KEY_ORIGIN = "origin"
KEY_DESTINATION = "destination"

# Keys used within extract_location_details and process_location_data
KEY_TIPLOC = "tiploc"
KEY_DESCRIPTION = "description"
KEY_WORKING_TIME = "workingTime"
KEY_PUBLIC_TIME = "publicTime"

# Keys for main service object / locationDetail
KEY_RUN_DATE = "runDate"
KEY_TRAIN_IDENTITY = "trainIdentity"
KEY_SERVICE_UID = "serviceUid"
KEY_ATOC_CODE = "atocCode"
KEY_ATOC_NAME = "atocName"
# KEY_DESCRIPTION is already defined above, can be reused
KEY_GBTT_BOOKED_DEPARTURE = "gbttBookedDeparture"
KEY_GBTT_BOOKED_DEPARTURE_NEXT_DAY = "gbttBookedDepartureNextDay"
KEY_SERVICE_TYPE = "serviceType"
KEY_REALTIME_ARRIVAL = "realtimeArrival"
KEY_REALTIME_DEPARTURE = "realtimeDeparture"
KEY_CANCEL_REASON_CODE = "cancelReasonCode"
KEY_CANCEL_REASON_SHORT_TEXT = "cancelReasonShortText"
KEY_CANCEL_REASON_LONG_TEXT = "cancelReasonLongText"
KEY_DISPLAY_AS = "displayAs"

# Column name prefixes (not directly used in this change but good for context)
# PREFIX_ORIGIN = "origin_"
# PREFIX_DESTINATION = "destination_"

# Status Constants
STATUS_DATA_LOADED = "Data Loaded"
STATUS_DATA_TRANSFORMED = "Data Transformed"
STATUS_DF_CREATED = "Dataframe created"
STATUS_CSV_OUTPUT_COMPLETE = "Output to csv file complete"
STATUS_DB_OUTPUT_COMPLETE = "Output to database complete"
DEFAULT_MISSING_VALUE = "not-present"


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
        KEY_TIPLOC: extract_location_details(location_str, KEY_TIPLOC),
        KEY_DESCRIPTION: extract_location_details(location_str, KEY_DESCRIPTION),
        KEY_WORKING_TIME: extract_location_details(location_str, KEY_WORKING_TIME),
        KEY_PUBLIC_TIME: extract_location_details(location_str, KEY_PUBLIC_TIME),
    }


# Function to handle optional fields with defaults
def get_optional_field(data, key, default=""):
    return data.get(key, default)


# Main function to process train data
def process_train_data(data: dict) -> list[dict]:
    services = []
    for service in data[KEY_SERVICES]:
        origin_data = process_location_data(service[KEY_LOCATION_DETAIL], KEY_ORIGIN)
        destination_data = process_location_data(service[KEY_LOCATION_DETAIL], KEY_DESTINATION)

        # Append processed service details to the list
        services.append({
            "runDate": get_optional_field(service, KEY_RUN_DATE),
            "trainIdentity": get_optional_field(service, KEY_TRAIN_IDENTITY),
            "serviceUid": get_optional_field(service, KEY_SERVICE_UID),
            "atocCode": get_optional_field(service, KEY_ATOC_CODE),
            "atocName": get_optional_field(service, KEY_ATOC_NAME),
            "description": get_optional_field(service[KEY_LOCATION_DETAIL], KEY_DESCRIPTION),
            "gbttBookedDeparture": get_optional_field(service[KEY_LOCATION_DETAIL], KEY_GBTT_BOOKED_DEPARTURE),
            "gbttBookedDepartureNextDay": get_optional_field(service[KEY_LOCATION_DETAIL], KEY_GBTT_BOOKED_DEPARTURE_NEXT_DAY, "None"),
            "serviceType": get_optional_field(service, KEY_SERVICE_TYPE),
            "origin_tiploc": origin_data[KEY_TIPLOC],
            "origin_description": origin_data[KEY_DESCRIPTION],
            "origin_workingTime": origin_data[KEY_WORKING_TIME],
            "origin_publicTime": origin_data[KEY_PUBLIC_TIME],
            "destination_tiploc": destination_data[KEY_TIPLOC],
            "destination_description": destination_data[KEY_DESCRIPTION],
            "destination_workingTime": destination_data[KEY_WORKING_TIME],
            "destination_publicTime": destination_data[KEY_PUBLIC_TIME],
            "realtimeArrival": get_optional_field(service[KEY_LOCATION_DETAIL], KEY_REALTIME_ARRIVAL, "None"),
            "realtimeDeparture": get_optional_field(service[KEY_LOCATION_DETAIL], KEY_REALTIME_DEPARTURE, "None"),
            "cancelReasonCode": get_optional_field(service[KEY_LOCATION_DETAIL], KEY_CANCEL_REASON_CODE),
            "cancelReasonShortText": get_optional_field(service[KEY_LOCATION_DETAIL], KEY_CANCEL_REASON_SHORT_TEXT),
            "cancelReasonLongText": get_optional_field(service[KEY_LOCATION_DETAIL], KEY_CANCEL_REASON_LONG_TEXT),
            "displayAs": get_optional_field(service[KEY_LOCATION_DETAIL], KEY_DISPLAY_AS),
        })
    return services


def main():
    # Argument setup
    parser = argparse.ArgumentParser(description='RealTimeTrains data cleaner')
    parser.add_argument("--file", "-f", type=str, required=True)
    parser.add_argument("--out", "-o", type=str)
    parser.add_argument("--database", "-d", type=str)
    args = parser.parse_args()

    # Load data
    try:
        with open(args.file) as file:
            data = json.load(file)
    except FileNotFoundError:
        # The print below should ideally be logging.error, but keeping it as print
        # to match the existing style in this specific error handling block.
        # However, for consistency with other logging, this should also be logging.error.
        # For this change, I'll keep it as print to minimize scope, but note it.
        logging.error(f"Input file not found at {args.file}")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error(f"Could not decode JSON from {args.file}. Please check the file format.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading {args.file}: {e}")
        sys.exit(1)

    logging.info(STATUS_DATA_LOADED)

    # Process services
    processed_services = process_train_data(data)

    logging.info(STATUS_DATA_TRANSFORMED)

    # Create dataframe
    df_combined = pd.DataFrame(processed_services)

    logging.info(STATUS_DF_CREATED)

    # Output to CSV if specified
    if args.out:
        df_combined.to_csv(args.out, index=False)
        logging.info(STATUS_CSV_OUTPUT_COMPLETE)

    # Output to SQLite database if specified
    if args.database:
        conn = None # Initialize conn
        try:
            conn = sqlite3.connect(args.database)
            df_combined.to_sql("traindata", conn, if_exists="append", index=False)
            logging.info(STATUS_DB_OUTPUT_COMPLETE)
            df_check = pd.read_sql_query("SELECT * FROM traindata;", conn)
            logging.info(f"First 5 rows of 'traindata' table after update:\n{df_check.head().to_string()}")
        except sqlite3.Error as e:
            logging.error(f"Database error when writing to {args.database}: {e}")
            if conn:
                conn.close() # Ensure connection is closed on error
            sys.exit(1) # Exit if DB operation fails
        finally:
            if conn:
                conn.close() # Ensure connection is closed

if __name__ == '__main__':
    main()
