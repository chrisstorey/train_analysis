#!/usr/bin/env python
import sqlite3
import datetime
import pandas as pd
import sys
import argparse
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# Constants
# DB_PATH = "/home/chris/projects/trains/database/trains.sqlite" # Removed hardcoded path
SQL_QUERY = "SELECT * FROM traindata;"
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H%M"

# Helper functions (should be at global scope for import)
def update_date_as_date(row):
    if row['gbttBookedDepartureNextDay'] == "1":
        return row['runDate'] + datetime.timedelta(days=1)
    return row['runDate']

def process_realtime_departure(departure):
    if departure == "None": # Assuming 'None' is a string from the input
        return pd.NaT # Return NaT for pandas consistency
    return pd.to_datetime(departure, format=TIME_FORMAT, errors='coerce')

def calculate_time_difference(row):
    if pd.isna(row['realtime-departure-time']) or pd.isna(row['booked-time']):
        return float('nan') # Return NaN if either time is NaT
    return (row['realtime-departure-time'] - row['booked-time']).total_seconds()

def main():
    # Argument setup
    parser = argparse.ArgumentParser(description="Time-based analysis for train data.")
    parser.add_argument("--db_path", 
                        type=str, 
                        required=True, 
                        help="Path to the SQLite database file.")
    parser.add_argument("--output_csv", 
                        type=str, 
                        help="Optional path to save the processed DataFrame as a CSV file.")
    args = parser.parse_args()

    conn = None  # Initialize conn outside try block
    train_data = pd.DataFrame() # Initialize train_data to ensure it's defined

    try:
        # Load data
        conn = sqlite3.connect(args.db_path)
        
        # Check if table exists
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='traindata'")
        if cursor.fetchone() is None:
            logging.error(f"Table 'traindata' does not exist in database {args.db_path}.")
            if conn:
                conn.close()
            sys.exit(1)
            
        train_data_loaded = pd.read_sql_query(SQL_QUERY, conn)
        
        if train_data_loaded.empty:
            logging.warning(f"No data loaded from table 'traindata' in {args.db_path}. The table might be empty.")
            # Script will continue, and subsequent checks for train_data.empty will handle this
            train_data = train_data_loaded # ensure train_data is the empty dataframe
        else:
            train_data = train_data_loaded

    except sqlite3.OperationalError as e:
        logging.error(f"Database operational error with {args.db_path}: {e}")
        if conn:
            conn.close()
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred during database operations with {args.db_path}: {e}")
        if conn:
            conn.close()
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    # Proceed with processing only if data was loaded (or table was empty but schema might be relevant)
    if not train_data.empty:
        # Process 'runDate' as datetime
        train_data['runDate'] = pd.to_datetime(train_data['runDate'], format=DATE_FORMAT, errors='coerce')

        # Update 'date-as-date' column
        train_data['date-as-date'] = train_data.apply(update_date_as_date, axis=1)

        # Process booked and real-time departures
        train_data['booked-time'] = pd.to_datetime(train_data['gbttBookedDeparture'], format=TIME_FORMAT, errors='coerce')
        train_data['realtime-departure-time'] = train_data['realtimeDeparture'].apply(process_realtime_departure)

        # Calculate time differences
        train_data['time-difference'] = train_data.apply(calculate_time_difference, axis=1)

    # Save to CSV or log to console
    if args.output_csv:
        if not train_data.empty:
            try:
                train_data.to_csv(args.output_csv, index=False)
                logging.info(f"Processed data saved to CSV: {args.output_csv}")
            except Exception as e:
                logging.error(f"Error saving DataFrame to CSV {args.output_csv}: {e}")
        else:
            logging.info("DataFrame is empty. Nothing to save to CSV.")
    else:
        # If no output file, log some info to console
        if not train_data.empty:
            logging.info(f"DataFrame dtypes:\n{train_data.dtypes.to_string()}")
            logging.info(f"Calculated time-difference series (first 50 rows):\n{train_data['time-difference'].head(50).to_string()}")
        elif 'realtimeDeparture' in train_data.columns : # Checks if it's an empty DataFrame with expected columns
             logging.info("Processed DataFrame is empty. Nothing to display.")
        else: # train_data might not be properly initialized if an error occurred before its assignment after load
            logging.error("train_data DataFrame not properly defined or loaded. Cannot display dtypes or time-difference.")

    # Uncomment to save to database
    # Note: If enabling this, 'conn' would need to be re-established or passed,
    # as it's closed in the finally block above.
    # Consider managing connection scope if multiple DB operations are needed.
    # For example, open connection, do all DB work, then close.
    # if not train_data.empty:
    #     # Re-establish connection for saving if needed
    #     # train_data.to_sql('train_out', conn, if_exists='replace')
    #     pass

if __name__ == '__main__':
    main()
