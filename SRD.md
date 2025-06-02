# 1. Introduction

The `train_analysis` software suite is designed to process, clean, and analyze United Kingdom (UK) train service data. Its primary source of data is assumed to be daily pulls from APIs such as RealTimeTrains.

The core purpose of this software is to transform raw train movement data into a structured format suitable for analysis, enabling insights into service performance, punctuality, and other operational aspects.

This document is intended for:
*   **Software Developers:** Involved in maintaining, extending, or understanding the codebase.
*   **Data Analysts:** Who will use the cleaned data for performance analysis and reporting.
*   **System Administrators:** Responsible for deploying and managing the software components.

# 2. System Architecture

The `train_analysis` system is composed of several key components that work together to process and analyze train data:

*   **Input Data:** Raw train service information, typically in JSON format, obtained from external APIs like RealTimeTrains. An example input file provided is `temp1.json`.
*   **`data_clean.py` (Data Cleaning Script):** A Python script responsible for:
    *   Ingesting the raw JSON data.
    *   Parsing and transforming the data into a structured format.
    *   Cleaning the data by handling missing values and extracting relevant details.
    *   Loading the cleaned data into a Pandas DataFrame.
    *   Optionally outputting the data to a CSV file.
    *   Optionally storing the data in an SQLite database (default table: `traindata`).
*   **`time_sort.py` (Time-based Analysis Script):** A Python script that:
    *   Connects to the SQLite database populated by `data_clean.py`.
    *   Performs time-related calculations, such as converting timestamps and calculating differences between scheduled and actual departure/arrival times.
    *   Currently, its output is printed to the console.
*   **SQLite Database:** A local file-based database (e.g., `trains.sqlite`) used as persistent storage for the cleaned train data. This database acts as the primary data source for `time_sort.py`.
*   **CSV Files (Optional Output):** `data_clean.py` can generate CSV files containing the processed train data for use in other tools or for archival purposes.

## 2.1. Data Flow

The general data flow is as follows:

1.  Raw JSON data representing train services is fed into `data_clean.py`.
2.  `data_clean.py` parses, cleans, and transforms this data.
3.  The processed data is then loaded into an SQLite database table (e.g., `traindata`). Additionally, it can be saved as a CSV file.
4.  `time_sort.py` connects to the SQLite database, retrieves the data, and performs further time-based analysis.

## 2.2. Operational Environment

The scripts are designed to be run in a standard Python environment. They rely on common libraries such as Pandas and the built-in `sqlite3` and `json` modules. The system is typically run on a local machine or a server where the Python interpreter and required libraries are installed.

# 3. Data Model

This section outlines the structure of the data as it moves through the system, from the initial JSON input to its representation in Pandas DataFrames and the SQLite database.

## 3.1. Input JSON Structure

The `data_clean.py` script expects a JSON input file with a primary key, often `"services"`, which holds a list of service objects. Each service object contains details about a specific train journey.

Key fields within each service object typically include:
*   `runDate`: (String) The date the service is scheduled to run.
*   `trainIdentity`: (String) The identifier for the train.
*   `serviceUid`: (String) A unique identifier for the service.
*   `atocCode`: (String) The ATOC code for the train operating company.
*   `atocName`: (String) The name of the train operating company.
*   `serviceType`: (String) The type of service (e.g., "Express").
*   `locationDetail`: (Object) Contains detailed information about the service's journey, including origin, destination, and real-time updates.
    *   `description`: (String) A general description for the service at a particular location.
    *   `gbttBookedDeparture`: (String) The booked departure time in "HHMM" format.
    *   `gbttBookedDepartureNextDay`: (String/None) "1" if the departure is on the next day, otherwise "0" or None.
    *   `realtimeArrival`: (String/None) Real-time arrival time in "HHMM" format.
    *   `realtimeDeparture`: (String/None) Real-time departure time in "HHMM" format.
    *   `cancelReasonCode`: (String/None) Code for cancellation reason.
    *   `cancelReasonShortText`: (String/None) Short text for cancellation reason.
    *   `cancelReasonLongText`: (String/None) Long text for cancellation reason.
    *   `displayAs`: (String) How the service should be displayed (e.g., "On Time", "Delayed").
    *   `origin`: (Object) Details of the origin station.
        *   `tiploc`: (String) TIPLOC code for the origin.
        *   `description`: (String) Name/description of the origin station.
        *   `workingTime`: (String) Scheduled working time at the origin in "HHMMSS" or "HHMM" format.
        *   `publicTime`: (String) Scheduled public time at the origin in "HHMM" format.
    *   `destination`: (Object) Details of the destination station.
        *   `tiploc`: (String) TIPLOC code for the destination.
        *   `description`: (String) Name/description of the destination station.
        *   `workingTime`: (String) Scheduled working time at the destination.
        *   `publicTime`: (String) Scheduled public time at the destination.

*Note: The `extract_location_details` function attempts to parse stringified dictionary representations within the `origin` and `destination` fields if they are not proper JSON objects initially. It uses 'not-present' as a default for missing sub-fields.*

## 3.2. Pandas DataFrame Schema

The `process_train_data` function in `data_clean.py` transforms the input JSON into a Pandas DataFrame. The columns in this DataFrame are:

*   `runDate`: (object/string)
*   `trainIdentity`: (object/string)
*   `serviceUid`: (object/string)
*   `atocCode`: (object/string)
*   `atocName`: (object/string)
*   `description`: (object/string)
*   `gbttBookedDeparture`: (object/string)
*   `gbttBookedDepartureNextDay`: (object/string, defaults to "None")
*   `serviceType`: (object/string)
*   `origin_tiploc`: (object/string, defaults to "not-present")
*   `origin_description`: (object/string, defaults to "not-present")
*   `origin_workingTime`: (object/string, defaults to "not-present")
*   `origin_publicTime`: (object/string, defaults to "not-present")
*   `destination_tiploc`: (object/string, defaults to "not-present")
*   `destination_description`: (object/string, defaults to "not-present")
*   `destination_workingTime`: (object/string, defaults to "not-present")
*   `destination_publicTime`: (object/string, defaults to "not-present")
*   `realtimeArrival`: (object/string, defaults to "None")
*   `realtimeDeparture`: (object/string, defaults to "None")
*   `cancelReasonCode`: (object/string)
*   `cancelReasonShortText`: (object/string)
*   `cancelReasonLongText`: (object/string)
*   `displayAs`: (object/string)

*Note: Pandas often infers 'object' dtype for columns with mixed types or strings. Specific type conversions might occur later, e.g., in `time_sort.py`.*

## 3.3. SQLite Table Schema (`traindata`)

When the DataFrame is written to an SQLite database using `df_combined.to_sql("traindata", ...)`, the table schema generally mirrors the DataFrame structure. SQLite data types will be inferred by Pandas `to_sql` method. Typically:

*   String columns in Pandas (object dtype) become `TEXT` in SQLite.
*   Numeric columns (int64, float64) become `INTEGER` or `REAL`.
*   Datetime objects, if converted in Pandas, might be stored as `TEXT` (ISO format) or `TIMESTAMP`. (In the current `data_clean.py`, dates like `runDate` are kept as strings and Pandas infers them as TEXT).

The `traindata` table will thus have columns corresponding to those listed in section 3.2, with `TEXT` being the predominant data type given the nature of the source data and current processing.

# 4. Functional Description

This section details the functionality of the Python scripts within the `train_analysis` system.

## 4.1. `data_clean.py` - Data Cleaning and Transformation Script

**Purpose:**
The `data_clean.py` script is the primary tool for ingesting raw train service data (in JSON format), cleaning it, transforming it into a structured format, and then saving it to a CSV file and/or an SQLite database.

**Command-Line Arguments:**
The script accepts the following command-line arguments:
*   `--file` or `-f` (string, **required**): Specifies the path to the input JSON file containing the train data.
*   `--out` or `-o` (string, optional): If provided, the script will save the processed data to a CSV file at this path.
*   `--database` or `-d` (string, optional): If provided, the script will save the processed data to an SQLite database file with this name. The data is appended to a table named `traindata`.

**Core Logic:**

1.  **Argument Parsing:** The script begins by parsing the command-line arguments to determine the input file and desired output locations.
2.  **Data Loading:** The JSON data is loaded from the file specified by the `--file` argument. A status message "Data Loaded" is displayed.
3.  **Data Transformation (`process_train_data` function):** This is the central function for processing the loaded data.
    *   It iterates over each "service" entry within the input JSON's "services" list.
    *   For each service, it extracts and processes origin and destination details using the `process_location_data` function.
        *   **`process_location_data(location_detail, key)`:** This helper function is called for both "origin" and "destination" parts of a service's `locationDetail`. It takes the relevant sub-dictionary and the key (e.g., "origin") and uses `extract_location_details` to retrieve specific fields.
        *   **`extract_location_details(location_str, key, default)`:** This function is designed to parse individual values (like `tiploc`, `description`, `workingTime`, `publicTime`) from a string that often represents a dictionary (e.g., "{'tiploc': 'MAN', ...}"). It searches for the specified `key` within the string and extracts its associated value. If the key is not found, it returns the `default` value (which is `DEFAULT_MISSING_VALUE`, "not-present").
    *   Other service-level details (e.g., `runDate`, `trainIdentity`, `serviceUid`, `atocCode`, `realtimeArrival`, `cancelReasonCode`, etc.) are retrieved using the `get_optional_field(data, key, default)` helper. This function safely accesses keys from the service dictionary, returning a specified `default` (often an empty string or "None") if a key is absent.
    *   The processed information for each service is compiled into a dictionary and added to a list named `services`.
    *   A status message "Data Transformed" is displayed upon completion.
4.  **DataFrame Creation:** The list of processed service dictionaries (`processed_services`) is converted into a Pandas DataFrame (`df_combined`). A status message "Dataframe created" is shown.
5.  **Output to CSV:** If the `--out` argument was provided, the DataFrame is saved as a CSV file to the specified path. The `index=False` argument ensures that the DataFrame index is not written into the CSV. A status message "Output to csv file complete" is displayed.
6.  **Output to SQLite Database:** If the `--database` argument was provided:
    *   A connection to the specified SQLite database file is established.
    *   The DataFrame is written to a table named `traindata`. The `if_exists="append"` option ensures that if the table already exists, new data is appended. `index=False` prevents writing the DataFrame index.
    *   A status message "Output to database complete" is displayed.
    *   Finally, the script reads back all data from the `traindata` table and prints it to the console for immediate verification.

**Status Messages:**
Throughout its execution, the script uses a `print_status(message)` function to display formatted status messages on the console, clearing the screen for each new message. This provides a visual indication of the script's progress.

## 4.2. `time_sort.py` - Time-based Analysis Script

**Purpose:**
The `time_sort.py` script is designed to perform time-based calculations and analysis on the train data previously processed by `data_clean.py` and stored in the SQLite database. It focuses on converting time fields and calculating differences, likely for punctuality analysis.

**Key Constants:**
The script defines several constants at the beginning:
*   `DB_PATH`: (String) The path to the SQLite database file. **Currently, this is hardcoded to `/home/chris/projects/trains/database/trains.sqlite`.**
*   `SQL_QUERY`: (String) The SQL query used to fetch data: `"SELECT * FROM traindata;"`.
*   `DATE_FORMAT`: (String) The format string for parsing dates: `"%Y-%m-%d"`.
*   `TIME_FORMAT`: (String) The format string for parsing times: `"%H%M"`.

**Core Logic:**

1.  **Data Loading:**
    *   A connection to the SQLite database is established using the hardcoded `DB_PATH`.
    *   Data is loaded from the `traindata` table (as specified in `SQL_QUERY`) into a Pandas DataFrame named `train_data`.
2.  **Date Processing:**
    *   The `runDate` column is converted to Pandas datetime objects using `pd.to_datetime` and the `DATE_FORMAT`.
    *   A new column, `date-as-date`, is created. This column adjusts the `runDate`: if the `gbttBookedDepartureNextDay` column for a row is "1", it means the departure is on the next calendar day relative to the `runDate`, so one day is added to the `runDate` for that row. Otherwise, it's the same as `runDate`.
3.  **Time Processing:**
    *   The `gbttBookedDeparture` column (containing times like "1030") is converted to Pandas datetime objects (extracting the time component) using `pd.to_datetime` and the `TIME_FORMAT`. This is stored in a new column `booked-time`.
    *   The `realtimeDeparture` column is processed:
        *   If the value is "None" (string), it's kept as `None` (Python NoneType).
        *   Otherwise, it's converted to a Pandas datetime object (time component) using `pd.to_datetime` and `TIME_FORMAT`.
        *   The results are stored in a new column `realtime-departure-time`.
4.  **Time Difference Calculation:**
    *   A new column, `time-difference`, is calculated. This represents the difference in total seconds between the `realtime-departure-time` and `booked-time`.
    *   If `realtime-departure-time` is `None` for a row, the `time-difference` will also be `None` (or `NaT` which then results in `None` when `total_seconds()` is absent).
5.  **Output:**
    *   The script prints the data types (`dtypes`) of all columns in the `train_data` DataFrame to the console.
    *   It then prints the `time-difference` Series (column) to the console.
    *   There is a commented-out line: `# train_data.to_sql('train_out', conn, if_exists='replace')`. If uncommented, this line would save the entire modified `train_data` DataFrame (including the new calculated columns) to a new table named `train_out` in the same SQLite database, replacing the table if it already exists.

**Note:** The script currently does not take any command-line arguments and relies on the hardcoded `DB_PATH` for database access. Its primary output is to the console.

# 5. Non-Functional Requirements (NFRs) and Improvement Suggestions

This section outlines key non-functional aspects and provides suggestions for enhancing the robustness, maintainability, and usability of the `train_analysis` software.

### 5.1. Error Handling

Currently, error handling is minimal. Robust error handling is crucial for reliable operation, especially when dealing with external data sources and file operations.

*   **`data_clean.py`:**
    *   **Issue:** If the input JSON file specified by `--file` does not exist or is not readable, the script will raise an unhandled `FileNotFoundError`. If the JSON is malformed, a `json.JSONDecodeError` will occur.
    *   **Suggestion:** Implement `try-except` blocks around file reading (`json.load`) and critical parsing steps. Provide user-friendly error messages and exit gracefully. For example:
        ```python
        import sys # Add this import
        try:
            with open(args.file) as file:
                data = json.load(file)
        except FileNotFoundError:
            print(f"Error: Input file not found at {args.file}")
            sys.exit(1)
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from {args.file}. Check format.")
            sys.exit(1)
        ```
*   **`time_sort.py`:**
    *   **Issue:** If the SQLite database at `DB_PATH` is inaccessible or the `traindata` table doesn't exist, the script will fail with unhandled exceptions (e.g., `sqlite3.OperationalError`).
    *   **Suggestion:** Wrap database connection and query operations in `try-except` blocks. Check for table existence before querying if necessary.
        ```python
        import sys # Add this import
        import sqlite3 # Add this import
        import pandas as pd # Add this import
        # ... (DB_PATH, SQL_QUERY defined)
        try:
            conn = sqlite3.connect(DB_PATH)
            # Optional: Check if table exists before reading
            # table_exists_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='traindata';"
            # if pd.read_sql_query(table_exists_query, conn).empty:
            #     print(f"Error: Table 'traindata' not found in {DB_PATH}")
            #     sys.exit(1)
            train_data = pd.read_sql_query(SQL_QUERY, conn)
        except sqlite3.Error as e:
            print(f"Database error connecting to {DB_PATH}: {e}")
            sys.exit(1)
        finally:
            if 'conn' in locals() and conn: # Ensure conn was defined
                conn.close()
        ```

### 5.2. Configuration Management

Centralizing configuration improves maintainability and adaptability.

*   **Issue:** The database path `DB_PATH` in `time_sort.py` is hardcoded (`/home/chris/projects/trains/database/trains.sqlite`). This makes the script non-portable and difficult to configure for different environments.
*   **Suggestion:**
    *   For `DB_PATH` and similar parameters (like table names, file paths), use configuration files (e.g., `config.ini`, `config.yaml`, or a JSON config file) or environment variables.
    *   Alternatively, make them command-line arguments for `time_sort.py`, similar to `data_clean.py`.
    *   Example using environment variable:
        ```python
        import os
        DB_PATH = os.getenv("TRAIN_DB_PATH", "default_trains.sqlite")
        ```
    *   Example using a simple `config.json`:
        ```json
        // config.json
        {
            "db_path": "trains.sqlite",
            "traindata_table": "traindata"
        }
        ```
        ```python
        // In script
        import json
        with open("config.json") as f:
            config = json.load(f)
        DB_PATH = config["db_path"]
        ```

### 5.3. Logging

Effective logging is essential for debugging and monitoring.

*   **Issue:** `data_clean.py` uses a custom `print_status` function that clears the console. `time_sort.py` uses standard `print` for output. This is not ideal for non-interactive use or for capturing detailed logs.
*   **Suggestion:**
    *   Integrate Python's built-in `logging` module.
    *   Configure logging levels (DEBUG, INFO, WARNING, ERROR, CRITICAL) to control verbosity.
    *   Log messages with timestamps and module names.
    *   Allow logging to both console and a file for persistent records.
    *   Example basic setup:
        ```python
        import logging
        logging.basicConfig(level=logging.INFO, 
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            handlers=[logging.FileHandler("train_analysis.log"),
                                      logging.StreamHandler()])
        # Usage
        logging.info("Data loading complete.")
        logging.error("Failed to process record X.")
        ```
    *   The screen clearing in `print_status` should be removed or made optional for production/logging scenarios.

### 5.4. Modularity and Code Structure

Well-structured code is easier to understand, test, and maintain.

*   **`data_clean.py`:**
    *   **Issue:** This script is quite long and handles argument parsing, file I/O, data transformation logic, and constants.
    *   **Suggestion:**
        *   Move utility functions like `print_status`, `get_optional_field`, `extract_location_details`, and `process_location_data` into a separate `utils.py` or a dedicated processing module.
        *   The core data processing logic within `process_train_data` could be encapsulated in a class or a set of more focused functions.
        *   Constants like `DEFAULT_MISSING_VALUE` could be part of a `constants.py` file if shared across modules.
*   **`extract_location_details` function:**
    *   **Issue:** The current string parsing logic for `origin` and `destination` (which appear to be stringified dictionaries) is fragile. It relies on string searching for keys.
    *   **Suggestion:**
        *   The ideal solution is to ensure the input JSON has correctly structured objects for `origin` and `destination` rather than stringified dicts. If this is not possible:
        *   A more robust parsing approach could involve `ast.literal_eval` if the string is a valid Python literal, or careful regular expressions if the structure is consistent. This should be wrapped in `try-except` due to the risk of `ValueError` or other parsing issues.
        *   Consider if this implies that the `locationDetail` part of the input JSON needs a more rigorously defined schema.

### 5.5. Testing

Comprehensive testing ensures code correctness and prevents regressions.

*   **`time_sort.py`:**
    *   **Issue:** There are no unit tests for `time_sort.py`.
    *   **Suggestion:** Create `tests/test_time_sort.py`. Add tests for:
        *   Correct date and time conversions (especially handling of `gbttBookedDepartureNextDay`).
        *   Accurate calculation of `time-difference` for various scenarios (on-time, delayed, early, missing `realtimeDeparture`).
        *   Mock the database interaction (e.g., using `unittest.mock` or by providing a temporary test database with sample data) to test the script's logic independently of `data_clean.py`.
*   **`tests/test_data_clean.py`:**
    *   **Issue:** While tests exist, they can be expanded.
    *   **Suggestion:**
        *   Add more edge cases for `extract_location_details`: malformed input strings, strings with missing keys, different quoting styles if applicable.
        *   Test `get_optional_field` more directly with various inputs.
        *   Test the main `process_train_data` function with more diverse sample JSON inputs, covering different combinations of present/absent fields and "next day" flags.
        *   For file I/O and database interaction in `data_clean.py` (e.g., saving CSV, writing to DB), consider using mocking (e.g., `unittest.mock.patch` for `open`, `pd.DataFrame.to_csv`, `pd.DataFrame.to_sql`) to test the calls without actual file system or database side effects.

### 5.6. Docstrings and Comments

Clear documentation within the code is vital.

*   **Issue:** While some functions have brief comments or simple docstrings, they are not consistently applied or sufficiently detailed.
*   **Suggestion:**
    *   Implement comprehensive docstrings for all modules, classes, and functions (e.g., using Google, reStructuredText, or NumPy style). Docstrings should explain the purpose, arguments (name, type, description), return values (type, description), and any exceptions raised.
    *   Add more inline comments to explain complex or non-obvious logic steps, especially within `process_train_data` and `extract_location_details`.

### 5.7. Data Validation

Validating input data early can prevent many downstream errors.

*   **Issue:** The scripts currently assume the input JSON structure is largely correct. Errors in the structure or data types might lead to unexpected behavior or runtime errors.
*   **Suggestion:**
    *   For `data_clean.py`, consider using a library like Pydantic to define a schema for the expected input JSON. This allows for parsing and validation of the data as it's loaded, providing clear error messages if the input doesn't conform to the schema.
    *   Example with Pydantic (conceptual):
        ```python
        from pydantic import BaseModel, validator, ValidationError
        from typing import List, Optional

        class Location(BaseModel):
            tiploc: Optional[str] = "not-present"
            description: Optional[str] = "not-present"
            # ... other fields

        class Service(BaseModel):
            runDate: str
            trainIdentity: str
            # ... other fields
            locationDetail: dict # Further refinement possible here

        class InputData(BaseModel):
            services: List[Service]

        try:
            # raw_data from json.load()
            validated_data = InputData(**raw_data)
        except ValidationError as e:
            logging.error(f"Input data validation error: {e}")
            sys.exit(1)
        ```

### 5.8. `time_sort.py` Enhancements

The `time_sort.py` script could be made more versatile.

*   **Issue:** Its primary output is to the console. The line to save output to a database table (`train_out`) is commented out.
*   **Suggestion:**
    *   Uncomment and refine the database output option. Allow the output table name to be configurable.
    *   Add command-line arguments to allow saving the processed DataFrame to other formats like CSV or Excel.
    *   Make `DB_PATH` and output options (file paths, table names) configurable via command-line arguments or a configuration file.

### 5.9. Code Clarity and Maintainability

Minor changes can improve readability and reduce errors.

*   **Issue:** String literals like "tiploc", "description", "origin", "destination", "services" are used directly as dictionary keys and field names. This is prone to typos. The `print_status` function's screen clearing (`\x1b[2J`) can be problematic.
*   **Suggestion:**
    *   Define these common string literals as constants at the top of the respective scripts or in a shared `constants.py` module. This aids refactoring and prevents typos.
        ```python
        # constants.py
        KEY_TIPLOC = "tiploc"
        KEY_DESCRIPTION = "description"
        # In script
        # value = data.get(constants.KEY_TIPLOC)
        ```
    *   The screen-clearing behavior of `print_status` should be made optional (e.g., via a command-line flag or config) or removed if logging is implemented, as it's generally not suitable for non-interactive scripts or when logs are piped to files.

### 5.10. Security

While not a major concern for the current local setup, it's good practice.

*   **Issue:** The current scripts use local file paths and a local SQLite database, so advanced security measures are not critical.
*   **Suggestion:** If the application were to be extended to connect to remote databases or handle more sensitive information, proper credential management (e.g., using environment variables for passwords, avoiding hardcoding credentials) and secure connection practices (e.g., SSL/TLS for database connections) would be essential. This is a forward-looking point.

### 5.11. Dependency Management

Keeping dependencies current is important.

*   **Issue:** A `requirements.txt` file exists.
*   **Suggestion:** Ensure `requirements.txt` is kept up-to-date with all necessary packages and their versions (e.g., `pandas`, `pydantic` if added). Regularly review and update dependencies to patch security vulnerabilities and benefit from new features. Consider using virtual environments for project isolation.
```
