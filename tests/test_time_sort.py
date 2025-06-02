import pytest
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import datetime
import sys
import os

# Adjust path to import from the root directory where time_sort.py is located
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from time_sort import update_date_as_date, process_realtime_departure, calculate_time_difference, DATE_FORMAT, TIME_FORMAT

# Helper function to create a sample DataFrame
def create_sample_df():
    data = {
        'runDate': ['2023-10-01', '2023-10-02', '2023-10-03'],
        'gbttBookedDeparture': ['1000', '2350', '0010'],
        'gbttBookedDepartureNextDay': ['0', '0', '1'], # String '0' or '1' as in script
        'realtimeDeparture': ['1005', 'None', '0015']
    }
    df = pd.DataFrame(data)
    df['runDate'] = pd.to_datetime(df['runDate'], format=DATE_FORMAT)
    df['booked-time'] = pd.to_datetime(df['gbttBookedDeparture'], format=TIME_FORMAT, errors='coerce')
    return df

def test_update_date_as_date():
    df = pd.DataFrame({
        'runDate': [datetime.datetime(2023, 10, 1), datetime.datetime(2023, 10, 2)],
        'gbttBookedDepartureNextDay': ['0', '1']
    })
    expected_dates = pd.Series([
        datetime.datetime(2023, 10, 1),
        datetime.datetime(2023, 10, 3) # Next day
    ], name='date-as-date')

    result_series = df.apply(update_date_as_date, axis=1)
    # Ensure the result_series also has a name for comparison if needed, though apply might not set it.
    # If update_date_as_date is expected to return a Series with a specific name, adjust here.
    # For now, comparing values.
    # Convert to list for comparison if names are not matching or causing issues.
    # assert_series_equal by default checks names if both are Series.
    # If `result_series` doesn't get a name, pass `check_names=False` or assign name.
    result_series.name = 'date-as-date' # Assuming the function's application results in this name
    assert_series_equal(result_series, expected_dates, check_dtype=False)

def test_process_realtime_departure():
    series = pd.Series(['1000', 'None', '2359'])
    expected_values = [
        pd.to_datetime('1000', format=TIME_FORMAT),
        pd.NaT, # Expect NaT for 'None' after processing
        pd.to_datetime('2359', format=TIME_FORMAT)
    ]
    # Create expected Series with correct dtype for comparison with NaT
    expected = pd.Series(expected_values, dtype='datetime64[ns]')
    expected.name = 'realtime-departure-time'

    result = series.apply(process_realtime_departure)
    result.name = 'realtime-departure-time'

    assert_series_equal(result, expected, check_dtype=True)


def test_calculate_time_difference():
    df = pd.DataFrame({
        'booked-time': [
            pd.to_datetime('1000', format=TIME_FORMAT),
            pd.to_datetime('1200', format=TIME_FORMAT),
            pd.to_datetime('1400', format=TIME_FORMAT)
        ],
        'realtime-departure-time': [
            pd.to_datetime('1005', format=TIME_FORMAT),
            pd.NaT,
            pd.to_datetime('1355', format=TIME_FORMAT)
        ]
    })

    expected_diff_values = [
        300.0,
        float('nan'),
        -300.0
    ]
    expected_diff = pd.Series(expected_diff_values, name='time-difference', dtype='float64')

    result_series = df.apply(calculate_time_difference, axis=1)
    result_series.name = 'time-difference' # Ensure name for comparison
    assert_series_equal(result_series, expected_diff, check_dtype=True)


def test_time_sort_integration_on_sample_dataframe():
    df = create_sample_df()

    df['date-as-date'] = df.apply(update_date_as_date, axis=1)
    df['realtime-departure-time'] = df['realtimeDeparture'].apply(process_realtime_departure)
    df['time-difference'] = df.apply(calculate_time_difference, axis=1)

    expected_date_as_date = pd.Series([
        datetime.datetime(2023, 10, 1),
        datetime.datetime(2023, 10, 2),
        datetime.datetime(2023, 10, 4)
    ], name='date-as-date')

    expected_realtime_departure_time_values = [
        pd.to_datetime('1005', format=TIME_FORMAT),
        pd.NaT,
        pd.to_datetime('0015', format=TIME_FORMAT)
    ]
    expected_realtime_departure_time = pd.Series(expected_realtime_departure_time_values,
                                                 name='realtime-departure-time', dtype='datetime64[ns]')

    expected_time_difference_values = [
        (pd.to_datetime('1005', format=TIME_FORMAT) - pd.to_datetime('1000', format=TIME_FORMAT)).total_seconds(),
        float('nan'),
        (pd.to_datetime('0015', format=TIME_FORMAT) - pd.to_datetime('0010', format=TIME_FORMAT)).total_seconds()
    ]
    expected_time_difference = pd.Series(expected_time_difference_values, name='time-difference', dtype='float64')

    assert_series_equal(df['date-as-date'], expected_date_as_date, check_dtype=False)
    assert_series_equal(df['realtime-departure-time'], expected_realtime_departure_time, check_dtype=True, check_names=False) # Name is already set
    assert_series_equal(df['time-difference'], expected_time_difference, check_dtype=True, check_names=False) # Name is already set
