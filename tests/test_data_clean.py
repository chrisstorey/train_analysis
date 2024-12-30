# tests/test_data_clean.py

import pytest
from data_clean import process_train_data


def test_process_train_data_valid_input():
    data = {
        "services": [
            {
                "runDate": "2023-10-01",
                "trainIdentity": "12345A",
                "serviceUid": "uid678",
                "atocCode": "AB",
                "atocName": "Example TOC",
                "serviceType": "Express",
                "locationDetail": {
                    "description": "Some description",
                    "gbttBookedDeparture": "10:00",
                    "gbttBookedDepartureNextDay": None,
                    "realtimeArrival": "10:50",
                    "realtimeDeparture": "11:00",
                    "cancelReasonCode": "NA",
                    "cancelReasonShortText": "Not Cancelled",
                    "cancelReasonLongText": None,
                    "displayAs": "On Time",
                    "origin": {
                        "tiploc": "ORGN",
                        "description": "Origin Station",
                        "workingTime": "09:50",
                        "publicTime": "10:00"
                    },
                    "destination": {
                        "tiploc": "DEST",
                        "description": "Destination Station",
                        "workingTime": "10:50",
                        "publicTime": "11:00"
                    }
                }
            }
        ]
    }

    result = process_train_data(data)
    assert isinstance(result, list)
    assert len(result) == 1
    assert "trainIdentity" in result[0]
    assert result[0]["trainIdentity"] == "12345A"


def test_process_train_data_missing_optional_fields():
    data = {
        "services": [
            {
                "runDate": "2023-10-01",
                "trainIdentity": "12345A",
                "serviceUid": "uid678",
                "atocCode": "AB",
                "atocName": "Example TOC",
                "serviceType": "Express",
                "locationDetail": {
                    "description": None,
                    "gbttBookedDeparture": None,
                    "gbttBookedDepartureNextDay": None,
                    "realtimeArrival": None,
                    "realtimeDeparture": None,
                    "cancelReasonCode": None,
                    "cancelReasonShortText": None,
                    "cancelReasonLongText": None,
                    "displayAs": None,
                    "origin": {
                        "tiploc": None,
                        "description": None,
                        "workingTime": None,
                        "publicTime": None
                    },
                    "destination": {
                        "tiploc": None,
                        "description": None,
                        "workingTime": None,
                        "publicTime": None
                    }
                }
            }
        ]
    }

    result = process_train_data(data)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["description"] is None
    assert result[0]["gbttBookedDeparture"] is None


def test_process_train_data_empty_services_list():
    data = {"services": []}
    result = process_train_data(data)
    assert isinstance(result, list)
    assert len(result) == 0


def test_process_train_data_no_services_key():
    data = {}
    with pytest.raises(KeyError):
        process_train_data(data)


def test_process_train_data_invalid_data_type():
    data = "Invalid input"
    with pytest.raises(TypeError):
        process_train_data(data)
